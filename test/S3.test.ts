import { afterAll, describe, expect, test } from "bun:test";

import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import { S3, S3Error } from "../src/index.ts";
import {
	existsAfterSettling,
	makePrefix,
	MiB,
	randomBytes,
	removePrefix,
	run,
	sha256,
	testOptions,
} from "./utils.ts";

const prefix = makePrefix();
const key = (name: string) => `${prefix}${name}`;

afterAll(() => removePrefix(prefix));

const failureOf = <A>(effect: Effect.Effect<A, S3Error.S3Error, S3.S3>) => run(Effect.flip(effect));

describe("write and read", () => {
	test("round-trips text and bytes, with the content type", async () => {
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				const written = yield* s3.write(key("hello.txt"), "hello, world", { type: "text/plain" });
				return {
					written,
					text: yield* s3.text(key("hello.txt")),
					bytes: yield* s3.bytes(key("hello.txt")),
					stat: yield* s3.stat(key("hello.txt")),
					size: yield* s3.size(key("hello.txt")),
					exists: yield* s3.exists(key("hello.txt")),
				};
			}),
		);
		expect(result.written).toBe(12);
		expect(result.text).toBe("hello, world");
		expect(new TextDecoder().decode(result.bytes)).toBe("hello, world");
		expect(result.stat.size).toBe(12);
		expect(result.stat.type).toStartWith("text/plain");
		expect(result.size).toBe(12);
		expect(result.exists).toBe(true);
	});

	test("accepts Blob, Response and Bun.file bodies", async () => {
		const path = `${import.meta.dir}/../node_modules/.effect-bun-s3-upload.bin`;
		const local = randomBytes(256 * 1024);
		await Bun.write(path, local);
		const hashes = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("blob.bin"), new Blob([local]));
				yield* s3.write(key("response.bin"), new Response(local));
				yield* s3.write(key("file.bin"), Bun.file(path));
				return yield* Effect.forEach(["blob.bin", "response.bin", "file.bin"], (name) =>
					Effect.map(s3.bytes(key(name)), sha256),
				);
			}),
		);
		expect(hashes).toEqual([sha256(local), sha256(local), sha256(local)]);
		await Bun.file(path).delete();
	});

	test("reads a byte range", async () => {
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("range.txt"), "abcdefgh");
				const middle = yield* s3.text(key("range.txt"), { range: { start: 2, end: 5 } });
				const tail = yield* s3.text(key("range.txt"), { range: { start: 5 } });
				const streamed = yield* s3
					.stream(key("range.txt"), { range: { start: 1, end: 3 } })
					.pipe(Stream.decodeText(), Stream.mkString);
				return { middle, tail, streamed };
			}),
		);
		expect(result).toEqual({ middle: "cde", tail: "fgh", streamed: "bc" });
	});

	test("an empty range reads nothing and an invalid one is rejected", async () => {
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("empty-range.txt"), "abcdef");
				return {
					same: yield* s3.text(key("empty-range.txt"), { range: { start: 2, end: 2 } }),
					reversed: yield* s3.bytes(key("empty-range.txt"), { range: { start: 5, end: 2 } }),
					streamed: yield* s3
						.stream(key("empty-range.txt"), { range: { start: 3, end: 0 } })
						.pipe(Stream.runCollect),
					negative: yield* Effect.flip(s3.text(key("empty-range.txt"), { range: { start: -1 } })),
					fractional: yield* Effect.flip(
						s3.text(key("empty-range.txt"), { range: { start: 0, end: 1.5 } }),
					),
				};
			}),
		);
		expect(result.same).toBe("");
		expect(result.reversed).toEqual(new Uint8Array());
		expect(result.streamed).toEqual([]);
		expect(result.negative.reason._tag).toBe("InvalidRequest");
		expect(result.fractional.reason._tag).toBe("InvalidRequest");
	});

	test("reports a missing object as absent, and deleting it succeeds", async () => {
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("to-delete.txt"), "x");
				yield* s3.delete(key("to-delete.txt"));
				yield* s3.delete(key("to-delete.txt"));
				return yield* s3.exists(key("to-delete.txt"));
			}),
		);
		expect(result).toBe(false);
	});
});

describe("streams", () => {
	test("uploads a stream in parts and downloads it back", async () => {
		const chunks = Array.from({ length: 12 }, () => randomBytes(MiB));
		const expected = sha256(new Uint8Array(await new Blob(chunks).arrayBuffer()));
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				const written = yield* s3.writeStream(key("big.bin"), Stream.fromIterable(chunks), {
					type: "application/octet-stream",
				});
				const hasher = new Bun.CryptoHasher("sha256");
				yield* Stream.runForEach(s3.stream(key("big.bin")), (chunk) =>
					Effect.sync(() => hasher.update(chunk)),
				);
				const stat = yield* s3.stat(key("big.bin"));
				return { written, hash: hasher.digest("hex"), stat };
			}),
		);
		expect(result.written).toBe(12 * MiB);
		expect(result.hash).toBe(expected);
		expect(result.stat.size).toBe(12 * MiB);
		expect(result.stat.type).toStartWith("application/octet-stream");
	});

	test("a failing stream fails with its own error and leaves no object", async () => {
		class SourceFailed extends Data.TaggedError("SourceFailed")<{ readonly after: number }> {}
		const source = Stream.fromIterable(Array.from({ length: 8 }, () => randomBytes(MiB))).pipe(
			Stream.concat(Stream.fail(new SourceFailed({ after: 8 }))),
		);
		const error = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				return yield* Effect.flip(s3.writeStream(key("failed.bin"), source));
			}),
		);
		expect(error).toBeInstanceOf(SourceFailed);
		expect(await existsAfterSettling(key("failed.bin"))).toBe(false);
	});

	test("the stream's finalizers have run when an upload fails", async () => {
		let finalized = false;
		const source = Stream.fromIterable(Array.from({ length: 4 }, () => randomBytes(MiB))).pipe(
			Stream.ensuring(
				Effect.sync(() => {
					finalized = true;
				}),
			),
		);
		const outcome = await Effect.runPromise(
			Effect.flatMap(S3.make({ ...testOptions, bucket: "effect-bun-s3-missing-bucket" }), (s3) =>
				Effect.flip(s3.writeStream("never.bin", source)),
			).pipe(Effect.map((error) => ({ error, finalizedOnReturn: finalized }))),
		);
		expect(S3Error.isS3Error(outcome.error)).toBe(true);
		expect(outcome.finalizedOnReturn).toBe(true);
	});

	test("interrupting an upload abandons it", async () => {
		const endless = Stream.forever(Stream.fromEffect(Effect.sync(() => randomBytes(MiB))));
		await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				let finalized = false;
				const source = endless.pipe(
					Stream.ensuring(
						Effect.sync(() => {
							finalized = true;
						}),
					),
				);
				const fiber = yield* Effect.forkChild(s3.writeStream(key("interrupted.bin"), source));
				yield* Effect.sleep("1 second");
				const exit = yield* Fiber.interrupt(fiber).pipe(Effect.andThen(Fiber.await(fiber)));
				expect(Exit.hasInterrupts(exit)).toBe(true);
				expect(finalized).toBe(true);
			}),
		);
		expect(await existsAfterSettling(key("interrupted.bin"))).toBe(false);
	});

	test("taking part of a download stops it early", async () => {
		const first = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("partial.bin"), randomBytes(8 * MiB));
				return yield* s3.stream(key("partial.bin")).pipe(Stream.take(1), Stream.runCollect);
			}),
		);
		expect(first).toHaveLength(1);
	});
});

describe("listing", () => {
	test("pages through every object under a prefix", async () => {
		const names = ["list/a", "list/b", "list/c", "list/d", "list/e"];
		const result = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* Effect.forEach(names, (name) => s3.write(key(name), name), { concurrency: 5 });
				const page = yield* s3.list({ prefix: key("list/"), maxKeys: 2 });
				const all = yield* s3.listAll({ prefix: key("list/"), maxKeys: 2 }).pipe(Stream.runCollect);
				return { page, all };
			}),
		);
		expect(result.page.contents).toHaveLength(2);
		expect(result.page.isTruncated).toBe(true);
		expect(result.all.map((object) => object.key)).toEqual(names.map(key));
	});

	test("listAll can start after a key", async () => {
		const names = ["after/a", "after/b", "after/c"];
		const keys = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* Effect.forEach(names, (name) => s3.write(key(name), name), { concurrency: 3 });
				return yield* s3
					.listAll({ prefix: key("after/"), startAfter: key("after/a"), maxKeys: 1 })
					.pipe(
						Stream.map((object) => object.key),
						Stream.runCollect,
					);
			}),
		);
		expect(keys).toEqual([key("after/b"), key("after/c")]);
	});

	test("an empty prefix yields nothing", async () => {
		const all = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				return yield* s3.listAll({ prefix: key("nothing-here/") }).pipe(Stream.runCollect);
			}),
		);
		expect(all).toEqual([]);
	});
});

describe("presign", () => {
	test("a presigned GET downloads the object and a presigned PUT uploads one", async () => {
		const { getUrl, putUrl } = await run(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("presigned.txt"), "signed");
				return {
					getUrl: yield* s3.presign(key("presigned.txt"), { expiresIn: 60 }),
					putUrl: yield* s3.presign(key("uploaded.txt"), { method: "PUT", expiresIn: 60 }),
				};
			}),
		);
		expect(await (await fetch(getUrl)).text()).toBe("signed");
		expect((await fetch(putUrl, { method: "PUT", body: "via url" })).ok).toBe(true);
		expect(await run(S3.S3.use((s3) => s3.text(key("uploaded.txt"))))).toBe("via url");
	});

	test("signs for another endpoint without contacting it", async () => {
		const url = await run(
			S3.S3.use((s3) => s3.presign(key("public.txt"), { endpoint: "https://cdn.example.com" })),
		);
		expect(new URL(url).host).toBe("cdn.example.com");
	});
});

const withClient = (options: S3.Options) =>
	Effect.flip(Effect.flatMap(S3.make(options), (s3) => s3.write(key("denied.txt"), "x")));

describe("errors", () => {
	test("a missing object fails with NotFound for every read", async () => {
		const errors = await Promise.all([
			failureOf(S3.S3.use((s3) => s3.text(key("missing")))),
			failureOf(S3.S3.use((s3) => s3.bytes(key("missing")))),
			failureOf(S3.S3.use((s3) => s3.stat(key("missing")))),
			failureOf(S3.S3.use((s3) => s3.size(key("missing")))),
			failureOf(S3.S3.use((s3) => Stream.runDrain(s3.stream(key("missing"))))),
		]);
		for (const error of errors) {
			expect(error.reason._tag).toBe("NotFound");
			expect(error.reason.code).toBe("NoSuchKey");
			expect(error.reason.key).toBe(key("missing"));
		}
		expect(errors.map((error) => error.reason.operation)).toEqual([
			"text",
			"bytes",
			"stat",
			"size",
			"stream",
		]);
		expect(errors[0]?.message).toBe(
			`text "${key("missing")}": The specified key does not exist. (NoSuchKey)`,
		);
	});

	test("Effect.catchReason handles one reason", async () => {
		const result = await run(
			S3.S3.use((s3) => s3.text(key("missing"))).pipe(
				Effect.catchReason("S3Error", "NotFound", () => Effect.succeed("fallback")),
			),
		);
		expect(result).toBe("fallback");
	});

	test("classifies credential, connection and client-side failures", async () => {
		const [badSecret, unreachable, missingBucket, invalidKey] = await Promise.all([
			Effect.runPromise(withClient({ ...testOptions, secretAccessKey: "wrong" })),
			Effect.runPromise(withClient({ ...testOptions, endpoint: "http://127.0.0.1:1" })),
			Effect.runPromise(withClient({ ...testOptions, bucket: "effect-bun-s3-missing-bucket" })),
			failureOf(S3.S3.use((s3) => s3.write("", "x"))),
		]);
		expect(badSecret.reason._tag).toBe("AccessDenied");
		expect(unreachable.reason._tag).toBe("TransportError");
		expect(missingBucket.reason._tag).toBe("NotFound");
		expect(missingBucket.reason.code).toBe("NoSuchBucket");
		expect(invalidKey.reason._tag).toBe("InvalidRequest");
		expect(invalidKey.reason.code).toStartWith("ERR_S3_");
	});

	test("a missing local file is an UnknownError, not a server error", async () => {
		const error = await failureOf(
			S3.S3.use((s3) => s3.write(key("from-disk.bin"), Bun.file("/nonexistent/effect-bun-s3"))),
		);
		expect(error.reason._tag).toBe("UnknownError");
	});

	test("use maps a thrown error to UnknownError", async () => {
		const error = await failureOf(
			S3.S3.use((s3) => s3.use(() => Promise.reject(new TypeError("not an S3 failure")))),
		);
		expect(error.reason._tag).toBe("UnknownError");
		expect(error.reason.cause).toBeInstanceOf(TypeError);
		expect(S3Error.isS3Error(error)).toBe(true);
	});
});

describe("layers", () => {
	test("layerConfig reads options, secrets included, from Config", async () => {
		const provider = ConfigProvider.fromUnknown({
			S3_ENDPOINT: testOptions.endpoint,
			S3_BUCKET: testOptions.bucket,
			S3_ACCESS_KEY_ID: testOptions.accessKeyId,
			S3_SECRET_ACCESS_KEY: testOptions.secretAccessKey,
		});
		const layer = S3.layerConfig({
			endpoint: Config.String("S3_ENDPOINT"),
			bucket: Config.String("S3_BUCKET"),
			accessKeyId: Config.String("S3_ACCESS_KEY_ID"),
			secretAccessKey: Config.Redacted("S3_SECRET_ACCESS_KEY"),
			region: Config.succeed("us-east-1"),
		});
		const text = await Effect.runPromise(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				yield* s3.write(key("config.txt"), "from config");
				return yield* s3.text(key("config.txt"));
			}).pipe(Effect.provide(Layer.provide(layer, ConfigProvider.layer(provider)))),
		);
		expect(text).toBe("from config");
	});
});
