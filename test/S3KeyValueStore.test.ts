import { afterAll, describe, expect, test } from "bun:test";

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as KeyValueStore from "effect/persistence/KeyValueStore";

import { S3, S3Error } from "../src/index.ts";
import * as S3KeyValueStore from "../src/S3KeyValueStore.ts";
import { makePrefix, removePrefix, run, testOptions } from "./utils.ts";

const prefix = makePrefix();

afterAll(() => removePrefix(prefix));

const runStore = <A, E>(
	effect: Effect.Effect<A, E, KeyValueStore.KeyValueStore | S3.S3>,
	storePrefix = `${prefix}kv/`,
) => run(Effect.provide(effect, S3KeyValueStore.layer({ prefix: storePrefix })));

describe("S3KeyValueStore", () => {
	test("implements the KeyValueStore contract", async () => {
		const result = await runStore(
			Effect.gen(function* () {
				const store = yield* KeyValueStore.KeyValueStore;
				yield* store.set("a", "1");
				yield* store.set("b", new Uint8Array([1, 2, 3]));
				yield* store.set("nested/c", "3");
				const snapshot = {
					a: yield* store.get("a"),
					b: yield* store.getUint8Array("b"),
					missing: yield* store.get("missing"),
					missingBytes: yield* store.getUint8Array("missing"),
					hasA: yield* store.has("a"),
					hasMissing: yield* store.has("missing"),
					size: yield* store.size,
					keys: (yield* store.keys).toSorted(),
					modified: yield* store.modify("a", (value) => `${value}!`),
					modifiedMissing: yield* store.modify("missing", (value) => value),
				};
				yield* store.remove("nested/c");
				const afterRemove = yield* store.size;
				yield* store.clear;
				return {
					...snapshot,
					afterRemove,
					afterClear: yield* store.size,
					isEmpty: yield* store.isEmpty,
				};
			}),
		);
		expect(result).toEqual({
			a: "1",
			b: new Uint8Array([1, 2, 3]),
			missing: undefined,
			missingBytes: undefined,
			hasA: true,
			hasMissing: false,
			size: 3,
			keys: ["a", "b", "nested/c"],
			modified: "1!",
			modifiedMissing: undefined,
			afterRemove: 2,
			afterClear: 0,
			isEmpty: true,
		});
	});

	test("clear only removes objects under its prefix", async () => {
		const outside = `${prefix}outside.txt`;
		const remaining = await runStore(
			Effect.gen(function* () {
				const s3 = yield* S3.S3;
				const store = yield* KeyValueStore.KeyValueStore;
				yield* s3.write(outside, "keep me");
				yield* store.set("x", "1");
				yield* store.clear;
				return yield* s3.exists(outside);
			}),
			`${prefix}scoped/`,
		);
		expect(remaining).toBe(true);
	});

	test("clearPrefix removes only the matching entries", async () => {
		const result = await runStore(
			Effect.gen(function* () {
				const store = yield* KeyValueStore.KeyValueStore;
				yield* store.set("users/1", "a");
				yield* store.set("users/2", "b");
				yield* store.set("orders/1", "c");
				yield* store.clearPrefix("users/");
				return { keys: yield* store.keys, isEmpty: yield* store.isEmpty };
			}),
			`${prefix}clear-prefix/`,
		);
		expect(result).toEqual({ keys: ["orders/1"], isEmpty: false });
	});

	test("a missing bucket fails instead of reading as an empty store", async () => {
		const wrongBucket = S3KeyValueStore.layer({ prefix }).pipe(
			Layer.provide(S3.layer({ ...testOptions, bucket: "effect-bun-s3-missing-bucket" })),
		);
		const error = await Effect.runPromise(
			Effect.gen(function* () {
				const store = yield* KeyValueStore.KeyValueStore;
				return yield* Effect.flip(store.get("k"));
			}).pipe(Effect.provide(wrongBucket)),
		);
		expect(error.method).toBe("get");
		expect(S3Error.isS3Error(error.cause) && error.cause.reason.code).toBe("NoSuchBucket");
	});

	test("S3 failures become KeyValueStoreError with the S3Error as cause", async () => {
		const denied = S3KeyValueStore.layer({ prefix }).pipe(
			Layer.provide(S3.layer({ ...testOptions, secretAccessKey: "wrong" })),
		);
		const error = await Effect.runPromise(
			Effect.gen(function* () {
				const store = yield* KeyValueStore.KeyValueStore;
				return yield* Effect.flip(store.set("k", "v"));
			}).pipe(Effect.provide(denied)),
		);
		expect(error).toBeInstanceOf(KeyValueStore.KeyValueStoreError);
		expect(error.method).toBe("set");
		expect(error.key).toBe("k");
		expect(S3Error.isS3Error(error.cause)).toBe(true);
	});
});
