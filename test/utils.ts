import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { S3 } from "../src/index.ts";

// Matches compose.yaml; CI sets the same through S3_TEST_* variables.
export const testOptions = {
	endpoint: Bun.env["S3_TEST_ENDPOINT"] ?? "http://127.0.0.1:9100",
	bucket: Bun.env["S3_TEST_BUCKET"] ?? "effect-bun-s3-test",
	accessKeyId: Bun.env["S3_TEST_ACCESS_KEY_ID"] ?? "effect-bun-s3",
	secretAccessKey: Bun.env["S3_TEST_SECRET_ACCESS_KEY"] ?? "effect-bun-s3-secret",
	region: "us-east-1",
} as const;

const TestS3 = S3.layer(testOptions);

// Every test file writes under its own prefix and removes it afterwards.
export const makePrefix = (): string => `test-${crypto.randomUUID()}/`;

export const run = <A, E>(effect: Effect.Effect<A, E, S3.S3>): Promise<A> =>
	Effect.runPromise(Effect.provide(effect, TestS3));

export const removePrefix = (prefix: string): Promise<void> =>
	run(
		Effect.gen(function* () {
			const s3 = yield* S3.S3;
			yield* s3.listAll({ prefix }).pipe(
				Stream.mapEffect((object) => s3.delete(object.key), { concurrency: 8 }),
				Stream.runDrain,
			);
		}),
	);

export const sha256 = (data: Uint8Array): string =>
	new Bun.CryptoHasher("sha256").update(data).digest("hex");

export const randomBytes = (size: number): Uint8Array<ArrayBuffer> => {
	const bytes = new Uint8Array(size);
	// getRandomValues fills at most 64 KiB per call.
	for (let offset = 0; offset < size; offset += 65536) {
		crypto.getRandomValues(bytes.subarray(offset, offset + 65536));
	}
	return bytes;
};

export const MiB = 1024 * 1024;

// Polls until an abandoned upload has had time to settle, then reports
// whether the object exists.
export const existsAfterSettling = (key: string): Promise<boolean> =>
	run(
		Effect.gen(function* () {
			const s3 = yield* S3.S3;
			yield* Effect.sleep("500 millis");
			return yield* s3.exists(key);
		}),
	);
