/**
 * A `KeyValueStore` (from `effect/persistence`) backed by the {@link S3}
 * service: each entry is one object, named `prefix + key`.
 *
 * Like Effect's file-system store, `get` reads an entry as UTF-8 text; read
 * entries stored as bytes with `getUint8Array`. Effect marks `KeyValueStore`
 * as unstable, so this module lives on its own entry point
 * (`@vortexdd/effect-bun-s3/S3KeyValueStore`).
 *
 * @since 0.1.0
 */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as KeyValueStore from "effect/persistence/KeyValueStore";
import * as Stream from "effect/Stream";

import { S3 } from "./S3.ts";
import type { S3Error } from "./S3Error.ts";

/**
 * @since 0.1.0
 * @category models
 */
export interface Options {
	/**
	 * Prepended to every key. End it with a delimiter (`"cache/"`): it is a
	 * plain string prefix, so `"cache"` would also match `"cache-old/..."`.
	 * Without one the store spans the whole bucket, and `clear` deletes every
	 * object in it.
	 */
	readonly prefix?: string | undefined;
	/** How many objects `clear` and `clearPrefix` delete at once. Defaults to 16. */
	readonly concurrency?: number | undefined;
}

const toStoreError =
	(method: string, key?: string) =>
	(error: S3Error): KeyValueStore.KeyValueStoreError =>
		new KeyValueStore.KeyValueStoreError({
			method,
			...(key === undefined ? {} : { key }),
			message: error.message,
			cause: error,
		});

// A missing object is an absent entry; a missing bucket (`NoSuchBucket`) is
// a configuration error and still fails.
const orUndefined = <A>(effect: Effect.Effect<A, S3Error>): Effect.Effect<A | undefined, S3Error> =>
	Effect.catchIf(
		effect,
		(error) => error.reason._tag === "NotFound" && error.reason.code === "NoSuchKey",
		() => Effect.undefined,
	);

/**
 * Creates the store on top of the {@link S3} service.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = (
	options: Options = {},
): Effect.Effect<KeyValueStore.KeyValueStore, never, S3> =>
	Effect.gen(function* () {
		const s3 = yield* S3;
		const prefix = options.prefix ?? "";
		const concurrency = options.concurrency ?? 16;
		const objectKey = (key: string) => `${prefix}${key}`;

		const keysUnder = (keyPrefix: string) =>
			s3.listAll({ prefix: objectKey(keyPrefix) }).pipe(Stream.map((object) => object.key));

		const deleteUnder = (keyPrefix: string) =>
			keysUnder(keyPrefix).pipe(
				Stream.mapEffect((key) => s3.delete(key), { concurrency }),
				Stream.runDrain,
			);

		return KeyValueStore.make({
			get: (key) =>
				orUndefined(s3.text(objectKey(key))).pipe(Effect.mapError(toStoreError("get", key))),
			getUint8Array: (key) =>
				orUndefined(s3.bytes(objectKey(key))).pipe(
					Effect.mapError(toStoreError("getUint8Array", key)),
				),
			set: (key, value) =>
				s3
					.write(objectKey(key), value)
					.pipe(Effect.asVoid, Effect.mapError(toStoreError("set", key))),
			remove: (key) => s3.delete(objectKey(key)).pipe(Effect.mapError(toStoreError("remove", key))),
			// A HEAD request: cheaper than reading the entry, but it cannot tell a
			// missing bucket from a missing entry.
			has: (key) => s3.exists(objectKey(key)).pipe(Effect.mapError(toStoreError("has", key))),
			isEmpty: s3.list({ prefix, maxKeys: 1 }).pipe(
				Effect.map((page) => (page.contents?.length ?? 0) === 0),
				Effect.mapError(toStoreError("isEmpty")),
			),
			clear: deleteUnder("").pipe(Effect.mapError(toStoreError("clear"))),
			clearPrefix: (keyPrefix) =>
				deleteUnder(keyPrefix).pipe(Effect.mapError(toStoreError("clearPrefix", keyPrefix))),
			size: Stream.runCount(keysUnder("")).pipe(Effect.mapError(toStoreError("size"))),
			keys: keysUnder("").pipe(
				Stream.map((key) => key.slice(prefix.length)),
				Stream.runCollect,
				Effect.mapError(toStoreError("keys")),
			),
		});
	});

/**
 * A layer providing `KeyValueStore` on top of the {@link S3} service.
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (options?: Options): Layer.Layer<KeyValueStore.KeyValueStore, never, S3> =>
	Layer.effect(KeyValueStore.KeyValueStore, make(options));
