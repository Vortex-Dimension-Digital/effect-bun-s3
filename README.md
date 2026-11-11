# effect-bun-s3

[Effect](https://effect.website) v4 service for [Bun's native S3 client](https://bun.com/docs/runtime/s3).

- **Native**: Bun's built-in `S3Client`, with no AWS SDK and no other runtime dependency.
- **Typed errors**: every failure is an `S3Error` whose `reason` says what went wrong (`NotFound`, `AccessDenied`, ...).
- **Streams**: upload an Effect `Stream` in parts, download an object as a `Stream`. A failed or interrupted upload leaves no partial object behind.
- **Traced**: each operation runs in its own span.
- **`KeyValueStore`**: a ready-made layer for Effect's `KeyValueStore` on top of a bucket.

It works with any S3-compatible store: AWS S3, Cloudflare R2, MinIO, RustFS, Garage, DigitalOcean Spaces, Backblaze B2, ...

## Install

```sh
bun add effect-bun-s3 effect
```

Requires Bun 1.4.4 or newer and Effect 4.1 or newer.

## Usage

```ts
import { Effect, Stream } from "effect";
import { S3 } from "effect-bun-s3";

const program = Effect.gen(function* () {
	const s3 = yield* S3.S3;

	yield* s3.write("reports/2026.json", JSON.stringify({ total: 42 }), {
		type: "application/json",
	});
	const report = yield* s3.text("reports/2026.json");

	// Stream large objects in and out.
	yield* s3.writeStream("backups/db.dump", Stream.fromIterable(chunks));
	const download = s3.stream("backups/db.dump");

	// A link anyone can download from for the next hour.
	const url = yield* s3.presign("reports/2026.json", { expiresIn: 3600 });
});

program.pipe(
	Effect.provide(
		S3.layer({
			endpoint: "https://<account>.r2.cloudflarestorage.com",
			bucket: "my-bucket",
			accessKeyId: "...",
			secretAccessKey: "...",
		}),
	),
	Effect.runPromise,
);
```

### Configuration

`S3.layer(options)` takes Bun's `S3Options` (`endpoint`, `bucket`, `region`, `accessKeyId`, `secretAccessKey`, `sessionToken`, `virtualHostedStyle`, `partSize`, `queueSize`, `retry`, `acl`, ...), with the secrets also accepted as `Redacted` values. Anything left out falls back to Bun's environment variables: `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_ENDPOINT`, `S3_BUCKET` and `S3_SESSION_TOKEN`, or their `AWS_*` equivalents. So `S3.layer()` with no arguments is configured entirely by the environment.

To read the options through Effect's `Config` instead:

```ts
import { Config } from "effect";
import { S3 } from "effect-bun-s3";

const S3Live = S3.layerConfig({
	endpoint: Config.String("STORAGE_ENDPOINT"),
	bucket: Config.String("STORAGE_BUCKET"),
	accessKeyId: Config.String("STORAGE_ACCESS_KEY_ID"),
	secretAccessKey: Config.Redacted("STORAGE_SECRET_ACCESS_KEY"),
});
```

Every operation also takes per-call options that override the client's defaults for that call: another `bucket`, a content `type`, an `acl`, ...

### Operations

| Operation                            | Returns                                                                                |
| ------------------------------------ | -------------------------------------------------------------------------------------- |
| `write(key, data, options?)`         | Bytes written. `data` is a string, bytes, a `Blob`, a `Response`, `Bun.file(...)`, ... |
| `writeStream(key, stream, options?)` | Bytes written. Uploads a `Stream<Uint8Array>` in parts as it is produced.              |
| `bytes(key, options?)`               | The object as a `Uint8Array`.                                                          |
| `text(key, options?)`                | The object as UTF-8 text.                                                              |
| `stream(key, options?)`              | The object as a `Stream<Uint8Array>`.                                                  |
| `stat(key, options?)`                | `{ size, etag, lastModified, type }`.                                                  |
| `exists(key, options?)`              | Whether the object exists.                                                             |
| `size(key, options?)`                | The object's size in bytes.                                                            |
| `delete(key, options?)`              | Nothing. Deleting a missing object succeeds, as in S3.                                 |
| `list(input?, options?)`             | One page of objects and common prefixes (`prefix`, `delimiter`, `maxKeys`, ...).       |
| `listAll(input?, options?)`          | A `Stream` of every matching object, fetching pages as it is consumed.                 |
| `presign(key, options?)`             | A presigned URL (`method`, `expiresIn`, ...). Signed locally, no request is made.      |
| `use(f)`                             | Runs `f(client)` against the Bun client, mapping its failure to an `S3Error`.          |
| `file(key, options?)`, `client`      | Bun's own `S3File` and `S3Client`, for anything not covered above.                     |

`bytes`, `text` and `stream` read part of an object with `range`: `s3.bytes(key, { range: { start: 0, end: 1024 } })` reads its first KiB (`end` is exclusive and defaults to the end of the object).

### Errors

Every operation fails with `S3Error`. Its `reason` is one of:

| Reason           | When                                                                                                    |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| `NotFound`       | The object or bucket does not exist: `code` is `NoSuchKey` or `NoSuchBucket`.                           |
| `AccessDenied`   | The credentials were rejected or lack permission (`AccessDenied`, `SignatureDoesNotMatch`, ...).        |
| `InvalidRequest` | Refused before sending: an invalid key or range, missing credentials, a bad endpoint (`ERR_S3_*`, ...). |
| `TransportError` | The server could not be reached (`ConnectionRefused`, `DNSResolveFailed`, `TLSHandshakeFailed`, ...).   |
| `ServiceError`   | Any other S3 error (`SlowDown`, `InternalError`, ...).                                                  |
| `UnknownError`   | Anything else: an exception inside `use`, a missing local file passed to `write`, ...                   |

Each reason carries the `operation`, the object `key`, the S3 `code` and the original `cause`. Handle a whole reason with `Effect.catchReason`, or match the `code` to be precise:

```ts
// Any NotFound: a missing object, or a misspelled bucket.
const settings = s3
	.text("settings.json")
	.pipe(Effect.catchReason("S3Error", "NotFound", () => Effect.succeed("{}")));

// Only a missing object: a misspelled bucket still fails.
const strictSettings = s3.text("settings.json").pipe(
	Effect.catchIf(
		(error) => error.reason.code === "NoSuchKey",
		() => Effect.succeed("{}"),
	),
);
```

Some failures arrive without an S3 error body, so Bun cannot say what went wrong:

- `stat`, `size` and `exists` are `HEAD` requests. Rejected credentials fail them with `UnknownError`, a missing bucket reads as `NoSuchKey`, and `exists` returns `false`.
- A `writeStream` to a missing bucket fails with `UnknownError` ("Failed to initiate multipart upload"), where a `write` gets `NoSuchBucket`.

### Uploading streams

`writeStream` uploads the stream in parts as it is produced, without holding it in memory, and returns the number of bytes written. If the stream fails, or the effect is interrupted before the stream ends, the upload is abandoned and no object is created. A stream failure is raised as the stream's own error, not as an `S3Error`:

```ts
const upload = s3.writeStream("exports/big.csv", rows.pipe(Stream.encodeText));
//    ^? Effect<number, S3Error | RowError>
```

The stream runs in the calling fiber, so its finalizers have run by the time `writeStream` completes, whatever the outcome. Once the stream has ended, `writeStream` waits for the upload to finish, even if interrupted, so the result always tells whether the object exists.

To upload a local file, pass `Bun.file(path)` to `write`.

### Interruption

Interrupting `bytes`, `text` or a `stream` download cancels the request, and interrupting `writeStream` before its stream ends abandons the upload. Bun's other S3 calls (`write`, `stat`, `exists`, `size`, `delete`, `list`) take no abort signal: interrupting them stops waiting for the result, but the request itself runs to completion.

### KeyValueStore

`effect-bun-s3/S3KeyValueStore` provides Effect's `KeyValueStore` on top of the `S3` service, one object per entry:

```ts
import { Effect, Layer } from "effect";
import { S3 } from "effect-bun-s3";
import * as S3KeyValueStore from "effect-bun-s3/S3KeyValueStore";
import { KeyValueStore } from "effect/persistence";

const StoreLive = S3KeyValueStore.layer({ prefix: "cache/" }).pipe(Layer.provide(S3.layer()));

const program = Effect.gen(function* () {
	const store = yield* KeyValueStore.KeyValueStore;
	yield* store.set("greeting", "hello");
	return yield* store.get("greeting");
}).pipe(Effect.provide(StoreLive));
```

- **Prefix:** use one, ending in a delimiter (`"cache/"`). It is a plain string prefix, so `"cache"` would also cover `cache-old/...`. Without one the store spans the whole bucket, and `clear` deletes every object in it.
- **Binary values:** `get` reads an entry as UTF-8 text, like Effect's file-system store; read entries stored as bytes with `getUint8Array`.
- **Missing bucket:** a missing entry reads as `undefined`, but a missing bucket fails with a `KeyValueStoreError`. `has` is a `HEAD` request, so it reports `false` for both.
- **Stability:** Effect marks `KeyValueStore` as unstable, which is why this layer has its own entry point.

### Tracing

Each operation runs in a span named after it (`S3.write`, `S3.stream`, ...) with the `aws.s3.bucket` and `aws.s3.key` attributes, so it shows up in any tracer you provide to Effect, OpenTelemetry included.

## Development

```sh
bun install
docker compose up -d   # a local S3 server (RustFS) with the test bucket
bun run validate       # lint, types, unused code, package checks, tests
```

The integration tests run against the server from `compose.yaml`. Point them at another one with `S3_TEST_ENDPOINT`, `S3_TEST_BUCKET`, `S3_TEST_ACCESS_KEY_ID` and `S3_TEST_SECRET_ACCESS_KEY`. CI runs them on the oldest supported and the latest Bun and Effect versions, plus Bun canary, and weekly to catch new releases.

Releases use [Changesets](https://github.com/changesets/changesets): add one with `bun changeset` in your pull request, and merging the release pull request it opens publishes to npm from CI, with provenance.

## License

[MIT](./LICENSE)
