/**
 * An Effect service over Bun's native S3 client (`Bun.S3Client`).
 *
 * Works with any S3-compatible store: AWS S3, Cloudflare R2, MinIO, RustFS,
 * Garage, DigitalOcean Spaces, Backblaze B2, ... Every operation fails with a
 * typed {@link S3Error}, traces itself as a span, and reads or writes Effect
 * `Stream`s where data can be large.
 *
 * @since 0.1.0
 */
import { S3Client } from "bun";
import type {
	S3File,
	S3FilePresignOptions,
	S3ListObjectsOptions,
	S3ListObjectsResponse,
	S3Options,
	S3Stats,
} from "bun";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";

import {
	fromCause,
	InvalidRequest,
	type Operation,
	S3Error,
	ServiceError,
	UnknownError,
} from "./S3Error.ts";

/**
 * Anything Bun can upload: strings, bytes, `Blob`s, `Bun.file()`s, other
 * `S3File`s, and `Response`/`Request` bodies.
 *
 * @since 0.1.0
 * @category models
 */
export type WriteData = Parameters<S3Client["write"]>[1];

/**
 * Per-call options for reads. `range` reads only the bytes `[start, end)` of
 * the object: `start` is a non-negative integer and `end`, exclusive,
 * defaults to the end of the object. A range with `end <= start` is empty.
 *
 * @since 0.1.0
 * @category models
 */
export interface ReadOptions extends S3Options {
	readonly range?:
		| {
				readonly start: number;
				readonly end?: number | undefined;
		  }
		| undefined;
}

/**
 * What {@link Service.listAll} takes: {@link Service.list}'s options without
 * `continuationToken` (it pages by itself) or `delimiter` (it yields objects
 * only; use `list` for common prefixes).
 *
 * @since 0.1.0
 * @category models
 */
export type ListAllOptions = Omit<S3ListObjectsOptions, "continuationToken" | "delimiter">;

/**
 * One object returned by {@link Service.list} and {@link Service.listAll}.
 *
 * @since 0.1.0
 * @category models
 */
export type ListedObject = NonNullable<S3ListObjectsResponse["contents"]>[number];

/**
 * The operations of the {@link S3} service. Every per-call `options` argument
 * overrides the client's defaults for that call (another bucket, a content
 * type, an ACL, ...).
 *
 * @since 0.1.0
 * @category models
 */
export interface Service {
	/** The underlying Bun client, for anything this service does not cover. */
	readonly client: S3Client;
	/** Runs a function against the Bun client, mapping its failure to an {@link S3Error}. */
	readonly use: <A>(f: (client: S3Client) => Promise<A>) => Effect.Effect<A, S3Error>;
	/** A lazy reference to an object; no request is made until it is read or written. */
	readonly file: (key: string, options?: S3Options) => S3File;
	/**
	 * Uploads `data` and resolves with the number of bytes written. Large
	 * bodies are uploaded in parts.
	 */
	readonly write: (
		key: string,
		data: WriteData,
		options?: S3Options,
	) => Effect.Effect<number, S3Error>;
	/**
	 * Uploads a stream in parts as it is produced, and resolves with the number
	 * of bytes written. If the stream fails or the effect is interrupted before
	 * the stream ends, the upload is abandoned and no object is created; the
	 * stream's own failure is preserved. Once the stream has ended, the effect
	 * waits for the upload's outcome, even if interrupted. The stream's
	 * finalizers have run by the time the effect completes.
	 */
	readonly writeStream: <E, R>(
		key: string,
		stream: Stream.Stream<Uint8Array, E, R>,
		options?: S3Options,
	) => Effect.Effect<number, S3Error | E, R>;
	/** Reads the object (or `range` of it) into memory. Interrupting it cancels the download. */
	readonly bytes: (key: string, options?: ReadOptions) => Effect.Effect<Uint8Array, S3Error>;
	/** Reads the object (or `range` of it) as UTF-8 text. Interrupting it cancels the download. */
	readonly text: (key: string, options?: ReadOptions) => Effect.Effect<string, S3Error>;
	/** Streams the object (or `range` of it). Interrupting the stream cancels the download. */
	readonly stream: (key: string, options?: ReadOptions) => Stream.Stream<Uint8Array, S3Error>;
	/** Size, ETag, last modified date and content type of an object. */
	readonly stat: (key: string, options?: S3Options) => Effect.Effect<S3Stats, S3Error>;
	/** Whether the object exists. A missing bucket also reads as `false`. */
	readonly exists: (key: string, options?: S3Options) => Effect.Effect<boolean, S3Error>;
	/** The object's size in bytes. */
	readonly size: (key: string, options?: S3Options) => Effect.Effect<number, S3Error>;
	/** Deletes the object. Deleting a missing object succeeds, as in S3. */
	readonly delete: (key: string, options?: S3Options) => Effect.Effect<void, S3Error>;
	/** One page of objects (up to `maxKeys`, 1000 by default) and common prefixes. */
	readonly list: (
		input?: S3ListObjectsOptions,
		options?: S3Options,
	) => Effect.Effect<S3ListObjectsResponse, S3Error>;
	/** Every object matching `input`, fetching further pages as the stream is consumed. */
	readonly listAll: (
		input?: ListAllOptions,
		options?: S3Options,
	) => Stream.Stream<ListedObject, S3Error>;
	/**
	 * A presigned URL for the object (GET for one day unless `method` and
	 * `expiresIn` say otherwise). Signing happens locally: no request is made.
	 */
	readonly presign: (key: string, options?: S3FilePresignOptions) => Effect.Effect<string, S3Error>;
}

/**
 * The S3 service.
 *
 * @since 0.1.0
 * @category services
 */
export class S3 extends Context.Service<S3, Service>()("effect-bun-s3/S3") {}

/**
 * Client options: Bun's `S3Options`, with the secrets also accepted as
 * `Redacted` values. Anything omitted falls back to Bun's environment
 * variables (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`,
 * `S3_ENDPOINT`, `S3_BUCKET`, `S3_SESSION_TOKEN`, or their `AWS_*`
 * equivalents).
 *
 * @since 0.1.0
 * @category models
 */
export interface Options extends Omit<S3Options, "secretAccessKey" | "sessionToken"> {
	readonly secretAccessKey?: string | Redacted.Redacted | undefined;
	readonly sessionToken?: string | Redacted.Redacted | undefined;
}

const reveal = (value: string | Redacted.Redacted | undefined): string | undefined =>
	Redacted.isRedacted(value) ? Redacted.value(value) : value;

const toBunOptions = ({ secretAccessKey, sessionToken, ...options }: Options): S3Options => {
	const bunOptions: S3Options = { ...options };
	const secret = reveal(secretAccessKey);
	const token = reveal(sessionToken);
	if (secret !== undefined) bunOptions.secretAccessKey = secret;
	if (token !== undefined) bunOptions.sessionToken = token;
	return bunOptions;
};

const fileOptions = (options: ReadOptions | undefined): S3Options | undefined => {
	if (options === undefined) return undefined;
	const { range: _range, ...rest } = options;
	return rest;
};

const concat = (chunks: ReadonlyArray<Uint8Array>): Uint8Array => {
	if (chunks.length === 1 && chunks[0] !== undefined) return chunks[0];
	const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.byteLength, 0));
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return bytes;
};

const utf8 = new TextDecoder();

/**
 * A request body fed from an Effect stream with backpressure: each `push`
 * waits until the reader wants more. `push` resolves `false` once the reader
 * is gone (Bun cancelled the body, or the upload already settled).
 */
const makeBody = () => {
	let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
	let wake: (() => void) | undefined;
	let readerGone = false;
	const readable = new ReadableStream<Uint8Array>({
		start: (c) => {
			controller = c;
		},
		pull: () => {
			wake?.();
		},
		cancel: () => {
			readerGone = true;
			wake?.();
		},
	});
	return {
		readable,
		isReaderGone: () => readerGone,
		readerGone: () => {
			readerGone = true;
			wake?.();
		},
		push: (chunk: Uint8Array): Promise<boolean> => {
			if (readerGone || controller === undefined) return Promise.resolve(false);
			controller.enqueue(chunk);
			if ((controller.desiredSize ?? 0) > 0) return Promise.resolve(true);
			return new Promise<boolean>((resolve) => {
				wake = () => {
					wake = undefined;
					resolve(!readerGone);
				};
			});
		},
		close: () => controller?.close(),
		error: () => controller?.error(new Error("The upload's source stream did not complete")),
	};
};

/**
 * Builds the service around an existing Bun `S3Client`. `bucket`, the
 * client's default bucket, is only used to label spans.
 *
 * @since 0.1.0
 * @category constructors
 */
export const fromClient = (
	client: S3Client,
	{ bucket: defaultBucket }: { readonly bucket?: string | undefined } = {},
): Service => {
	const spanAttributes = (key: string | undefined, callOptions: S3Options | undefined) => {
		const bucket = callOptions?.bucket ?? defaultBucket;
		return {
			"rpc.system": "aws-api",
			"rpc.service": "S3",
			...(bucket === undefined ? {} : { "aws.s3.bucket": bucket }),
			...(key === undefined ? {} : { "aws.s3.key": key }),
		};
	};

	const span =
		(operation: Operation, key: string | undefined, callOptions: S3Options | undefined) =>
		<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
			Effect.withSpan(effect, `S3.${operation}`, {
				kind: "client",
				attributes: spanAttributes(key, callOptions),
			});

	const attempt = <A>(
		operation: Operation,
		key: string | undefined,
		callOptions: S3Options | undefined,
		f: () => Promise<A>,
	): Effect.Effect<A, S3Error> =>
		Effect.tryPromise({
			try: f,
			catch: (cause) => fromCause(cause, operation, key),
		}).pipe(span(operation, key, callOptions));

	// The file a read targets: the whole object, a slice of it, or nothing
	// for an empty range.
	const readTarget = (
		operation: Operation,
		key: string,
		options: ReadOptions | undefined,
	): Effect.Effect<Option.Option<S3File>, S3Error> =>
		Effect.suspend(() => {
			const range = options?.range;
			if (
				range !== undefined &&
				(!Number.isSafeInteger(range.start) ||
					range.start < 0 ||
					(range.end !== undefined && !Number.isSafeInteger(range.end)))
			) {
				return Effect.fail(
					new S3Error({
						reason: new InvalidRequest({
							operation,
							key,
							cause: new RangeError(
								"range.start must be a non-negative integer and range.end an integer",
							),
						}),
					}),
				);
			}
			if (range?.end !== undefined && range.end <= range.start) {
				return Effect.succeedNone;
			}
			return Effect.try({
				try: () => {
					const file = client.file(key, fileOptions(options));
					return Option.some(range === undefined ? file : file.slice(range.start, range.end));
				},
				catch: (cause) => fromCause(cause, operation, key),
			});
		});

	const readStream = (
		operation: Operation,
		key: string,
		options: ReadOptions | undefined,
	): Stream.Stream<Uint8Array, S3Error> =>
		Stream.unwrap(
			Effect.map(
				readTarget(operation, key, options),
				Option.match({
					onNone: () => Stream.empty,
					onSome: (file) =>
						// Cancelling the reader (the default) closes the connection on
						// interruption.
						Stream.fromReadableStream({
							evaluate: () => file.stream(),
							onError: (cause) => fromCause(cause, operation, key),
						}),
				}),
			),
		);

	const readBytes = (operation: Operation, key: string, options: ReadOptions | undefined) =>
		readStream(operation, key, options).pipe(Stream.runCollect, Effect.map(concat));

	const list = (input?: S3ListObjectsOptions, options?: S3Options) =>
		attempt("list", undefined, options, () => client.list(input, options));

	const writeStream = <E, R>(
		key: string,
		stream: Stream.Stream<Uint8Array, E, R>,
		options?: S3Options,
	): Effect.Effect<number, S3Error | E, R> =>
		Effect.uninterruptibleMask((restore) =>
			Effect.gen(function* () {
				const body = makeBody();
				// Bun uploads a body of unknown length in parts, and abandons the
				// upload (no object) if the body errors.
				const upload = client.write(key, new Response(body.readable), options);
				// If the upload settles while the stream is still running, it has
				// failed: stop feeding it.
				upload.then(body.readerGone, body.readerGone);
				const awaitUpload = Effect.tryPromise({
					try: () => upload,
					catch: (cause) => fromCause(cause, "writeStream", key),
				});

				// Run in this fiber, so the stream's finalizers have run when it ends.
				const fed = yield* Effect.exit(
					restore(
						Stream.runForEachWhile(stream, (chunk) => Effect.promise(() => body.push(chunk))),
					),
				);

				if (body.isReaderGone()) {
					// The upload ended first: report its failure.
					return yield* Effect.flatMap(awaitUpload, () =>
						Effect.fail(
							new S3Error({
								reason: new UnknownError({
									operation: "writeStream",
									key,
									cause: new Error("The upload ended before its source stream"),
								}),
							}),
						),
					);
				}
				if (Exit.isFailure(fed)) {
					body.error();
					return yield* Effect.failCause(fed.cause);
				}
				body.close();
				return yield* awaitUpload;
			}),
		).pipe(span("writeStream", key, options));

	return {
		client,
		use: (f) => attempt("use", undefined, undefined, () => f(client)),
		file: (key, options) => client.file(key, options),
		write: (key, data, options) =>
			attempt("write", key, options, () => client.write(key, data, options)),
		writeStream,
		bytes: (key, options) => readBytes("bytes", key, options).pipe(span("bytes", key, options)),
		text: (key, options) =>
			readBytes("text", key, options).pipe(
				Effect.map((bytes) => utf8.decode(bytes)),
				span("text", key, options),
			),
		stream: (key, options) =>
			readStream("stream", key, options).pipe(
				Stream.withSpan("S3.stream", { kind: "client", attributes: spanAttributes(key, options) }),
			),
		stat: (key, options) => attempt("stat", key, options, () => client.stat(key, options)),
		exists: (key, options) => attempt("exists", key, options, () => client.exists(key, options)),
		size: (key, options) => attempt("size", key, options, () => client.size(key, options)),
		delete: (key, options) => attempt("delete", key, options, () => client.delete(key, options)),
		list,
		listAll: (input, options) =>
			Stream.paginate(undefined as string | undefined, (continuationToken) =>
				Effect.flatMap(
					list(continuationToken === undefined ? input : { ...input, continuationToken }, options),
					(page) => {
						const contents = page.contents ?? [];
						if (page.isTruncated !== true) {
							return Effect.succeed([contents, Option.none<string>()] as const);
						}
						if (page.nextContinuationToken === undefined) {
							return Effect.fail(
								new S3Error({
									reason: new ServiceError({
										operation: "list",
										cause: new Error("The listing is truncated but has no continuation token"),
									}),
								}),
							);
						}
						return Effect.succeed([contents, Option.some(page.nextContinuationToken)] as const);
					},
				),
			),
		presign: (key, options) =>
			Effect.try({
				try: () => client.presign(key, options),
				catch: (cause) => fromCause(cause, "presign", key),
			}).pipe(span("presign", key, options)),
	};
};

/**
 * Creates the service with a new Bun `S3Client`.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = (options: Options = {}): Effect.Effect<Service> =>
	Effect.sync(() => {
		const bunOptions = toBunOptions(options);
		return fromClient(new S3Client(bunOptions), {
			bucket: bunOptions.bucket ?? Bun.env["S3_BUCKET"] ?? Bun.env["AWS_BUCKET"],
		});
	});

/**
 * A layer providing {@link S3}. Without options, Bun reads the credentials,
 * endpoint and bucket from the environment.
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (options?: Options): Layer.Layer<S3> => Layer.effect(S3, make(options));

/**
 * A layer providing {@link S3} from Effect `Config`, e.g.
 * `{ bucket: Config.String("BUCKET"), secretAccessKey: Config.Redacted("SECRET") }`.
 *
 * @since 0.1.0
 * @category layers
 */
export const layerConfig = (options: Config.Wrap<Options>): Layer.Layer<S3, Config.ConfigError> =>
	Layer.effect(S3, Effect.flatMap(Config.unwrap(options), make));
