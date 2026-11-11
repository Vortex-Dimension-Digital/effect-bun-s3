/**
 * The error raised by every {@link S3} operation.
 *
 * Each failure is one `S3Error` whose `reason` says what went wrong. Handle
 * the whole family with `Effect.catchTag("S3Error", ...)`, or a single reason
 * with `Effect.catchReason("S3Error", "NotFound", ...)`.
 *
 * @since 0.1.0
 */
import * as Data from "effect/Data";
import * as Predicate from "effect/Predicate";

const TypeId = "~effect-bun-s3/S3Error" as const;

/**
 * The service operation that failed.
 *
 * @since 0.1.0
 * @category models
 */
export type Operation =
	| "use"
	| "write"
	| "writeStream"
	| "bytes"
	| "text"
	| "stream"
	| "stat"
	| "exists"
	| "size"
	| "delete"
	| "list"
	| "presign";

/**
 * Fields shared by every reason.
 *
 * @since 0.1.0
 * @category models
 */
export interface ReasonFields {
	/** The operation that failed. */
	readonly operation: Operation;
	/** The object key involved, when the operation targets one. */
	readonly key?: string | undefined;
	/** The S3 error code (`NoSuchKey`, `AccessDenied`, ...) or Bun's client error code (`ERR_S3_*`). */
	readonly code?: string | undefined;
	/** The error thrown by Bun. */
	readonly cause?: unknown;
}

const describeCause = (cause: unknown): string | undefined =>
	Predicate.hasProperty(cause, "message") &&
	typeof cause.message === "string" &&
	cause.message.length > 0
		? cause.message
		: undefined;

const formatMessage = (fields: ReasonFields, fallback: string): string => {
	const target =
		fields.key === undefined ? fields.operation : `${fields.operation} "${fields.key}"`;
	const code = fields.code === undefined ? "" : ` (${fields.code})`;
	return `${target}: ${describeCause(fields.cause) ?? fallback}${code}`;
};

/**
 * The object or bucket does not exist: `code` is `NoSuchKey` for a missing
 * object and `NoSuchBucket` for a missing bucket. Requests answered without a
 * body (`stat`, `size`) report a missing bucket as `NoSuchKey`.
 *
 * @since 0.1.0
 * @category reasons
 */
export class NotFound extends Data.TaggedError("NotFound")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "not found");
	}
}

/**
 * The credentials were rejected or lack permission (`AccessDenied`,
 * `SignatureDoesNotMatch`, `InvalidAccessKeyId`, ...).
 *
 * @since 0.1.0
 * @category reasons
 */
export class AccessDenied extends Data.TaggedError("AccessDenied")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "access denied");
	}
}

/**
 * Bun refused the request before sending it: an invalid key or bucket,
 * missing credentials, an invalid endpoint (`ERR_S3_*` codes).
 *
 * @since 0.1.0
 * @category reasons
 */
export class InvalidRequest extends Data.TaggedError("InvalidRequest")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "invalid request");
	}
}

/**
 * The server could not be reached: connection refused or reset, DNS
 * failure, timeout.
 *
 * @since 0.1.0
 * @category reasons
 */
export class TransportError extends Data.TaggedError("TransportError")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "transport error");
	}
}

/**
 * The server answered with any other S3 error (`SlowDown`, `InternalError`,
 * `PreconditionFailed`, ...). The S3 code is in `code`.
 *
 * @since 0.1.0
 * @category reasons
 */
export class ServiceError extends Data.TaggedError("ServiceError")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "service error");
	}
}

/**
 * Anything else: an exception thrown inside `use`, a local failure such as a
 * missing file passed to `write`, or a failed response without an S3 error
 * body (Bun's `UnknownError` code), which Bun gives for requests answered
 * without one, e.g. `stat`, `size` or `exists` with rejected credentials.
 *
 * @since 0.1.0
 * @category reasons
 */
export class UnknownError extends Data.TaggedError("UnknownError")<ReasonFields> {
	override get message(): string {
		return formatMessage(this, "unknown error");
	}
}

/**
 * Every reason an {@link S3Error} can carry.
 *
 * @since 0.1.0
 * @category models
 */
export type S3ErrorReason =
	NotFound | AccessDenied | InvalidRequest | TransportError | ServiceError | UnknownError;

/**
 * The error raised by every {@link S3} operation; `reason` holds the detail.
 *
 * @since 0.1.0
 * @category errors
 */
export class S3Error extends Data.TaggedError("S3Error")<{
	readonly reason: S3ErrorReason;
}> {
	constructor(props: { readonly reason: S3ErrorReason }) {
		super({ ...props, cause: props.reason.cause } as { readonly reason: S3ErrorReason });
	}

	/**
	 * Marks the value as an `S3Error` for {@link isS3Error}.
	 *
	 * @since 0.1.0
	 */
	readonly [TypeId]: typeof TypeId = TypeId;

	override get message(): string {
		return this.reason.message;
	}
}

/**
 * Whether a value is an {@link S3Error}.
 *
 * @since 0.1.0
 * @category guards
 */
export const isS3Error = (u: unknown): u is S3Error => Predicate.hasProperty(u, TypeId);

const notFoundCodes = new Set([
	"NoSuchKey",
	"NoSuchBucket",
	"NotFound",
	"NoSuchUpload",
	"NoSuchVersion",
]);

const accessDeniedCodes = new Set([
	"AccessDenied",
	"AccountProblem",
	"AllAccessDisabled",
	"ExpiredToken",
	"InvalidAccessKeyId",
	"InvalidSecurity",
	"InvalidToken",
	"RequestTimeTooSkewed",
	"SignatureDoesNotMatch",
	"TokenRefreshRequired",
]);

const transportCodes = new Set([
	"ConnectionClosed",
	"ConnectionRefused",
	"ConnectionReset",
	"ConnectionTimeout",
	"DNSResolveFailed",
	"FailedToOpenSocket",
	"Timeout",
	"TimedOut",
	"UnableToConnect",
	"EAI_AGAIN",
	"ECONNREFUSED",
	"ECONNRESET",
	"ENOTFOUND",
	"EPIPE",
	"ETIMEDOUT",
	// TLS: a certificate that does not verify, or a non-TLS server.
	"TLSHandshakeFailed",
	"CERT_HAS_EXPIRED",
	"DEPTH_ZERO_SELF_SIGNED_CERT",
	"ERR_TLS_CERT_ALTNAME_INVALID",
	"SELF_SIGNED_CERT_IN_CHAIN",
	"UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
	"UNABLE_TO_VERIFY_LEAF_SIGNATURE",
]);

// Rejected before any request: bad arguments or an unusable endpoint.
const invalidRequestCodes = new Set([
	"ERR_INVALID_ARG_TYPE",
	"ERR_INVALID_ARG_VALUE",
	"ERR_OUT_OF_RANGE",
	"InvalidURL",
]);

const codeOf = (cause: unknown): string | undefined =>
	Predicate.hasProperty(cause, "code") && typeof cause.code === "string" && cause.code.length > 0
		? cause.code
		: undefined;

// Bun's S3 client throws errors named "S3Error"; anything else (a missing
// local file, an exception in `use`) did not come from the server.
const isBunS3Error = (cause: unknown): boolean =>
	Predicate.hasProperty(cause, "name") && cause.name === "S3Error";

const classify = (cause: unknown, fields: ReasonFields): S3ErrorReason => {
	const code = fields.code;
	if (code === undefined) return new UnknownError(fields);
	if (code.startsWith("ERR_S3_") || invalidRequestCodes.has(code))
		return new InvalidRequest(fields);
	if (transportCodes.has(code)) return new TransportError(fields);
	// Bun reports "UnknownError" when the response has no S3 error body, e.g.
	// a HEAD request (stat, size, exists) rejected for bad credentials.
	if (!isBunS3Error(cause) || code === "UnknownError") return new UnknownError(fields);
	if (notFoundCodes.has(code)) return new NotFound(fields);
	if (accessDeniedCodes.has(code)) return new AccessDenied(fields);
	return new ServiceError(fields);
};

/**
 * Classifies an error thrown by Bun's S3 client into an {@link S3Error}.
 *
 * @since 0.1.0
 * @category constructors
 */
export const fromCause = (cause: unknown, operation: Operation, key?: string): S3Error =>
	isS3Error(cause)
		? cause
		: new S3Error({ reason: classify(cause, { operation, key, code: codeOf(cause), cause }) });
