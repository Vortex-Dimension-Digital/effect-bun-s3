import { describe, expect, test } from "bun:test";

import { S3Error } from "../src/index.ts";

const bunError = (code: string, message = "boom") =>
	Object.assign(new Error(message), { name: "S3Error", code });

const cases: ReadonlyArray<readonly [string, S3Error.S3ErrorReason["_tag"]]> = [
	["NoSuchKey", "NotFound"],
	["NoSuchBucket", "NotFound"],
	["AccessDenied", "AccessDenied"],
	["SignatureDoesNotMatch", "AccessDenied"],
	["InvalidAccessKeyId", "AccessDenied"],
	["ConnectionRefused", "TransportError"],
	["DNSResolveFailed", "TransportError"],
	["ECONNRESET", "TransportError"],
	["ERR_S3_INVALID_PATH", "InvalidRequest"],
	["ERR_S3_MISSING_CREDENTIALS", "InvalidRequest"],
	["SlowDown", "ServiceError"],
	["InternalError", "ServiceError"],
	["TLSHandshakeFailed", "TransportError"],
	["InvalidURL", "InvalidRequest"],
	// Bun's code for a failed response without an S3 error body.
	["UnknownError", "UnknownError"],
];

describe("fromCause", () => {
	test.each(cases)("code %s is %s", (code, reason) => {
		const error = S3Error.fromCause(bunError(code), "bytes", "a/b.txt");
		expect(error.reason._tag).toBe(reason);
		expect(error.reason.code).toBe(code);
		expect(error.reason.operation).toBe("bytes");
		expect(error.reason.key).toBe("a/b.txt");
	});

	test("errors not raised by Bun's S3 client are classified by their code alone", () => {
		const missingFile = Object.assign(new Error("No such file or directory"), { code: "ENOENT" });
		const badArgument = Object.assign(new TypeError("expiresIn must be positive"), {
			code: "ERR_INVALID_ARG_TYPE",
		});
		const lookalike = Object.assign(new Error("not from S3"), { code: "NoSuchKey" });
		expect(S3Error.fromCause(missingFile, "write").reason._tag).toBe("UnknownError");
		expect(S3Error.fromCause(badArgument, "presign").reason._tag).toBe("InvalidRequest");
		expect(S3Error.fromCause(lookalike, "use").reason._tag).toBe("UnknownError");
	});

	test("an error without a code is UnknownError", () => {
		expect(S3Error.fromCause(new TypeError("nope"), "use").reason._tag).toBe("UnknownError");
		expect(S3Error.fromCause("a string", "use").reason._tag).toBe("UnknownError");
	});

	test("an S3Error passes through unchanged", () => {
		const error = S3Error.fromCause(bunError("NoSuchKey"), "text", "x");
		expect(S3Error.fromCause(error, "use")).toBe(error);
	});

	test("message names the operation, key and code; cause is kept", () => {
		const cause = bunError("NoSuchKey", "The specified key does not exist.");
		const error = S3Error.fromCause(cause, "text", "a/b.txt");
		expect(error.message).toBe('text "a/b.txt": The specified key does not exist. (NoSuchKey)');
		expect(error.cause).toBe(cause);
		expect(error.reason.cause).toBe(cause);
		expect(S3Error.fromCause(new Error(""), "list").message).toBe("list: unknown error");
	});

	test("isS3Error", () => {
		expect(S3Error.isS3Error(S3Error.fromCause(bunError("SlowDown"), "write", "k"))).toBe(true);
		expect(S3Error.isS3Error(bunError("SlowDown"))).toBe(false);
		expect(S3Error.isS3Error(null)).toBe(false);
	});
});
