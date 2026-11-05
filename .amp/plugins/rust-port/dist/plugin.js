import { createHash } from 'node:crypto';
import { writeFile, readFile, readdir, stat, mkdir, appendFile, rename } from 'node:fs/promises';
import path, { join, normalize, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters, debuglog, inspect as inspect$1, promisify, callbackify, aborted } from 'node:util';
import { ChildProcess, execFile, spawnSync, spawn } from 'node:child_process';
import process$2, { platform, hrtime, execPath, execArgv } from 'node:process';
import { StringDecoder } from 'node:string_decoder';
import tty from 'node:tty';
import { setTimeout, scheduler, setImmediate } from 'node:timers/promises';
import { constants } from 'node:os';
import { once, addAbortListener, EventEmitter, on, setMaxListeners } from 'node:events';
import { serialize } from 'node:v8';
import fs, { openSync, readSync, closeSync, statSync, readFileSync, appendFileSync, writeFileSync, createWriteStream, createReadStream } from 'node:fs';
import { Buffer } from 'node:buffer';
import { Transform, getDefaultHighWaterMark, Duplex, Writable, Readable, PassThrough } from 'node:stream';
import { finished } from 'node:stream/promises';
import path$1 from 'node:path/win32';

function isPlainObject$2(value) {
	if (typeof value !== 'object' || value === null) {
		return false;
	}

	const prototype = Object.getPrototypeOf(value);
	return (prototype === null || prototype === Object.prototype || Object.getPrototypeOf(prototype) === null) && !(Symbol.toStringTag in value) && !(Symbol.iterator in value);
}

// Allow some arguments/options to be either a file path string or a file URL
const safeNormalizeFileUrl = (file, name) => {
	const fileString = normalizeFileUrl(normalizeDenoExecPath(file));

	if (typeof fileString !== 'string') {
		throw new TypeError(`${name} must be a string or a file URL: ${fileString}.`);
	}

	return fileString;
};

// In Deno node:process execPath is a special object, not just a string:
// https://github.com/denoland/deno/blob/f460188e583f00144000aa0d8ade08218d47c3c1/ext/node/polyfills/process.ts#L344
const normalizeDenoExecPath = file => isDenoExecPath(file)
	? file.toString()
	: file;

const isDenoExecPath = file => typeof file !== 'string'
	&& file
	&& Object.getPrototypeOf(file) === String.prototype;

// Same but also allows other values, e.g. `boolean` for the `shell` option
const normalizeFileUrl = file => file instanceof URL ? fileURLToPath(file) : file;

// The command `arguments` and `options` are both optional.
// This also does basic validation on them and on the command file.
const normalizeParameters = (rawFile, rawArguments = [], rawOptions, ...rawExtraArguments) => {
	if (rawExtraArguments.length > 0) {
		throw new TypeError('Execa accepts at most three arguments: file, arguments, options.');
	}

	if (rawOptions !== undefined && isPlainObject$2(rawArguments)) {
		throw new TypeError('Execa accepts at most one options object. Please merge the second and third arguments.');
	}

	const filePath = safeNormalizeFileUrl(rawFile, 'First argument');
	const [commandArguments, options = {}] = isPlainObject$2(rawArguments)
		? [[], rawArguments]
		: [rawArguments, rawOptions];

	if (!Array.isArray(commandArguments)) {
		throw new TypeError(`Second argument must be either an array of arguments or an options object: ${commandArguments}`);
	}

	if (commandArguments.some(commandArgument => typeof commandArgument === 'object' && commandArgument !== null)) {
		throw new TypeError(`Second argument must be an array of strings: ${commandArguments}`);
	}

	const normalizedArguments = commandArguments.map(String);
	const nullByteArgument = normalizedArguments.find(normalizedArgument => normalizedArgument.includes('\0'));
	if (nullByteArgument !== undefined) {
		throw new TypeError(`Arguments cannot contain null bytes ("\\0"): ${nullByteArgument}`);
	}

	if (!isPlainObject$2(options)) {
		throw new TypeError(`Last argument must be an options object: ${options}`);
	}

	// Prevent prototype pollution by copying only own properties to a null-prototype object
	return [filePath, normalizedArguments, {__proto__: null, ...options}];
};

// Compute `result.command` and `result.escapedCommand`
const joinCommand = (filePath, rawArguments, shell) => {
	const fileAndArguments = [filePath, ...rawArguments];
	const command = fileAndArguments.join(' ');
	// With `shell`, the shell interprets the command, so it must not be quoted
	const escapedCommand = shell
		? escapeControlCharacters(command)
		: fileAndArguments
			.map(fileAndArgument => quoteString(escapeControlCharacters(fileAndArgument)))
			.join(' ');
	return {command, escapedCommand};
};

// Remove ANSI sequences and escape control characters and newlines
const escapeLines = lines => stripVTControlCharacters(lines)
	.split('\n')
	.map(line => escapeControlCharacters(line))
	.join('\n');

const escapeControlCharacters = line => line.replaceAll(SPECIAL_CHAR_REGEXP, character => escapeControlCharacter(character));

const escapeControlCharacter = character => {
	const commonEscape = COMMON_ESCAPES[character];
	if (commonEscape !== undefined) {
		return commonEscape;
	}

	const codepoint = character.codePointAt(0);
	const codepointHex = codepoint.toString(16);
	return codepoint <= ASTRAL_START
		? `\\u${codepointHex.padStart(4, '0')}`
		: `\\U${codepointHex}`;
};

// Characters that would create issues when printed are escaped using the \u or \U notation.
// Those include control characters and newlines.
// The \u and \U notation is Bash specific, but there is no way to do this in a shell-agnostic way.
// Some shells do not even have a way to print those characters in an escaped fashion.
// Therefore, we prioritize printing those safely, instead of allowing those to be copy-pasted.
// List of Unicode character categories: https://www.fileformat.info/info/unicode/category/index.htm
const getSpecialCharRegExp = () => {
	try {
		// This throws when using Node.js without ICU support.
		// When using a RegExp literal, this would throw at parsing-time, instead of runtime.
		// eslint-disable-next-line prefer-regex-literals
		return new RegExp('\\p{Separator}|\\p{Other}', 'gu');
	} catch {
		// Similar to the above RegExp, but works even when Node.js has been built without ICU support.
		// Unlike the above RegExp, it only covers whitespaces and C0/C1 control characters.
		// It does not cover some edge cases, such as Unicode reserved characters.
		// See https://github.com/sindresorhus/execa/issues/1143
		// eslint-disable-next-line no-control-regex, regexp/no-control-character, unicorn/prefer-unicode-code-point-escapes
		return /[\s\u0000-\u001F\u007F-\u009F\u00AD]/g;
	}
};

const SPECIAL_CHAR_REGEXP = getSpecialCharRegExp();

// Accepted by $'...' in Bash.
// Exclude \a \e \v which are accepted in Bash but not in JavaScript (except \v) and JSON.
const COMMON_ESCAPES = {
	' ': ' ',
	'\b': '\\b',
	'\f': '\\f',
	'\n': '\\n',
	'\r': '\\r',
	'\t': '\\t',
};

// Up until that code point, a character is a single UTF-16 code unit, so the \u notation can be used instead of \U
const ASTRAL_START = 0xFF_FF;

// Some characters are shell-specific, i.e. need to be escaped when the command is copy-pasted then run.
// Escaping is shell-specific. We cannot know which shell is used: `process.platform` detection is not enough.
// For example, Windows users could be using `cmd.exe`, Powershell or Bash for Windows which all use different escaping.
// We use '...' on Unix, which is POSIX shell compliant and escape all characters but ' so this is fairly safe.
// On Windows, we assume cmd.exe is used and escape with "...", which also works with Powershell.
const quoteString = escapedArgument => {
	if (NO_ESCAPE_REGEXP.test(escapedArgument)) {
		return escapedArgument;
	}

	return platform === 'win32'
		? `"${escapedArgument.replaceAll('"', '""')}"`
		: `'${escapedArgument.replaceAll('\'', '\'\\\'\'')}'`;
};

const NO_ESCAPE_REGEXP = /^[\w\-./]+$/;

const {toString: objectToString$1} = Object.prototype;

const isArrayBuffer = value => objectToString$1.call(value) === '[object ArrayBuffer]';

// Is either Uint8Array or Buffer
const isUint8Array = value => objectToString$1.call(value) === '[object Uint8Array]';

const bufferToUint8Array = buffer => new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);

const textEncoder$1 = new TextEncoder();
const stringToUint8Array = string => textEncoder$1.encode(string);

const textDecoder = new TextDecoder();
const uint8ArrayToString = uint8Array => textDecoder.decode(uint8Array);

const joinToString = (uint8ArraysOrStrings, encoding) => {
	const strings = uint8ArraysToStrings(uint8ArraysOrStrings, encoding);
	return strings.join('');
};

const uint8ArraysToStrings = (uint8ArraysOrStrings, encoding) => {
	if (encoding === 'utf8' && uint8ArraysOrStrings.every(uint8ArrayOrString => typeof uint8ArrayOrString === 'string')) {
		return uint8ArraysOrStrings;
	}

	const decoder = new StringDecoder(encoding);
	const strings = uint8ArraysOrStrings
		.map(uint8ArrayOrString => typeof uint8ArrayOrString === 'string'
			? stringToUint8Array(uint8ArrayOrString)
			: uint8ArrayOrString)
		.map(uint8Array => decoder.write(uint8Array));
	const finalString = decoder.end();
	return finalString === '' ? strings : [...strings, finalString];
};

const joinToUint8Array = uint8ArraysOrStrings => uint8ArraysOrStrings.length === 1 && isUint8Array(uint8ArraysOrStrings[0]) ? uint8ArraysOrStrings[0] : concatUint8Arrays(stringsToUint8Arrays(uint8ArraysOrStrings));

const stringsToUint8Arrays = uint8ArraysOrStrings => uint8ArraysOrStrings.map(uint8ArrayOrString => typeof uint8ArrayOrString === 'string'
	? stringToUint8Array(uint8ArrayOrString)
	: uint8ArrayOrString);

const concatUint8Arrays = uint8Arrays => {
	const result = new Uint8Array(getJoinLength(uint8Arrays));

	let index = 0;
	for (const uint8Array of uint8Arrays) {
		result.set(uint8Array, index);
		index += uint8Array.length;
	}

	return result;
};

const getJoinLength = uint8Arrays => {
	let joinLength = 0;
	for (const uint8Array of uint8Arrays) {
		joinLength += uint8Array.length;
	}

	return joinLength;
};

// When the subprocess fails, this is the error instance being returned.
// If another error instance is being thrown, it is kept as `error.cause`.
const getFinalError = (originalError, message, isSync) => {
	const ErrorClass = isSync ? ExecaSyncError : ExecaError;
	const options = originalError instanceof DiscardedError ? {} : {cause: originalError};
	return new ErrorClass(message, options);
};

// Indicates that the error is used only to interrupt control flow, but not in the return value
class DiscardedError extends Error {}

// Proper way to set `error.name`: it should be inherited and non-enumerable
const setErrorName = (ErrorClass, value) => {
	Object.defineProperties(ErrorClass.prototype, {
		name: {
			value,
			writable: true,
			enumerable: false,
			configurable: true,
		},
		[execaErrorSymbol]: {
			value: true,
			writable: false,
			enumerable: false,
			configurable: false,
		},
	});
};

// Unlike `instanceof`, this works across realms
const isExecaError = error => isErrorInstance(error) && execaErrorSymbol in error;

const execaErrorSymbol = Symbol('isExecaError');

// `instanceof` handles `DOMException` and subclasses with a custom `Symbol.toStringTag`, `Object.prototype.toString()` handles other realms
const isErrorInstance = value => value instanceof Error || Object.prototype.toString.call(value) === '[object Error]';

// We use two different Error classes for async/sync methods since they have slightly different shape and types
class ExecaError extends Error {}
// eslint-disable-next-line unicorn/no-top-level-side-effects -- `error.name` must be set as soon as the class is defined
setErrorName(ExecaError, ExecaError.name);

class ExecaSyncError extends Error {}
// eslint-disable-next-line unicorn/no-top-level-side-effects -- `error.name` must be set as soon as the class is defined
setErrorName(ExecaSyncError, ExecaSyncError.name);

// Like `typeof`, but `null` is reported as `null` instead of `object`, so error messages name the actual value
const getTypeName = value => value === null ? 'null' : typeof value;

// Check whether the template string syntax is being used
const isTemplateString = templates => Array.isArray(templates) && Array.isArray(templates.raw);

// Convert execa`file ...commandArguments` to execa(file, commandArguments)
const parseTemplates = (templates, expressions) => {
	let tokens = [];
	// Whether whitespaces separate the last token from the next one
	let isSeparated = false;

	for (const [index, template] of templates.entries()) {
		({tokens, isSeparated} = parseTemplate({
			templates,
			expressions,
			tokens,
			isSeparated,
			index,
			template,
		}));
	}

	if (tokens.length === 0) {
		throw new TypeError('Template script must not be empty');
	}

	const [file, ...commandArguments] = tokens;
	return [file, commandArguments, {}];
};

const parseTemplate = ({templates, expressions, tokens, isSeparated, index, template}) => {
	if (template === undefined) {
		throw new TypeError(`Invalid backslash sequence: ${templates.raw[index]}`);
	}

	const {nextTokens, leadingWhitespaces, trailingWhitespaces} = splitByWhitespaces(template, templates.raw[index]);
	const newTokens = concatTokens(tokens, nextTokens, isSeparated || leadingWhitespaces);
	// When the template has no tokens, the whitespaces before it still separate the next token, e.g. after an empty array expression
	const isTemplateSeparated = trailingWhitespaces || (nextTokens.length === 0 && isSeparated);

	if (index === expressions.length) {
		return {tokens: newTokens, isSeparated: isTemplateSeparated};
	}

	const expression = expressions[index];
	const expressionTokens = Array.isArray(expression)
		? expression.map(expression => parseExpression(expression))
		: [parseExpression(expression)];
	return {
		tokens: concatTokens(newTokens, expressionTokens, isTemplateSeparated),
		isSeparated: expressionTokens.length === 0 && isTemplateSeparated,
	};
};

// Like `string.split(/[ \t\r\n]+/)` except newlines and tabs are:
//  - ignored when input as a backslash sequence like: `echo foo\n bar`
//  - not ignored when input directly
// The only way to distinguish those in JavaScript is to use a tagged template and compare:
//  - the first array argument, which does not escape backslash sequences
//  - its `raw` property, which escapes them
const splitByWhitespaces = (template, rawTemplate) => {
	// The template is empty either between two `${expression}`, or when it only contains escaped newlines.
	// Neither holds any whitespace, so neither separates tokens.
	if (template.length === 0) {
		return {nextTokens: [], leadingWhitespaces: false, trailingWhitespaces: false};
	}

	const nextTokens = [];
	let templateStart = 0;
	let isLeadingWhitespaces = false;

	for (
		let templateIndex = 0, rawIndex = 0;
		templateIndex < template.length;
		templateIndex += 1, rawIndex += 1
	) {
		const rawCharacter = rawTemplate[rawIndex];
		if (DELIMITERS.has(rawCharacter)) {
			// Escaped newlines are ignored, so a whitespace after them is still a leading one
			if (templateIndex === 0) {
				isLeadingWhitespaces = true;
			}

			if (templateStart !== templateIndex) {
				nextTokens.push(template.slice(templateStart, templateIndex));
			}

			templateStart = templateIndex + 1;
		} else if (rawCharacter === '\\') {
			const nextRawCharacter = rawTemplate[rawIndex + 1];
			if (nextRawCharacter === '\n') {
				// Handles escaped newlines in templates
				templateIndex -= 1;
				rawIndex += 1;
			} else if (nextRawCharacter === 'u' && rawTemplate[rawIndex + 2] === '{') {
				const closingBraceIndex = rawTemplate.indexOf('}', rawIndex + 3);
				// A code point above the BMP is a surrogate pair, i.e. two characters, in the template string
				if (isAstralEscape(rawTemplate.slice(rawIndex + 3, closingBraceIndex))) {
					templateIndex += 1;
				}

				rawIndex = closingBraceIndex;
			} else {
				rawIndex += ESCAPE_LENGTH[nextRawCharacter] ?? 1;
			}
		}
	}

	const isTrailingWhitespaces = templateStart === template.length;
	if (!isTrailingWhitespaces) {
		nextTokens.push(template.slice(templateStart));
	}

	return {nextTokens, leadingWhitespaces: isLeadingWhitespaces, trailingWhitespaces: isTrailingWhitespaces};
};

const isAstralEscape = codePointHex => Number.parseInt(codePointHex, 16) > ASTRAL_START;

const DELIMITERS = new Set([' ', '\t', '\r', '\n']);

// Number of characters in backslash escape sequences: \0 \xXX or \uXXXX
// \cX is allowed in RegExps but not in strings
// Octal sequences are not allowed in strict mode
const ESCAPE_LENGTH = {x: 3, u: 5};

const concatTokens = (tokens, nextTokens, isSeparated) => isSeparated
	|| tokens.length === 0
	|| nextTokens.length === 0
	? [...tokens, ...nextTokens]
	: [
		...tokens.slice(0, -1),
		`${tokens.at(-1)}${nextTokens[0]}`,
		...nextTokens.slice(1),
	];

// Handle `${expression}` inside the template string syntax
const parseExpression = expression => {
	const typeOfExpression = getTypeName(expression);

	if (typeOfExpression === 'string') {
		return expression;
	}

	if (typeOfExpression === 'number') {
		return String(expression);
	}

	// Only own properties are checked: a polluted `Object.prototype.stdout` must not turn any plain object into a subprocess result.
	// A failed subprocess result is an error instance instead.
	if ((isPlainObject$2(expression) || isExecaError(expression)) && (Object.hasOwn(expression, 'stdout') || Object.hasOwn(expression, 'isMaxBuffer'))) {
		return getSubprocessResult(expression);
	}

	if (expression instanceof ChildProcess || Object.prototype.toString.call(expression) === '[object Promise]') {
		// eslint-disable-next-line no-template-curly-in-string
		throw new TypeError('Unexpected subprocess in template expression. Please use ${await subprocess} instead of ${subprocess}.');
	}

	throw new TypeError(`Unexpected "${typeOfExpression}" in template expression`);
};

const getSubprocessResult = result => {
	// Read the own property only: results with `stdout: 'ignore'` have no `stdout` property, which must not be read from a polluted prototype
	const stdout = Object.hasOwn(result, 'stdout') ? result.stdout : undefined;
	if (typeof stdout === 'string') {
		return stdout;
	}

	if (isUint8Array(stdout)) {
		return uint8ArrayToString(stdout);
	}

	if (stdout === undefined) {
		throw new TypeError('Missing result.stdout in template expression. This is probably due to the previous subprocess\' "stdout" option.');
	}

	throw new TypeError(`Unexpected "${getTypeName(stdout)}" stdout in template expression`);
};

const isStandardStream = stream => STANDARD_STREAMS.includes(stream);
const STANDARD_STREAMS = [process$2.stdin, process$2.stdout, process$2.stderr];
const STANDARD_STREAMS_ALIASES = ['stdin', 'stdout', 'stderr'];
const getStreamName = fdNumber => STANDARD_STREAMS_ALIASES[fdNumber] ?? `stdio[${fdNumber}]`;

// `subprocess.all` is created by Execa itself, which then pipes `stdout`/`stderr` into it.
// `verbose` needs to tell that pipe apart from a user pipe, which it disables.
const allStreamSymbol = Symbol('execa.allStream');
const setAllStream = stream => Object.assign(stream, {[allStreamSymbol]: true});
const isAllStream = stream => stream?.[allStreamSymbol] === true;

// The options of methods like `subprocess.pipe()`, `subprocess.readable()` or `sendMessage()` are kept on a null-prototype object, otherwise a polluted `Object.prototype` would inject option values.
// Like destructuring them, `null` throws.
const copyOptions = (options = {}) => {
	if (options === null) {
		throw new TypeError('The options must be an object, not `null`.');
	}

	return {__proto__: null, ...options};
};

const checkBooleanOption = (value, optionName) => {
	if (value !== undefined && typeof value !== 'boolean') {
		throw new TypeError(`The \`${optionName}\` option must use a boolean.`);
	}
};

// Some options can have different values for `stdout`/`stderr`/`fd3`.
// This normalizes those to array of values.
// For example, `{verbose: {stdout: 'none', stderr: 'full'}}` becomes `{verbose: ['none', 'none', 'full']}`
const normalizeFdSpecificOptions = options => {
	// Keep the null prototype, otherwise a polluted `Object.prototype` would inject option values
	const optionsCopy = {__proto__: null, ...options};

	for (const optionName of FD_SPECIFIC_OPTIONS) {
		optionsCopy[optionName] = normalizeFdSpecificOption(options, optionName);
	}

	return optionsCopy;
};

const normalizeFdSpecificOption = (options, optionName) => {
	const stdioLength = getStdioLength(options);
	const optionBaseArray = Array.from({length: stdioLength + 1});
	const optionArray = normalizeFdSpecificValue(options[optionName], optionBaseArray, optionName, stdioLength);
	return addDefaultValue$1(optionArray, optionName);
};

const getStdioLength = ({stdio}) => Array.isArray(stdio)
	? Math.max(stdio.length, STANDARD_STREAMS_ALIASES.length)
	: STANDARD_STREAMS_ALIASES.length;

const normalizeFdSpecificValue = (optionValue, optionArray, optionName, stdioLength) => {
	if (isPlainObject$2(optionValue)) {
		return normalizeOptionObject(optionValue, optionArray, optionName, stdioLength);
	}

	validateFdSpecificValue(optionValue, optionName);
	return optionArray.fill(optionValue);
};

const normalizeOptionObject = (optionValue, optionArray, optionName, stdioLength) => {
	for (const fdName of Object.keys(optionValue).sort(compareFdName)) {
		validateFdSpecificValue(optionValue[fdName], optionName, `${optionName}.${fdName}`);
		for (const fdNumber of parseFdName(fdName, optionName, stdioLength)) {
			optionArray[fdNumber] = optionValue[fdName];
		}
	}

	return optionArray;
};

// Ensure priority order when setting both `stdout`/`stderr`, `fd1`/`fd2`, and `all`
const compareFdName = (fdNameA, fdNameB) => getFdNameOrder(fdNameA) < getFdNameOrder(fdNameB) ? 1 : -1;

const getFdNameOrder = fdName => {
	if (fdName === 'stdout' || fdName === 'stderr') {
		return 0;
	}

	return fdName === 'all' ? 2 : 1;
};

const parseFdName = (fdName, optionName, stdioLength) => {
	if (fdName === 'ipc') {
		return [stdioLength];
	}

	const fdNumber = parseFd(fdName);
	if (fdNumber === undefined || fdNumber === 0) {
		throw new TypeError(`"${optionName}.${fdName}" is invalid.
It must be "${optionName}.stdout", "${optionName}.stderr", "${optionName}.all", "${optionName}.ipc", or "${optionName}.fd3", "${optionName}.fd4" (and so on).`);
	}

	if (fdNumber !== 'all' && fdNumber >= stdioLength) {
		throw new TypeError(`"${optionName}.${fdName}" is invalid: that file descriptor does not exist.
Please set the "stdio" option to ensure that file descriptor exists.`);
	}

	return fdNumber === 'all' ? [1, 2] : [fdNumber];
};

// Use the same syntax for fd-specific options and the `from`/`to` options
const parseFd = fdName => {
	if (fdName === 'all') {
		return fdName;
	}

	if (STANDARD_STREAMS_ALIASES.includes(fdName)) {
		return STANDARD_STREAMS_ALIASES.indexOf(fdName);
	}

	const regexpResult = FD_REGEXP.exec(fdName);
	if (regexpResult !== null) {
		return Number(regexpResult.groups.fdNumber);
	}
};

const FD_REGEXP = /^fd(?<fdNumber>\d+)$/;

const addDefaultValue$1 = (optionArray, optionName) => optionArray.map(optionValue => optionValue === undefined
	? DEFAULT_OPTIONS[optionName]
	: optionValue);

const BOOLEAN_FD_SPECIFIC_OPTIONS = new Set(['lines', 'buffer', 'stripFinalNewline']);

// Wrong types must be reported instead of being silently coerced, e.g. a truthy string enabling the `buffer` option.
// `undefined` uses the default value.
const validateFdSpecificValue = (optionValue, optionName, fullOptionName = optionName) => {
	if (BOOLEAN_FD_SPECIFIC_OPTIONS.has(optionName)) {
		checkBooleanOption(optionValue, fullOptionName);
	} else if (optionName === 'maxBuffer' && optionValue !== undefined && (typeof optionValue !== 'number' || Number.isNaN(optionValue))) {
		throw new TypeError(`The \`${fullOptionName}\` option must use a number.`);
	}
};

// Default value for the `verbose` option
const verboseDefault = debuglog('execa').enabled ? 'full' : 'none';

const DEFAULT_OPTIONS = {
	lines: false,
	buffer: true,
	maxBuffer: 1000 * 1000 * 100,
	verbose: verboseDefault,
	stripFinalNewline: true,
};

// List of options which can have different values for `stdout`/`stderr`
const FD_SPECIFIC_OPTIONS = ['lines', 'buffer', 'maxBuffer', 'verbose', 'stripFinalNewline'];

// Retrieve fd-specific option
const getFdSpecificValue = (optionArray, fdNumber) => fdNumber === 'ipc'
	? optionArray.at(-1)
	: optionArray[fdNumber];

// The `verbose` option can have different values for `stdout`/`stderr`
const isVerbose = ({verbose}, fdNumber) => getFdVerbose(verbose, fdNumber) !== 'none';

// Whether IPC and output and logged
const isFullVerbose = ({verbose}, fdNumber) => !['none', 'short'].includes(getFdVerbose(verbose, fdNumber));

// The `verbose` option can be a function to customize logging
const getVerboseFunction = ({verbose}, fdNumber) => {
	const fdVerbose = getFdVerbose(verbose, fdNumber);
	return isVerboseFunction(fdVerbose) ? fdVerbose : undefined;
};

// When using `verbose: {stdout, stderr, fd3, ipc}`:
//  - `verbose.stdout|stderr|fd3` is used for 'output'
//  - `verbose.ipc` is only used for 'ipc'
//  - highest `verbose.*` value is used for 'command', 'error' and 'duration'
const getFdVerbose = (verbose, fdNumber) => fdNumber === undefined
	? getFdGenericVerbose(verbose)
	: getFdSpecificValue(verbose, fdNumber);

// When using `verbose: {stdout, stderr, fd3, ipc}` and logging is not specific to a file descriptor.
// We then use the highest `verbose.*` value, using the following order:
//  - function > 'full' > 'short' > 'none'
//  - if several functions are defined: stdout > stderr > fd3 > ipc
const getFdGenericVerbose = verbose => verbose.find(fdVerbose => isVerboseFunction(fdVerbose))
	?? VERBOSE_VALUES.findLast(fdVerbose => verbose.includes(fdVerbose));

// Whether the `verbose` option is customized using a function
const isVerboseFunction = fdVerbose => typeof fdVerbose === 'function';

const VERBOSE_VALUES = ['none', 'short', 'full'];

function isUnicodeSupported() {
	const {env} = process$2;
	const {TERM, TERM_PROGRAM} = env;

	if (process$2.platform !== 'win32') {
		return TERM !== 'linux'; // Linux console (kernel)
	}

	return Boolean(env.WT_SESSION) // Windows Terminal
		|| Boolean(env.TERMINUS_SUBLIME) // Terminus (<0.2.27)
		|| env.ConEmuTask === '{cmd::Cmder}' // ConEmu and cmder
		|| TERM_PROGRAM === 'Terminus-Sublime'
		|| TERM_PROGRAM === 'vscode'
		|| TERM === 'xterm-256color'
		|| TERM === 'alacritty'
		|| TERM === 'rxvt-unicode'
		|| TERM === 'rxvt-unicode-256color'
		|| env.TERMINAL_EMULATOR === 'JetBrains-JediTerm';
}

const common = {
	circleQuestionMark: '(?)',
	questionMarkPrefix: '(?)',
	square: '█',
	squareDarkShade: '▓',
	squareMediumShade: '▒',
	squareLightShade: '░',
	squareTop: '▀',
	squareBottom: '▄',
	squareLeft: '▌',
	squareRight: '▐',
	squareCenter: '■',
	bullet: '●',
	dot: '․',
	ellipsis: '…',
	pointerSmall: '›',
	triangleUp: '▲',
	triangleUpSmall: '▴',
	triangleDown: '▼',
	triangleDownSmall: '▾',
	triangleLeftSmall: '◂',
	triangleRightSmall: '▸',
	home: '⌂',
	heart: '♥',
	musicNote: '♪',
	musicNoteBeamed: '♫',
	arrowUp: '↑',
	arrowDown: '↓',
	arrowLeft: '←',
	arrowRight: '→',
	arrowLeftRight: '↔',
	arrowUpDown: '↕',
	almostEqual: '≈',
	notEqual: '≠',
	lessOrEqual: '≤',
	greaterOrEqual: '≥',
	identical: '≡',
	infinity: '∞',
	subscriptZero: '₀',
	subscriptOne: '₁',
	subscriptTwo: '₂',
	subscriptThree: '₃',
	subscriptFour: '₄',
	subscriptFive: '₅',
	subscriptSix: '₆',
	subscriptSeven: '₇',
	subscriptEight: '₈',
	subscriptNine: '₉',
	oneHalf: '½',
	oneThird: '⅓',
	oneQuarter: '¼',
	oneFifth: '⅕',
	oneSixth: '⅙',
	oneEighth: '⅛',
	twoThirds: '⅔',
	twoFifths: '⅖',
	threeQuarters: '¾',
	threeFifths: '⅗',
	threeEighths: '⅜',
	fourFifths: '⅘',
	fiveSixths: '⅚',
	fiveEighths: '⅝',
	sevenEighths: '⅞',
	line: '─',
	lineBold: '━',
	lineDouble: '═',
	lineDashed0: '┄',
	lineDashed1: '┅',
	lineDashed2: '┈',
	lineDashed3: '┉',
	lineDashed4: '╌',
	lineDashed5: '╍',
	lineDashed6: '╴',
	lineDashed7: '╶',
	lineDashed8: '╸',
	lineDashed9: '╺',
	lineDashed10: '╼',
	lineDashed11: '╾',
	lineDashed12: '−',
	lineDashed13: '–',
	lineDashed14: '‐',
	lineDashed15: '⁃',
	lineVertical: '│',
	lineVerticalBold: '┃',
	lineVerticalDouble: '║',
	lineVerticalDashed0: '┆',
	lineVerticalDashed1: '┇',
	lineVerticalDashed2: '┊',
	lineVerticalDashed3: '┋',
	lineVerticalDashed4: '╎',
	lineVerticalDashed5: '╏',
	lineVerticalDashed6: '╵',
	lineVerticalDashed7: '╷',
	lineVerticalDashed8: '╹',
	lineVerticalDashed9: '╻',
	lineVerticalDashed10: '╽',
	lineVerticalDashed11: '╿',
	lineDownLeft: '┐',
	lineDownLeftArc: '╮',
	lineDownBoldLeftBold: '┓',
	lineDownBoldLeft: '┒',
	lineDownLeftBold: '┑',
	lineDownDoubleLeftDouble: '╗',
	lineDownDoubleLeft: '╖',
	lineDownLeftDouble: '╕',
	lineDownRight: '┌',
	lineDownRightArc: '╭',
	lineDownBoldRightBold: '┏',
	lineDownBoldRight: '┎',
	lineDownRightBold: '┍',
	lineDownDoubleRightDouble: '╔',
	lineDownDoubleRight: '╓',
	lineDownRightDouble: '╒',
	lineUpLeft: '┘',
	lineUpLeftArc: '╯',
	lineUpBoldLeftBold: '┛',
	lineUpBoldLeft: '┚',
	lineUpLeftBold: '┙',
	lineUpDoubleLeftDouble: '╝',
	lineUpDoubleLeft: '╜',
	lineUpLeftDouble: '╛',
	lineUpRight: '└',
	lineUpRightArc: '╰',
	lineUpBoldRightBold: '┗',
	lineUpBoldRight: '┖',
	lineUpRightBold: '┕',
	lineUpDoubleRightDouble: '╚',
	lineUpDoubleRight: '╙',
	lineUpRightDouble: '╘',
	lineUpDownLeft: '┤',
	lineUpBoldDownBoldLeftBold: '┫',
	lineUpBoldDownBoldLeft: '┨',
	lineUpDownLeftBold: '┥',
	lineUpBoldDownLeftBold: '┩',
	lineUpDownBoldLeftBold: '┪',
	lineUpDownBoldLeft: '┧',
	lineUpBoldDownLeft: '┦',
	lineUpDoubleDownDoubleLeftDouble: '╣',
	lineUpDoubleDownDoubleLeft: '╢',
	lineUpDownLeftDouble: '╡',
	lineUpDownRight: '├',
	lineUpBoldDownBoldRightBold: '┣',
	lineUpBoldDownBoldRight: '┠',
	lineUpDownRightBold: '┝',
	lineUpBoldDownRightBold: '┡',
	lineUpDownBoldRightBold: '┢',
	lineUpDownBoldRight: '┟',
	lineUpBoldDownRight: '┞',
	lineUpDoubleDownDoubleRightDouble: '╠',
	lineUpDoubleDownDoubleRight: '╟',
	lineUpDownRightDouble: '╞',
	lineDownLeftRight: '┬',
	lineDownBoldLeftBoldRightBold: '┳',
	lineDownLeftBoldRightBold: '┯',
	lineDownBoldLeftRight: '┰',
	lineDownBoldLeftBoldRight: '┱',
	lineDownBoldLeftRightBold: '┲',
	lineDownLeftRightBold: '┮',
	lineDownLeftBoldRight: '┭',
	lineDownDoubleLeftDoubleRightDouble: '╦',
	lineDownDoubleLeftRight: '╥',
	lineDownLeftDoubleRightDouble: '╤',
	lineUpLeftRight: '┴',
	lineUpBoldLeftBoldRightBold: '┻',
	lineUpLeftBoldRightBold: '┷',
	lineUpBoldLeftRight: '┸',
	lineUpBoldLeftBoldRight: '┹',
	lineUpBoldLeftRightBold: '┺',
	lineUpLeftRightBold: '┶',
	lineUpLeftBoldRight: '┵',
	lineUpDoubleLeftDoubleRightDouble: '╩',
	lineUpDoubleLeftRight: '╨',
	lineUpLeftDoubleRightDouble: '╧',
	lineUpDownLeftRight: '┼',
	lineUpBoldDownBoldLeftBoldRightBold: '╋',
	lineUpDownBoldLeftBoldRightBold: '╈',
	lineUpBoldDownLeftBoldRightBold: '╇',
	lineUpBoldDownBoldLeftRightBold: '╊',
	lineUpBoldDownBoldLeftBoldRight: '╉',
	lineUpBoldDownLeftRight: '╀',
	lineUpDownBoldLeftRight: '╁',
	lineUpDownLeftBoldRight: '┽',
	lineUpDownLeftRightBold: '┾',
	lineUpBoldDownBoldLeftRight: '╂',
	lineUpDownLeftBoldRightBold: '┿',
	lineUpBoldDownLeftBoldRight: '╃',
	lineUpBoldDownLeftRightBold: '╄',
	lineUpDownBoldLeftBoldRight: '╅',
	lineUpDownBoldLeftRightBold: '╆',
	lineUpDoubleDownDoubleLeftDoubleRightDouble: '╬',
	lineUpDoubleDownDoubleLeftRight: '╫',
	lineUpDownLeftDoubleRightDouble: '╪',
	lineCross: '╳',
	lineBackslash: '╲',
	lineSlash: '╱',
};

const specialMainSymbols = {
	tick: '✔',
	info: 'ℹ',
	warning: '⚠',
	cross: '✘',
	squareSmall: '◻',
	squareSmallFilled: '◼',
	circle: '◯',
	circleFilled: '◉',
	circleDotted: '◌',
	circleDouble: '◎',
	circleCircle: 'ⓞ',
	circleCross: 'ⓧ',
	circlePipe: 'Ⓘ',
	radioOn: '◉',
	radioOff: '◯',
	checkboxOn: '☒',
	checkboxOff: '☐',
	checkboxCircleOn: 'ⓧ',
	checkboxCircleOff: 'Ⓘ',
	pointer: '❯',
	triangleUpOutline: '△',
	triangleLeft: '◀',
	triangleRight: '▶',
	lozenge: '◆',
	lozengeOutline: '◇',
	hamburger: '☰',
	smiley: '㋡',
	mustache: '෴',
	star: '★',
	play: '▶',
	nodejs: '⬢',
	oneSeventh: '⅐',
	oneNinth: '⅑',
	oneTenth: '⅒',
};

const specialFallbackSymbols = {
	tick: '√',
	info: 'i',
	warning: '‼',
	cross: '×',
	squareSmall: '□',
	squareSmallFilled: '■',
	circle: '( )',
	circleFilled: '(*)',
	circleDotted: '( )',
	circleDouble: '( )',
	circleCircle: '(○)',
	circleCross: '(×)',
	circlePipe: '(│)',
	radioOn: '(*)',
	radioOff: '( )',
	checkboxOn: '[×]',
	checkboxOff: '[ ]',
	checkboxCircleOn: '(×)',
	checkboxCircleOff: '( )',
	pointer: '>',
	triangleUpOutline: '∆',
	triangleLeft: '◄',
	triangleRight: '►',
	lozenge: '♦',
	lozengeOutline: '◊',
	hamburger: '≡',
	smiley: '☺',
	mustache: '┌─┐',
	star: '✶',
	play: '►',
	nodejs: '♦',
	oneSeventh: '1/7',
	oneNinth: '1/9',
	oneTenth: '1/10',
};

const mainSymbols = {...common, ...specialMainSymbols};
const fallbackSymbols = {...common, ...specialFallbackSymbols};

const shouldUseMain = isUnicodeSupported();
const figures = shouldUseMain ? mainSymbols : fallbackSymbols;

// eslint-disable-next-line no-warning-comments
// TODO: Use a better method when it's added to Node.js (https://github.com/nodejs/node/pull/40240)
// Lots of optionals here to support Deno.
const hasColors$1 = tty?.WriteStream?.prototype?.hasColors?.() ?? false;

const format = (open, close) => {
	if (!hasColors$1) {
		return input => input;
	}

	const openCode = `\u001B[${open}m`;
	const closeCode = `\u001B[${close}m`;

	return input => {
		const string = input + ''; // eslint-disable-line no-implicit-coercion -- This is faster.
		let index = string.indexOf(closeCode);

		if (index === -1) {
			// Note: Intentionally not using string interpolation for performance reasons.
			return openCode + string + closeCode;
		}

		// Handle nested colors.

		// We could have done this, but it's too slow (as of Node.js 22).
		// return openCode + string.replaceAll(closeCode, (close === 22 ? closeCode : '') + openCode) + closeCode;

		let result = openCode;
		let lastIndex = 0;

		// SGR 22 resets both bold (1) and dim (2). When we encounter a nested
		// close for styles that use 22, we need to re-open the outer style.
		const reopenOnNestedClose = close === 22;
		const replaceCode = (reopenOnNestedClose ? closeCode : '') + openCode;

		while (index !== -1) {
			result += string.slice(lastIndex, index) + replaceCode;
			lastIndex = index + closeCode.length;
			index = string.indexOf(closeCode, lastIndex);
		}

		result += string.slice(lastIndex) + closeCode;

		return result;
	};
};
const bold$1 = format(1, 22);
const gray$1 = format(90, 39);

const redBright$1 = format(91, 39);
const yellowBright$1 = format(93, 39);

// `yoctocolors` checks whether the terminal type supports colors, but not whether `stderr`, where this is printed, is a terminal.
// Like `util.styleText()`, colors are not used when `stderr` is redirected, e.g. to a file, unless `FORCE_COLOR` is set.
const hasColors = process$2.env.FORCE_COLOR !== undefined || process$2.stderr.isTTY === true;
const identity$1 = string => string;
const getColor = color => hasColors ? color : identity$1;
const gray = getColor(gray$1);
const bold = getColor(bold$1);
const redBright = getColor(redBright$1);
const yellowBright = getColor(yellowBright$1);

// Default when `verbose` is not a function
const defaultVerboseFunction = ({
	type,
	message,
	timestamp,
	piped,
	commandId,
	result: {failed = false} = {},
	options: {reject = true},
}) => {
	const timestampString = serializeTimestamp(timestamp);
	const icon = ICONS[type]({failed, reject, piped});
	const color = COLORS[type]({reject});
	return `${gray(`[${timestampString}]`)} ${gray(`[${commandId}]`)} ${color(icon)} ${color(message)}`;
};

// Prepending the timestamp allows debugging the slow paths of a subprocess
const serializeTimestamp = timestamp => `${padField(timestamp.getHours(), 2)}:${padField(timestamp.getMinutes(), 2)}:${padField(timestamp.getSeconds(), 2)}.${padField(timestamp.getMilliseconds(), 3)}`;

const padField = (field, padding) => String(field).padStart(padding, '0');

const getFinalIcon = ({failed, reject}) => {
	if (!failed) {
		return figures.tick;
	}

	return reject ? figures.cross : figures.warning;
};

const ICONS = {
	command: ({piped}) => piped ? '|' : '$',
	output: () => ' ',
	ipc: () => '*',
	error: getFinalIcon,
	duration: getFinalIcon,
};

const COLORS = {
	command: () => bold,
	output: () => identity$1,
	ipc: () => identity$1,
	error: ({reject}) => reject ? redBright : yellowBright,
	duration: () => gray,
};

// Apply the `verbose` function on each line
const applyVerboseOnLines = (printedLines, verboseInfo, fdNumber) => {
	const verboseFunction = getVerboseFunction(verboseInfo, fdNumber);
	return printedLines
		.map(({verboseLine, verboseObject}) => applyVerboseFunction(verboseLine, verboseObject, verboseFunction))
		.filter(printedLine => printedLine !== undefined)
		.map(printedLine => appendNewline(printedLine))
		.join('');
};

const applyVerboseFunction = (verboseLine, verboseObject, verboseFunction) => {
	if (verboseFunction === undefined) {
		return verboseLine;
	}

	const printedLine = verboseFunction(verboseLine, verboseObject);
	if (typeof printedLine === 'string') {
		return printedLine;
	}
};

const appendNewline = printedLine => printedLine.endsWith('\n')
	? printedLine
	: `${printedLine}\n`;

// This prints on stderr.
// If the subprocess prints on stdout and is using `stdout: 'inherit'`,
// there is a chance both writes will compete (introducing a race condition).
// This means their respective order is not deterministic.
// In particular, this means the verbose command lines might be after the start of the subprocess output.
// Using synchronous I/O does not solve this problem.
// However, this only seems to happen when the stdout/stderr target
// (e.g. a terminal) is being written to by many subprocesses at once, which is unlikely in real scenarios.
const verboseLog = ({type, verboseMessage, fdNumber, verboseInfo, result}) => {
	const verboseObject = getVerboseObject({type, result, verboseInfo});
	const printedLines = getPrintedLines(verboseMessage, verboseObject);
	const finalLines = applyVerboseOnLines(printedLines, verboseInfo, fdNumber);
	if (finalLines !== '') {
		console.warn(finalLines.slice(0, -1));
	}
};

const getVerboseObject = ({type, result, verboseInfo}) => {
	const {escapedCommand, commandId, rawOptions} = verboseInfo;
	const {piped = false, ...options} = rawOptions;
	return {
		type,
		escapedCommand,
		commandId: `${commandId}`,
		timestamp: new Date(),
		piped,
		result,
		options,
	};
};

const getPrintedLines = (verboseMessage, verboseObject) => verboseMessage
	.split('\n')
	.map(message => getPrintedLine({...verboseObject, message}));

const getPrintedLine = verboseObject => {
	const verboseLine = defaultVerboseFunction(verboseObject);
	return {verboseLine, verboseObject};
};

// Serialize any type to a line string, for logging.
// Tabs are escaped by `escapeLines()`, like any other control character.
const serializeVerboseMessage = message => {
	const messageString = typeof message === 'string' ? message : inspect$1(message);
	return escapeLines(messageString);
};

// When `verbose` is `short|full|custom`, print each command
const logCommand = (escapedCommand, verboseInfo) => {
	if (!isVerbose(verboseInfo)) {
		return;
	}

	verboseLog({
		type: 'command',
		verboseMessage: escapedCommand,
		verboseInfo,
	});
};

// Information computed before spawning, used by the `verbose` option
const getVerboseInfo = (verbose, escapedCommand, rawOptions) => {
	validateVerbose(verbose);
	const commandId = getCommandId(verbose);
	return {
		verbose,
		escapedCommand,
		commandId,
		rawOptions,
	};
};

const getCommandId = verbose => isVerbose({verbose}) ? COMMAND_ID++ : undefined;

// Prepending the `pid` is useful when multiple commands print their output at the same time.
// However, we cannot use the real PID since this is not available with `child_process.spawnSync()`.
// Also, we cannot use the real PID if we want to print it before `child_process.spawn()` is run.
// As a pro, it is shorter than a normal PID and never re-uses the same id.
// As a con, it cannot be used to send signals.
let COMMAND_ID = 0n;

const validateVerbose = verbose => {
	for (const fdVerbose of verbose) {
		if (fdVerbose === false) {
			throw new TypeError('The "verbose: false" option was renamed to "verbose: \'none\'".');
		}

		if (fdVerbose === true) {
			throw new TypeError('The "verbose: true" option was renamed to "verbose: \'short\'".');
		}

		if (!VERBOSE_VALUES.includes(fdVerbose) && !isVerboseFunction(fdVerbose)) {
			const allowedValues = VERBOSE_VALUES.map(allowedValue => `'${allowedValue}'`).join(', ');
			throw new TypeError(`The "verbose" option must not be ${fdVerbose}. Allowed values are: ${allowedValues} or a function.`);
		}
	}
};

// Start counting time before spawning the subprocess
const getStartTime = () => hrtime.bigint();

// Compute duration after the subprocess ended.
// Printed by the `verbose` option.
const getDurationMs = startTime => Number(hrtime.bigint() - startTime) / 1e6;

// Compute `result.command`, `result.escapedCommand` and `verbose`-related information
const handleCommand = (filePath, rawArguments, rawOptions) => {
	const startTime = getStartTime();
	// The `node` option ignores `shell`
	const {command, escapedCommand} = joinCommand(filePath, rawArguments, rawOptions.shell && !rawOptions.node);
	const verbose = normalizeFdSpecificOption(rawOptions, 'verbose');
	// Keep the null prototype, otherwise a polluted `Object.prototype` would inject option values
	const verboseInfo = getVerboseInfo(verbose, escapedCommand, {__proto__: null, ...rawOptions});
	logCommand(escapedCommand, verboseInfo);
	return {
		command,
		escapedCommand,
		startTime,
		verboseInfo,
	};
};

function pathKey(options = {}) {
	const {
		env = process.env,
		platform = process.platform
	} = options;

	if (platform !== 'win32') {
		return 'PATH';
	}

	return Object.keys(env).reverse().find(key => key.toUpperCase() === 'PATH') || 'Path';
}

promisify(execFile);

function toPath(urlOrPath) {
	return urlOrPath instanceof URL ? fileURLToPath(urlOrPath) : urlOrPath;
}

function traversePathUp(startPath) {
	return {
		* [Symbol.iterator]() {
			let currentPath = path.resolve(toPath(startPath));
			let previousPath;

			while (previousPath !== currentPath) {
				yield currentPath;
				previousPath = currentPath;
				currentPath = path.resolve(currentPath, '..');
			}
		},
	};
}

const npmRunPath = ({
	cwd = process$2.cwd(),
	path: pathOption = process$2.env[pathKey()],
	preferLocal = true,
	execPath = process$2.execPath,
	addExecPath = true,
} = {}) => {
	const cwdPath = path.resolve(toPath(cwd));
	const result = [];
	const pathParts = pathOption.split(path.delimiter);

	if (preferLocal) {
		applyPreferLocal(result, pathParts, cwdPath);
	}

	if (addExecPath) {
		applyExecPath(result, pathParts, execPath, cwdPath);
	}

	return pathOption === '' || pathOption === path.delimiter
		? `${result.join(path.delimiter)}${pathOption}`
		: [...result, pathOption].join(path.delimiter);
};

const applyPreferLocal = (result, pathParts, cwdPath) => {
	for (const directory of traversePathUp(cwdPath)) {
		const pathPart = path.join(directory, 'node_modules/.bin');
		if (!pathParts.includes(pathPart)) {
			result.push(pathPart);
		}
	}
};

// Ensure the running `node` binary is used
const applyExecPath = (result, pathParts, execPath, cwdPath) => {
	const pathPart = path.resolve(cwdPath, toPath(execPath), '..');
	if (!pathParts.includes(pathPart)) {
		result.push(pathPart);
	}
};

const npmRunPathEnv = ({env = process$2.env, ...options} = {}) => {
	env = {...env};

	const pathName = pathKey({env});
	options.path = env[pathName];
	env[pathName] = npmRunPath(options);

	return env;
};

const getRealtimeSignals=()=>{
const length=SIGRTMAX-SIGRTMIN+1;
return Array.from({length},getRealtimeSignal)
};

const getRealtimeSignal=(value,index)=>({
name:`SIGRT${index+1}`,
number:SIGRTMIN+index,
action:"terminate",
description:"Application-specific signal (realtime)",
standard:"posix"
});

const SIGRTMIN=34;
const SIGRTMAX=64;

const SIGNALS=[
{
name:"SIGHUP",
number:1,
action:"terminate",
description:"Terminal closed",
standard:"posix"
},
{
name:"SIGINT",
number:2,
action:"terminate",
description:"User interruption with CTRL-C",
standard:"ansi"
},
{
name:"SIGQUIT",
number:3,
action:"core",
description:"User interruption with CTRL-\\",
standard:"posix"
},
{
name:"SIGILL",
number:4,
action:"core",
description:"Invalid machine instruction",
standard:"ansi"
},
{
name:"SIGTRAP",
number:5,
action:"core",
description:"Debugger breakpoint",
standard:"posix"
},
{
name:"SIGABRT",
number:6,
action:"core",
description:"Aborted",
standard:"ansi"
},
{
name:"SIGIOT",
number:6,
action:"core",
description:"Aborted",
standard:"bsd"
},
{
name:"SIGBUS",
number:7,
action:"core",
description:
"Bus error due to misaligned, non-existing address or paging error",
standard:"bsd"
},
{
name:"SIGEMT",
number:7,
action:"terminate",
description:"Command should be emulated but is not implemented",
standard:"other"
},
{
name:"SIGFPE",
number:8,
action:"core",
description:"Floating point arithmetic error",
standard:"ansi"
},
{
name:"SIGKILL",
number:9,
action:"terminate",
description:"Forced termination",
standard:"posix",
forced:true
},
{
name:"SIGUSR1",
number:10,
action:"terminate",
description:"Application-specific signal",
standard:"posix"
},
{
name:"SIGSEGV",
number:11,
action:"core",
description:"Segmentation fault",
standard:"ansi"
},
{
name:"SIGUSR2",
number:12,
action:"terminate",
description:"Application-specific signal",
standard:"posix"
},
{
name:"SIGPIPE",
number:13,
action:"terminate",
description:"Broken pipe or socket",
standard:"posix"
},
{
name:"SIGALRM",
number:14,
action:"terminate",
description:"Timeout or timer",
standard:"posix"
},
{
name:"SIGTERM",
number:15,
action:"terminate",
description:"Termination",
standard:"ansi"
},
{
name:"SIGSTKFLT",
number:16,
action:"terminate",
description:"Stack is empty or overflowed",
standard:"other"
},
{
name:"SIGCHLD",
number:17,
action:"ignore",
description:"Child process terminated, paused or unpaused",
standard:"posix"
},
{
name:"SIGCLD",
number:17,
action:"ignore",
description:"Child process terminated, paused or unpaused",
standard:"other"
},
{
name:"SIGCONT",
number:18,
action:"unpause",
description:"Unpaused",
standard:"posix",
forced:true
},
{
name:"SIGSTOP",
number:19,
action:"pause",
description:"Paused",
standard:"posix",
forced:true
},
{
name:"SIGTSTP",
number:20,
action:"pause",
description:"Paused using CTRL-Z or \"suspend\"",
standard:"posix"
},
{
name:"SIGTTIN",
number:21,
action:"pause",
description:"Background process cannot read terminal input",
standard:"posix"
},
{
name:"SIGBREAK",
number:21,
action:"terminate",
description:"User interruption with CTRL-BREAK",
standard:"other"
},
{
name:"SIGTTOU",
number:22,
action:"pause",
description:"Background process cannot write to terminal output",
standard:"posix"
},
{
name:"SIGURG",
number:23,
action:"ignore",
description:"Socket received out-of-band data",
standard:"bsd"
},
{
name:"SIGXCPU",
number:24,
action:"core",
description:"Process timed out",
standard:"bsd"
},
{
name:"SIGXFSZ",
number:25,
action:"core",
description:"File too big",
standard:"bsd"
},
{
name:"SIGVTALRM",
number:26,
action:"terminate",
description:"Timeout or timer",
standard:"bsd"
},
{
name:"SIGPROF",
number:27,
action:"terminate",
description:"Timeout or timer",
standard:"bsd"
},
{
name:"SIGWINCH",
number:28,
action:"ignore",
description:"Terminal window size changed",
standard:"bsd"
},
{
name:"SIGIO",
number:29,
action:"terminate",
description:"I/O is available",
standard:"other"
},
{
name:"SIGPOLL",
number:29,
action:"terminate",
description:"Watched event",
standard:"other"
},
{
name:"SIGINFO",
number:29,
action:"ignore",
description:"Request for process information",
standard:"other"
},
{
name:"SIGPWR",
number:30,
action:"terminate",
description:"Device running out of power",
standard:"systemv"
},
{
name:"SIGSYS",
number:31,
action:"core",
description:"Invalid system call",
standard:"other"
},
{
name:"SIGUNUSED",
number:31,
action:"terminate",
description:"Invalid system call",
standard:"other"
}];

const getSignals=()=>{
const realtimeSignals=getRealtimeSignals();
const signals=[...SIGNALS,...realtimeSignals].map(normalizeSignal$1);
return signals
};







const normalizeSignal$1=({
name,
number:defaultNumber,
description,
action,
forced=false,
standard
})=>{
const{
signals:{[name]:constantSignal}
}=constants;
const supported=constantSignal!==undefined;
const number=supported?constantSignal:defaultNumber;
return {name,number,description,supported,action,forced,standard}
};

const getSignalsByName=()=>{
const signals=getSignals();
return Object.fromEntries(signals.map(getSignalByName))
};

const getSignalByName=({
name,
number,
description,
supported,
action,
forced,
standard
})=>[name,{name,number,description,supported,action,forced,standard}];

const signalsByName=getSignalsByName();




const getSignalsByNumber=()=>{
const signals=getSignals();
const length=SIGRTMAX+1;
const signalsA=Array.from({length},(value,number)=>
getSignalByNumber(number,signals)
);
return Object.assign({},...signalsA)
};

const getSignalByNumber=(number,signals)=>{
const signal=findSignalByNumber(number,signals);

if(signal===undefined){
return {}
}

const{name,description,supported,action,forced,standard}=signal;
return {
[number]:{
name,
number,
description,
supported,
action,
forced,
standard
}
}
};



const findSignalByNumber=(number,signals)=>{
const signal=signals.find(({name})=>constants.signals[name]===number);

if(signal!==undefined){
return signal
}

return signals.find((signalA)=>signalA.number===number)
};

getSignalsByNumber();

// Normalize signals for comparison purpose.
// Also validate the signal exists.
const normalizeKillSignal = killSignal => {
	const optionName = 'option `killSignal`';
	if (killSignal === 0) {
		throw new TypeError(`Invalid ${optionName}: 0 cannot be used.`);
	}

	return normalizeSignal(killSignal, optionName);
};

const normalizeSignalArgument = signal => signal === 0
	? signal
	: normalizeSignal(signal, '`subprocess.kill()`\'s argument');

const normalizeSignal = (signalNameOrInteger, optionName) => {
	if (Number.isInteger(signalNameOrInteger)) {
		return normalizeSignalInteger(signalNameOrInteger, optionName);
	}

	if (typeof signalNameOrInteger === 'string') {
		return normalizeSignalName(signalNameOrInteger, optionName);
	}

	throw new TypeError(`Invalid ${optionName} ${String(signalNameOrInteger)}: it must be a string or an integer.\n${getAvailableSignals()}`);
};

const normalizeSignalInteger = (signalInteger, optionName) => {
	if (signalsIntegerToName.has(signalInteger)) {
		return signalsIntegerToName.get(signalInteger);
	}

	throw new TypeError(`Invalid ${optionName} ${signalInteger}: this signal integer does not exist.\n${getAvailableSignals()}`);
};

const getSignalsIntegerToName = () => new Map(Object.entries(constants.signals)
	.reverse()
	.map(([signalName, signalInteger]) => [signalInteger, signalName]));

const signalsIntegerToName = getSignalsIntegerToName();

const normalizeSignalName = (signalName, optionName) => {
	if (signalName in constants.signals) {
		return signalName;
	}

	if (signalName.toUpperCase() in constants.signals) {
		throw new TypeError(`Invalid ${optionName} '${signalName}': please rename it to '${signalName.toUpperCase()}'.`);
	}

	throw new TypeError(`Invalid ${optionName} '${signalName}': this signal name does not exist.\n${getAvailableSignals()}`);
};

const getAvailableSignals = () => `Available signal names: ${getAvailableSignalNames()}.
Available signal numbers: ${getAvailableSignalIntegers()}.`;

const getAvailableSignalNames = () => Object.keys(constants.signals)
	.sort()
	.map(signalName => `'${signalName}'`)
	.join(', ');

const getAvailableSignalIntegers = () => [...new Set(Object.values(constants.signals)
	.sort((signalInteger, signalIntegerTwo) => signalInteger - signalIntegerTwo))]
	.join(', ');

// Human-friendly description of a signal
const getSignalDescription = signal => signalsByName[signal].description;

// Validate `timeout` option
const validateTimeout = ({timeout}) => {
	if (timeout !== undefined) {
		validateDelay(timeout, 'timeout');
	}
};

// Used by the `timeout` and `forceKillAfterDelay` options.
// Fractional numbers are excluded since `spawnSync()` requires an integer `timeout`.
const validateDelay = (delay, optionName) => {
	if (!Number.isInteger(delay) || delay < 0 || delay > MAX_DELAY) {
		throw new TypeError(`Expected the \`${optionName}\` option to be a non-negative integer up to ${MAX_DELAY}, got \`${delay}\` (${getTypeName(delay)})`);
	}
};

// Timers fire right away when their delay is larger than this
const MAX_DELAY = (2 ** 31) - 1;

// Fails when the `timeout` option is exceeded
const throwOnTimeout = (kill, timeout, context, controller) => timeout === 0 || timeout === undefined
	? []
	: [killAfterTimeout(kill, timeout, context, controller)];

const killAfterTimeout = async (kill, timeout, context, {signal}) => {
	await setTimeout(timeout, undefined, {signal});
	context.terminationReason ??= 'timeout';
	kill();
	throw new DiscardedError();
};

// Normalize the `forceKillAfterDelay` option
const normalizeForceKillAfterDelay = forceKillAfterDelay => {
	if (forceKillAfterDelay === false) {
		return forceKillAfterDelay;
	}

	if (forceKillAfterDelay === true) {
		return DEFAULT_FORCE_KILL_TIMEOUT;
	}

	validateDelay(forceKillAfterDelay, 'forceKillAfterDelay');
	return forceKillAfterDelay;
};

const DEFAULT_FORCE_KILL_TIMEOUT = 1000 * 5;

// Monkey-patches `subprocess.kill()` to add `forceKillAfterDelay` behavior and `.kill(error)`
const subprocessKill = (
	{kill, options: {forceKillAfterDelay, killSignal}, onInternalError, context, controller},
	signalOrError,
	errorArgument,
) => {
	const {signal, error} = parseKillArguments(signalOrError, errorArgument, killSignal);
	emitKillError(error, onInternalError);
	const killResult = kill(signal);
	setKillTimeout({
		kill,
		signal,
		forceKillAfterDelay,
		killSignal,
		killResult,
		context,
		controller,
	});
	return killResult;
};

const parseKillArguments = (signalOrError, errorArgument, killSignal) => {
	const [signal = killSignal, error] = isErrorInstance(signalOrError)
		? [undefined, signalOrError]
		: [signalOrError, errorArgument];

	if (typeof signal !== 'string' && !Number.isInteger(signal)) {
		throw new TypeError(`The first argument must be an error instance or a signal name string/integer: ${String(signal)}`);
	}

	if (error !== undefined && !isErrorInstance(error)) {
		throw new TypeError(`The second argument is optional. If specified, it must be an error instance: ${error}`);
	}

	return {signal: normalizeSignalArgument(signal), error};
};

// Fails right away when calling `subprocess.kill(error)`.
// Does not wait for actual signal termination.
// Uses a deferred promise instead of the `error` event on the subprocess, as this is less intrusive.
const emitKillError = (error, onInternalError) => {
	if (error !== undefined) {
		onInternalError.reject(error);
	}
};

const setKillTimeout = async ({kill, signal, forceKillAfterDelay, killSignal, killResult, context, controller}) => {
	if (signal === killSignal && killResult) {
		killOnTimeout({
			kill,
			forceKillAfterDelay,
			context,
			controllerSignal: controller.signal,
		});
	}
};

// Forcefully terminate a subprocess after a timeout
const killOnTimeout = async ({kill, forceKillAfterDelay, context, controllerSignal}) => {
	if (forceKillAfterDelay === false) {
		return;
	}

	try {
		await setTimeout(forceKillAfterDelay, undefined, {signal: controllerSignal});
		if (kill('SIGKILL')) {
			context.isForcefullyTerminated ??= true;
		}
	} catch {}
};

// Combines `util.aborted()` and `events.addAbortListener()`: promise-based and cleaned up with a stop signal
const onAbortedSignal = async (mainSignal, stopSignal) => {
	if (!mainSignal.aborted) {
		await once(mainSignal, 'abort', {signal: stopSignal});
	}
};

// Validate the `cancelSignal` and `unpipeSignal` options, so a wrong type fails right away instead of failing later with an unrelated error
const validateAbortSignal = (signal, optionName) => {
	if (signal !== undefined && Object.prototype.toString.call(signal) !== '[object AbortSignal]') {
		throw new TypeError(`The \`${optionName}\` option must be an AbortSignal: ${String(signal)}.`);
	}
};

// Validate the `cancelSignal` option
const validateCancelSignal = ({cancelSignal}) => {
	validateAbortSignal(cancelSignal, 'cancelSignal');
};

// The `signal` option was renamed to `cancelSignal`. Validated for every method, otherwise synchronous methods would silently ignore it, since `child_process.spawnSync()` does not support that option either.
const validateRenamedSignalOption = ({signal}) => {
	if (signal !== undefined) {
		throw new TypeError('The "signal" option has been renamed to "cancelSignal" instead.');
	}
};

// Terminate the subprocess when aborting the `cancelSignal` option and `gracefulSignal` is `false`
const throwOnCancel = ({kill, cancelSignal, gracefulCancel, context, controller}) => cancelSignal === undefined || gracefulCancel
	? []
	: [terminateOnCancel(kill, cancelSignal, context, controller)];

const terminateOnCancel = async (kill, cancelSignal, context, {signal}) => {
	await onAbortedSignal(cancelSignal, signal);
	context.terminationReason ??= 'cancel';
	kill();
	throw cancelSignal.reason;
};

// Validate the IPC channel is connected before receiving/sending messages
const validateIpcMethod = ({methodName, isSubprocess, ipc, isConnected}) => {
	validateIpcOption(methodName, isSubprocess, ipc);
	validateConnection(methodName, isSubprocess, isConnected);
};

// Better error message when forgetting to set `ipc: true` and using the IPC methods
const validateIpcOption = (methodName, isSubprocess, ipc) => {
	if (!ipc) {
		throw new Error(`${getMethodName(methodName, isSubprocess)} can only be used if the \`ipc\` option is \`true\`.`);
	}
};

// Better error message when one process does not send/receive messages once the other process has disconnected.
// This also makes it clear that any buffered messages are lost once either process has disconnected.
// Also when aborting `cancelSignal` after disconnecting the IPC.
const validateConnection = (methodName, isSubprocess, isConnected) => {
	if (!isConnected) {
		throw new Error(`${getMethodName(methodName, isSubprocess)} cannot be used: the ${getOtherProcessName(isSubprocess)} has already exited or disconnected.`);
	}
};

// When `getOneMessage()` could not complete due to an early disconnection
const throwOnEarlyDisconnect = isSubprocess => {
	throw new Error(`${getMethodName('getOneMessage', isSubprocess)} could not complete: the ${getOtherProcessName(isSubprocess)} exited or disconnected.`);
};

// When both processes use `sendMessage()` with `strict` at the same time
const throwOnStrictDeadlockError = isSubprocess => {
	throw new Error(`${getMethodName('sendMessage', isSubprocess)} failed: the ${getOtherProcessName(isSubprocess)} is sending a message too, instead of listening to incoming messages.
This can be fixed by both sending a message and listening to incoming messages at the same time:

const [receivedMessage] = await Promise.all([
	${getMethodName('getOneMessage', isSubprocess)},
	${getMethodName('sendMessage', isSubprocess, 'message, {strict: true}')},
]);`);
};

// When the other process used `strict` but the current process had I/O error calling `sendMessage()` for the response
const getStrictResponseError = (error, isSubprocess) => new Error(`${getMethodName('sendMessage', isSubprocess)} failed when sending an acknowledgment response to the ${getOtherProcessName(isSubprocess)}.`, {cause: error});

// When using `strict` but the other process was not listening for messages
const throwOnMissingStrict = isSubprocess => {
	throw new Error(`${getMethodName('sendMessage', isSubprocess)} failed: the ${getOtherProcessName(isSubprocess)} is not listening to incoming messages.`);
};

// When using `strict` but the other process disconnected before receiving the message
const throwOnStrictDisconnect = isSubprocess => {
	throw new Error(`${getMethodName('sendMessage', isSubprocess)} failed: the ${getOtherProcessName(isSubprocess)} exited without listening to incoming messages.`);
};

// When the current process disconnects while the subprocess is listening to `cancelSignal`
const getAbortDisconnectError = () => new Error(`\`cancelSignal\` aborted: the ${getOtherProcessName(true)} disconnected.`);

// When the subprocess uses `cancelSignal` but not the current process
const throwOnMissingParent = () => {
	throw new Error('`getCancelSignal()` cannot be used without setting the `gracefulCancel` option to `true`.');
};

// EPIPE can happen when sending a message to a subprocess that is closing but has not disconnected yet
const handleEpipeError = ({error, methodName, isSubprocess}) => {
	if (error.code === 'EPIPE') {
		throw new Error(`${getMethodName(methodName, isSubprocess)} cannot be used: the ${getOtherProcessName(isSubprocess)} is disconnecting.`, {cause: error});
	}
};

// Better error message when sending messages which cannot be serialized.
// Works with both `serialization: 'advanced'` and `serialization: 'json'`.
const handleSerializationError = ({error, methodName, isSubprocess, message}) => {
	if (isSerializationError(error)) {
		throw new Error(`${getMethodName(methodName, isSubprocess)}'s argument type is invalid: the message cannot be serialized: ${serializeInvalidMessage(message)}.`, {cause: error});
	}
};

// `String()` throws with objects that have a `null` prototype or a throwing `toString()` method
const serializeInvalidMessage = message => {
	try {
		return String(message);
	} catch {
		return inspect$1(message);
	}
};

const isSerializationError = ({code, message}) => SERIALIZATION_ERROR_CODES.has(code)
	|| SERIALIZATION_ERROR_MESSAGES.some(serializationErrorMessage => message.includes(serializationErrorMessage));

// `error.code` set by Node.js when it failed to serialize the message
const SERIALIZATION_ERROR_CODES = new Set([
	// Message is `undefined`
	'ERR_MISSING_ARGS',
	// Message is a function, a bigint, a symbol
	'ERR_INVALID_ARG_TYPE',
]);

// `error.message` set by Node.js when it failed to serialize the message
const SERIALIZATION_ERROR_MESSAGES = [
	// Message is a promise or a proxy, with `serialization: 'advanced'`
	'could not be cloned',
	// Message has cycles, with `serialization: 'json'`
	'circular structure',
	// Message has cycles inside toJSON(), with `serialization: 'json'`
	'call stack size exceeded',
];

const getMethodName = (methodName, isSubprocess, parameters = '') => methodName === 'cancelSignal'
	? '`cancelSignal`\'s `controller.abort()`'
	: `${getNamespaceName(isSubprocess)}${methodName}(${parameters})`;

const getNamespaceName = isSubprocess => isSubprocess ? '' : 'subprocess.';

const getOtherProcessName = isSubprocess => isSubprocess ? 'parent process' : 'subprocess';

// When any error arises, we disconnect the IPC.
// Otherwise, it is likely that one of the processes will stop sending/receiving messages.
// This would leave the other process hanging.
const disconnect = anyProcess => {
	if (anyProcess.connected) {
		anyProcess.disconnect();
	}
};

const createDeferred = () => {
	const methods = {};
	const promise = new Promise((resolve, reject) => {
		Object.assign(methods, {resolve, reject});
	});
	return Object.assign(promise, methods);
};

// Temporarily increase the maximum number of listeners on an eventEmitter
const incrementMaxListeners = (eventEmitter, maxListenersIncrement, signal) => {
	const maxListeners = eventEmitter.getMaxListeners();
	if (maxListeners === 0 || maxListeners === Infinity) {
		return;
	}

	eventEmitter.setMaxListeners(maxListeners + maxListenersIncrement);
	addAbortListener(signal, () => {
		eventEmitter.setMaxListeners(eventEmitter.getMaxListeners() - maxListenersIncrement);
	});
};

// By default, Node.js keeps the subprocess alive while it has a `message` or `disconnect` listener.
// We replicate the same logic for the events that we proxy.
// This ensures the subprocess is kept alive while `getOneMessage()` and `getEachMessage()` are ongoing.
// This is not a problem with `sendMessage()` since Node.js handles that method automatically.
// We do not use `anyProcess.channel.ref()` since this would prevent the automatic `.channel.refCounted()` Node.js is doing.
// We keep a reference to `anyProcess.channel` since it might be `null` while `getOneMessage()` or `getEachMessage()` is still processing debounced messages.
// See https://github.com/nodejs/node/blob/2aaeaa863c35befa2ebaa98fb7737ec84df4d8e9/lib/internal/child_process.js#L547
const addReference = (channel, reference) => {
	if (reference) {
		addReferenceCount(channel);
	}
};

const addReferenceCount = channel => {
	channel.refCounted();
};

const removeReference = (channel, reference) => {
	if (reference) {
		removeReferenceCount(channel);
	}
};

const removeReferenceCount = channel => {
	channel.unrefCounted();
};

// To proxy events, we setup some global listeners on the `message` and `disconnect` events.
// Those should not keep the subprocess alive, so we remove the automatic counting that Node.js is doing.
// See https://github.com/nodejs/node/blob/1b965270a9c273d4cf70e8808e9d28b9ada7844f/lib/child_process.js#L180
const undoAddedReferences = (channel, isSubprocess) => {
	if (!isSubprocess) {
		return;
	}

	removeReferenceCount(channel);
	removeReferenceCount(channel);
};

// Reverse it during `disconnect`
const redoAddedReferences = (channel, isSubprocess) => {
	if (!isSubprocess) {
		return;
	}

	addReferenceCount(channel);
	addReferenceCount(channel);
};

// By default, Node.js buffers `message` events.
//  - Buffering happens when there is a `message` event is emitted but there is no handler.
//  - As soon as a `message` event handler is set, all buffered `message` events are emitted, emptying the buffer.
//  - This happens both in the current process and the subprocess.
//  - See https://github.com/nodejs/node/blob/501546e8f37059cd577041e23941b640d0d4d406/lib/internal/child_process.js#L719
// This is helpful. Notably, this allows sending messages to a subprocess that's still initializing.
// However, it has several problems.
//  - This works with `events.on()` but not `events.once()` since all buffered messages are emitted at once.
//    For example, users cannot call `await getOneMessage()`/`getEachMessage()` multiple times in a row.
//  - When a user intentionally starts listening to `message` at a specific point in time, past `message` events are replayed, which might be unexpected.
//  - Buffering is unlimited, which might lead to an out-of-memory crash.
//  - This does not work well with multiple consumers.
//    For example, Execa consumes events with both `result.ipcOutput` and manual IPC calls like `getOneMessage()`.
//    Since `result.ipcOutput` reads all incoming messages, no buffering happens for manual IPC calls.
//  - Forgetting to setup a `message` listener, or setting it up too late, is a programming mistake.
//    The default behavior does not allow users to realize they made that mistake.
// To solve those problems, instead of buffering messages, we debounce them.
// The `message` event so it is emitted at most once per macrotask.
const onMessage = async ({anyProcess, channel, isSubprocess, ipcEmitter}, wrappedMessage) => {
	// `gracefulCancel` messages are only ever sent by the current process to its subprocess.
	// Only the subprocess must interpret them, otherwise a subprocess could abort its parent's own `cancelSignal`, or make it silently drop a message.
	if (handleStrictResponse(wrappedMessage, anyProcess) || (isSubprocess && handleAbort(wrappedMessage))) {
		return;
	}

	if (!INCOMING_MESSAGES.has(anyProcess)) {
		INCOMING_MESSAGES.set(anyProcess, []);
	}

	const incomingMessages = INCOMING_MESSAGES.get(anyProcess);
	incomingMessages.push(wrappedMessage);

	if (incomingMessages.length > 1) {
		return;
	}

	while (incomingMessages.length > 0) {
		// The message being processed is the oldest one, which is not `wrappedMessage` after the first iteration
		// eslint-disable-next-line no-await-in-loop
		await waitForOutgoingMessages(anyProcess, ipcEmitter, incomingMessages[0]);
		// eslint-disable-next-line no-await-in-loop
		await scheduler.yield();

		// eslint-disable-next-line no-await-in-loop
		const message = await handleStrictRequest({
			wrappedMessage: incomingMessages[0],
			anyProcess,
			channel,
			isSubprocess,
			ipcEmitter,
		});

		incomingMessages.shift();
		ipcEmitter.emit('message', message);
		ipcEmitter.emit('message:done');
	}
};

// If the `message` event is currently debounced, the `disconnect` event must wait for it
const onDisconnect = async ({anyProcess, channel, isSubprocess, ipcEmitter, boundOnMessage}) => {
	// Like `gracefulCancel` messages, only the disconnection from the parent process aborts `getCancelSignal()`.
	// Otherwise, disconnecting from one of the current process' own subprocesses would abort it.
	if (isSubprocess) {
		abortOnDisconnect();
	}

	const incomingMessages = INCOMING_MESSAGES.get(anyProcess);
	while (incomingMessages?.length > 0) {
		// eslint-disable-next-line no-await-in-loop
		await once(ipcEmitter, 'message:done');
	}

	anyProcess.removeListener('message', boundOnMessage);
	redoAddedReferences(channel, isSubprocess);
	ipcEmitter.connected = false;
	ipcEmitter.emit('disconnect');
};

const INCOMING_MESSAGES = new WeakMap();

// Forward the `message` and `disconnect` events from the process and subprocess to a proxy emitter.
// This prevents the `error` event from stopping IPC.
// This also allows debouncing the `message` event.
const getIpcEmitter = (anyProcess, channel, isSubprocess) => {
	if (IPC_EMITTERS.has(anyProcess)) {
		return IPC_EMITTERS.get(anyProcess);
	}

	// Use an `EventEmitter`, like the `process` that is being proxied
	// eslint-disable-next-line unicorn/prefer-event-target
	const ipcEmitter = new EventEmitter();
	// Each pending `getOneMessage()` or `getEachMessage()` call adds listeners, which are removed once it ends. Users cannot reach this emitter to increase its maximum number of listeners.
	ipcEmitter.setMaxListeners(0);
	ipcEmitter.connected = true;
	IPC_EMITTERS.set(anyProcess, ipcEmitter);
	forwardEvents({
		ipcEmitter,
		anyProcess,
		channel,
		isSubprocess,
	});
	return ipcEmitter;
};

const IPC_EMITTERS = new WeakMap();

// The `message` and `disconnect` events are buffered in the subprocess until the first listener is setup.
// However, unbuffering happens after one tick, so this give enough time for the caller to setup the listener on the proxy emitter first.
// See https://github.com/nodejs/node/blob/2aaeaa863c35befa2ebaa98fb7737ec84df4d8e9/lib/internal/child_process.js#L721
const forwardEvents = ({ipcEmitter, anyProcess, channel, isSubprocess}) => {
	const boundOnMessage = onMessage.bind(undefined, {
		anyProcess,
		channel,
		isSubprocess,
		ipcEmitter,
	});
	anyProcess.on('message', boundOnMessage);
	anyProcess.once('disconnect', onDisconnect.bind(undefined, {
		anyProcess,
		channel,
		isSubprocess,
		ipcEmitter,
		boundOnMessage,
	}));
	undoAddedReferences(channel, isSubprocess);
};

// Check whether there might still be some `message` events to receive
const isConnected = anyProcess => {
	const ipcEmitter = IPC_EMITTERS.get(anyProcess);
	return ipcEmitter === undefined
		? anyProcess.channel !== undefined && anyProcess.channel !== null
		: ipcEmitter.connected;
};

// When using the `strict` option, wrap the message with metadata during `sendMessage()`
const handleSendStrict = ({anyProcess, channel, isSubprocess, message, strict}) => {
	if (!strict) {
		return message;
	}

	const ipcEmitter = getIpcEmitter(anyProcess, channel, isSubprocess);
	const hasListeners = hasMessageListeners(anyProcess, ipcEmitter);
	return {
		id: count++,
		type: REQUEST_TYPE,
		message,
		hasListeners,
	};
};

// This is a plain number, not a `BigInt`, since it must be serializable with `serialization: 'json'`
let count = 0;

// Handles when both processes are calling `sendMessage()` with `strict` at the same time.
// If neither process is listening, this would create a deadlock. We detect it and throw.
const validateStrictDeadlock = (outgoingMessages, wrappedMessage) => {
	if (!isStrictRequest(wrappedMessage) || wrappedMessage.hasListeners) {
		return;
	}

	for (const {id} of outgoingMessages) {
		if (id !== undefined) {
			STRICT_RESPONSES[id].deferred.resolve({isDeadlock: true, hasListeners: false});
		}
	}
};

// The other process then sends the acknowledgment back as a response
const handleStrictRequest = async ({wrappedMessage, anyProcess, channel, isSubprocess, ipcEmitter}) => {
	if (!isStrictRequest(wrappedMessage)) {
		return wrappedMessage;
	}

	const {id, message} = wrappedMessage;
	// The acknowledgment cannot be sent anymore, but the message is still delivered
	if (!anyProcess.connected) {
		return message;
	}

	const response = {id, type: RESPONSE_TYPE, message: hasMessageListeners(anyProcess, ipcEmitter)};

	try {
		await sendMessage({
			anyProcess,
			channel,
			isSubprocess,
			ipc: true,
		}, response);
	} catch (error) {
		// When the other process disconnects right after sending the message, it cannot receive the acknowledgment anymore. Like above, the message is still delivered.
		if (error.cause?.code !== 'EPIPE') {
			ipcEmitter.emit('strict:error', error);
		}
	}

	return message;
};

// Reception of the acknowledgment response.
// Ids are shared by all processes, so the response must come from the process the message was sent to. Otherwise, a subprocess could acknowledge a message sent to another one.
const handleStrictResponse = (wrappedMessage, anyProcess) => {
	if (wrappedMessage?.type !== RESPONSE_TYPE) {
		return false;
	}

	const {id, message: hasListeners} = wrappedMessage;
	const strictResponse = STRICT_RESPONSES[id];
	if (strictResponse?.anyProcess === anyProcess) {
		strictResponse.deferred.resolve({isDeadlock: false, hasListeners});
	}

	return true;
};

// Wait for the other process to receive the message from `sendMessage()`
const waitForStrictResponse = async (wrappedMessage, anyProcess, isSubprocess) => {
	const deferred = createDeferred();
	STRICT_RESPONSES[wrappedMessage.id] = {deferred, anyProcess};
	const controller = new AbortController();

	try {
		const {isDeadlock, hasListeners} = await Promise.race([
			deferred,
			throwOnDisconnect$1(anyProcess, isSubprocess, controller),
		]);

		if (isDeadlock) {
			throwOnStrictDeadlockError(isSubprocess);
		}

		if (!hasListeners) {
			throwOnMissingStrict(isSubprocess);
		}
	} finally {
		controller.abort();
		delete STRICT_RESPONSES[wrappedMessage.id];
	}
};

const STRICT_RESPONSES = {__proto__: null};

const throwOnDisconnect$1 = async (anyProcess, isSubprocess, {signal}) => {
	incrementMaxListeners(anyProcess, 1, signal);
	await once(anyProcess, 'disconnect', {signal});
	throwOnStrictDisconnect(isSubprocess);
};

const REQUEST_TYPE = 'execa:ipc:request';
const RESPONSE_TYPE = 'execa:ipc:response';

// `id` and `hasListeners` are always defined in messages sent by `sendMessage()` with the `strict` option.
// Messages with the same `type` but without them are user messages, which must be delivered as is.
const isStrictRequest = wrappedMessage => wrappedMessage?.type === REQUEST_TYPE
	&& Number.isInteger(wrappedMessage.id)
	&& typeof wrappedMessage.hasListeners === 'boolean';

// When `sendMessage()` is ongoing, any `message` being received waits before being emitted.
// This allows calling one or multiple `await sendMessage()` followed by `await getOneMessage()`/`await getEachMessage()`.
// Without running into a race condition when the other process sends a response too fast, before the current process set up a listener.
const startSendMessage = (anyProcess, wrappedMessage, strict) => {
	if (!OUTGOING_MESSAGES.has(anyProcess)) {
		OUTGOING_MESSAGES.set(anyProcess, new Set());
	}

	const outgoingMessages = OUTGOING_MESSAGES.get(anyProcess);
	const onMessageSent = createDeferred();
	const id = strict ? wrappedMessage.id : undefined;
	const outgoingMessage = {onMessageSent, id};
	outgoingMessages.add(outgoingMessage);
	return {outgoingMessages, outgoingMessage};
};

const endSendMessage = ({outgoingMessages, outgoingMessage}) => {
	outgoingMessages.delete(outgoingMessage);
	outgoingMessage.onMessageSent.resolve();
};

// Await while `sendMessage()` is ongoing, unless there is already a `message` listener
const waitForOutgoingMessages = async (anyProcess, ipcEmitter, wrappedMessage) => {
	while (!hasMessageListeners(anyProcess, ipcEmitter) && OUTGOING_MESSAGES.get(anyProcess)?.size > 0) {
		const outgoingMessages = [...OUTGOING_MESSAGES.get(anyProcess)];
		validateStrictDeadlock(outgoingMessages, wrappedMessage);
		// eslint-disable-next-line no-await-in-loop
		await Promise.all(outgoingMessages.map(({onMessageSent}) => onMessageSent));
	}
};

const OUTGOING_MESSAGES = new WeakMap();
const IPC_SUBPROCESS_OPTIONS = new WeakMap();

const setIpcSubprocessOptions = (subprocess, options) => {
	IPC_SUBPROCESS_OPTIONS.set(subprocess, options);
};

// Whether any `message` listener is setup
const hasMessageListeners = (anyProcess, ipcEmitter) => ipcEmitter.listenerCount('message') > getMinListenerCount(anyProcess);

// When `buffer` is `false`, we set up a `message` listener that should be ignored.
// That listener is only meant to intercept `strict` acknowledgement responses.
const getMinListenerCount = anyProcess => getOptions(anyProcess) !== undefined
	&& !getFdSpecificValue(getOptions(anyProcess).buffer, 'ipc')
	? 1
	: 0;

const getOptions = anyProcess => IPC_SUBPROCESS_OPTIONS.get(anyProcess);

// Like `[sub]process.send()` but promise-based.
// We do not `await subprocess` during `.sendMessage()` nor `.getOneMessage()` since those methods are transient.
// Users would still need to `await subprocess` after the method is done.
// Also, this would prevent `unhandledRejection` event from being emitted, making it silent.
const sendMessage = ({anyProcess, channel, isSubprocess, ipc}, message, options) => {
	const {strict = false} = copyOptions(options);
	checkBooleanOption(strict, 'strict');
	const methodName = 'sendMessage';
	validateIpcMethod({
		methodName,
		isSubprocess,
		ipc,
		isConnected: anyProcess.connected,
	});

	return sendMessageAsync({
		anyProcess,
		channel,
		methodName,
		isSubprocess,
		message,
		strict,
	});
};

const sendMessageAsync = async ({anyProcess, channel, methodName, isSubprocess, message, strict}) => {
	const wrappedMessage = handleSendStrict({
		anyProcess,
		channel,
		isSubprocess,
		message,
		strict,
	});
	const outgoingMessagesState = startSendMessage(anyProcess, wrappedMessage, strict);
	try {
		await sendOneMessage({
			anyProcess,
			methodName,
			isSubprocess,
			wrappedMessage,
			message,
			strict,
		});
	} catch (error) {
		disconnect(anyProcess);
		throw error;
	} finally {
		endSendMessage(outgoingMessagesState);
	}
};

// Used internally by `cancelSignal`
const sendOneMessage = async ({anyProcess, methodName, isSubprocess, wrappedMessage, message, strict = false}) => {
	const sendMethod = getSendMethod(anyProcess);

	try {
		await Promise.all([
			// Only messages sent with the `strict` option wait for an acknowledgment response
			strict ? waitForStrictResponse(wrappedMessage, anyProcess, isSubprocess) : undefined,
			sendMethod(wrappedMessage),
		]);
	} catch (error) {
		handleEpipeError({error, methodName, isSubprocess});
		handleSerializationError({
			error,
			methodName,
			isSubprocess,
			message,
		});
		throw error;
	}
};

// [sub]process.send() promisified, memoized
const getSendMethod = anyProcess => {
	if (PROCESS_SEND_METHODS.has(anyProcess)) {
		return PROCESS_SEND_METHODS.get(anyProcess);
	}

	const sendMethod = promisify(anyProcess.send.bind(anyProcess));
	PROCESS_SEND_METHODS.set(anyProcess, sendMethod);
	return sendMethod;
};

const PROCESS_SEND_METHODS = new WeakMap();

// Send an IPC message so the subprocess performs a graceful termination
const sendAbort = (subprocess, message) => {
	const methodName = 'cancelSignal';
	validateConnection(methodName, false, subprocess.connected);
	return sendOneMessage({
		anyProcess: subprocess,
		methodName,
		isSubprocess: false,
		wrappedMessage: {type: GRACEFUL_CANCEL_TYPE, message},
		message,
	});
};

// When the signal is being used, start listening for incoming messages.
// Unbuffering messages takes one microtask to complete, so this must be async.
const getCancelSignal = async ({anyProcess, channel, isSubprocess, ipc}) => {
	await startIpc({
		anyProcess,
		channel,
		isSubprocess,
		ipc,
	});
	return cancelController.signal;
};

const startIpc = async ({anyProcess, channel, isSubprocess, ipc}) => {
	if (isCancelListening) {
		return;
	}

	// This is validated before the flag is set, so that a call which throws keeps failing on subsequent calls
	if (!ipc) {
		throwOnMissingParent();
		return;
	}

	isCancelListening = true;

	// `channel` is retrieved when Execa is imported, so the current process might have disconnected since then
	if (!anyProcess.connected) {
		abortOnDisconnect();
		return;
	}

	getIpcEmitter(anyProcess, channel, isSubprocess);
	await scheduler.yield();
};

let isCancelListening = false;

// Reception of IPC message to perform a graceful termination
const handleAbort = wrappedMessage => {
	if (wrappedMessage?.type !== GRACEFUL_CANCEL_TYPE) {
		return false;
	}

	cancelController.abort(wrappedMessage.message);
	return true;
};

const GRACEFUL_CANCEL_TYPE = 'execa:ipc:cancel';

// When the current process disconnects early, the subprocess `cancelSignal` is aborted.
// Otherwise, the signal would never be able to be aborted later on.
const abortOnDisconnect = () => {
	cancelController.abort(getAbortDisconnectError());
};

const cancelController = new AbortController();

// Validate the `gracefulCancel` option
const validateGracefulCancel = ({gracefulCancel, cancelSignal, ipc, serialization}) => {
	if (!gracefulCancel) {
		return;
	}

	if (cancelSignal === undefined) {
		throw new Error('The `cancelSignal` option must be defined when setting the `gracefulCancel` option.');
	}

	if (!ipc) {
		throw new Error('The `ipc` option cannot be false when setting the `gracefulCancel` option.');
	}

	if (serialization === 'json') {
		throw new Error('The `serialization` option cannot be \'json\' when setting the `gracefulCancel` option.');
	}
};

// Send abort reason to the subprocess when aborting the `cancelSignal` option and `gracefulCancel` is `true`
const throwOnGracefulCancel = ({
	subprocess,
	kill,
	cancelSignal,
	gracefulCancel,
	forceKillAfterDelay,
	context,
	controller,
}) => gracefulCancel
	? [sendOnAbort({
		subprocess,
		kill,
		cancelSignal,
		forceKillAfterDelay,
		context,
		controller,
	})]
	: [];

const sendOnAbort = async ({subprocess, kill, cancelSignal, forceKillAfterDelay, context, controller: {signal}}) => {
	await onAbortedSignal(cancelSignal, signal);
	const reason = getReason(cancelSignal);
	await sendAbort(subprocess, reason);
	killOnTimeout({
		kill,
		forceKillAfterDelay,
		context,
		controllerSignal: signal,
	});
	context.terminationReason ??= 'gracefulCancel';
	throw cancelSignal.reason;
};

// The default `reason` is a DOMException, which is not serializable with V8
// See https://github.com/nodejs/node/issues/53225
const getReason = ({reason}) => {
	if (!(reason instanceof DOMException)) {
		return reason;
	}

	const error = new Error(reason.message);
	Object.defineProperty(error, 'stack', {
		value: reason.stack,
		enumerable: false,
		configurable: true,
		writable: true,
	});
	return error;
};

// `execaNode()` is a shortcut for `execa(..., {node: true})`
const mapNode = ({options}) => {
	if (options.node === false) {
		throw new TypeError('The "node" option cannot be false with `execaNode()`.');
	}

	return {options: {__proto__: null, ...options, node: true}};
};

// The subprocess must not reuse the current process' `--inspect` port.
// It must not run the current process' `node --eval` or `node --print` code either, instead of its file. Like `child_process.fork()`, this prevents fork bombs.
const getDefaultNodeOptions = () => {
	const nodeOptions = [];
	for (let index = 0; index < execArgv.length; index += 1) {
		const nodeOption = execArgv[index];
		if (REMOVED_OPTIONS_WITH_VALUE.has(nodeOption)) {
			// `-p` and `--print` do not take a value when followed by another flag, e.g. `node -p -e code`. With the other options, Node.js fails when their value starts with `-`.
			if (!execArgv[index + 1]?.startsWith('-')) {
				index += 1;
			}
		} else if (!IGNORED_OPTIONS_REGEXP.test(nodeOption)) {
			nodeOptions.push(nodeOption);
		}
	}

	return nodeOptions;
};

// Those options are followed by a separate value, which is removed too
const REMOVED_OPTIONS_WITH_VALUE = new Set(['-e', '--eval', '-p', '--print', '-pe', '--input-type', '--inspect-port']);
const IGNORED_OPTIONS_REGEXP = /^--(?:inspect|eval=|input-type=)/;

// Applies the `node: true` option, and the related `nodePath`/`nodeOptions` options.
// Modifies the file commands/arguments to ensure the same Node binary and flags are re-used.
// Also adds `ipc: true` and `shell: false`.
const handleNodeOption = (file, commandArguments, {
	node: shouldHandleNode = false,
	nodePath = execPath,
	nodeOptions = getDefaultNodeOptions(),
	cwd,
	execPath: formerNodePath,
	...options
}) => {
	if (formerNodePath !== undefined) {
		throw new TypeError('The "execPath" option has been removed. Please use the "nodePath" option instead.');
	}

	if (!Array.isArray(nodeOptions) || nodeOptions.some(nodeOption => typeof nodeOption !== 'string')) {
		throw new TypeError('The "nodeOptions" option must be an array of strings.');
	}

	const normalizedNodePath = safeNormalizeFileUrl(nodePath, 'The "nodePath" option');
	const resolvedNodePath = path.resolve(cwd, normalizedNodePath);
	// Use spread (which only copies own properties) to safely get shell without reading polluted prototype
	const newOptions = {
		__proto__: null,
		shell: false,
		...options,
		nodePath: resolvedNodePath,
		node: shouldHandleNode,
		cwd,
	};

	if (!shouldHandleNode) {
		return [file, commandArguments, newOptions];
	}

	if (path.basename(file, '.exe') === 'node') {
		throw new TypeError('When the "node" option is true, the first argument does not need to be "node".');
	}

	return [
		resolvedNodePath,
		[
			...nodeOptions,
			file,
			...commandArguments,
		],
		{
			__proto__: null,
			...newOptions,
			// Like other options, `undefined` uses the default value
			ipc: newOptions.ipc === undefined ? true : newOptions.ipc,
			shell: false,
		},
	];
};

// Validate the `ipcInput` option
const validateIpcInputOption = ({ipcInput, ipc, serialization}) => {
	if (ipcInput === undefined) {
		return;
	}

	if (!ipc) {
		throw new Error('The `ipcInput` option cannot be set unless the `ipc` option is `true`.');
	}

	// An invalid `serialization` option is reported by `node:child_process` when spawning, like when `ipcInput` is not used
	validateIpcInput[serialization]?.(ipcInput);
};

const validateAdvancedInput = ipcInput => {
	try {
		serialize(ipcInput);
	} catch (error) {
		throw new Error('The `ipcInput` option is not serializable with a structured clone.', {cause: error});
	}
};

const validateJsonInput = ipcInput => {
	try {
		// `JSON.stringify()` returns `undefined` instead of throwing for values it cannot represent, such as functions and symbols
		if (JSON.stringify(ipcInput) === undefined) {
			throw new TypeError('The value cannot be represented as JSON.');
		}
	} catch (error) {
		throw new Error('The `ipcInput` option is not serializable with JSON.', {cause: error});
	}
};

const validateIpcInput = {
	__proto__: null,
	advanced: validateAdvancedInput,
	json: validateJsonInput,
};

// When the `ipcInput` option is set, it is sent as an initial IPC message to the subprocess
const sendIpcInput = async (subprocess, ipcInput, ipc) => {
	if (ipcInput === undefined) {
		return;
	}

	await sendMessage({
		anyProcess: subprocess,
		channel: subprocess.channel,
		isSubprocess: false,
		ipc,
	}, ipcInput);
};

// Validate `encoding` option
const validateEncoding = ({encoding}) => {
	if (ENCODINGS.has(encoding)) {
		return;
	}

	const correctEncoding = getCorrectEncoding(encoding);
	if (correctEncoding !== undefined) {
		throw new TypeError(`Invalid option \`encoding: ${serializeEncoding(encoding)}\`.
Please rename it to ${serializeEncoding(correctEncoding)}.`);
	}

	const correctEncodings = [...ENCODINGS].map(correctEncoding => serializeEncoding(correctEncoding)).join(', ');
	throw new TypeError(`Invalid option \`encoding: ${serializeEncoding(encoding)}\`.
Please rename it to one of: ${correctEncodings}.`);
};

const TEXT_ENCODINGS = new Set(['utf8', 'utf16le']);
const BINARY_ENCODINGS = new Set(['buffer', 'hex', 'base64', 'base64url', 'latin1', 'ascii']);
const ENCODINGS = TEXT_ENCODINGS.union(BINARY_ENCODINGS);

/*
Encoding to use when converting a transform's string to bytes.
Text encodings describe how text is represented as bytes, so those must be used, otherwise the string would be decoded with a different encoding than the one it was encoded with, which corrupts it.
Binary encodings instead describe how bytes are serialized as text, so the string's UTF-8 bytes are used. The same applies with the `binary` transform option, since the transform then deals with raw bytes.
*/
const getStringEncoding = (binary, encoding) => binary || !TEXT_ENCODINGS.has(encoding) ? 'utf8' : encoding;

// Text encodings only apply to the subprocess' output. Its input is always text encoded with UTF-8, so input transforms decode and encode strings with UTF-8 too, whichever text encoding the `encoding` option uses.
const getInputEncoding = encoding => TEXT_ENCODINGS.has(encoding) ? 'utf8' : encoding;

const getCorrectEncoding = encoding => {
	if (encoding === null) {
		return 'buffer';
	}

	if (typeof encoding !== 'string') {
		return;
	}

	const lowerEncoding = encoding.toLowerCase();
	if (lowerEncoding in ENCODING_ALIASES) {
		return ENCODING_ALIASES[lowerEncoding];
	}

	if (ENCODINGS.has(lowerEncoding)) {
		return lowerEncoding;
	}
};

const ENCODING_ALIASES = {
	__proto__: null,
	// eslint-disable-next-line unicorn/text-encoding-identifier-case
	'utf-8': 'utf8',
	'utf-16le': 'utf16le',
	'ucs-2': 'utf16le',
	ucs2: 'utf16le',
	binary: 'latin1',
};

const serializeEncoding = encoding => typeof encoding === 'string' ? `"${encoding}"` : String(encoding);

const isWindows$1 = process$2.platform === 'win32';

// A command containing a directory separator is treated as a path and looked up directly, instead of being searched for in `PATH`. Backslash only counts as a separator on Windows, since it's a valid filename character on other platforms.
const separatorPattern = isWindows$1 ? /[\/\\]/v : /\//v;

// The standard Windows executable extensions, used when `PATHEXT` is not set.
const defaultPathExt = '.COM;.EXE;.BAT;.CMD;.VBS;.VBE;.JS;.JSE;.WSF;.WSH;.MSC';

function resolveOptions(command, options) {
	if (typeof command !== 'string' || command.length === 0) {
		throw new TypeError('Expected a non-empty string.');
	}

	const {
		cwd = process$2.cwd(),
		// On Windows, `process.env.PATH` can be undefined in some contexts (for example, worker threads), where it's only exposed as `Path`.
		path: searchPath = process$2.env.PATH ?? (isWindows$1 ? process$2.env.Path : undefined) ?? '',
		// `||` (not `??`) so an empty `PATHEXT` falls back to the default instead of disabling all lookup on Windows.
		pathExt = process$2.env.PATHEXT || defaultPathExt,
	} = options;

	return {cwd, searchPath, pathExt};
}

function windowsExtensions(command, pathExt) {
	const extensions = pathExt.split(path.delimiter).filter(Boolean);
	const commandExtension = path.extname(command).toLowerCase();

	// If the command already ends with a known executable extension, also try it verbatim.
	if (commandExtension !== '' && extensions.some(extension => extension.toLowerCase() === commandExtension)) {
		return ['', ...extensions];
	}

	return extensions;
}

function * candidatePaths(command, {cwd, searchPath, pathExt}) {
	const extensions = isWindows$1 ? windowsExtensions(command, pathExt) : [''];

	if (separatorPattern.test(command)) {
		const base = path.resolve(cwd, command);
		for (const extension of extensions) {
			yield base + extension;
		}

		return;
	}

	const directories = [
		// Windows searches the current directory before `PATH`.
		...(isWindows$1 ? [cwd] : []),
		...searchPath.split(path.delimiter),
	];

	for (const directory of directories) {
		// `PATH` entries can be wrapped in double quotes on Windows.
		const unquoted = isWindows$1 && directory.length > 1 && directory.startsWith('"') && directory.endsWith('"') ? directory.slice(1, -1) : directory;

		// Skip empty entries. Unlike a POSIX shell, an empty entry is not treated as the current directory, since implicitly searching the current directory is a security risk. Checked after unquoting so a quoted-empty entry (`""`) is skipped too.
		if (unquoted === '') {
			continue;
		}

		const base = path.resolve(cwd, unquoted, command);
		for (const extension of extensions) {
			yield base + extension;
		}
	}
}

function isExecutableSync(filePath) {
	let stats;
	try {
		stats = fs.statSync(filePath);
	} catch (error) {
		return isWindows$1 && error.code === 'EACCES';
	}

	if (!stats.isFile()) {
		return false;
	}

	if (isWindows$1) {
		return true;
	}

	try {
		fs.accessSync(filePath, fs.constants.X_OK);
		return true;
	} catch {
		return false;
	}
}

function whichCommandSync(command, options = {}) {
	const resolved = resolveOptions(command, options);

	for (const candidate of candidatePaths(command, resolved)) {
		if (isExecutableSync(candidate)) {
			return candidate;
		}
	}

	return undefined;
}

/*
On Windows, `node:child_process` cannot natively run many kinds of files (`.cmd`, `.bat`, shebang scripts, ...): without a shell it resolves neither `PATHEXT` nor shebangs, and it does not escape arguments. We resolve the command to its full file path and escape its arguments ourselves, so those work without an explicit shell, just like on Unix, where the OS handles all of this for us.
*/
const parseCommandFile = (file, commandArguments, options) => {
	// The arguments are cloned since a shebang interpreter might be prepended below, which must not mutate the caller's array
	const parsed = {file, commandArguments: [...commandArguments], options};

	// Under a shell, or on Unix, the OS resolves the file and escapes the arguments itself
	return options.shell || process$2.platform !== 'win32' ? parsed : escapeWindowsCommand(parsed);
};

// Only `.exe` and `.com` files can be spawned directly; anything else needs `cmd.exe`
const directlyExecutableRegExp = /\.(?:com|exe)$/i;

const escapeWindowsCommand = parsed => {
	// Resolve the file to an absolute path, following its shebang to the interpreter if any
	const resolvedFile = resolveWithShebang(parsed);

	// A directly executable file is spawned by its resolved path, bypassing `cmd.exe` and its escaping
	if (resolvedFile !== undefined && directlyExecutableRegExp.test(resolvedFile)) {
		if (parsed.options.argv0 === undefined) {
			parsed.options.argv0 = parsed.file;
		}

		parsed.file = resolvedFile;
		return parsed;
	}

	/*
	`cmd.exe` treats CR and LF as command separators and offers no way to escape them, so allowing either would enable command injection.
	Reject them instead.
	*/
	for (const value of [parsed.file, ...parsed.commandArguments]) {
		assertNoLineBreak(value);
	}

	/*
	POSIX separators must become Windows ones (`foo/bar` -> `foo\bar`), otherwise resolution always fails with ENOENT.

	Unlike Rust, which quotes the batch file's path, the file is caret-escaped. `cmd.exe` only interprets it once, and the file might not be resolved, for example when it is a `cmd.exe` builtin, which would not run if quoted.
	*/
	const escapedFile = escapeMetaChars(path.normalize(resolvedFile ?? parsed.file));
	const escapedArguments = parsed.commandArguments.map(argument => escapeArgument(argument));
	const commandLine = `"${[escapedFile, ...escapedArguments].join(' ')}"`;

	// Let `node:child_process` pass the already-escaped command line through untouched
	parsed.options.windowsVerbatimArguments = true;
	return {
		file: process$2.env.comspec || 'cmd.exe',
		// `/e:on` enables the command extensions the `%` escaping relies on, and `/v:off` disables delayed expansion so `!` is literal, even when the registry changes either default
		commandArguments: ['/d', '/e:on', '/v:off', '/s', '/c', commandLine],
		options: parsed.options,
	};
};

// Resolve the command's absolute path, then, if it is a shebang script, resolve its interpreter instead, since Windows cannot run shebangs natively
const resolveWithShebang = parsed => {
	const resolvedFile = resolvePath(parsed);
	const interpreter = resolvedFile !== undefined && readShebang(resolvedFile);
	if (!interpreter) {
		return resolvedFile;
	}

	// Run the interpreter with the script as its first argument, then resolve the interpreter's own path
	parsed.commandArguments.unshift(resolvedFile);
	parsed.file = interpreter;
	return resolvePath(parsed);
};

// Search `PATH` for the command, resolving its Windows executable extension via `PATHEXT`
const resolvePath = parsed => {
	const environment = parsed.options.env || process$2.env;
	const cwd = parsed.options.cwd ?? process$2.cwd();
	const environmentPathExt = getWindowsEnvironmentValue(environment, 'PATHEXT');
	const commandExtension = path.extname(parsed.file);
	// Commands with an explicit extension must be tried verbatim so shebang scripts work even when their extension is excluded from PATHEXT.
	const pathExt = commandExtension === '' ? environmentPathExt : `${commandExtension}${path.delimiter}${environmentPathExt ?? ''}`;
	if (hasWindowsPathSeparator(parsed.file)) {
		return whichCommandSync(path.resolve(cwd, parsed.file), {cwd, pathExt});
	}

	const searchPath = getWindowsEnvironmentValue(environment, 'PATH') ?? getWindowsEnvironmentValue(process$2.env, 'PATH') ?? '';
	const resolveOptions = {
		cwd,
		path: searchPath,
		pathExt,
	};

	return shouldSearchCurrentDirectory(environment) ? whichCommandSync(parsed.file, resolveOptions) : resolvePathDirectories(parsed.file, resolveOptions);
};

const hasWindowsPathSeparator = file => file.includes('/') || file.includes('\\') || file.includes(':');

const shouldSearchCurrentDirectory = environment => getWindowsEnvironmentValue(process$2.env, 'NODEFAULTCURRENTDIRECTORYINEXEPATH') === undefined && getWindowsEnvironmentValue(environment, 'NODEFAULTCURRENTDIRECTORYINEXEPATH') === undefined;

const resolvePathDirectories = (file, {cwd, path: searchPath, pathExt}) => {
	for (const directory of searchPath.split(path.delimiter)) {
		const unquotedDirectory = directory.length > 1 && directory.startsWith('"') && directory.endsWith('"') ? directory.slice(1, -1) : directory;
		if (unquotedDirectory === '') {
			continue;
		}

		const resolvedFile = whichCommandSync(path.resolve(cwd, unquotedDirectory, file), {cwd, pathExt});
		if (resolvedFile !== undefined) {
			return resolvedFile;
		}
	}
};

// Node sorts Windows environment keys and uses the first case-insensitive match when spawning.
const getWindowsEnvironmentValue = (environment, name) => {
	const environmentKey = Object.keys(environment).sort().find(key => key.toUpperCase() === name);
	return environmentKey === undefined ? undefined : environment[environmentKey];
};

const SHEBANG_BYTE_LENGTH = 150;

// Read the file's first bytes to find its shebang interpreter, if it has one
const readShebang = file => {
	const buffer = Buffer.alloc(SHEBANG_BYTE_LENGTH);
	let bytesRead = 0;

	try {
		const fileDescriptor = openSync(file, 'r');
		try {
			bytesRead = readSync(fileDescriptor, buffer, 0, SHEBANG_BYTE_LENGTH, 0);
		} finally {
			closeSync(fileDescriptor);
		}
	} catch {
		return undefined;
	}

	// A file shorter than the buffer leaves it zero-filled, so only the bytes actually read must be parsed.
	// Otherwise, a shebang line which is not terminated by a newline would include those `\0` characters.
	return parseShebang(buffer.toString('utf8', 0, bytesRead));
};

const shebangRegExp = /^#!(?<line>.*)/;

/*
Extract the interpreter from a shebang line, e.g. `#!/usr/bin/env node` -> `node`.
*/
const parseShebang = contents => {
	const shebangLine = contents.match(shebangRegExp)?.groups.line.trim();
	if (!shebangLine) {
		return undefined;
	}

	const [interpreterPath, argument] = shebangLine.split(' ', 2);
	const interpreter = interpreterPath.split('/').at(-1);
	if (interpreter === 'env') {
		return argument;
	}

	return argument ? `${interpreter} ${argument}` : interpreter;
};

const lineBreakRegExp = /[\n\r]/;

const assertNoLineBreak = value => {
	if (lineBreakRegExp.test(value)) {
		throw new TypeError(`The command and its arguments cannot contain a line break on Windows without a shell.\nThis would allow a command injection with \`cmd.exe\`.\nInvalid value: ${JSON.stringify(`${value}`)}`);
	}
};

// See https://web.archive.org/web/20241220221102/https://www.robvanderwoude.com/escapechars.php
// eslint-disable-next-line regexp/sort-character-class-elements
const metaCharsRegExp = /[()\][%!^"`<>&|;, *?]/g;

// Prefix every `cmd.exe` metacharacter with a caret to neutralize it. This is only used for the file, since arguments are quoted instead.
const escapeMetaChars = value => value.replaceAll(metaCharsRegExp, '^$&');

const backslashRunRegExp = /\\+/g;

// Matches any character requiring quoting: anything except ASCII letters, digits, a few punctuation characters that `cmd.exe` never interprets, and non-ASCII characters other than control characters
const quotedCharacterRegExp = /[^\w#$*+\-./:?@\\\u{80}-\u{10FFFF}]|\p{Control}/u;

/*
Escape an argument the same way as Rust's `std::process::Command` does for batch files, which fixed the "BatBadBut" vulnerability (CVE-2024-24576): https://github.com/rust-lang/rust/blob/main/library/std/src/sys/args/windows.rs (`append_bat_arg()`). yt-dlp uses the same algorithm.

Any argument containing a `cmd.exe` metacharacter is wrapped in double quotes, which keeps `cmd.exe` from interpreting it. `cmd.exe` might interpret it more than once: when invoking a batch file, then again when that batch file forwards it with `%*`, like npm cmd-shims do. Caret escaping depends on that count, so it breaks batch files reading their arguments directly with `%~1`, like Maven's `mvn.cmd`. Quoting does not, as long as the quotes stay balanced.

Rust only uses this for batch files. We use it for any file run through `cmd.exe`, since it is also correct when `cmd.exe` interprets the argument only once.

Limitations, which no escaping can fix since they depend on how the batch file uses its arguments:
- A batch file reading `%~1` gets the escaped argument without its surrounding quotes: double quotes are doubled, and trailing backslashes too.
- A batch file using `%~1` outside double quotes, or `%1` inside double quotes (like `if "%1" == ""`), lets `cmd.exe` interpret its metacharacters. Since `%1` includes the argument's surrounding quotes, the extra ones unquote it.
- A batch file forwarding its arguments with `call` expands `%` again, and one enabling delayed expansion expands `!` again.
*/
const escapeArgument = rawArgument => {
	/*
	Escape backslashes, following the algorithm at https://web.archive.org/web/20240930203505/https://qntm.org/cmd.

	A run of backslashes only needs doubling when it precedes a double quote, or the end of the argument since that becomes a double quote once the argument is wrapped below. Otherwise the backslashes would be taken as escaping that quote. Every double quote is then escaped in turn.

	Each backslash run is matched exactly once and consumed, so a long run cannot trigger the quadratic backtracking a naive pattern would, which would be a denial-of-service risk.

	Double quotes are escaped as `""` instead of `\"`. Both are a literal double quote inside a quoted argument for the subprocess, but only `""` keeps the quotes balanced for `cmd.exe`, which does not know about backslashes. Otherwise, the metacharacters after the double quote would not be quoted anymore, allowing command injection.

	`%` cannot be escaped inside double quotes, so it is replaced by `%%cd:~,%` instead. That is a literal `%` followed by an empty substring of the `%cd%` variable, which `cmd.exe` expands to nothing. This prevents `%VAR%` from being expanded.
	*/
	const argument = `${rawArgument}`;
	if (!shouldQuoteArgument(argument)) {
		return argument;
	}

	const escapedArgument = argument
		.replaceAll(backslashRunRegExp, (backslashes, offset, string) => {
			const nextCharacter = string[offset + backslashes.length];
			const isPrecedesDoubleQuote = nextCharacter === '"' || nextCharacter === undefined;
			return isPrecedesDoubleQuote ? backslashes.repeat(2) : backslashes;
		})
		.replaceAll('"', '""')
		.replaceAll('%', '%%cd:~,%');

	return `"${escapedArgument}"`;
};

/*
An empty argument must be quoted, otherwise it would be dropped.
An argument ending with a backslash must be quoted, otherwise a batch file wrapping it in double quotes, like `"%~1"`, would have that backslash escape its closing double quote.
Since `"` and `%` require quoting, an unquoted argument never needs any escaping.
*/
const shouldQuoteArgument = argument => argument === ''
	|| argument.endsWith('\\')
	|| quotedCharacterRegExp.test(argument);

// Normalize `cwd` option
const normalizeCwd = (cwd = getDefaultCwd()) => {
	const cwdString = safeNormalizeFileUrl(cwd, 'The "cwd" option');
	return path.resolve(cwdString);
};

const getDefaultCwd = () => {
	try {
		return process$2.cwd();
	} catch (error) {
		error.message = `The current directory does not exist.\n${error.message}`;
		throw error;
	}
};

// `process.cwd()` throws if the current directory has been deleted.
// This is only used to improve an error message, so it must not throw itself, which would hide that error.
const getCurrentCwd = () => {
	try {
		return process$2.cwd();
	} catch {}
};

// When `cwd` option has an invalid value, provide with a better error message
const fixCwdError = (originalMessage, cwd) => {
	if (cwd === getCurrentCwd()) {
		return originalMessage;
	}

	let cwdStat;
	try {
		cwdStat = statSync(cwd);
	} catch (error) {
		return `The "cwd" option is invalid: ${cwd}.\n${error.message}\n${originalMessage}`;
	}

	return cwdStat.isDirectory() ? originalMessage : `The "cwd" option is not a directory: ${cwd}.\n${originalMessage}`;
};

const cmdExeRegExp = /^cmd(?:\.exe)?$/i;

// Normalize the options object, and sometimes also the file paths and arguments.
// Applies default values, validate allowed options, normalize them.
const normalizeOptions = (filePath, rawArguments, rawOptions) => {
	// Prevent prototype pollution by copying only own properties to a null-prototype object
	const sanitizedOptions = {__proto__: null, ...rawOptions};
	sanitizedOptions.cwd = normalizeCwd(sanitizedOptions.cwd);
	const [processedFile, processedArguments, processedOptions] = handleNodeOption(filePath, rawArguments, sanitizedOptions);
	const fdOptions = normalizeFdSpecificOptions(processedOptions);
	const options = addDefaultOptions(fdOptions);
	validateBooleanOptions(options);
	validateEnv(options);
	options.env = getEnv(options);

	const {file, commandArguments} = parseCommandFile(processedFile, processedArguments, options);

	validateTimeout(options);
	validateEncoding(options);
	validateIpcInputOption(options);
	validateCancelSignal(options);
	validateRenamedSignalOption(options);
	validateGracefulCancel(options);
	options.shell = normalizeFileUrl(options.shell);
	options.killSignal = normalizeKillSignal(options.killSignal);
	options.forceKillAfterDelay = normalizeForceKillAfterDelay(options.forceKillAfterDelay);
	options.lines = options.lines.map((lines, fdNumber) => lines && !BINARY_ENCODINGS.has(options.encoding) && options.buffer[fdNumber]);

	// The file is now an absolute path resolved via `PATHEXT`, so its extension might be uppercase (`cmd.EXE`)
	if (process$2.platform === 'win32' && cmdExeRegExp.test(path.basename(file))) {
		// #116
		commandArguments.unshift('/q');
	}

	return {file, commandArguments, options};
};

const BOOLEAN_OPTIONS = [
	'extendEnv',
	'preferLocal',
	'reject',
	'cleanup',
	'killDescendants',
	'all',
	'gracefulCancel',
	'ipc',
	'node',
];

// Wrong types must be reported instead of being silently coerced, e.g. the string 'false' enabling an option
const validateBooleanOptions = options => {
	for (const optionName of BOOLEAN_OPTIONS) {
		checkBooleanOption(options[optionName], optionName);
	}
};

// A non-object `env` would be spread into unrelated properties, e.g. its characters when a string
const validateEnv = ({env}) => {
	if (env !== undefined && (typeof env !== 'object' || env === null || Array.isArray(env))) {
		throw new TypeError(`The \`env\` option must be an object: ${String(env)}.`);
	}
};

// Use null prototype to prevent prototype pollution from leaking through
const addDefaultOptions = ({
	extendEnv = true,
	preferLocal = false,
	cwd,
	localDir: localDirectory = cwd,
	encoding = 'utf8',
	reject = true,
	cleanup = true,
	killDescendants = false,
	all = false,
	windowsHide = true,
	killSignal = 'SIGTERM',
	forceKillAfterDelay = true,
	gracefulCancel = false,
	ipcInput,
	ipc = ipcInput !== undefined || gracefulCancel,
	serialization = 'advanced',
	...options
}) => ({
	__proto__: null,
	...options,
	extendEnv,
	preferLocal,
	cwd,
	localDirectory,
	encoding,
	reject,
	cleanup,
	killDescendants,
	all,
	windowsHide,
	killSignal,
	forceKillAfterDelay,
	gracefulCancel,
	ipcInput,
	ipc,
	serialization,
});

/*
Keep a null prototype, otherwise a polluted `Object.prototype` would inject environment variables.
Unlike the other options, `child_process` intentionally passes the inherited properties of `env` to the subprocess, so they are the ones which must not leak. This includes `PATH`, which decides which file the command resolves to.
`npmRunPathEnv()` returns a plain object, so this is applied to its result too.
*/
const getEnv = options => ({__proto__: null, ...computeEnv(options)});

const computeEnv = ({env: envOption, extendEnv, preferLocal, node, localDirectory, nodePath}) => {
	// Spread `envOption` even when it is `undefined`, so `extendEnv: false` results in an empty environment instead of inheriting `process.env`
	const env = {__proto__: null, ...(extendEnv && process$2.env), ...envOption};

	if (preferLocal || node) {
		return npmRunPathEnv({
			env,
			cwd: localDirectory,
			execPath: nodePath,
			preferLocal,
			addExecPath: node,
		});
	}

	return env;
};

// When the `shell` option is set, any command argument is concatenated as a single string by Node.js:
// https://github.com/nodejs/node/blob/e38ce27f3ca0a65f68a31cedd984cddb927d4002/lib/child_process.js#L614-L624
// However, since Node 24, it also prints a deprecation warning.
// To avoid this warning, we perform that same operation before calling `node:child_process`.
// Shells only understand strings, which is why Node.js performs that concatenation.
// However, we rely on users splitting command arguments as an array.
// For example, this allows us to easily detect which arguments are passed.
// So we do want users to pass array of arguments even with `shell: true`, but we also want to avoid any warning.
const concatenateShell = (file, commandArguments, options) => options.shell && commandArguments.length > 0
	? [[file, ...commandArguments].join(' '), [], options]
	: [file, commandArguments, options];

// Split chunks line-wise for generators passed to the `std*` options
const getSplitLinesGenerator = (binary, preserveNewlines, skipped, state) => binary || skipped
	? undefined
	: initializeSplitLines(preserveNewlines, state);

// Same but for synchronous methods
const splitLinesSync = (chunk, preserveNewlines, objectMode) => objectMode
	? chunk.flatMap(item => splitLinesItemSync(item, preserveNewlines))
	: splitLinesItemSync(chunk, preserveNewlines);

const splitLinesItemSync = (chunk, preserveNewlines) => {
	const {transform, final} = initializeSplitLines(preserveNewlines, {});
	return [...transform(chunk), ...final()];
};

const initializeSplitLines = (preserveNewlines, state) => {
	state.previousChunks = '';
	return {
		transform: splitGenerator.bind(undefined, state, preserveNewlines),
		final: linesFinal.bind(undefined, state),
	};
};

// This imperative logic is much faster than using `String.split()` and uses very low memory.
const splitGenerator = function * (state, preserveNewlines, chunk) {
	if (typeof chunk !== 'string') {
		yield chunk;
		return;
	}

	let {previousChunks} = state;
	let start = -1;

	for (let end = 0; end < chunk.length; end += 1) {
		if (chunk[end] !== '\n') {
			continue;
		}

		// The `\r` of a `\r\n` sequence is stripped further below, once `previousChunks` has been prepended.
		// Otherwise, a `\r\n` sequence split across two chunks would not be detected.
		let line = chunk.slice(start + 1, preserveNewlines ? end + 1 : end);

		if (previousChunks.length > 0) {
			line = concatString(previousChunks, line);
			previousChunks = '';
		}

		yield preserveNewlines ? line : stripCarriageReturn(line, state);
		start = end;
	}

	if (start !== chunk.length - 1) {
		previousChunks = concatString(previousChunks, chunk.slice(start + 1));
	}

	state.previousChunks = previousChunks;
};

const stripCarriageReturn = (line, state) => {
	state.isWindowsNewline = line.endsWith('\r');
	return state.isWindowsNewline ? line.slice(0, -1) : line;
};

const linesFinal = function * ({previousChunks}) {
	if (previousChunks.length > 0) {
		yield previousChunks;
	}
};

// Unless `preserveNewlines: true` is used, we strip the newline of each line.
// This re-adds them after the user `transform` code has run.
// The re-added newline must be encoded with the `encoding` option when the chunk is bytes: a UTF-8 newline would misalign `utf16le` bytes.
const getAppendNewlineGenerator = ({binary, preserveNewlines, readableObjectMode, state, encoding}) => binary || preserveNewlines || readableObjectMode
	? undefined
	: {transform: appendNewlineGenerator.bind(undefined, state, getStringEncoding(binary, encoding))};

const appendNewlineGenerator = function * ({isWindowsNewline = false}, stringEncoding, chunk) {
	const {unixNewline, windowsNewline, concat} = typeof chunk === 'string'
		? linesStringInfo
		: LINES_BYTES_INFO[stringEncoding];

	if (endsWithNewline(chunk, unixNewline)) {
		yield chunk;
		return;
	}

	const newline = isWindowsNewline ? windowsNewline : unixNewline;
	yield concat(chunk, newline);
};

const endsWithNewline = (chunk, unixNewline) => typeof chunk === 'string'
	? chunk.endsWith(unixNewline)
	: chunk.length >= unixNewline.length
		&& chunk.subarray(-unixNewline.length).every((byte, index) => byte === unixNewline[index]);

const concatString = (firstChunk, secondChunk) => `${firstChunk}${secondChunk}`;

const linesStringInfo = {
	windowsNewline: '\r\n',
	unixNewline: '\n',
	concat: concatString,
};

const concatUint8Array = (firstChunk, secondChunk) => {
	const chunk = new Uint8Array(firstChunk.length + secondChunk.length);
	chunk.set(firstChunk, 0);
	chunk.set(secondChunk, firstChunk.length);
	return chunk;
};

// Only indexed with text encodings, since the `binary` option and binary encodings disable this generator
const LINES_BYTES_INFO = {
	__proto__: null,
	utf8: {
		windowsNewline: new Uint8Array([0x0D, 0x0A]),
		unixNewline: new Uint8Array([0x0A]),
		concat: concatUint8Array,
	},
	utf16le: {
		windowsNewline: new Uint8Array([0x0D, 0x00, 0x0A, 0x00]),
		unixNewline: new Uint8Array([0x0A, 0x00]),
		concat: concatUint8Array,
	},
};

function stripFinalNewline(input) {
	if (typeof input === 'string') {
		return stripFinalNewlineString(input);
	}

	if (!(ArrayBuffer.isView(input) && input.BYTES_PER_ELEMENT === 1)) {
		throw new Error('Input must be a string or a Uint8Array');
	}

	return stripFinalNewlineBinary(input);
}

const stripFinalNewlineString = input =>
	input.at(-1) === LF
		? input.slice(0, input.at(-2) === CR ? -2 : -1)
		: input;

const stripFinalNewlineBinary = input =>
	input.at(-1) === LF_BINARY
		? input.subarray(0, input.at(-2) === CR_BINARY ? -2 : -1)
		: input;

const LF = '\n';
const LF_BINARY = LF.codePointAt(0);
const CR = '\r';
const CR_BINARY = CR.codePointAt(0);

// Apply `stripFinalNewline` option, which applies to `result.stdout|stderr|all|stdio[*]`.
// If the `lines` option is used, it is applied on each line, but using a different function.
const stripNewline = (value, {stripFinalNewline: stripFinalNewline$1}, fdNumber, allFds) => value !== undefined && !Array.isArray(value) && getStripFinalNewline(stripFinalNewline$1, fdNumber, allFds)
	? stripFinalNewline(value)
	: value;

// Retrieve `stripFinalNewline` option value, including with `subprocess.all`
// With `subprocess.all`, `allFds` are the file descriptors it actually interleaves. Only those are considered, otherwise a single one would be stripped because another one defaults to being stripped.
const getStripFinalNewline = (stripFinalNewline, fdNumber, allFds) => fdNumber === 'all'
	? allFds.some(allFd => stripFinalNewline[allFd])
	: stripFinalNewline[fdNumber];

// The file descriptors which `result.all` interleaves. Those are the ones with an output, like with the `all` stream.
const getAllSyncFds = ([, stdout, stderr]) => [
	...(stdout === undefined ? [] : [1]),
	...(stderr === undefined ? [] : [2]),
];

// Retrieve `result.all` with synchronous methods
const getAllSync = ([, stdout, stderr], allFds, options) => {
	if (!options.all) {
		return;
	}

	if (stdout === undefined) {
		return splitAllLines(stderr, allFds, options);
	}

	if (stderr === undefined) {
		return splitAllLines(stdout, allFds, options);
	}

	// Like asynchronous methods, `all` is split into lines as soon as the `lines` option applies to either file descriptor.
	// A file descriptor which uses the `lines` option is already split with its own `stripFinalNewline` value, like `result.stdout` and `result.stderr` are. The other one is split with the `stripFinalNewline` value of `result.all`.
	// This differs from asynchronous methods, which cannot tell which file descriptor the merged output's last newline belongs to.
	if (hasLines(options)) {
		return [splitAllLines(stdout, allFds, options), splitAllLines(stderr, allFds, options)].flat();
	}

	// Without the `lines` option, arrays can only come from object mode, which does not apply to the other file descriptor.
	// Like asynchronous methods, that other file descriptor is omitted when it is empty.
	if (Array.isArray(stdout) || Array.isArray(stderr)) {
		return [stdout, stderr].filter(value => value.length > 0).flat();
	}

	if (isUint8Array(stdout) && isUint8Array(stderr)) {
		return concatUint8Arrays([stdout, stderr]);
	}

	return BASE64_ENCODINGS.has(options.encoding) ? concatBase64(stdout, stderr, options.encoding) : `${stdout}${stderr}`;
};

// Unlike other encodings, base64 strings cannot be concatenated, since the first one might end with padding
const concatBase64 = (stdout, stderr, encoding) => Buffer.concat([Buffer.from(stdout, encoding), Buffer.from(stderr, encoding)]).toString(encoding);

const BASE64_ENCODINGS = new Set(['base64', 'base64url']);

const hasLines = ({lines}) => Boolean(lines[1] || lines[2]);

// Each file descriptor is either already split into lines, or is a string which still needs to be split
const splitAllLines = (value, allFds, options) => value === undefined || Array.isArray(value) || !hasLines(options)
	? value
	: splitLinesSync(value, !getStripFinalNewline(options.stripFinalNewline, 'all', allFds), false);

function isStream(stream, {checkOpen = true} = {}) {
	return stream !== null
		&& typeof stream === 'object'
		&& (stream.writable || stream.readable || !checkOpen || (stream.writable === undefined && stream.readable === undefined))
		&& typeof stream.pipe === 'function';
}

function isWritableStream$1(stream, {checkOpen = true} = {}) {
	return isStream(stream, {checkOpen})
		&& (stream.writable || !checkOpen)
		&& typeof stream.write === 'function'
		&& typeof stream.end === 'function'
		&& typeof stream.writable === 'boolean'
		&& typeof stream.writableObjectMode === 'boolean'
		&& typeof stream.destroy === 'function'
		&& typeof stream.destroyed === 'boolean';
}

function isReadableStream$1(stream, {checkOpen = true} = {}) {
	return isStream(stream, {checkOpen})
		&& (stream.readable || !checkOpen)
		&& typeof stream.read === 'function'
		&& typeof stream.readable === 'boolean'
		&& typeof stream.readableObjectMode === 'boolean'
		&& typeof stream.destroy === 'function'
		&& typeof stream.destroyed === 'boolean';
}

function isDuplexStream(stream, options) {
	return isWritableStream$1(stream, options)
		&& isReadableStream$1(stream, options);
}

const a = Object.getPrototypeOf(
  Object.getPrototypeOf(
    /* istanbul ignore next */
    async function* () {
    }
  ).prototype
);
class c {
  #t;
  #n;
  #r = false;
  #e = void 0;
  constructor(e, t) {
    this.#t = e, this.#n = t;
  }
  next() {
    const e = () => this.#s();
    return this.#e = this.#e ? this.#e.then(e, e) : e(), this.#e;
  }
  return(e) {
    const t = () => this.#i(e);
    return this.#e ? this.#e.then(t, t) : t();
  }
  async #s() {
    if (this.#r)
      return {
        done: true,
        value: void 0
      };
    let e;
    try {
      e = await this.#t.read();
    } catch (t) {
      throw this.#e = void 0, this.#r = true, this.#t.releaseLock(), t;
    }
    return e.done && (this.#e = void 0, this.#r = true, this.#t.releaseLock()), e;
  }
  async #i(e) {
    if (this.#r)
      return {
        done: true,
        value: e
      };
    if (this.#r = true, !this.#n) {
      const t = this.#t.cancel(e);
      return this.#t.releaseLock(), await t, {
        done: true,
        value: e
      };
    }
    return this.#t.releaseLock(), {
      done: true,
      value: e
    };
  }
}
const n = Symbol();
function i() {
  return this[n].next();
}
Object.defineProperty(i, "name", { value: "next" });
function o(r) {
  return this[n].return(r);
}
Object.defineProperty(o, "name", { value: "return" });
const u = Object.create(a, {
  next: {
    enumerable: true,
    configurable: true,
    writable: true,
    value: i
  },
  return: {
    enumerable: true,
    configurable: true,
    writable: true,
    value: o
  }
});
function h({ preventCancel: r = false } = {}) {
  const e = this.getReader(), t = new c(
    e,
    r
  ), s = Object.create(u);
  return s[n] = t, s;
}

const getAsyncIterable = stream => {
	if (isReadableStream$1(stream, {checkOpen: false}) && nodeImports.on !== undefined) {
		return getStreamIterable(stream);
	}

	if (typeof stream?.[Symbol.asyncIterator] === 'function') {
		return stream;
	}

	// `ReadableStream[Symbol.asyncIterator]` support is missing in multiple browsers, so we ponyfill it
	if (toString.call(stream) === '[object ReadableStream]') {
		return h.call(stream);
	}

	throw new TypeError('The first argument must be a Readable, a ReadableStream, or an async iterable.');
};

const {toString} = Object.prototype;

// The default iterable for Node.js streams does not allow for multiple readers at once, so we re-implement it
const getStreamIterable = async function * (stream) {
	const controller = new AbortController();
	const state = {};
	handleStreamEnd(stream, controller, state);

	try {
		for await (const [chunk] of nodeImports.on(stream, 'data', {signal: controller.signal})) {
			yield chunk;
		}
	} catch (error) {
		// Stream failure, for example due to `stream.destroy(error)`
		if (state.error !== undefined) {
			throw state.error;
		// `error` event directly emitted on stream
		} else if (!controller.signal.aborted) {
			throw error;
		// Otherwise, stream completed successfully
		}
		// The `finally` block also runs when the caller throws, for example due to the `maxBuffer` option
	} finally {
		stream.destroy();
	}
};

const handleStreamEnd = async (stream, controller, state) => {
	try {
		await nodeImports.finished(stream, {
			cleanup: true,
			readable: true,
			writable: false,
			error: false,
		});
	} catch (error) {
		state.error = error;
	} finally {
		controller.abort();
	}
};

// Loaded by the Node entrypoint, but not by the browser one.
// This prevents using dynamic imports.
const nodeImports = {};

const getStreamContents$1 = async (stream, {init, convertChunk, getSize, truncateChunk, addChunk, getFinalChunk, finalize}, {maxBuffer = Number.POSITIVE_INFINITY} = {}) => {
	const asyncIterable = getAsyncIterable(stream);

	const state = init();
	state.length = 0;

	try {
		for await (const chunk of asyncIterable) {
			const chunkType = getChunkType(chunk);
			const convertedChunk = convertChunk[chunkType](chunk, state);
			appendChunk({
				convertedChunk,
				state,
				getSize,
				truncateChunk,
				addChunk,
				maxBuffer,
			});
		}

		appendFinalChunk({
			state,
			convertChunk,
			getSize,
			truncateChunk,
			addChunk,
			getFinalChunk,
			maxBuffer,
		});
		return finalize(state);
	} catch (error) {
		const normalizedError = typeof error === 'object' && error !== null ? error : new Error(error);
		normalizedError.bufferedData = finalize(state);
		throw normalizedError;
	}
};

const appendFinalChunk = ({state, getSize, truncateChunk, addChunk, getFinalChunk, maxBuffer}) => {
	const convertedChunk = getFinalChunk(state);
	if (convertedChunk !== undefined) {
		appendChunk({
			convertedChunk,
			state,
			getSize,
			truncateChunk,
			addChunk,
			maxBuffer,
		});
	}
};

const appendChunk = ({convertedChunk, state, getSize, truncateChunk, addChunk, maxBuffer}) => {
	const chunkSize = getSize(convertedChunk);
	const newLength = state.length + chunkSize;

	if (newLength <= maxBuffer) {
		addNewChunk(convertedChunk, state, addChunk, newLength);
		return;
	}

	const truncatedChunk = truncateChunk(convertedChunk, maxBuffer - state.length);

	if (truncatedChunk !== undefined) {
		addNewChunk(truncatedChunk, state, addChunk, maxBuffer);
	}

	throw new MaxBufferError();
};

const addNewChunk = (convertedChunk, state, addChunk, newLength) => {
	state.contents = addChunk(convertedChunk, state, newLength);
	state.length = newLength;
};

const getChunkType = chunk => {
	const typeOfChunk = typeof chunk;

	if (typeOfChunk === 'string') {
		return 'string';
	}

	if (typeOfChunk !== 'object' || chunk === null) {
		return 'others';
	}

	if (globalThis.Buffer?.isBuffer(chunk)) {
		return 'buffer';
	}

	const prototypeName = objectToString.call(chunk);

	if (prototypeName === '[object ArrayBuffer]') {
		return 'arrayBuffer';
	}

	if (prototypeName === '[object DataView]') {
		return 'dataView';
	}

	if (
		Number.isInteger(chunk.byteLength)
		&& Number.isInteger(chunk.byteOffset)
		&& objectToString.call(chunk.buffer) === '[object ArrayBuffer]'
	) {
		return 'typedArray';
	}

	return 'others';
};

const {toString: objectToString} = Object.prototype;

class MaxBufferError extends Error {
	name = 'MaxBufferError';

	constructor() {
		super('maxBuffer exceeded');
	}
}

const identity = value => value;

const noop$3 = () => undefined;

const getContentsProperty = ({contents}) => contents;

const throwObjectStream = chunk => {
	throw new Error(`Streams in object mode are not supported: ${String(chunk)}`);
};

const getLengthProperty = convertedChunk => convertedChunk.length;

async function getStreamAsArray(stream, options) {
	return getStreamContents$1(stream, arrayMethods, options);
}

const initArray = () => ({contents: []});

const increment = () => 1;

const addArrayChunk = (convertedChunk, {contents}) => {
	contents.push(convertedChunk);
	return contents;
};

const arrayMethods = {
	init: initArray,
	convertChunk: {
		string: identity,
		buffer: identity,
		arrayBuffer: identity,
		dataView: identity,
		typedArray: identity,
		others: identity,
	},
	getSize: increment,
	truncateChunk: noop$3,
	addChunk: addArrayChunk,
	getFinalChunk: noop$3,
	finalize: getContentsProperty,
};

async function getStreamAsArrayBuffer(stream, options) {
	return getStreamContents$1(stream, arrayBufferMethods, options);
}

const initArrayBuffer = () => ({contents: new ArrayBuffer(0)});

const useTextEncoder = chunk => textEncoder.encode(chunk);
const textEncoder = new TextEncoder();

const useUint8Array = chunk => new Uint8Array(chunk);

const useUint8ArrayWithOffset = chunk => new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);

const truncateArrayBufferChunk = (convertedChunk, chunkSize) => convertedChunk.slice(0, chunkSize);

// `contents` is an increasingly growing `Uint8Array`.
const addArrayBufferChunk = (convertedChunk, {contents, length: previousLength}, length) => {
	const newContents = hasArrayBufferResize() ? resizeArrayBuffer(contents, length) : resizeArrayBufferSlow(contents, length);
	new Uint8Array(newContents).set(convertedChunk, previousLength);
	return newContents;
};

// Without `ArrayBuffer.resize()`, `contents` size is always a power of 2.
// This means its last bytes are zeroes (not stream data), which need to be
// trimmed at the end with `ArrayBuffer.slice()`.
const resizeArrayBufferSlow = (contents, length) => {
	if (length <= contents.byteLength) {
		return contents;
	}

	const arrayBuffer = new ArrayBuffer(getNewContentsLength(length));
	new Uint8Array(arrayBuffer).set(new Uint8Array(contents), 0);
	return arrayBuffer;
};

// With `ArrayBuffer.resize()`, `contents` size matches exactly the size of
// the stream data. It does not include extraneous zeroes to trim at the end.
// The underlying `ArrayBuffer` does allocate a number of bytes that is a power
// of 2, but those bytes are only visible after calling `ArrayBuffer.resize()`.
const resizeArrayBuffer = (contents, length) => {
	if (length <= contents.maxByteLength) {
		contents.resize(length);
		return contents;
	}

	const arrayBuffer = new ArrayBuffer(length, {maxByteLength: getNewContentsLength(length)});
	new Uint8Array(arrayBuffer).set(new Uint8Array(contents), 0);
	return arrayBuffer;
};

// Retrieve the closest `length` that is both >= and a power of 2
const getNewContentsLength = length => SCALE_FACTOR ** Math.ceil(Math.log(length) / Math.log(SCALE_FACTOR));

const SCALE_FACTOR = 2;

const finalizeArrayBuffer = ({contents, length}) => hasArrayBufferResize() ? contents : contents.slice(0, length);

// `ArrayBuffer.slice()` is slow. When `ArrayBuffer.resize()` is available
// (Node >=20.0.0, Safari >=16.4 and Chrome), we can use it instead.
// eslint-disable-next-line no-warning-comments
// TODO: remove after dropping support for Node 20.
// eslint-disable-next-line no-warning-comments
// TODO: use `ArrayBuffer.transferToFixedLength()` instead once it is available
const hasArrayBufferResize = () => 'resize' in ArrayBuffer.prototype;

const arrayBufferMethods = {
	init: initArrayBuffer,
	convertChunk: {
		string: useTextEncoder,
		buffer: useUint8Array,
		arrayBuffer: useUint8Array,
		dataView: useUint8ArrayWithOffset,
		typedArray: useUint8ArrayWithOffset,
		others: throwObjectStream,
	},
	getSize: getLengthProperty,
	truncateChunk: truncateArrayBufferChunk,
	addChunk: addArrayBufferChunk,
	getFinalChunk: noop$3,
	finalize: finalizeArrayBuffer,
};

async function getStreamAsString(stream, options) {
	return getStreamContents$1(stream, stringMethods, options);
}

const initString = () => ({contents: '', textDecoder: new TextDecoder()});

const useTextDecoder = (chunk, {textDecoder}) => textDecoder.decode(chunk, {stream: true});

const addStringChunk = (convertedChunk, {contents}) => contents + convertedChunk;

const truncateStringChunk = (convertedChunk, chunkSize) => convertedChunk.slice(0, chunkSize);

const getFinalStringChunk = ({textDecoder}) => {
	const finalChunk = textDecoder.decode();
	return finalChunk === '' ? undefined : finalChunk;
};

const stringMethods = {
	init: initString,
	convertChunk: {
		string: identity,
		buffer: useTextDecoder,
		arrayBuffer: useTextDecoder,
		dataView: useTextDecoder,
		typedArray: useTextDecoder,
		others: throwObjectStream,
	},
	getSize: getLengthProperty,
	truncateChunk: truncateStringChunk,
	addChunk: addStringChunk,
	getFinalChunk: getFinalStringChunk,
	finalize: getContentsProperty,
};

// When the `maxBuffer` option is hit, a MaxBufferError is thrown.
// The stream is aborted, then specific information is kept for the error message.
const handleMaxBuffer = ({error, stream, readableObjectMode, lines, encoding, fdNumber}) => {
	if (!(error instanceof MaxBufferError)) {
		throw error;
	}

	if (fdNumber === 'all') {
		return error;
	}

	const unit = getMaxBufferUnit(readableObjectMode, lines, encoding);
	error.maxBufferInfo = {fdNumber, unit};
	stream.destroy();
	throw error;
};

const getMaxBufferUnit = (readableObjectMode, lines, encoding) => {
	if (readableObjectMode) {
		return 'objects';
	}

	if (lines) {
		return 'lines';
	}

	return encoding === 'buffer' ? 'bytes' : 'characters';
};

// Check the `maxBuffer` option with `result.ipcOutput`, before buffering one more message.
// Like the stream-based `maxBuffer`, the threshold is hit when the buffered amount would exceed it, which also applies to non-integer values.
const checkIpcMaxBuffer = (ipcOutput, maxBuffer) => {
	if (ipcOutput.length + 1 <= maxBuffer) {
		return;
	}

	const error = new MaxBufferError();
	error.maxBufferInfo = {fdNumber: 'ipc'};
	throw error;
};

// Error message when `maxBuffer` is hit
const getMaxBufferMessage = (error, maxBuffer) => {
	const {streamName, threshold, unit} = getMaxBufferInfo(error, maxBuffer);
	return `Command's ${streamName} was larger than ${threshold} ${unit}`;
};

const getMaxBufferInfo = (error, maxBuffer) => {
	if (error?.maxBufferInfo === undefined) {
		return {streamName: 'output', threshold: maxBuffer[1], unit: 'bytes'};
	}

	const {maxBufferInfo: {fdNumber, unit, threshold = getFdSpecificValue(maxBuffer, fdNumber)}} = error;
	delete error.maxBufferInfo;

	return fdNumber === 'ipc' ? {streamName: 'IPC output', threshold, unit: 'messages'} : {streamName: getStreamName(fdNumber), threshold, unit};
};

// The native `maxBuffer` option passed to `spawnSync()` does not allow differentiating values per file descriptor, so we use `stdout`'s.
// Since each other file descriptor's own limit is capped by `stdout`'s, this is also the highest one.
// Node.js applies it to the total output of all piped file descriptors, not to each of them. When it is hit, the subprocess is killed and a `ENOBUFS` error is thrown.
const getMaxBufferSync = ([, stdoutMaxBuffer]) => stdoutMaxBuffer;

// File descriptors with a limit lower than `spawnSync()`'s native `maxBuffer` are enforced from the output, after the subprocess exits.
// Since no native error was thrown, we create the `ENOBUFS` error ourselves.
// Like with asynchronous methods, file descriptors which are not buffered are not limited by their own value. However, when they are piped, e.g. to a file, they still count towards the native limit, which is `stdout`'s value.
const handleMaxBufferSync = ({resultError, output, maxBuffer, buffer}) => {
	const fdNumber = findMaxBufferSync(output, maxBuffer, buffer);
	if (fdNumber === undefined) {
		// The native limit can be hit by the total output without any single file descriptor being over its own limit
		return {resultError, isMaxBuffer: resultError?.code === 'ENOBUFS'};
	}

	const threshold = getMaxBufferValueSync(maxBuffer, fdNumber);
	const error = resultError ?? Object.assign(new MaxBufferError(), {code: 'ENOBUFS'});
	error.maxBufferInfo = {fdNumber, unit: 'bytes', threshold};
	return {resultError: error, isMaxBuffer: true};
};

const findMaxBufferSync = (output, maxBuffer, buffer) => {
	if (output === null) {
		return;
	}

	const fdNumber = output.findIndex((result, index) => result !== null
		&& buffer[index]
		&& result.length > getMaxBufferValueSync(maxBuffer, index));
	return fdNumber === -1 ? undefined : fdNumber;
};

// `spawnSync()` uses `maxBuffer.stdout` as its native `maxBuffer`, so other file descriptors' limits can only be lower than that
const getMaxBufferValueSync = (maxBuffer, fdNumber) => Math.min(maxBuffer[1], maxBuffer[fdNumber]);

// When `maxBuffer` is hit, ensure each buffered file descriptor's result is truncated at its own limit
const truncateMaxBufferSync = (result, fdNumber, {maxBuffer, buffer}) => {
	const maxBufferValue = getMaxBufferValueSync(maxBuffer, fdNumber);
	return buffer[fdNumber] && result.length > maxBufferValue ? result.slice(0, maxBufferValue) : result;
};

// Computes `error.message`, `error.shortMessage` and `error.originalMessage`
const createMessages = ({
	stdio,
	all,
	ipcOutput,
	originalError,
	signal,
	signalDescription,
	exitCode,
	escapedCommand,
	timedOut,
	isCanceled,
	isGracefullyCanceled,
	isMaxBuffer,
	isForcefullyTerminated,
	forceKillAfterDelay,
	killSignal,
	maxBuffer,
	timeout,
	cwd,
}) => {
	const errorCode = getErrorCode(originalError);
	const prefix = getErrorPrefix({
		originalError,
		timedOut,
		timeout,
		isMaxBuffer,
		maxBuffer,
		errorCode,
		signal,
		signalDescription,
		exitCode,
		isCanceled,
		isGracefullyCanceled,
		isForcefullyTerminated,
		forceKillAfterDelay,
		killSignal,
	});
	const originalMessage = getOriginalMessage(originalError, cwd);
	const suffix = originalMessage === undefined ? '' : `\n${originalMessage}`;
	const shortMessage = `${prefix}: ${escapedCommand}${suffix}`;
	const messageStdio = all === undefined ? [stdio[2], stdio[1]] : [all];
	const message = [
		shortMessage,
		...messageStdio,
		...stdio.slice(3),
		ipcOutput.map(ipcMessage => serializeIpcMessage(ipcMessage)).join('\n'),
	]
		.map(messagePart => escapeLines(stripFinalNewline(serializeMessagePart(messagePart))))
		.filter(Boolean)
		.join('\n\n');
	return {originalMessage, shortMessage, message};
};

// `DOMException`, e.g. the default `cancelSignal` reason, has a legacy numeric `code`, which is not an error code like `ENOENT`
const getErrorCode = error => typeof error?.code === 'string' ? error.code : undefined;

const getErrorPrefix = ({
	originalError,
	timedOut,
	timeout,
	isMaxBuffer,
	maxBuffer,
	errorCode,
	signal,
	signalDescription,
	exitCode,
	isCanceled,
	isGracefullyCanceled,
	isForcefullyTerminated,
	forceKillAfterDelay,
	killSignal,
}) => {
	const forcefulSuffix = getForcefulSuffix(isForcefullyTerminated, forceKillAfterDelay);

	if (timedOut) {
		return `Command timed out after ${timeout} milliseconds${forcefulSuffix}`;
	}

	if (isGracefullyCanceled) {
		if (signal === undefined) {
			return `Command was gracefully canceled with exit code ${exitCode}`;
		}

		return isForcefullyTerminated
			? `Command was gracefully canceled${forcefulSuffix}`
			: `Command was gracefully canceled with ${signal} (${signalDescription})`;
	}

	if (isCanceled) {
		return `Command was canceled${forcefulSuffix}`;
	}

	if (isMaxBuffer) {
		return `${getMaxBufferMessage(originalError, maxBuffer)}${forcefulSuffix}`;
	}

	if (errorCode !== undefined) {
		return `Command failed with ${errorCode}${forcefulSuffix}`;
	}

	if (isForcefullyTerminated) {
		return `Command was killed with ${killSignal} (${getSignalDescription(killSignal)})${forcefulSuffix}`;
	}

	if (signal !== undefined) {
		return `Command was killed with ${signal} (${signalDescription})`;
	}

	return exitCode === undefined ? 'Command failed' : `Command failed with exit code ${exitCode}`;
};

const getForcefulSuffix = (isForcefullyTerminated, forceKillAfterDelay) => isForcefullyTerminated
	? ` and was forcefully terminated after ${forceKillAfterDelay} milliseconds`
	: '';

const getOriginalMessage = (originalError, cwd) => {
	if (originalError instanceof DiscardedError) {
		return;
	}

	// An `ExecaError` has no `originalMessage` when it was not caused by another error, e.g. with a non-zero exit code
	const originalMessage = isExecaError(originalError)
		? originalError.originalMessage ?? ''
		: String(originalError?.message ?? originalError);
	const escapedOriginalMessage = escapeLines(fixCwdError(originalMessage, cwd));
	return escapedOriginalMessage === '' ? undefined : escapedOriginalMessage;
};

const serializeIpcMessage = ipcMessage => typeof ipcMessage === 'string'
	? ipcMessage
	: inspect$1(ipcMessage);

// With the `lines` option, empty lines are kept. Only objects are omitted.
const serializeMessagePart = messagePart => Array.isArray(messagePart)
	? messagePart.filter(messageItem => isTextItem(messageItem)).map(messageItem => stripFinalNewline(serializeMessageItem(messageItem))).join('\n')
	: serializeMessageItem(messagePart);

const isTextItem = messageItem => typeof messageItem === 'string' || isUint8Array(messageItem);

const serializeMessageItem = messageItem => {
	if (typeof messageItem === 'string') {
		return messageItem;
	}

	return isUint8Array(messageItem) ? uint8ArrayToString(messageItem) : '';
};

// Object returned on subprocess success
const makeSuccessResult = ({
	command,
	escapedCommand,
	stdio,
	all,
	ipcOutput,
	options: {cwd},
	startTime,
}) => omitUndefinedProperties({
	command,
	escapedCommand,
	cwd,
	durationMs: getDurationMs(startTime),
	failed: false,
	timedOut: false,
	isCanceled: false,
	isGracefullyCanceled: false,
	isTerminated: false,
	isMaxBuffer: false,
	isForcefullyTerminated: false,
	exitCode: 0,
	stdout: stdio[1],
	stderr: stdio[2],
	all,
	stdio,
	ipcOutput,
	pipedFrom: [],
});

// Object returned on subprocess failure before spawning
const makeEarlyError = ({
	error,
	command,
	escapedCommand,
	fileDescriptors,
	options,
	startTime,
	isSync,
	stdio = Array.from({length: fileDescriptors.length}),
	all,
}) => makeError({
	error,
	command,
	escapedCommand,
	startTime,
	timedOut: false,
	isCanceled: false,
	isGracefullyCanceled: false,
	isMaxBuffer: false,
	isForcefullyTerminated: false,
	stdio,
	all,
	ipcOutput: [],
	options,
	isSync,
});

// When the subprocess fails to spawn, its output is empty, with the same shape as when it spawns but prints nothing
const getEarlyErrorOutput = (fileDescriptors, options) => {
	const stdio = fileDescriptors.map((fileDescriptor, fdNumber) => getEmptyOutput(fileDescriptor, fdNumber, options));
	return {stdio, all: getAllSync(stdio, getAllSyncFds(stdio), options)};
};

const getEmptyOutput = ({direction, objectMode}, fdNumber, {stdio, buffer, lines, encoding}) => {
	if (direction === 'input' || !PIPED_STDIO.has(stdio[fdNumber]) || !buffer[fdNumber]) {
		return;
	}

	if (objectMode || lines[fdNumber]) {
		return [];
	}

	return encoding === 'buffer' ? new Uint8Array() : '';
};

const PIPED_STDIO = new Set(['pipe', 'overlapped']);

// Object returned on subprocess failure
const makeError = ({
	error: originalError,
	command,
	escapedCommand,
	startTime,
	timedOut,
	isCanceled,
	isGracefullyCanceled,
	isMaxBuffer,
	isForcefullyTerminated,
	exitCode: rawExitCode,
	signal: rawSignal,
	stdio,
	all,
	ipcOutput,
	options: {
		timeoutDuration,
		timeout = timeoutDuration,
		forceKillAfterDelay,
		killSignal,
		cwd,
		maxBuffer,
	},
	isSync,
}) => {
	const {exitCode, signal, signalDescription} = normalizeExitPayload(rawExitCode, rawSignal);
	const {originalMessage, shortMessage, message} = createMessages({
		stdio,
		all,
		ipcOutput,
		originalError,
		signal,
		signalDescription,
		exitCode,
		escapedCommand,
		timedOut,
		isCanceled,
		isGracefullyCanceled,
		isMaxBuffer,
		isForcefullyTerminated,
		forceKillAfterDelay,
		killSignal,
		maxBuffer,
		timeout,
		cwd,
	});
	const error = getFinalError(originalError, message, isSync);
	Object.assign(error, getErrorProperties({
		error,
		command,
		escapedCommand,
		startTime,
		timedOut,
		isCanceled,
		isGracefullyCanceled,
		isMaxBuffer,
		isForcefullyTerminated,
		exitCode,
		signal,
		signalDescription,
		stdio,
		all,
		ipcOutput,
		cwd,
		originalMessage,
		shortMessage,
	}));
	return error;
};

const getErrorProperties = ({
	error,
	command,
	escapedCommand,
	startTime,
	timedOut,
	isCanceled,
	isGracefullyCanceled,
	isMaxBuffer,
	isForcefullyTerminated,
	exitCode,
	signal,
	signalDescription,
	stdio,
	all,
	ipcOutput,
	cwd,
	originalMessage,
	shortMessage,
}) => omitUndefinedProperties({
	shortMessage,
	originalMessage,
	command,
	escapedCommand,
	cwd,
	durationMs: getDurationMs(startTime),
	failed: true,
	timedOut,
	isCanceled,
	isGracefullyCanceled,
	isTerminated: signal !== undefined,
	isMaxBuffer,
	isForcefullyTerminated,
	exitCode,
	signal,
	signalDescription,
	code: getErrorCode(error.cause),
	stdout: stdio[1],
	stderr: stdio[2],
	all,
	stdio,
	ipcOutput,
	pipedFrom: [],
});

const omitUndefinedProperties = result => Object.fromEntries(Object.entries(result).filter(([, value]) => value !== undefined));

// `signal` and `exitCode` emitted on `subprocess.on('exit')` event can be `null`.
// We normalize them to `undefined`
const normalizeExitPayload = (rawExitCode, rawSignal) => {
	const exitCode = rawExitCode === null ? undefined : rawExitCode;
	const signal = rawSignal === null ? undefined : rawSignal;
	const signalDescription = signal === undefined ? undefined : getSignalDescription(rawSignal);
	return {exitCode, signal, signalDescription};
};

const toZeroIfInfinity = value => Number.isFinite(value) ? value : 0;

function parseNumber(milliseconds) {
	return {
		days: Math.trunc(milliseconds / 86_400_000),
		hours: Math.trunc(milliseconds / 3_600_000 % 24),
		minutes: Math.trunc(milliseconds / 60_000 % 60),
		seconds: Math.trunc(milliseconds / 1000 % 60),
		milliseconds: Math.trunc(milliseconds % 1000),
		microseconds: Math.trunc(toZeroIfInfinity(milliseconds * 1000) % 1000),
		nanoseconds: Math.trunc(toZeroIfInfinity(milliseconds * 1e6) % 1000),
	};
}

function parseBigint(milliseconds) {
	return {
		days: milliseconds / 86_400_000n,
		hours: milliseconds / 3_600_000n % 24n,
		minutes: milliseconds / 60_000n % 60n,
		seconds: milliseconds / 1000n % 60n,
		milliseconds: milliseconds % 1000n,
		microseconds: 0n,
		nanoseconds: 0n,
	};
}

function parseMilliseconds(milliseconds) {
	switch (typeof milliseconds) {
		case 'number': {
			if (Number.isFinite(milliseconds)) {
				return parseNumber(milliseconds);
			}

			break;
		}

		case 'bigint': {
			return parseBigint(milliseconds);
		}

		// No default
	}

	throw new TypeError('Expected a finite number or bigint');
}

const isZero = value => value === 0 || value === 0n;
const pluralize = (word, count) => (count === 1 || count === 1n) ? word : `${word}s`;

const SECOND_ROUNDING_EPSILON = 0.000_000_1;
const ONE_DAY_IN_MILLISECONDS = 24n * 60n * 60n * 1000n;

function prettyMilliseconds(milliseconds, options) {
	const isBigInt = typeof milliseconds === 'bigint';
	if (!isBigInt && !Number.isFinite(milliseconds)) {
		throw new TypeError('Expected a finite number or bigint');
	}

	options = {...options};

	const sign = milliseconds < 0 ? '-' : '';
	milliseconds = milliseconds < 0 ? -milliseconds : milliseconds; // Cannot use `Math.abs()` because of BigInt support.

	if (options.colonNotation) {
		options.compact = false;
		options.formatSubMilliseconds = false;
		options.separateMilliseconds = false;
		options.verbose = false;
	}

	if (options.compact) {
		options.unitCount = 1;
		options.secondsDecimalDigits = 0;
		options.millisecondsDecimalDigits = 0;
	}

	let result = [];

	const floorDecimals = (value, decimalDigits) => {
		const flooredInterimValue = Math.floor((value * (10 ** decimalDigits)) + SECOND_ROUNDING_EPSILON);
		const flooredValue = Math.round(flooredInterimValue) / (10 ** decimalDigits);
		return flooredValue.toFixed(decimalDigits);
	};

	const add = (value, long, short, valueString) => {
		if (
			(result.length === 0 || !options.colonNotation)
			&& isZero(value)
			&& !(options.colonNotation && short === 'm')) {
			return;
		}

		valueString ??= String(value);
		if (options.colonNotation) {
			const wholeDigits = valueString.includes('.') ? valueString.split('.')[0].length : valueString.length;
			const minLength = result.length > 0 ? 2 : 1;
			valueString = '0'.repeat(Math.max(0, minLength - wholeDigits)) + valueString;
		} else {
			valueString += options.verbose ? ' ' + pluralize(long, value) : short;
		}

		result.push(valueString);
	};

	const parsed = parseMilliseconds(milliseconds);
	const days = BigInt(parsed.days);

	if (options.hideYearAndDays) {
		add((BigInt(days) * 24n) + BigInt(parsed.hours), 'hour', 'h');
	} else {
		if (options.hideYear) {
			add(days, 'day', 'd');
		} else {
			add(days / 365n, 'year', 'y');
			add(days % 365n, 'day', 'd');
		}

		add(Number(parsed.hours), 'hour', 'h');
	}

	add(Number(parsed.minutes), 'minute', 'm');

	if (!options.hideSeconds) {
		if (
			options.separateMilliseconds
			|| options.formatSubMilliseconds
			|| (!options.colonNotation && milliseconds < 1000 && !options.subSecondsAsDecimals)
		) {
			const seconds = Number(parsed.seconds);
			const milliseconds = Number(parsed.milliseconds);
			const microseconds = Number(parsed.microseconds);
			const nanoseconds = Number(parsed.nanoseconds);

			add(seconds, 'second', 's');

			if (options.formatSubMilliseconds) {
				add(milliseconds, 'millisecond', 'ms');
				add(microseconds, 'microsecond', 'µs');
				add(nanoseconds, 'nanosecond', 'ns');
			} else {
				const millisecondsAndBelow
					= milliseconds
					+ (microseconds / 1000)
					+ (nanoseconds / 1e6);

				const millisecondsDecimalDigits
					= typeof options.millisecondsDecimalDigits === 'number'
						? options.millisecondsDecimalDigits
						: 0;

				const roundedMilliseconds = millisecondsAndBelow >= 1
					? Math.round(millisecondsAndBelow)
					: Math.ceil(millisecondsAndBelow);
				const maximumMilliseconds = 1000 - (10 ** -millisecondsDecimalDigits);

				const millisecondsString = millisecondsDecimalDigits
					? Math.min(millisecondsAndBelow, maximumMilliseconds).toFixed(millisecondsDecimalDigits)
					: Math.min(roundedMilliseconds, maximumMilliseconds);

				add(
					Number.parseFloat(millisecondsString),
					'millisecond',
					'ms',
					millisecondsString,
				);
			}
		} else {
			const seconds = (
				(isBigInt ? Number(milliseconds % ONE_DAY_IN_MILLISECONDS) : milliseconds)
				/ 1000
			) % 60;
			const secondsDecimalDigits
				= typeof options.secondsDecimalDigits === 'number'
					? options.secondsDecimalDigits
					: 1;
			const secondsFixed = floorDecimals(seconds, secondsDecimalDigits);
			const secondsString = options.keepDecimalsOnWholeSeconds
				? secondsFixed
				: secondsFixed.replace(/\.0+$/, '');
			add(Number.parseFloat(secondsString), 'second', 's', secondsString);
		}
	}

	if (result.length === 0) {
		return sign + '0' + (options.verbose ? ' milliseconds' : 'ms');
	}

	const separator = options.colonNotation ? ':' : ' ';
	if (typeof options.unitCount === 'number') {
		result = result.slice(0, Math.max(options.unitCount, 1));
	}

	return sign + result.join(separator);
}

// When `verbose` is `short|full|custom`, print each command's error when it fails
const logError = (result, verboseInfo) => {
	if (result.failed) {
		verboseLog({
			type: 'error',
			verboseMessage: result.shortMessage,
			verboseInfo,
			result,
		});
	}
};

// When `verbose` is `short|full|custom`, print each command's completion, duration and error
const logResult = (result, verboseInfo) => {
	if (!isVerbose(verboseInfo)) {
		return;
	}

	logError(result, verboseInfo);
	logDuration(result, verboseInfo);
};

const logDuration = (result, verboseInfo) => {
	const verboseMessage = `(done in ${prettyMilliseconds(result.durationMs)})`;
	verboseLog({
		type: 'duration',
		verboseMessage,
		verboseInfo,
		result,
	});
};

// Applies the `reject` option.
// Also print the final log line with `verbose`.
const handleResult = (result, verboseInfo, {reject}) => {
	logResult(result, verboseInfo);

	if (reject && result.failed) {
		throw result;
	}

	return result;
};

// The `stdin`/`stdout`/`stderr` option can be of many types. This detects it.
const getStdioItemType = (value, optionName) => {
	if (isAsyncGenerator(value)) {
		return 'asyncGenerator';
	}

	if (isSyncGenerator(value)) {
		return 'generator';
	}

	if (isUrl(value)) {
		return 'fileUrl';
	}

	if (isFilePathObject(value)) {
		return 'filePath';
	}

	if (isWebStream(value)) {
		return 'webStream';
	}

	if (isStream(value, {checkOpen: false})) {
		return 'native';
	}

	if (isUint8Array(value)) {
		return 'uint8Array';
	}

	if (isAsyncIterableObject(value)) {
		return 'asyncIterable';
	}

	if (isIterableObject(value)) {
		return 'iterable';
	}

	if (isTransformStream(value)) {
		return getTransformStreamType({}, optionName);
	}

	return isTransformOptions(value) ? getTransformObjectType(value, optionName) : 'native';
};

const getTransformObjectType = (value, optionName) => {
	if (isDuplexStream(value.transform, {checkOpen: false})) {
		return getDuplexType(value, optionName);
	}

	return isTransformStream(value.transform) ? getTransformStreamType(value, optionName) : getGeneratorObjectType(value, optionName);
};

const getDuplexType = (value, optionName) => {
	validateNonGeneratorType(value, optionName, 'Duplex stream');
	return 'duplex';
};

const getTransformStreamType = (value, optionName) => {
	validateNonGeneratorType(value, optionName, 'web TransformStream');
	return 'webTransform';
};

const validateNonGeneratorType = ({final, binary, objectMode}, optionName, typeName) => {
	checkUndefinedOption(final, `${optionName}.final`, typeName);
	checkUndefinedOption(binary, `${optionName}.binary`, typeName);
	checkBooleanOption(objectMode, `${optionName}.objectMode`);
};

const checkUndefinedOption = (value, optionName, typeName) => {
	if (value !== undefined) {
		throw new TypeError(`The \`${optionName}\` option can only be defined when using a generator, not a ${typeName}.`);
	}
};

const getGeneratorObjectType = ({transform, final, binary, objectMode}, optionName) => {
	if (transform !== undefined && !isGenerator(transform)) {
		throw new TypeError(`The \`${optionName}.transform\` option must be a generator, a Duplex stream or a web TransformStream.`);
	}

	if (isDuplexStream(final, {checkOpen: false})) {
		throw new TypeError(`The \`${optionName}.final\` option must not be a Duplex stream.`);
	}

	if (isTransformStream(final)) {
		throw new TypeError(`The \`${optionName}.final\` option must not be a web TransformStream.`);
	}

	if (final !== undefined && !isGenerator(final)) {
		throw new TypeError(`The \`${optionName}.final\` option must be a generator.`);
	}

	checkBooleanOption(binary, `${optionName}.binary`);
	checkBooleanOption(objectMode, `${optionName}.objectMode`);

	return isAsyncGenerator(transform) || isAsyncGenerator(final) ? 'asyncGenerator' : 'generator';
};

const isGenerator = value => isAsyncGenerator(value) || isSyncGenerator(value);
const isAsyncGenerator = value => Object.prototype.toString.call(value) === '[object AsyncGeneratorFunction]';
const isSyncGenerator = value => Object.prototype.toString.call(value) === '[object GeneratorFunction]';
const isTransformOptions = value => isPlainObject$2(value)
	&& (value.transform !== undefined || value.final !== undefined);

const isUrl = value => Object.prototype.toString.call(value) === '[object URL]';
const isRegularUrl = value => isUrl(value) && value.protocol !== 'file:';

const isFilePathObject = value => isPlainObject$2(value)
	&& Object.keys(value).length > 0
	&& Object.keys(value).every(key => FILE_PATH_KEYS.has(key))
	&& isFilePathString(value.file);
const FILE_PATH_KEYS = new Set(['file', 'append']);
const isFilePathString = file => typeof file === 'string';

// The `{value, input}` object form marks an additional file descriptor's direction explicitly.
// This is needed for direction-ambiguous values (like `'pipe'`, `'inherit'`, a file or a transform) on fd3+, which otherwise default to output.
const isStdioValueObject = value => isPlainObject$2(value)
	&& Object.keys(value).length > 0
	&& Object.keys(value).every(key => STDIO_VALUE_KEYS.has(key))
	&& 'value' in value;
const STDIO_VALUE_KEYS = new Set(['value', 'input']);

const isUnknownStdioString = (type, value) => type === 'native'
	&& typeof value === 'string'
	&& !KNOWN_STDIO_STRINGS.has(value);
const KNOWN_STDIO_STRINGS = new Set(['ipc', 'ignore', 'inherit', 'overlapped', 'pipe']);

const isReadableStream = value => Object.prototype.toString.call(value) === '[object ReadableStream]';
const isWritableStream = value => Object.prototype.toString.call(value) === '[object WritableStream]';
const isWebStream = value => isReadableStream(value) || isWritableStream(value);
const isTransformStream = value => isReadableStream(value?.readable) && isWritableStream(value?.writable);

const isAsyncIterableObject = value => isObject(value) && typeof value[Symbol.asyncIterator] === 'function';
const isIterableObject = value => isObject(value) && typeof value[Symbol.iterator] === 'function';
const isObject = value => typeof value === 'object' && value !== null;

// Types which modify `subprocess.std*`
const TRANSFORM_TYPES = new Set(['generator', 'asyncGenerator', 'duplex', 'webTransform']);
// Types which write to a file or a file descriptor
const FILE_TYPES = new Set(['fileUrl', 'filePath', 'fileNumber']);
// When two file descriptors of this type share the same target, we need to do some special logic
const SPECIAL_DUPLICATE_TYPES_SYNC = new Set(['fileUrl', 'filePath']);
const SPECIAL_DUPLICATE_TYPES = new Set([...SPECIAL_DUPLICATE_TYPES_SYNC, 'webStream', 'nodeStream']);
// Do not allow two file descriptors of this type sharing the same target
const FORBID_DUPLICATE_TYPES = new Set(['webTransform', 'duplex']);

// Convert types to human-friendly strings for error messages
const TYPE_TO_MESSAGE = {
	generator: 'a generator',
	asyncGenerator: 'an async generator',
	fileUrl: 'a file URL',
	filePath: 'a file path string',
	fileNumber: 'a file descriptor number',
	webStream: 'a web stream',
	nodeStream: 'a Node.js stream',
	webTransform: 'a web TransformStream',
	duplex: 'a Duplex stream',
	native: 'any value',
	iterable: 'an iterable',
	asyncIterable: 'an async iterable',
	string: 'a string',
	uint8Array: 'a Uint8Array',
};

/*
Retrieve the `objectMode`s of a single transform.
`objectMode` determines the return value's type, i.e. the `readableObjectMode`.
The chunk argument's type is based on the previous generator's return value, i.e. the `writableObjectMode` is based on the previous `readableObjectMode`.
The last input's generator is read by `subprocess.stdin` which:
- should not be in `objectMode` for performance reasons.
- can only be strings, Buffers and Uint8Arrays.
Therefore its `readableObjectMode` must be `false`.
The same applies to the first output's generator's `writableObjectMode`.
*/
const getTransformObjectModes = (objectMode, index, newTransforms, direction) => direction === 'output'
	? getOutputObjectModes(objectMode, index, newTransforms)
	: getInputObjectModes(objectMode, index, newTransforms);

const getOutputObjectModes = (objectMode, index, newTransforms) => {
	const writableObjectMode = index !== 0 && newTransforms[index - 1].value.readableObjectMode;
	const readableObjectMode = objectMode ?? writableObjectMode;
	return {writableObjectMode, readableObjectMode};
};

const getInputObjectModes = (objectMode, index, newTransforms) => {
	const writableObjectMode = index === 0
		? objectMode === true
		: newTransforms[index - 1].value.readableObjectMode;
	const readableObjectMode = index !== newTransforms.length - 1 && (objectMode ?? writableObjectMode);
	return {writableObjectMode, readableObjectMode};
};

// Retrieve the `objectMode` of a file descriptor, e.g. `stdout` or `stderr`
const getFdObjectMode = (stdioItems, direction) => {
	const lastTransform = stdioItems.findLast(({type}) => TRANSFORM_TYPES.has(type));
	if (lastTransform === undefined) {
		return false;
	}

	return direction === 'input'
		? lastTransform.value.writableObjectMode
		: lastTransform.value.readableObjectMode;
};

// Transforms generators/duplex/TransformStream can have multiple shapes.
// This normalizes it and applies default values.
const normalizeTransforms = (stdioItems, optionName, direction, options) => [
	...stdioItems.filter(({type}) => !TRANSFORM_TYPES.has(type)),
	...getTransforms(stdioItems, optionName, direction, options),
];

const getTransforms = (stdioItems, optionName, direction, {encoding}) => {
	const transforms = stdioItems.filter(({type}) => TRANSFORM_TYPES.has(type));
	const newTransforms = Array.from({length: transforms.length});

	for (const [index, stdioItem] of Object.entries(transforms)) {
		newTransforms[index] = normalizeTransform({
			stdioItem,
			index: Number(index),
			newTransforms,
			optionName,
			direction,
			encoding,
		});
	}

	return sortTransforms(newTransforms, direction);
};

const normalizeTransform = ({stdioItem, stdioItem: {type}, index, newTransforms, optionName, direction, encoding}) => {
	if (type === 'duplex') {
		return normalizeDuplex({stdioItem, optionName});
	}

	if (type === 'webTransform') {
		return normalizeTransformStream({
			stdioItem,
			index,
			newTransforms,
			direction,
		});
	}

	return normalizeGenerator({
		stdioItem,
		index,
		newTransforms,
		direction,
		encoding,
	});
};

const normalizeDuplex = ({stdioItem, optionName}) => {
	const {value} = stdioItem;
	const {transform} = value;
	const {writableObjectMode, readableObjectMode} = transform;
	const {objectMode = readableObjectMode} = value;

	if (objectMode && !readableObjectMode) {
		throw new TypeError(`The \`${optionName}.objectMode\` option can only be \`true\` if \`new Duplex({objectMode: true})\` is used.`);
	}

	if (!objectMode && readableObjectMode) {
		throw new TypeError(`The \`${optionName}.objectMode\` option cannot be \`false\` if \`new Duplex({objectMode: true})\` is used.`);
	}

	return {
		...stdioItem,
		value: {transform, writableObjectMode, readableObjectMode},
	};
};

const normalizeTransformStream = ({stdioItem, stdioItem: {value}, index, newTransforms, direction}) => {
	const {transform, objectMode} = isPlainObject$2(value) ? value : {transform: value};
	const {writableObjectMode, readableObjectMode} = getTransformObjectModes(objectMode, index, newTransforms, direction);
	return ({
		...stdioItem,
		value: {transform, writableObjectMode, readableObjectMode},
	});
};

const normalizeGenerator = ({stdioItem, stdioItem: {value}, index, newTransforms, direction, encoding}) => {
	const {
		transform,
		final,
		binary: binaryOption = false,
		preserveNewlines = false,
		objectMode,
	} = isPlainObject$2(value) ? value : {transform: value};
	const binary = binaryOption || BINARY_ENCODINGS.has(encoding);
	const {writableObjectMode, readableObjectMode} = getTransformObjectModes(objectMode, index, newTransforms, direction);
	return {
		...stdioItem,
		value: {
			transform,
			final,
			binary,
			preserveNewlines,
			writableObjectMode,
			readableObjectMode,
		},
	};
};

const sortTransforms = (newTransforms, direction) => direction === 'input' ? newTransforms.reverse() : newTransforms;

// For `stdio[fdNumber]` beyond stdin/stdout/stderr, we need to guess whether the value passed is intended for inputs or outputs.
// This allows us to know whether to pipe _into_ or _from_ the stream.
// When `stdio[fdNumber]` is a single value, this guess is fairly straightforward.
// However, when it is an array instead, we also need to make sure the different values are not incompatible with each other.
const getStreamDirection = ({stdioItems, isStdioArray, fdNumber, optionName}) => {
	const directions = stdioItems.map(stdioItem => getStdioItemDirection(stdioItem, fdNumber, isStdioArray));

	if (directions.includes('input') && directions.includes('output')) {
		throw new TypeError(`The \`${optionName}\` option must not be an array of both readable and writable values.`);
	}

	return directions.find(Boolean) ?? DEFAULT_DIRECTION;
};

const getStdioItemDirection = (stdioItem, fdNumber, isStdioArray) => {
	const knownDirection = KNOWN_DIRECTIONS[fdNumber];
	const direction = getRequestedDirection(stdioItem);
	if (knownDirection === undefined) {
		return direction;
	}

	validateKnownDirection(stdioItem, knownDirection, isStdioArray);
	return knownDirection;
};

// `direction` is set when the user explicitly requests it, e.g. `{value: 'pipe', input: true}`.
// It takes precedence over guessing, but not over the fixed direction of `stdin`/`stdout`/`stderr`.
// The explicitly requested `direction` must not contradict a value that is intrinsically the other direction.
// For example, `{value: writableStream, input: true}` is invalid since a writable value can only be an output.
const getRequestedDirection = ({type, value, direction, optionName}) => {
	const guessedDirection = guessStreamDirection[type](value);
	if (direction === 'input' && guessedDirection === 'output') {
		throw new TypeError(`The \`${optionName}\` option is invalid: \`input: true\` cannot be used with a writable value, which is always an output.`);
	}

	return direction ?? guessedDirection;
};

// `stdin`/`stdout`/`stderr` have a fixed direction, which each value must not contradict.
// Otherwise, asynchronous methods pipe in the wrong direction and crash, e.g. with `dest.end is not a function`.
// Only values whose direction is intrinsic to a stream are checked: other values (e.g. `Uint8Array`) are already
// rejected in the wrong direction when their stream properties are added.
// A single file descriptor 0, 1 or 2, e.g. `stdin: 1`, is passed as is to `child_process.spawn()` instead of being piped by Execa, so it is not checked. Those are often the same terminal, which is both readable and writable.
const validateKnownDirection = ({type, value, optionName}, knownDirection, isStdioArray) => {
	if (!STREAM_TYPES.has(type) || (!isStdioArray && getStandardStreamDirection(value) !== undefined)) {
		return;
	}

	const guessedDirection = guessStreamDirection[type](value);
	if (guessedDirection === undefined || guessedDirection === knownDirection) {
		return;
	}

	const valueName = guessedDirection === 'input' ? 'readable' : 'writable';
	throw new TypeError(`The \`${optionName}\` option is invalid: a ${valueName} value is always an ${guessedDirection}, but \`${optionName}\` is an ${knownDirection}.`);
};

const STREAM_TYPES = new Set(['native', 'nodeStream', 'webStream']);

// `stdin`/`stdout`/`stderr` have a known direction
const KNOWN_DIRECTIONS = ['input', 'output', 'output'];

const anyDirection = () => undefined;
const alwaysInput = () => 'input';

// `string` can only be added through the `input` option, i.e. on `stdin`, so it is always an input
const guessStreamDirection = {
	generator: anyDirection,
	asyncGenerator: anyDirection,
	fileUrl: anyDirection,
	filePath: anyDirection,
	iterable: alwaysInput,
	asyncIterable: alwaysInput,
	uint8Array: alwaysInput,
	string: alwaysInput,
	webStream: value => isWritableStream(value) ? 'output' : 'input',
	nodeStream(value) {
		if (!isReadableStream$1(value, {checkOpen: false})) {
			return 'output';
		}

		return isWritableStream$1(value, {checkOpen: false}) ? undefined : 'input';
	},
	webTransform: anyDirection,
	duplex: anyDirection,
	native(value) {
		const standardStreamDirection = getStandardStreamDirection(value);
		if (standardStreamDirection !== undefined) {
			return standardStreamDirection;
		}

		if (isStream(value, {checkOpen: false})) {
			return guessStreamDirection.nodeStream(value);
		}
	},
};

const getStandardStreamDirection = value => {
	if ([0, process$2.stdin].includes(value)) {
		return 'input';
	}

	if ([1, 2, process$2.stdout, process$2.stderr].includes(value)) {
		return 'output';
	}
};

// When ambiguous, we initially keep the direction as `undefined`.
// This allows arrays of `stdio` values to resolve the ambiguity.
// For example, `stdio[3]: DuplexStream` is ambiguous, but `stdio[3]: [DuplexStream, WritableStream]` is not.
// When the ambiguity remains, we default to `output` since it is the most common use case for additional file descriptors.
const DEFAULT_DIRECTION = 'output';

// The `ipc` option adds an `ipc` item to the `stdio` option
const normalizeIpcStdioArray = (stdioArray, ipc) => ipc
	? [...stdioArray, 'ipc']
	: stdioArray;

// Add support for `stdin`/`stdout`/`stderr` as an alias for `stdio`.
// Also normalize the `stdio` option.
// A rest pattern is not used to read the other options, since the object it creates inherits from `Object.prototype`, which a prototype pollution could then use to inject option values
const normalizeStdioOption = (options, verboseInfo, isSync) => {
	const {stdio, ipc, buffer} = options;
	const stdioArray = getStdioArray(stdio, options).map((stdioOption, fdNumber) => addDefaultValue(stdioOption, fdNumber));
	validateIpcStdioOption(stdioArray);
	return isSync
		? normalizeStdioSync(stdioArray, buffer, verboseInfo)
		: normalizeIpcStdioArray(stdioArray, ipc);
};

// The `stdio: 'ipc'` value (raw `child_process` syntax) was replaced by the `ipc: true` option
const validateIpcStdioOption = stdioArray => {
	if (stdioArray.some(stdioOption => hasIpcStdioOption(stdioOption))) {
		throw new Error('The `ipc: true` option must be used instead of `stdio: \'ipc\'`.');
	}
};

const hasIpcStdioOption = stdioOption => Array.isArray(stdioOption) ? stdioOption.some(item => hasIpcStdioItem(item)) : hasIpcStdioItem(stdioOption);

const hasIpcStdioItem = stdioOption => isStdioValueObject(stdioOption) ? stdioOption.value === 'ipc' : stdioOption === 'ipc';

const getStdioArray = (stdio, options) => {
	if (stdio === undefined) {
		return STANDARD_STREAMS_ALIASES.map(alias => options[alias]);
	}

	if (hasAlias(options)) {
		throw new Error(`It's not possible to provide \`stdio\` in combination with one of ${STANDARD_STREAMS_ALIASES.map(alias => `\`${alias}\``).join(', ')}`);
	}

	if (typeof stdio === 'string') {
		return [stdio, stdio, stdio];
	}

	if (!Array.isArray(stdio)) {
		throw new TypeError(`Expected \`stdio\` to be of type \`string\` or \`Array\`, got \`${getTypeName(stdio)}\``);
	}

	const length = Math.max(stdio.length, STANDARD_STREAMS_ALIASES.length);
	return Array.from({length}, (_, fdNumber) => stdio[fdNumber]);
};

const hasAlias = options => STANDARD_STREAMS_ALIASES.some(alias => options[alias] !== undefined);

const addDefaultValue = (stdioOption, fdNumber) => {
	if (Array.isArray(stdioOption)) {
		return stdioOption.map(item => addDefaultValue(item, fdNumber));
	}

	if (stdioOption === null || stdioOption === undefined) {
		return fdNumber >= STANDARD_STREAMS_ALIASES.length ? 'ignore' : 'pipe';
	}

	return stdioOption;
};

// Using `buffer: false` with synchronous methods implies `stdout`/`stderr`: `ignore`.
// Unless the output is needed, e.g. due to `verbose: 'full'` or to redirecting to a file.
// This is only done for `stdout`/`stderr`, since `ignore` redirects them to `/dev/null`.
// With additional file descriptors, it does not pass them to the subprocess at all, which would make it fail when writing to them.
const normalizeStdioSync = (stdioArray, buffer, verboseInfo) => stdioArray.map((stdioOption, fdNumber) =>
	!buffer[fdNumber]
	&& isStandardOutputFd(fdNumber)
	&& !isFullVerbose(verboseInfo, fdNumber)
	&& isOutputPipeOnly(stdioOption, fdNumber)
		? 'ignore'
		: stdioOption);

// An empty array is not output pipe only: it must keep being rejected as an empty array below
const isOutputPipeOnly = (stdioOption, fdNumber) => isOutputPipe(stdioOption, fdNumber)
	|| (Array.isArray(stdioOption) && stdioOption.length > 0 && stdioOption.every(item => isOutputPipe(item, fdNumber)));

const isOutputPipe = (stdioOption, fdNumber) => stdioOption === 'pipe'
	|| isOutputPipeObject(stdioOption, fdNumber);

const isOutputPipeObject = (stdioOption, fdNumber) => isStdioValueObject(stdioOption)
	&& stdioOption.value === 'pipe'
	&& (stdioOption.input === undefined || stdioOption.input === false || isFixedOutputPipe(fdNumber, stdioOption.input));

// On `stdout`/`stderr`, the direction is always output, so `input: true` is ignored and the pipe stays output.
const isFixedOutputPipe = (fdNumber, input) => input === true && isStandardOutputFd(fdNumber);

const isStandardOutputFd = fdNumber => fdNumber === 1 || fdNumber === 2;

// Retrieve stream targeted by the `to` option
const getToStream = (destination, to = 'stdin') => {
	const isWritable = true;
	const {options, fileDescriptors} = SUBPROCESS_OPTIONS.get(destination);
	const fdNumber = getFdNumber(fileDescriptors, to, isWritable);
	const destinationStream = destination.stdio[fdNumber];

	if (destinationStream === null) {
		throw new TypeError(getInvalidStdioOptionMessage(fdNumber, to, options, isWritable));
	}

	return destinationStream;
};

// Retrieve stream targeted by the `from` option
const getFromStream = (source, from = 'stdout') => {
	const isWritable = false;
	const {options, fileDescriptors} = SUBPROCESS_OPTIONS.get(source);
	const fdNumber = getFdNumber(fileDescriptors, from, isWritable);
	const sourceStream = fdNumber === 'all' ? source.all : source.stdio[fdNumber];

	if (sourceStream === null || sourceStream === undefined) {
		throw new TypeError(getInvalidStdioOptionMessage(fdNumber, from, options, isWritable));
	}

	return sourceStream;
};

// Keeps track of the options passed to each Execa call
const SUBPROCESS_OPTIONS = new WeakMap();

const getFdNumber = (fileDescriptors, fdName, isWritable) => {
	const fdNumber = parseFdNumber(fdName, isWritable);
	validateFdNumber(fdNumber, fdName, isWritable, fileDescriptors);
	return fdNumber;
};

const parseFdNumber = (fdName, isWritable) => {
	const fdNumber = parseFd(fdName);
	if (fdNumber !== undefined) {
		return fdNumber;
	}

	const {validOptions, defaultValue} = isWritable
		? {validOptions: '"stdin"', defaultValue: 'stdin'}
		: {validOptions: '"stdout", "stderr", "all"', defaultValue: 'stdout'};
	throw new TypeError(`"${getOptionName(isWritable)}" must not be "${fdName}".
It must be ${validOptions} or "fd3", "fd4" (and so on).
It is optional and defaults to "${defaultValue}".`);
};

const validateFdNumber = (fdNumber, fdName, isWritable, fileDescriptors) => {
	const fileDescriptor = fileDescriptors[getUsedDescriptor(fdNumber)];
	if (fileDescriptor === undefined) {
		throw new TypeError(`"${getOptionName(isWritable)}" must not be ${fdName}. That file descriptor does not exist.
Please set the "stdio" option to ensure that file descriptor exists.`);
	}

	if (!isWritable && fileDescriptor.direction === 'input') {
		throw new TypeError(`"${getOptionName(isWritable)}" must not be ${fdName}. It must be a readable stream, not writable.`);
	}

	if (isWritable && fileDescriptor.direction !== 'input') {
		throw new TypeError(`"${getOptionName(isWritable)}" must not be ${fdName}. It must be a writable stream, not readable.
If you meant to use it as input, please set its "stdio" option to \`{value: 'pipe', input: true}\`.`);
	}
};

const getInvalidStdioOptionMessage = (fdNumber, fdName, options, isWritable) => {
	if (fdNumber === 'all' && !options.all) {
		return 'The "all" option must be true to use "from: \'all\'".';
	}

	const {optionName, optionValue} = getInvalidStdioOption(fdNumber, options);
	return `The "${optionName}: ${serializeOptionValue(optionValue)}" option is incompatible with using "${getOptionName(isWritable)}: ${serializeOptionValue(fdName)}".
Please set this option with "pipe" instead.`;
};

const getInvalidStdioOption = (fdNumber, {stdin, stdout, stderr, stdio}) => {
	const usedDescriptor = getUsedDescriptor(fdNumber);

	if (usedDescriptor === 0 && stdin !== undefined) {
		return {optionName: 'stdin', optionValue: stdin};
	}

	if (usedDescriptor === 1 && stdout !== undefined) {
		return {optionName: 'stdout', optionValue: stdout};
	}

	return usedDescriptor === 2 && stderr !== undefined ? {optionName: 'stderr', optionValue: stderr} : {optionName: `stdio[${usedDescriptor}]`, optionValue: stdio[usedDescriptor]};
};

const getUsedDescriptor = fdNumber => fdNumber === 'all' ? 1 : fdNumber;

const getOptionName = isWritable => isWritable ? 'to' : 'from';

const serializeOptionValue = value => {
	if (typeof value === 'string') {
		return `'${value}'`;
	}

	return typeof value === 'number' ? `${value}` : 'Stream';
};

// When we use multiple `stdio` values for the same streams, we pass 'pipe' to `child_process.spawn()`.
// We then emulate the piping done by core Node.js.
// To do so, we transform the following values:
//  - Node.js streams are marked as `type: nodeStream`
//  - 'inherit' becomes `process.stdin|stdout|stderr`
//  - any file descriptor integer becomes `process.stdio[fdNumber]`
// All of the above transformations tell Execa to perform manual piping.
const handleNativeStream = ({stdioItem, stdioItem: {type}, isStdioArray, fdNumber, direction, isSync}) => {
	if (!isStdioArray || type !== 'native') {
		return stdioItem;
	}

	return isSync
		? handleNativeStreamSync({stdioItem, fdNumber, direction})
		: handleNativeStreamAsync({stdioItem, fdNumber});
};

// Synchronous methods use a different logic.
// 'inherit', file descriptors and process.std* are handled by readFileSync()/writeFileSync().
const handleNativeStreamSync = ({stdioItem, stdioItem: {value, optionName}, fdNumber, direction}) => {
	const targetFd = getTargetFd({
		value,
		optionName,
		fdNumber,
		direction,
	});
	if (targetFd !== undefined) {
		return targetFd;
	}

	if (isStream(value, {checkOpen: false})) {
		throw new TypeError(`The \`${optionName}: Stream\` option cannot both be an array and include a stream with synchronous methods.`);
	}

	return stdioItem;
};

const getTargetFd = ({value, optionName, fdNumber, direction}) => {
	const targetFdNumber = getTargetFdNumber(value, fdNumber);
	if (targetFdNumber === undefined) {
		return;
	}

	if (direction === 'output') {
		return {type: 'fileNumber', value: targetFdNumber, optionName};
	}

	if (tty.isatty(targetFdNumber)) {
		throw new TypeError(`The \`${optionName}: ${serializeOptionValue(value)}\` option is invalid: it cannot be a TTY with synchronous methods.`);
	}

	// The file descriptor is read lazily, like `inputFile`, so that a read failure is reported like any other early error, instead of being thrown as a raw error
	return {type: 'uint8Array', contents: () => [bufferToUint8Array(readFileSync(targetFdNumber))], optionName};
};

const getTargetFdNumber = (value, fdNumber) => {
	if (value === 'inherit') {
		return fdNumber;
	}

	if (typeof value === 'number') {
		return value;
	}

	const standardStreamIndex = STANDARD_STREAMS.indexOf(value);
	if (standardStreamIndex !== -1) {
		return standardStreamIndex;
	}
};

const handleNativeStreamAsync = ({stdioItem, stdioItem: {value, optionName}, fdNumber}) => {
	if (value === 'inherit') {
		return {type: 'nodeStream', value: getStandardStream(fdNumber, value, optionName), optionName};
	}

	if (typeof value === 'number') {
		return {type: 'nodeStream', value: getStandardStream(value, value, optionName), optionName};
	}

	return isStream(value, {checkOpen: false}) ? {type: 'nodeStream', value, optionName} : stdioItem;
};

// Node.js does not allow to easily retrieve file descriptors beyond stdin/stdout/stderr as streams.
//  - `fs.createReadStream()`/`fs.createWriteStream()` with the `fd` option do not work with character devices that use blocking reads/writes (such as interactive TTYs).
//  - Using a TCP `Socket` would work but be rather complex to implement.
// Since this is an edge case, we simply throw an error message.
// See https://github.com/sindresorhus/execa/pull/643#discussion_r1435905707
const getStandardStream = (fdNumber, value, optionName) => {
	const standardStream = STANDARD_STREAMS[fdNumber];

	if (standardStream === undefined) {
		throw new TypeError(`The \`${optionName}: ${value}\` option is invalid: no such standard stream.`);
	}

	return standardStream;
};

// Append the `stdin` option with the `input` and `inputFile` options
const handleInputOptions = ({input, inputFile}, fdNumber) => fdNumber === 0
	? [
		...handleInputOption(input),
		...handleInputFileOption(inputFile),
	]
	: [];

const handleInputOption = input => input === undefined
	? []
	: [{
		type: getInputType(input),
		value: input,
		optionName: 'input',
	}];

const getInputType = input => {
	if (isReadableStream$1(input, {checkOpen: false})) {
		return 'nodeStream';
	}

	if (typeof input === 'string') {
		return 'string';
	}

	if (isUint8Array(input)) {
		return 'uint8Array';
	}

	throw new Error('The `input` option must be a string, a Uint8Array or a Node.js Readable stream.');
};

const handleInputFileOption = inputFile => inputFile === undefined
	? []
	: [{
		...getInputFileType(inputFile),
		optionName: 'inputFile',
	}];

const getInputFileType = inputFile => {
	if (isUrl(inputFile)) {
		return {type: 'fileUrl', value: inputFile};
	}

	if (isFilePathString(inputFile)) {
		return {type: 'filePath', value: {file: inputFile}};
	}

	throw new Error('The `inputFile` option must be a file path string or a file URL.');
};

// `{file: './output.txt'}` and a `file:` URL are two ways to target the same file, so both must be compared together
const isFileType = type => SPECIAL_DUPLICATE_TYPES_SYNC.has(type);

// The file that a `filePath` or `fileUrl` stdio item targets, as an absolute path
const getFileTarget = ({type, value}) => type === 'filePath'
	? path.resolve(value.file)
	: fileURLToPath(value);

// Duplicates in the same file descriptor is most likely an error.
// However, this can be useful with generators.
const filterDuplicates = stdioItems => stdioItems.filter((stdioItemOne, indexOne) =>
	stdioItems.every((stdioItemTwo, indexTwo) => !hasSameValueAndDirection(stdioItemOne, stdioItemTwo)
		|| indexOne >= indexTwo
		|| stdioItemOne.type === 'generator'
		|| stdioItemOne.type === 'asyncGenerator'));

const hasSameValueAndDirection = (stdioItemOne, stdioItemTwo) => stdioItemOne.value === stdioItemTwo.value
	&& stdioItemOne.direction === stdioItemTwo.direction;

// Check if two file descriptors are sharing the same target.
// For example `{stdout: {file: './output.txt'}, stderr: {file: './output.txt'}}`.
// The same applies to two items of a single file descriptor, e.g. `{stdout: [{file: './output.txt'}, {file: './output.txt'}]}`.
const getDuplicateStream = ({stdioItem: {type, value, optionName}, direction, fileDescriptors, isSync}) => {
	const otherStdioItems = getOtherStdioItems(fileDescriptors, type);
	if (otherStdioItems.length === 0) {
		return;
	}

	if (isSync) {
		validateDuplicateStreamSync({
			otherStdioItems,
			type,
			value,
			optionName,
			direction,
		});
		return;
	}

	if (SPECIAL_DUPLICATE_TYPES.has(type)) {
		return getDuplicateStreamInstance({
			otherStdioItems,
			type,
			value,
			optionName,
			direction,
		});
	}

	if (FORBID_DUPLICATE_TYPES.has(type)) {
		validateDuplicateTransform({
			otherStdioItems,
			type,
			value,
			optionName,
		});
	}
};

// Values shared by multiple file descriptors, including the other items of the file descriptor currently being built
const getOtherStdioItems = (fileDescriptors, type) => fileDescriptors
	.flatMap(({direction, stdioItems}) => stdioItems
		.map(stdioItem => ({...stdioItem, direction})),
	)
	.filter(stdioItem => isSameDuplicateType(stdioItem.type, type));

// A `filePath` and a `fileUrl` are compared together, since they can target the same file
const isSameDuplicateType = (otherType, type) => otherType === type || (isFileType(otherType) && isFileType(type));

// With `execaSync()`, do not allow setting a file path both in input and output
const validateDuplicateStreamSync = ({otherStdioItems, type, value, optionName, direction}) => {
	if (SPECIAL_DUPLICATE_TYPES_SYNC.has(type)) {
		getDuplicateStreamInstance({
			otherStdioItems,
			type,
			value,
			optionName,
			direction,
		});
	}
};

// When two file descriptors share the file or stream, we need to re-use the same underlying stream.
// Otherwise, the stream would be closed twice when piping ends.
// This is only an issue with output file descriptors.
// This is not a problem with generator functions since those create a new instance for each file descriptor.
// We also forbid input and output file descriptors sharing the same file or stream, since that does not make sense.
const getDuplicateStreamInstance = ({otherStdioItems, type, value, optionName, direction}) => {
	const duplicateStdioItems = otherStdioItems.filter(stdioItem => hasSameValue(stdioItem, type, value));
	if (duplicateStdioItems.length === 0) {
		return;
	}

	const differentStdioItem = duplicateStdioItems.find(stdioItem => stdioItem.direction !== direction);
	throwOnDuplicateStream(differentStdioItem, optionName, type);

	// Output streams must be re-used so they are not closed twice.
	// Web streams must be re-used in the input direction too, since they cannot be converted twice: reading one locks it.
	return direction === 'output' || type === 'webStream' ? duplicateStdioItems[0].stream : undefined;
};

// `type` and `value` are the current item's, `otherStdioItem` is the other one
const hasSameValue = (otherStdioItem, type, value) => isFileType(otherStdioItem.type)
	? getFileTarget(otherStdioItem) === getFileTarget({type, value})
	: otherStdioItem.value === value;

// We do not allow two file descriptors to share the same Duplex or TransformStream.
// This is because those are set directly to `subprocess.std*`.
// For example, this could result in `subprocess.stdout` and `subprocess.stderr` being the same value.
// This means reading from either would get data from both stdout and stderr.
const validateDuplicateTransform = ({otherStdioItems, type, value, optionName}) => {
	const duplicateStdioItem = otherStdioItems.find(({value: {transform}}) => transform === value.transform);
	throwOnDuplicateStream(duplicateStdioItem, optionName, type);
};

const throwOnDuplicateStream = (stdioItem, optionName, type) => {
	if (stdioItem !== undefined) {
		throw new TypeError(`The \`${stdioItem.optionName}\` and \`${optionName}\` options must not target ${TYPE_TO_MESSAGE[type]} that is the same.`);
	}
};

// Handle `input`, `inputFile`, `stdin`, `stdout` and `stderr` options, before spawning, in async/sync mode
// They are converted into an array of `fileDescriptors`.
// Each `fileDescriptor` is normalized, validated and contains all information necessary for further handling.
const handleStdio = (addProperties, options, verboseInfo, isSync) => {
	const stdio = normalizeStdioOption(options, verboseInfo, isSync);
	const initialFileDescriptors = stdio.map((stdioOption, fdNumber) => getFileDescriptor({
		stdioOption,
		fdNumber,
		options,
		isSync,
	}));
	const fileDescriptors = getFinalFileDescriptors({
		initialFileDescriptors,
		addProperties,
		options,
		isSync,
	});
	options.stdio = fileDescriptors.map(({stdioItems}) => forwardStdio(stdioItems));
	return fileDescriptors;
};

const getFileDescriptor = ({stdioOption, fdNumber, options, isSync}) => {
	const optionName = getStreamName(fdNumber);
	const {stdioItems: initialStdioItems, isStdioArray} = initializeStdioItems({
		stdioOption,
		fdNumber,
		options,
		optionName,
	});
	const direction = getStreamDirection({
		stdioItems: initialStdioItems,
		isStdioArray,
		fdNumber,
		optionName,
	});
	const stdioItems = initialStdioItems.map(stdioItem => handleNativeStream({
		stdioItem,
		isStdioArray,
		fdNumber,
		direction,
		isSync,
	}));
	const normalizedStdioItems = normalizeTransforms(stdioItems, optionName, direction, options);
	const objectMode = getFdObjectMode(normalizedStdioItems, direction);
	validateFileObjectMode(normalizedStdioItems, objectMode);
	return {direction, objectMode, stdioItems: normalizedStdioItems};
};

// We make sure passing an array with a single item behaves the same as passing that item without an array.
// This is what users would expect.
// For example, `stdout: ['ignore']` behaves the same as `stdout: 'ignore'`.
const initializeStdioItems = ({stdioOption, fdNumber, options, optionName}) => {
	const values = Array.isArray(stdioOption) ? stdioOption : [stdioOption];
	const inputStdioItems = handleInputOptions(options, fdNumber);
	const initialStdioItems = [
		...omitInheritedStdin(values.map(value => initializeStdioItem(value, optionName)), inputStdioItems),
		...inputStdioItems,
	];

	const stdioItems = filterDuplicates(initialStdioItems);
	const isStdioArray = stdioItems.length > 1;
	validateStdioArray(stdioItems, isStdioArray, optionName);
	validateStreams(stdioItems, isStdioArray);
	return {stdioItems, isStdioArray};
};

const omitInheritedStdin = (stdioItems, inputStdioItems) => inputStdioItems.length > 0 && isInheritedStdinOnly(stdioItems)
	? []
	: stdioItems;

const isInheritedStdinOnly = stdioItems => stdioItems.length === 1
	&& stdioItems[0].type === 'native'
	&& stdioItems[0].value === 'inherit';

const initializeStdioItem = (value, optionName) => {
	if (isStdioValueObject(value)) {
		return initializeStdioValueObject(value, optionName);
	}

	return {
		type: getStdioItemType(value, optionName),
		value,
		optionName,
	};
};

// The `{value, input}` object form wraps any stdio value with an explicit direction.
const initializeStdioValueObject = ({value, input}, optionName) => {
	checkBooleanOption(input, `${optionName}.input`);

	return {
		type: getStdioItemType(value, optionName),
		value,
		direction: input ? 'input' : undefined,
		optionName,
	};
};

const validateStdioArray = (stdioItems, isStdioArray, optionName) => {
	if (stdioItems.length === 0) {
		throw new TypeError(`The \`${optionName}\` option must not be an empty array.`);
	}

	if (!isStdioArray) {
		return;
	}

	for (const {value, optionName} of stdioItems) {
		if (INVALID_STDIO_ARRAY_OPTIONS.has(value)) {
			throw new Error(`The \`${optionName}\` option must not include \`${value}\`.`);
		}
	}
};

// Using the `ignore` value together with others for the same stream does not make sense, so we make it fail.
// However, we do allow it if the array has a single item.
const INVALID_STDIO_ARRAY_OPTIONS = new Set(['ignore']);

const validateStreams = (stdioItems, isStdioArray) => {
	for (const stdioItem of stdioItems) {
		validateFileStdio(stdioItem);
		validateFileDescriptorStdio(stdioItem);
		validateNativeStdio(stdioItem, isStdioArray);
	}
};

// A number is a file descriptor. Anything else is not a valid one, and passing it to `node:child_process` aborts the current process with a fatal assertion instead of throwing.
const validateFileDescriptorStdio = ({type, value, optionName}) => {
	if (type === 'native' && typeof value === 'number' && (!Number.isInteger(value) || value < 0 || value > MAX_FILE_DESCRIPTOR)) {
		throw new TypeError(`The \`${optionName}\` option must be a non-negative 32-bit integer: got ${value}.`);
	}
};

// `node:child_process` converts file descriptors to a 32-bit signed integer, and aborts the process when it cannot
const MAX_FILE_DESCRIPTOR = (2 ** 31) - 1;

// When passed alone, any unknown value fails when calling `node:child_process`.
// However, when an array is used, Execa only forwards `pipe` to `node:child_process`, so unknown values would be silently ignored.
// Valid values are the ones `node:child_process` accepts: known strings, file descriptor integers, Node.js streams and file handles.
// File handles are the exception: they are valid alone, but Execa's array emulation cannot handle them, so they would be silently ignored.
const validateNativeStdio = ({type, value, optionName}, isStdioArray) => {
	if (type !== 'native') {
		return;
	}

	if (isFileHandle(value)) {
		if (isStdioArray) {
			throw new TypeError(`The \`${optionName}\` option cannot combine file handles with other values.`);
		}

		return;
	}

	if (typeof value !== 'string' && typeof value !== 'number' && !isStream(value, {checkOpen: false})) {
		throw new TypeError(`The \`${optionName}\` option is invalid: ${String(value)} (${typeof value}).`);
	}
};

const isFileHandle = value => typeof value?.fd === 'number' && !isStream(value, {checkOpen: false});

const validateFileStdio = ({type, value, optionName}) => {
	if (isRegularUrl(value)) {
		throw new TypeError(`The \`${optionName}: URL\` option must use the \`file:\` scheme.
For example, you can use the \`pathToFileURL()\` method of the \`url\` core module.`);
	}

	if (isUnknownStdioString(type, value)) {
		throw new TypeError(`The \`${optionName}: { file: '...' }\` option must be used instead of \`${optionName}: '...'\`.`);
	}
};

const validateFileObjectMode = (stdioItems, objectMode) => {
	if (!objectMode) {
		return;
	}

	const fileStdioItem = stdioItems.find(({type}) => FILE_TYPES.has(type));
	if (fileStdioItem !== undefined) {
		throw new TypeError(`The \`${fileStdioItem.optionName}\` option cannot use both files and transforms in objectMode.`);
	}
};

// Some `stdio` values require Execa to create streams.
// For example, file paths create file read/write streams.
// Those transformations are specified in `addProperties`, which is both direction-specific and type-specific.
const getFinalFileDescriptors = ({initialFileDescriptors, addProperties, options, isSync}) => {
	const fileDescriptors = [];

	try {
		for (const fileDescriptor of initialFileDescriptors) {
			addFinalFileDescriptor({
				fileDescriptor,
				fileDescriptors,
				addProperties,
				options,
				isSync,
			});
		}

		return fileDescriptors;
	} catch (error) {
		cleanupCustomStreams(fileDescriptors);
		throw error;
	}
};

/*
The file descriptor is added to `fileDescriptors` before its items are, for two reasons:
  - its items are added one at a time, so that each one knows about the previous ones, to detect duplicates within the same file descriptor
  - if an item throws, the streams created by the previous ones are cleaned up too
*/
const addFinalFileDescriptor = ({fileDescriptor, fileDescriptors, addProperties, options, isSync}) => {
	const {direction, objectMode, stdioItems} = fileDescriptor;
	const finalFileDescriptor = {direction, objectMode, stdioItems: []};
	fileDescriptors.push(finalFileDescriptor);

	for (const stdioItem of stdioItems) {
		finalFileDescriptor.stdioItems.push(addStreamProperties({
			stdioItem,
			addProperties,
			direction,
			objectMode,
			options,
			fileDescriptors,
			isSync,
		}));
	}
};

const addStreamProperties = ({stdioItem, addProperties, direction, objectMode, options, fileDescriptors, isSync}) => {
	const duplicateStream = getDuplicateStream({
		stdioItem,
		direction,
		fileDescriptors,
		isSync,
	});

	if (duplicateStream !== undefined) {
		return {...stdioItem, stream: duplicateStream};
	}

	return {
		...stdioItem,
		...addProperties[direction][stdioItem.type](stdioItem, options, objectMode),
	};
};

// The stream error handling is performed by the piping logic above, which cannot be performed before subprocess spawning.
// If the subprocess spawning fails (e.g. due to an invalid command), the streams need to be manually destroyed.
// We need to create those streams before subprocess spawning, in case their creation fails, e.g. when passing an invalid generator as argument.
// Like this, an exception would be thrown, which would prevent spawning a subprocess.
const cleanupCustomStreams = fileDescriptors => {
	for (const {stream} of fileDescriptors.flatMap(({stdioItems}) => stdioItems)) {
		if (stream === undefined || isStandardStream(stream)) {
			continue;
		}

		// The stream can still error after being destroyed, e.g. when its file cannot be opened.
		// That error would be uncaught and crash the current process, although it is irrelevant since execa already fails with its own error.
		stream.on('error', () => {});
		stream.destroy();
	}
};

// When the `std*: Iterable | WebStream | URL | filePath`, `input` or `inputFile` option is used, we pipe to `subprocess.std*`.
// When the `std*: Array` option is used, we emulate some of the native values ('inherit', Node.js stream and file descriptor integer). To do so, we also need to pipe to `subprocess.std*`.
// Therefore the `std*` options must be either `pipe` or `overlapped`. Other values do not set `subprocess.std*`.
const forwardStdio = stdioItems => {
	if (stdioItems.length > 1) {
		return stdioItems.some(({value}) => value === 'overlapped') ? 'overlapped' : 'pipe';
	}

	const [{type, value}] = stdioItems;
	return type === 'native' ? value : 'pipe';
};

// Normalize `input`, `inputFile`, `stdin`, `stdout` and `stderr` options, before spawning, in sync mode
const handleStdioSync = (options, verboseInfo) => handleStdio(addPropertiesSync, options, verboseInfo, true);

const forbiddenIfSync = ({type, optionName}) => {
	throwInvalidSyncValue(optionName, TYPE_TO_MESSAGE[type]);
};

const forbiddenNativeIfSync = ({optionName, value}) => {
	if (value === 'overlapped') {
		throwInvalidSyncValue(optionName, `"${value}"`);
	}

	return {};
};

// A native input pipe on an additional file descriptor cannot be fed in sync mode, since there is no way to write to it after spawning, so it would hang.
const forbiddenNativeInputIfSync = stdioItem => {
	const {optionName, value} = stdioItem;
	if (value === 'pipe' && optionName !== 'stdin') {
		throw new TypeError(`Only the \`stdin\` option, not \`${optionName}\`, can be an input pipe with synchronous methods.`);
	}

	return forbiddenNativeIfSync(stdioItem);
};

// For the same reason, an input transform can only be used with `stdin`, which is fed with the `input` option
const forbiddenInputTransformIfSync = ({optionName}) => {
	if (optionName !== 'stdin') {
		throw new TypeError(`Only the \`stdin\` option, not \`${optionName}\`, can be an input transform with synchronous methods.`);
	}
};

const throwInvalidSyncValue = (optionName, value) => {
	throw new TypeError(`The \`${optionName}\` option cannot be ${value} with synchronous methods.`);
};

// Create streams used internally for redirecting when using specific values for the `std*` options, in sync mode.
// For example, `stdin: {file}` reads the file synchronously, then passes it as the `input` option.
const addProperties$1 = {
	generator() {},
	asyncGenerator: forbiddenIfSync,
	webStream: forbiddenIfSync,
	nodeStream: forbiddenIfSync,
	webTransform: forbiddenIfSync,
	duplex: forbiddenIfSync,
	asyncIterable: forbiddenIfSync,
	native: forbiddenNativeIfSync,
};

const addPropertiesSync = {
	input: {
		...addProperties$1,
		generator: forbiddenInputTransformIfSync,
		native: forbiddenNativeInputIfSync,
		// Files and iterables are read lazily, so that a file which cannot be read, or an iterable which throws, is reported like any other early error, instead of being thrown as a raw error
		fileUrl: ({value}) => ({contents: () => [bufferToUint8Array(readFileSync(value))]}),
		filePath: ({value: {file}}) => ({contents: () => [bufferToUint8Array(readFileSync(file))]}),
		fileNumber: forbiddenIfSync,
		iterable: ({value}) => ({contents: () => [...value]}),
		string: ({value}) => ({contents: [value]}),
		// `contents` is already set when reading from a file descriptor
		uint8Array: ({value, contents = [value]}) => ({contents}),
	},
	output: {
		...addProperties$1,
		fileUrl: ({value}) => ({path: value}),
		filePath: ({value: {file, append}}) => ({path: file, append}),
		fileNumber: ({value}) => ({path: value}),
		iterable: forbiddenIfSync,
		string: forbiddenIfSync,
		uint8Array: forbiddenIfSync,
	},
};

// Validate the type of chunk argument passed to transform generators
const getValidateTransformInput = (writableObjectMode, optionName) => writableObjectMode
	? undefined
	: validateStringTransformInput.bind(undefined, optionName);

const validateStringTransformInput = function * (optionName, chunk) {
	if (typeof chunk !== 'string' && !isUint8Array(chunk) && !Buffer.isBuffer(chunk)) {
		throw new TypeError(`The \`${optionName}\` option's transform must use "objectMode: true" to receive as input: ${typeof chunk}.`);
	}

	yield chunk;
};

// Validate the type of the value returned by transform generators
const getValidateTransformReturn = (readableObjectMode, optionName) => readableObjectMode
	? validateObjectTransformReturn.bind(undefined, optionName)
	: validateStringTransformReturn.bind(undefined, optionName);

const validateObjectTransformReturn = function * (optionName, chunk) {
	validateEmptyReturn(optionName, chunk);
	yield chunk;
};

const validateStringTransformReturn = function * (optionName, chunk) {
	validateEmptyReturn(optionName, chunk);

	if (typeof chunk !== 'string' && !isUint8Array(chunk)) {
		throw new TypeError(`The \`${optionName}\` option's function must yield a string or an Uint8Array, not ${typeof chunk}.`);
	}

	yield chunk;
};

const validateEmptyReturn = (optionName, chunk) => {
	if (chunk === null || chunk === undefined) {
		throw new TypeError(`The \`${optionName}\` option's function must not call \`yield ${chunk}\`.
Instead, \`yield\` should either be called with a value, or not be called at all. For example:
  if (condition) { yield value; }`);
	}
};

/*
When using binary encodings, add an internal generator that converts chunks from `Buffer` to `string` or `Uint8Array`.
Chunks might be Buffer, Uint8Array or strings since:
- `subprocess.stdout|stderr` emits Buffers
- `subprocess.stdin.write()` accepts Buffer, Uint8Array or string
- Previous generators might return Uint8Array or string

However, those are converted to Buffer:
- on writes: `Duplex.writable` `decodeStrings: true` default option
- on reads: `Duplex.readable` `readableEncoding: null` default option
*/
const getEncodingTransformGenerator = (binary, encoding, skipped) => {
	if (skipped) {
		return;
	}

	if (binary) {
		return {transform: encodingUint8ArrayGenerator.bind(undefined, new TextEncoder())};
	}

	const stringDecoder = new StringDecoder(encoding);
	return {
		transform: encodingStringGenerator.bind(undefined, stringDecoder),
		final: encodingStringFinal.bind(undefined, stringDecoder),
	};
};

const encodingUint8ArrayGenerator = function * (textEncoder, chunk) {
	if (Buffer.isBuffer(chunk)) {
		yield bufferToUint8Array(chunk);
	} else if (typeof chunk === 'string') {
		yield textEncoder.encode(chunk);
	} else {
		yield chunk;
	}
};

const encodingStringGenerator = function * (stringDecoder, chunk) {
	yield isUint8Array(chunk) ? stringDecoder.write(chunk) : chunk;
};

const encodingStringFinal = function * (stringDecoder) {
	const lastChunk = stringDecoder.end();
	if (lastChunk !== '') {
		yield lastChunk;
	}
};

/*
Once the user's transform has run, its string chunks are converted to bytes: by `Transform.push()` with the asynchronous methods, and by `joinToString()` with the synchronous ones. Both use UTF-8, yet the result is then decoded with the `encoding` option, which would corrupt the text when that option is a different text encoding.
Therefore, those chunks are converted using the `encoding` option beforehand.
With the `binary` transform option, they are converted with UTF-8 even when the `encoding` option is another text encoding. Otherwise, with synchronous methods, the next transform would receive the string itself, while with asynchronous methods it receives those UTF-8 bytes decoded with the `encoding` option.
When the `encoding` option is UTF-8, strings round-trip unchanged, so there is nothing to do.
*/
const getStringToBytesGenerator = (binary, encoding, skipped) => skipped || encoding === 'utf8'
	? undefined
	: {transform: stringToBytesGenerator.bind(undefined, getStringEncoding(binary, encoding))};

const stringToBytesGenerator = function * (stringEncoding, chunk) {
	yield typeof chunk === 'string' ? bufferToUint8Array(Buffer.from(chunk, stringEncoding)) : chunk;
};

// Applies a series of generator functions asynchronously
const pushChunks = callbackify(async (getChunks, state, getChunksArguments, transformStream) => {
	state.currentIterable = getChunks(...getChunksArguments);

	try {
		for await (const chunk of state.currentIterable) {
			transformStream.push(chunk);
		}
	} finally {
		delete state.currentIterable;
	}
});

// For each new chunk, apply each `transform()` method
const transformChunk = async function * (chunk, generators, index) {
	if (index === generators.length) {
		yield chunk;
		return;
	}

	const {transform = identityGenerator$1} = generators[index];
	for await (const transformedChunk of transform(chunk)) {
		yield * transformChunk(transformedChunk, generators, index + 1);
	}
};

// At the end, apply each `final()` method, followed by the `transform()` method of the next transforms
const finalChunks = async function * (generators) {
	for (const [index, {final}] of Object.entries(generators)) {
		yield * generatorFinalChunks(final, Number(index), generators);
	}
};

const generatorFinalChunks = async function * (final, index, generators) {
	if (final === undefined) {
		return;
	}

	for await (const finalChunk of final()) {
		yield * transformChunk(finalChunk, generators, index + 1);
	}
};

// Cancel any ongoing async generator when the Transform is destroyed, e.g. when the subprocess errors
const destroyTransform = callbackify(async ({currentIterable}, error) => {
	if (currentIterable !== undefined) {
		await (error ? currentIterable.throw(error) : currentIterable.return());
		return;
	}

	if (error) {
		throw error;
	}
});

const identityGenerator$1 = function * (chunk) {
	yield chunk;
};

// Duplicate the code from `run-async.js` but as synchronous functions
const pushChunksSync = (getChunksSync, getChunksArguments, transformStream, done) => {
	try {
		for (const chunk of getChunksSync(...getChunksArguments)) {
			transformStream.push(chunk);
		}

		done();
	} catch (error) {
		done(error);
	}
};

// Run synchronous generators with `execaSync()`
const runTransformSync = (generators, chunks) => [
	...chunks.flatMap(chunk => [...transformChunkSync(chunk, generators, 0)]),
	...finalChunksSync(generators),
];

const transformChunkSync = function * (chunk, generators, index) {
	if (index === generators.length) {
		yield chunk;
		return;
	}

	const {transform = identityGenerator} = generators[index];
	for (const transformedChunk of transform(chunk)) {
		yield * transformChunkSync(transformedChunk, generators, index + 1);
	}
};

const finalChunksSync = function * (generators) {
	for (const [index, {final}] of Object.entries(generators)) {
		yield * generatorFinalChunksSync(final, Number(index), generators);
	}
};

const generatorFinalChunksSync = function * (final, index, generators) {
	if (final === undefined) {
		return;
	}

	for (const finalChunk of final()) {
		yield * transformChunkSync(finalChunk, generators, index + 1);
	}
};

const identityGenerator = function * (chunk) {
	yield chunk;
};

/*
Generators can be used to transform/filter standard streams.

Generators have a simple syntax, yet allows all of the following:
- Sharing `state` between chunks
- Flushing logic, by using a `final` function
- Asynchronous logic
- Emitting multiple chunks from a single source chunk, even if spaced in time, by using multiple `yield`
- Filtering, by using no `yield`

Therefore, there is no need to allow Node.js or web transform streams.

The `highWaterMark` is kept as the default value, since this is what `subprocess.std*` uses.

Chunks are currently processed serially. We could add a `concurrency` option to parallelize in the future.

Transform an array of generator functions into a `Transform` stream.
`Duplex.from(generator)` cannot be used because it does not allow setting the `objectMode` and `highWaterMark`.
*/
const generatorToStream = ({
	value,
	value: {
		transform,
		final,
		binary,
		writableObjectMode,
		readableObjectMode,
	},
	optionName,
}, {encoding}) => {
	const state = {};
	const generators = addInternalGenerators(value, encoding, optionName);

	const transformAsync = isAsyncGenerator(transform);
	const finalAsync = isAsyncGenerator(final);
	const transformMethod = transformAsync
		? pushChunks.bind(undefined, transformChunk, state)
		: pushChunksSync.bind(undefined, transformChunkSync);
	const finalMethod = transformAsync || finalAsync
		? pushChunks.bind(undefined, finalChunks, state)
		: pushChunksSync.bind(undefined, finalChunksSync);
	const destroyMethod = transformAsync || finalAsync
		? destroyTransform.bind(undefined, state)
		: undefined;

	const stream = new Transform({
		writableObjectMode,
		writableHighWaterMark: getDefaultHighWaterMark(writableObjectMode),
		readableObjectMode,
		readableHighWaterMark: getDefaultHighWaterMark(readableObjectMode),
		// A string written to the stream is converted to bytes with this encoding, which `getEncodingTransformGenerator()` then decodes with the `encoding` option
		defaultEncoding: getStringEncoding(binary, encoding),
		transform(chunk, encoding, done) {
			transformMethod([chunk, generators, 0], this, done);
		},
		flush(done) {
			finalMethod([generators], this, done);
		},
		destroy: destroyMethod,
	});
	return {stream};
};

// Applies transform generators in sync mode
const runGeneratorsSync = (chunks, stdioItems, encoding, isInput) => {
	const generators = stdioItems.filter(({type}) => type === 'generator');
	const reversedGenerators = isInput ? generators.reverse() : generators;

	for (const {value, optionName} of reversedGenerators) {
		const generators = addInternalGenerators(value, encoding, optionName);
		chunks = runTransformSync(generators, chunks);
	}

	return chunks;
};

// Generators used internally to convert the chunk type, validate it, and split into lines
const addInternalGenerators = (
	{transform, final, binary, writableObjectMode, readableObjectMode, preserveNewlines},
	encoding,
	optionName,
) => {
	const state = {};
	return [
		{transform: getValidateTransformInput(writableObjectMode, optionName)},
		getEncodingTransformGenerator(binary, encoding, writableObjectMode),
		getSplitLinesGenerator(binary, preserveNewlines, writableObjectMode, state),
		{transform, final},
		{transform: getValidateTransformReturn(readableObjectMode, optionName)},
		getAppendNewlineGenerator({
			binary,
			preserveNewlines,
			readableObjectMode,
			state,
			encoding,
		}),
		getStringToBytesGenerator(binary, encoding, readableObjectMode),
	].filter(Boolean);
};

// Apply `stdin`/`input`/`inputFile` options, before spawning, in sync mode, by converting it to the `input` option
const addInputOptionsSync = (fileDescriptors, options) => {
	for (const fdNumber of getInputFdNumbers(fileDescriptors)) {
		addInputOptionSync(fileDescriptors, fdNumber, options);
	}
};

const getInputFdNumbers = fileDescriptors => new Set(Object.entries(fileDescriptors)
	.filter(([, {direction}]) => direction === 'input')
	.map(([fdNumber]) => Number(fdNumber)));

const addInputOptionSync = (fileDescriptors, fdNumber, options) => {
	const {stdioItems} = fileDescriptors[fdNumber];
	const allStdioItems = stdioItems.filter(({contents}) => contents !== undefined);
	if (allStdioItems.length === 0) {
		return;
	}

	if (fdNumber !== 0) {
		const [{type, optionName}] = allStdioItems;
		throw new TypeError(`Only the \`stdin\` option, not \`${optionName}\`, can be ${TYPE_TO_MESSAGE[type]} with synchronous methods.`);
	}

	// All input sources are merged into a single list of chunks, like asynchronous methods merge input streams, so that transforms receive a single stream of data: line splitting spans all sources and `final` runs only once.
	// Files and iterables are read lazily, so that a read failure is reported like any other early error
	const chunks = allStdioItems.flatMap(({type, contents}) => getChunks(type, typeof contents === 'function' ? contents() : contents));
	options.input = applyInputGeneratorsSync(chunks, stdioItems, options.encoding);
};

// Like with asynchronous methods, an empty input is not passed as a chunk, otherwise binary transforms, which do not split lines, would run on it.
// Iterables are left as is, since each of their items is a chunk.
const getChunks = (type, chunks) => type === 'iterable' ? chunks : chunks.filter(chunk => chunk.length > 0);

const applyInputGeneratorsSync = (chunks, stdioItems, encoding) => {
	// Input transforms use UTF-8 with text encodings, since those only apply to the output
	const newContents = runGeneratorsSync(chunks, stdioItems, getInputEncoding(encoding), true);
	validateSerializable(newContents);
	return joinToUint8Array(newContents);
};

const validateSerializable = newContents => {
	const invalidItem = newContents.find(item => !isSerializableInput(item));
	if (invalidItem !== undefined) {
		throwOnSerializableInput('stdin', invalidItem);
	}
};

// Values written to the subprocess, which is not in object mode
const isSerializableInput = item => typeof item === 'string' || isUint8Array(item);

const throwOnSerializableInput = (optionName, invalidItem) => {
	throw new TypeError(`The \`${optionName}\` option is invalid: when passing objects as input, a transform must be used to serialize them to strings or Uint8Arrays: ${invalidItem}.`);
};

// `ignore` opts-out of `verbose` for a specific stream.
// `ipc` cannot use piping.
// `inherit` would result in double printing.
// They can also lead to double printing when passing file descriptor integers or `process.std*`.
// This only leaves with `pipe` and `overlapped`.
const shouldLogOutput = ({stdioItems, encoding, verboseInfo, fdNumber}) => fdNumber !== 'all'
	&& isFullVerbose(verboseInfo, fdNumber)
	&& !BINARY_ENCODINGS.has(encoding)
	&& isFdVerbose(fdNumber)
	&& (stdioItems.some(({type, value}) => type === 'native' && PIPED_STDIO_VALUES.has(value))
		|| stdioItems.every(({type}) => TRANSFORM_TYPES.has(type)));

// Printing input streams would be confusing.
// Files and streams can produce big outputs, which we don't want to print.
// We could print `stdio[3+]` but it often is redirected to files and streams, with the same issue.
// So we only print stdout and stderr.
const isFdVerbose = fdNumber => fdNumber === 1 || fdNumber === 2;

const PIPED_STDIO_VALUES = new Set(['pipe', 'overlapped']);

// `verbose: 'full'` printing logic with async methods
const logLines = async (linesIterable, stream, fdNumber, verboseInfo) => {
	for await (const line of linesIterable) {
		if (!isPipingStream(stream)) {
			logLine(line, fdNumber, verboseInfo);
		}
	}
};

// `verbose: 'full'` printing logic with sync methods
const logLinesSync = (linesArray, fdNumber, verboseInfo) => {
	for (const line of linesArray) {
		logLine(line, fdNumber, verboseInfo);
	}
};

// When `subprocess.stdout|stderr.pipe()` is called, `verbose` becomes a noop.
// This prevents the following problems:
//  - `.pipe()` achieves the same result as using `stdout: 'inherit'`, `stdout: stream`, etc. which also make `verbose` a noop.
//    For example, `subprocess.stdout.pipe(process.stdin)` would print each line twice.
//  - When chaining subprocesses with `subprocess.pipe(otherSubprocess)`, only the last one should print its output.
// Detecting whether `.pipe()` is impossible without monkey-patching it, so we use the following undocumented property.
// The pipe into `subprocess.all` is done by Execa itself, so it only counts when `subprocess.all` is itself piped, e.g. `subprocess.all.pipe(process.stdout)`.
// This is not a critical behavior since changes of the following property would only make `verbose` more verbose.
const isPipingStream = stream => stream._readableState.pipes.some(pipe => !isAllStream(pipe) || isPipingStream(pipe));

// When `verbose` is `full`, print stdout|stderr
const logLine = (line, fdNumber, verboseInfo) => {
	const verboseMessage = serializeVerboseMessage(line);
	verboseLog({
		type: 'output',
		verboseMessage,
		fdNumber,
		verboseInfo,
	});
};

// Apply `stdout`/`stderr` options, after spawning, in sync mode
const transformOutputSync = ({fileDescriptors, syncResult, options, verboseInfo}) => {
	const output = syncResult.output ?? getSpawnFailureOutput(options);
	const outputFiles = new Set();
	// Transforms, `verbose` and file writes apply to a single file descriptor each, so their errors must not affect the other ones
	const errors = [];
	const transformedOutput = output.map((result, fdNumber) =>
		transformOutputResultSync({
			result,
			fileDescriptors,
			fdNumber,
			errors,
			outputFiles,
			verboseInfo,
		}, options));
	return {output: transformedOutput, error: errors.find(error => error !== undefined)};
};

// When the subprocess fails to spawn, `spawnSync()` returns no output. Like with asynchronous methods, each piped output is empty instead.
// This is the output `spawnSync()` returns when the subprocess succeeds without printing anything: `stdin` and non-piped file descriptors are `null`. Input pipes on other file descriptors cannot be used with synchronous methods.
const getSpawnFailureOutput = ({stdio}) => stdio.map((stdioOption, fdNumber) => fdNumber !== 0 && stdioOption === 'pipe' ? new Uint8Array() : null);

const transformOutputResultSync = (
	{result, fileDescriptors, fdNumber, errors, outputFiles, verboseInfo},
	{buffer, encoding, lines, stripFinalNewline, maxBuffer},
) => {
	if (result === null) {
		return;
	}

	const truncatedResult = truncateMaxBufferSync(result, fdNumber, {maxBuffer, buffer});
	const uint8ArrayResult = bufferToUint8Array(truncatedResult);
	const {stdioItems, objectMode} = fileDescriptors[fdNumber];
	// Empty output must not be passed as a chunk: with a binary encoding, no line is split, so transforms would wrongly run on it
	const {chunks, error: transformError} = runOutputGeneratorsSync(uint8ArrayResult.length === 0 ? [] : [uint8ArrayResult], stdioItems, encoding);
	const {serializedResult, finalResult} = serializeChunks({
		chunks,
		objectMode,
		encoding,
		lines,
		stripFinalNewline,
		fdNumber,
	});

	const logError = logOutputSync({
		serializedResult,
		fdNumber,
		verboseInfo,
		encoding,
		stdioItems,
		objectMode,
	});

	const error = transformError ?? logError;
	errors.push(error);

	const returnedResult = buffer[fdNumber] ? finalResult : undefined;

	try {
		if (error === undefined) {
			writeToFiles(chunks, stdioItems, outputFiles);
		}

		return returnedResult;
	} catch (writeError) {
		errors.push(writeError);
		return returnedResult;
	}
};

// Applies transform generators to `stdout`/`stderr`
const runOutputGeneratorsSync = (chunks, stdioItems, encoding) => {
	try {
		// `error` is set, so that a polluted `Object.prototype.error` is not read when destructuring
		return {chunks: runGeneratorsSync(chunks, stdioItems, encoding, false), error: undefined};
	} catch (error) {
		return {chunks, error};
	}
};

// The contents is converted to three stages:
//  - serializedResult: used by `verbose`
//  - finalResult/returnedResult: returned as `result.std*`
const serializeChunks = ({chunks, objectMode, encoding, lines, stripFinalNewline, fdNumber}) => {
	if (objectMode) {
		return {serializedResult: chunks, finalResult: chunks};
	}

	if (encoding === 'buffer') {
		const serializedResult = joinToUint8Array(chunks);
		return {serializedResult, finalResult: serializedResult};
	}

	const serializedResult = joinToString(chunks, encoding);
	const finalResult = lines[fdNumber] ? splitLinesSync(serializedResult, !stripFinalNewline[fdNumber], objectMode) : serializedResult;
	return {serializedResult, finalResult};
};

const logOutputSync = ({serializedResult, fdNumber, verboseInfo, encoding, stdioItems, objectMode}) => {
	if (!shouldLogOutput({
		stdioItems,
		encoding,
		verboseInfo,
		fdNumber,
	})) {
		return;
	}

	const linesArray = splitLinesSync(serializedResult, false, objectMode);

	try {
		logLinesSync(linesArray, fdNumber, verboseInfo);
	} catch (error) {
		return error;
	}
};

// When the `std*` target is a file path/URL or a file descriptor
// Object mode cannot be used with those targets.
const writeToFiles = (chunks, stdioItems, outputFiles) => {
	const fileItems = stdioItems.filter(({type}) => FILE_TYPES.has(type));
	if (fileItems.length === 0) {
		return;
	}

	// Like with asynchronous methods, the bytes are written as is, not the text decoded with the `encoding` option, which would change them
	const contents = joinToUint8Array(chunks);
	const writtenPaths = new Set();

	for (const stdioItem of fileItems) {
		const {type, append} = stdioItem;
		// A `filePath` and a `fileUrl` can target the same file, so they must share the same key
		const path = type === 'fileNumber' ? stdioItem.path : getFileTarget(stdioItem);

		// A single file descriptor can target the same file twice, e.g. `stdout: [{file}, {file}]`.
		// Its output is a single stream, so it must only be written once.
		if (writtenPaths.has(path)) {
			continue;
		}

		writtenPaths.add(path);
		// Like with asynchronous methods, when several file descriptors target the same file, the first one decides whether it is appended to
		if (append || outputFiles.has(path)) {
			appendFileSync(path, contents);
		} else {
			writeFileSync(path, contents);
		}

		outputFiles.add(path);
	}
};

// If `error` is emitted before `spawn`, `exit` will never be emitted.
// However, `error` might be emitted after `spawn`.
// In that case, `exit` will still be emitted.
// Since the `exit` event contains the signal name, we want to make sure we are listening for it.
// This function also takes into account the following unlikely cases:
//  - `exit` being emitted in the same microtask as `spawn`
//  - `error` being emitted multiple times
const waitForExit = async (subprocess, context) => {
	const [exitCode, signal] = await waitForExitOrError(subprocess);
	context.isForcefullyTerminated ??= false;
	return [exitCode, signal];
};

const waitForExitOrError = async subprocess => {
	const [spawnPayload, exitPayload] = await Promise.allSettled([
		once(subprocess, 'spawn'),
		once(subprocess, 'exit'),
	]);

	if (spawnPayload.status === 'rejected') {
		return [];
	}

	return exitPayload.status === 'rejected'
		? waitForSubprocessExit(subprocess)
		: exitPayload.value;
};

const waitForSubprocessExit = async subprocess => {
	try {
		return await once(subprocess, 'exit');
	} catch {
		return waitForSubprocessExit(subprocess);
	}
};

// Retrieve the final exit code and|or signal name
const waitForSuccessfulExit = async exitPromise => {
	const [exitCode, signal] = await exitPromise;

	if (!isSubprocessErrorExit(exitCode, signal) && isFailedExit(exitCode, signal)) {
		throw new DiscardedError();
	}

	return [exitCode, signal];
};

// When the subprocess fails due to an `error` event
const isSubprocessErrorExit = (exitCode, signal) => exitCode === undefined && signal === undefined;
// When the subprocess fails due to a non-0 exit code or to a signal termination
const isFailedExit = (exitCode, signal) => exitCode !== 0 || signal !== null;

// Retrieve exit code, signal name and error information, with synchronous methods
const getExitResultSync = ({error, status: exitCode, signal, output}, {maxBuffer, buffer}) => {
	const initialError = getResultError(error, exitCode, signal);
	const isTimedOut = initialError?.code === 'ETIMEDOUT';
	// A file descriptor over its `maxBuffer` does not stop the subprocess, so the timeout that did is reported instead. That file descriptor is still truncated.
	const {resultError, isMaxBuffer} = isTimedOut
		? {resultError: initialError, isMaxBuffer: false}
		: handleMaxBufferSync({
			resultError: initialError,
			output,
			maxBuffer,
			buffer,
		});
	return {
		resultError,
		exitCode,
		signal,
		timedOut: isTimedOut,
		isMaxBuffer,
	};
};

const getResultError = (error, exitCode, signal) => {
	if (error !== undefined) {
		return error;
	}

	return isFailedExit(exitCode, signal) ? new DiscardedError() : undefined;
};

// Main shared logic for all sync methods: `execaSync()`, `$.sync()`
const execaCoreSync = (rawFile, rawArguments, rawOptions) => {
	const {file, commandArguments, command, escapedCommand, startTime, verboseInfo, options, fileDescriptors} = handleSyncArguments(rawFile, rawArguments, rawOptions);
	const result = spawnSubprocessSync({
		file,
		commandArguments,
		options,
		command,
		escapedCommand,
		verboseInfo,
		fileDescriptors,
		startTime,
	});
	return handleResult(result, verboseInfo, options);
};

// Compute arguments to pass to `child_process.spawnSync()`
const handleSyncArguments = (rawFile, rawArguments, rawOptions) => {
	const {command, escapedCommand, startTime, verboseInfo} = handleCommand(rawFile, rawArguments, rawOptions);
	const syncOptions = normalizeSyncOptions(rawOptions);
	const {file, commandArguments, options} = normalizeOptions(rawFile, rawArguments, syncOptions);
	validateSyncOptions(options);
	const fileDescriptors = handleStdioSync(options, verboseInfo);
	return {
		file,
		commandArguments,
		command,
		escapedCommand,
		startTime,
		verboseInfo,
		options,
		fileDescriptors,
	};
};

// Options normalization logic specific to sync methods
const normalizeSyncOptions = options => options.node && !options.ipc ? {...options, ipc: false} : options;

// Options validation logic specific to sync methods
const validateSyncOptions = ({ipc, ipcInput, detached, cancelSignal, killDescendants}) => {
	// Any value can be sent over IPC, including falsy ones, so only `undefined` means the option is not set
	if (ipcInput !== undefined) {
		throwInvalidSyncOption('ipcInput');
	}

	if (ipc) {
		throwInvalidSyncOption('ipc: true');
	}

	if (detached) {
		throwInvalidSyncOption('detached: true');
	}

	if (killDescendants) {
		throwInvalidSyncOption('killDescendants: true');
	}

	if (cancelSignal) {
		throwInvalidSyncOption('cancelSignal');
	}
};

const throwInvalidSyncOption = value => {
	throw new TypeError(`The "${value}" option cannot be used with synchronous methods.`);
};

const spawnSubprocessSync = ({file, commandArguments, options, command, escapedCommand, verboseInfo, fileDescriptors, startTime}) => {
	const syncResult = runSubprocessSync({
		file,
		commandArguments,
		options,
		command,
		escapedCommand,
		fileDescriptors,
		startTime,
	});
	if (syncResult.failed) {
		return syncResult;
	}

	const {resultError, exitCode, signal, timedOut, isMaxBuffer} = getExitResultSync(syncResult, options);
	const {output, error = resultError} = transformOutputSync({
		fileDescriptors,
		syncResult,
		options,
		verboseInfo,
	});
	const stdio = output.map((stdioOutput, fdNumber) => stripNewline(stdioOutput, options, fdNumber));
	const allFds = getAllSyncFds(output);
	const all = stripNewline(getAllSync(output, allFds, options), options, 'all', allFds);
	return getSyncResult({
		error,
		exitCode,
		signal,
		timedOut,
		isMaxBuffer,
		stdio,
		all,
		options,
		command,
		escapedCommand,
		startTime,
	});
};

const runSubprocessSync = ({file, commandArguments, options, command, escapedCommand, fileDescriptors, startTime}) => {
	try {
		addInputOptionsSync(fileDescriptors, options);
		const normalizedOptions = normalizeSpawnSyncOptions(options);
		// Keep the null prototype, otherwise a polluted `Object.prototype.failed` would make the result look like an early error
		return {__proto__: null, ...spawnSync(...concatenateShell(file, commandArguments, normalizedOptions))};
	} catch (error) {
		return makeEarlyError({
			error,
			command,
			escapedCommand,
			fileDescriptors,
			options,
			startTime,
			isSync: true,
			...getEarlyErrorOutput(fileDescriptors, options),
		});
	}
};

// The `encoding` option is handled by Execa, not by `child_process.spawnSync()`
const normalizeSpawnSyncOptions = ({encoding, maxBuffer, ...options}) => ({...options, encoding: 'buffer', maxBuffer: getMaxBufferSync(maxBuffer)});

const getSyncResult = ({error, exitCode, signal, timedOut, isMaxBuffer, stdio, all, options, command, escapedCommand, startTime}) => error === undefined
	? makeSuccessResult({
		command,
		escapedCommand,
		stdio,
		all,
		ipcOutput: [],
		options,
		startTime,
	})
	: makeError({
		error,
		command,
		escapedCommand,
		timedOut,
		isCanceled: false,
		isGracefullyCanceled: false,
		isMaxBuffer,
		isForcefullyTerminated: false,
		exitCode,
		signal,
		stdio,
		all,
		ipcOutput: [],
		options,
		startTime,
		isSync: true,
	});

const internalGetOneMessageOptions = Symbol('internalGetOneMessageOptions');

// Like `[sub]process.once('message')` but promise-based
const getOneMessage = ({anyProcess, channel, isSubprocess, ipc}, rawOptions) => {
	const options = copyOptions(rawOptions);
	const {reference = true, filter} = options;
	const {signal} = options[internalGetOneMessageOptions] ?? {};

	validateIpcMethod({
		methodName: 'getOneMessage',
		isSubprocess,
		ipc,
		isConnected: isConnected(anyProcess),
	});

	return getOneMessageAsync({
		anyProcess,
		channel,
		isSubprocess,
		filter,
		reference,
		signal,
	});
};

const getOneMessageAsync = async ({anyProcess, channel, isSubprocess, filter, reference, signal}) => {
	addReference(channel, reference);
	const ipcEmitter = getIpcEmitter(anyProcess, channel, isSubprocess);
	const controller = new AbortController();
	stopOnAbort$1(signal, controller);

	try {
		return await Promise.race([
			getMessage(ipcEmitter, filter, controller),
			throwOnDisconnect(ipcEmitter, isSubprocess, controller),
			throwOnStrictError(ipcEmitter, isSubprocess, controller),
		]);
	} catch (error) {
		// When the internal `signal` stops the read, e.g. when a `.pipe()` is canceled, the subprocess keeps running and can still use IPC
		if (!signal?.aborted) {
			disconnect(anyProcess);
		}

		throw error;
	} finally {
		controller.abort();
		removeReference(channel, reference);
	}
};

const stopOnAbort$1 = (signal, controller) => {
	if (signal === undefined) {
		return;
	}

	if (signal.aborted) {
		controller.abort();
		return;
	}

	signal.addEventListener('abort', () => {
		controller.abort();
	}, {once: true, signal: controller.signal});
};

const getMessage = async (ipcEmitter, filter, {signal}) => {
	if (filter === undefined) {
		const [message] = await once(ipcEmitter, 'message', {signal});
		return message;
	}

	for await (const [message] of on(ipcEmitter, 'message', {signal})) {
		if (filter(message)) {
			return message;
		}
	}
};

const throwOnDisconnect = async (ipcEmitter, isSubprocess, {signal}) => {
	await once(ipcEmitter, 'disconnect', {signal});
	throwOnEarlyDisconnect(isSubprocess);
};

const throwOnStrictError = async (ipcEmitter, isSubprocess, {signal}) => {
	const [error] = await once(ipcEmitter, 'strict:error', {signal});
	throw getStrictResponseError(error, isSubprocess);
};

const internalGetEachMessageOptions = Symbol('internalGetEachMessageOptions');

// Like `[sub]process.on('message')` but promise-based
const getEachMessage = (subprocessInfo, rawOptions) => {
	const options = copyOptions(rawOptions);
	const {reference = true} = options;
	// Internal callers can stop the IPC listener without exposing cancellation or pipe-awaiting options as public API.
	const {signal, shouldAwait = !subprocessInfo.isSubprocess} = options[internalGetEachMessageOptions] ?? {};

	return loopOnMessages({
		...subprocessInfo,
		shouldAwait,
		reference,
		signal,
	});
};

// Same but used internally
const loopOnMessages = ({anyProcess, waitProcess = anyProcess, channel, isSubprocess, ipc, shouldAwait, reference, signal}) => {
	validateIpcMethod({
		methodName: 'getEachMessage',
		isSubprocess,
		ipc,
		isConnected: isConnected(anyProcess),
	});

	addReference(channel, reference);
	const ipcEmitter = getIpcEmitter(anyProcess, channel, isSubprocess);
	const controller = new AbortController();
	const state = {};
	stopOnAbort(signal, controller);
	stopOnDisconnect(anyProcess, ipcEmitter, controller);
	abortOnStrictError({
		ipcEmitter,
		isSubprocess,
		controller,
		state,
	});
	return iterateOnMessages({
		anyProcess,
		waitProcess,
		channel,
		ipcEmitter,
		isSubprocess,
		shouldAwait,
		controller,
		state,
		reference,
		signal,
	});
};

const stopOnAbort = (signal, controller) => {
	if (signal === undefined) {
		return;
	}

	if (signal.aborted) {
		controller.abort();
		return;
	}

	// Reuse the iterator controller for listener cleanup when iteration ends before the internal signal aborts.
	signal.addEventListener('abort', () => {
		controller.abort();
	}, {once: true, signal: controller.signal});
};

const stopOnDisconnect = async (anyProcess, ipcEmitter, controller) => {
	try {
		await once(ipcEmitter, 'disconnect', {signal: controller.signal});
		controller.abort();
	} catch {}
};

const abortOnStrictError = async ({ipcEmitter, isSubprocess, controller, state}) => {
	try {
		const [error] = await once(ipcEmitter, 'strict:error', {signal: controller.signal});
		state.error = getStrictResponseError(error, isSubprocess);
		controller.abort();
	} catch {}
};

const iterateOnMessages = async function * ({anyProcess, waitProcess, channel, ipcEmitter, isSubprocess, shouldAwait, controller, state, reference, signal}) {
	try {
		for await (const [message] of on(ipcEmitter, 'message', {signal: controller.signal})) {
			throwIfStrictError(state);
			yield message;
		}
	} catch {
		throwIfStrictError(state);
	} finally {
		controller.abort();
		removeReference(channel, reference);

		// When the internal `signal` stops the iteration, e.g. when a `.pipe()` is canceled, the subprocess keeps running and can still use IPC
		if (!isSubprocess && !signal?.aborted) {
			disconnect(anyProcess);
		}

		if (shouldAwait) {
			await waitProcess;
		}
	}
};

const throwIfStrictError = ({error}) => {
	if (error) {
		throw error;
	}
};

// Add promise-based IPC methods in current process
const addIpcMethods = (target, subprocess, {ipc}) => {
	Object.assign(target, getIpcMethods(subprocess, false, ipc, target));
};

// Get promise-based IPC in the subprocess
const getIpcExport = () => {
	const anyProcess = process$2;
	const isSubprocess = true;
	const isIpc = process$2.channel !== undefined;

	return {
		...getIpcMethods(anyProcess, isSubprocess, isIpc),
		getCancelSignal: getCancelSignal.bind(undefined, {
			anyProcess,
			channel: anyProcess.channel,
			isSubprocess,
			ipc: isIpc,
		}),
	};
};

// Retrieve the `ipc` shared by both the current process and the subprocess
const getIpcMethods = (anyProcess, isSubprocess, ipc, waitProcess = anyProcess) => ({
	sendMessage: sendMessage.bind(undefined, {
		anyProcess,
		channel: anyProcess.channel,
		isSubprocess,
		ipc,
	}),
	getOneMessage: getOneMessage.bind(undefined, {
		anyProcess,
		channel: anyProcess.channel,
		isSubprocess,
		ipc,
	}),
	getEachMessage: getEachMessage.bind(undefined, {
		anyProcess,
		channel: anyProcess.channel,
		isSubprocess,
		ipc,
		waitProcess,
	}),
});

// When the subprocess fails to spawn.
// We ensure the returned error is always both a promise and a subprocess.
const handleEarlyError = ({error, command, escapedCommand, fileDescriptors, options, startTime, verboseInfo}) => {
	cleanupCustomStreams(fileDescriptors);

	const subprocess = new ChildProcess();
	// The subprocess never spawned, so there is no process to signal.
	// `ChildProcess.prototype.kill()` would signal the current process group instead, since its process id is `0`.
	subprocess.kill = () => false;
	const all = createDummyStreams(subprocess, fileDescriptors);

	const promise = handleDummyPromise({
		error,
		command,
		escapedCommand,
		fileDescriptors,
		options,
		startTime,
		verboseInfo,
	});
	return {
		subprocess,
		promise,
		all: options.all ? all : undefined,
		convertedStreams: getConvertedStreams(promise),
	};
};

const createDummyStreams = (subprocess, fileDescriptors) => {
	const stdin = createDummyStream();
	const stdout = createDummyStream();
	const stderr = createDummyStream();
	const extraStdio = Array.from({length: fileDescriptors.length - 3}, createDummyStream);
	const all = createDummyStream();
	const stdio = [stdin, stdout, stderr, ...extraStdio];
	Object.assign(subprocess, {
		stdin,
		stdout,
		stderr,
		stdio,
	});
	return all;
};

// Writing to an ended stream emits an error, which would crash the current process when the user writes to `subprocess.stdin`.
// Like when the subprocess exits before reading its input, that input is discarded instead.
const createDummyStream = () => {
	const stream = new PassThrough();
	stream.on('error', () => {});
	stream.end();
	return stream;
};

// The subprocess never started, so those streams have no contents.
// Like when the subprocess did start, they end once its promise resolves, or error with its error, as opposed to hanging forever.
const getConvertedStreams = promise => {
	const waitForPromise = async () => {
		await promise;
	};

	const endReadable = function () {
		callbackify(waitForPromise)(error => {
			if (error) {
				this.destroy(error);
			} else {
				this.push(null);
			}
		});
	};

	const endWritable = callbackify(waitForPromise);
	const writeWritable = (chunk, encoding, done) => {
		endWritable(done);
	};

	const readable = () => new Readable({read: endReadable});
	const writable = () => new Writable({write: writeWritable, final: endWritable});
	const duplex = () => new Duplex({read: endReadable, write: writeWritable, final: endWritable});
	const iterable = () => readable()[Symbol.asyncIterator]();

	return {
		readable,
		writable,
		duplex,
		readableStream: () => Readable.toWeb(readable()),
		writableStream: () => Writable.toWeb(writable()),
		transformStream: () => Duplex.toWeb(duplex()),
		iterable,
		[Symbol.asyncIterator]: iterable,
	};
};

// `error` is a promise when the spawn error is only emitted on the next tick
const handleDummyPromise = async ({error, command, escapedCommand, fileDescriptors, options, startTime, verboseInfo}) => {
	const earlyError = makeEarlyError({
		error: await error,
		command,
		escapedCommand,
		fileDescriptors,
		options,
		startTime,
		isSync: false,
		...getEarlyErrorOutput(fileDescriptors, options),
	});
	return handleResult(earlyError, verboseInfo, options);
};

// Handle `input`, `inputFile`, `stdin`, `stdout` and `stderr` options, before spawning, in async mode
const handleStdioAsync = (options, verboseInfo) => handleStdio(addPropertiesAsync, options, verboseInfo, false);

const forbiddenIfAsync = ({type, optionName}) => {
	throw new TypeError(`The \`${optionName}\` option cannot be ${TYPE_TO_MESSAGE[type]}.`);
};

// Create streams used internally for piping when using specific values for the `std*` options, in async mode.
// For example, `stdout: {file}` creates a file stream, which is piped from/to.
const addProperties = {
	fileNumber: forbiddenIfAsync,
	generator: generatorToStream,
	asyncGenerator: generatorToStream,
	nodeStream: ({value}) => ({stream: value}),
	// `Duplex.fromWeb()` uses the same `objectMode` for both sides
	webTransform: ({value: {transform, writableObjectMode, readableObjectMode}}) => ({
		stream: Duplex.from({
			writable: Writable.fromWeb(transform.writable, {objectMode: writableObjectMode}),
			readable: Readable.fromWeb(transform.readable, {objectMode: readableObjectMode}),
		}),
	}),
	duplex: ({value: {transform}}) => ({stream: transform}),
	native() {},
};

// Without transforms in object mode, iterable values are written to the subprocess, which only accepts strings and Uint8Arrays.
// `Readable.from()` emits the values of synchronous iterables on a later tick, where `subprocess.stdin.write()` throwing would crash the current process. Throwing while iterating errors the stream instead.
// Synchronous iterables must stay synchronous, otherwise their order would change when they are combined with other input sources.
const validateIterable = function * (iterable, optionName) {
	for (const value of iterable) {
		validateIterableValue(value, optionName);
		yield value;
	}
};

const validateAsyncIterable = async function * (iterable, optionName) {
	for await (const value of iterable) {
		validateIterableValue(value, optionName);
		yield value;
	}
};

const validateIterableValue = (value, optionName) => {
	if (!isSerializableInput(value)) {
		throwOnSerializableInput(optionName, value);
	}
};

// Empty input is not passed as a chunk, otherwise binary transforms, which do not split lines, would run on it
const readableFromChunk = chunk => Readable.from(chunk.length === 0 ? [] : [chunk]);

// Input transforms use UTF-8 with text encodings, since those only apply to the output
const generatorToInputStream = (stdioItem, {encoding}) => generatorToStream(stdioItem, {encoding: getInputEncoding(encoding)});

const addPropertiesAsync = {
	input: {
		...addProperties,
		generator: generatorToInputStream,
		asyncGenerator: generatorToInputStream,
		fileUrl: ({value}) => ({stream: createReadStream(value)}),
		filePath: ({value: {file}}) => ({stream: createReadStream(file)}),
		webStream: ({value}) => ({stream: Readable.fromWeb(value)}),
		iterable: ({value, optionName}, options, objectMode) => ({stream: Readable.from(objectMode ? value : validateIterable(value, optionName))}),
		asyncIterable: ({value, optionName}, options, objectMode) => ({stream: Readable.from(objectMode ? value : validateAsyncIterable(value, optionName))}),
		string: ({value}) => ({stream: readableFromChunk(value)}),
		uint8Array: ({value}) => ({stream: readableFromChunk(Buffer.from(value))}),
	},
	output: {
		...addProperties,
		fileUrl: ({value}) => ({stream: createWriteStream(value)}),
		filePath: ({value: {file, append}}) => ({stream: createWriteStream(file, append ? {flags: 'a'} : {})}),
		webStream: ({value}) => ({stream: Writable.fromWeb(value)}),
		iterable: forbiddenIfAsync,
		asyncIterable: forbiddenIfAsync,
		string: forbiddenIfAsync,
		uint8Array: forbiddenIfAsync,
	},
};

function mergeStreams(streams) {
	if (!Array.isArray(streams)) {
		throw new TypeError(`Expected an array, got \`${typeof streams}\`.`);
	}

	for (const stream of streams) {
		validateStream(stream);
	}

	const objectMode = streams.some(({readableObjectMode}) => readableObjectMode);
	const highWaterMark = getHighWaterMark(streams, objectMode);
	const passThroughStream = new MergedStream({
		objectMode,
		writableHighWaterMark: highWaterMark,
		readableHighWaterMark: highWaterMark,
	});

	for (const stream of streams) {
		passThroughStream.add(stream);
	}

	return passThroughStream;
}

const getHighWaterMark = (streams, objectMode) => {
	if (streams.length === 0) {
		return getDefaultHighWaterMark(objectMode);
	}

	const highWaterMarks = streams
		.filter(({readableObjectMode}) => readableObjectMode === objectMode)
		.map(({readableHighWaterMark}) => readableHighWaterMark);
	return Math.max(...highWaterMarks);
};

class MergedStream extends PassThrough {
	#streams = new Set([]);
	#ended = new Set([]);
	#aborted = new Set([]);
	#onFinished;
	#unpipeEvent = Symbol('unpipe');
	#streamPromises = new WeakMap();

	add(stream) {
		validateStream(stream);

		if (this.#streams.has(stream)) {
			return;
		}

		this.#streams.add(stream);

		this.#onFinished ??= onMergedStreamFinished(this, this.#streams, this.#unpipeEvent);
		const streamPromise = endWhenStreamsDone({
			passThroughStream: this,
			stream,
			streams: this.#streams,
			ended: this.#ended,
			aborted: this.#aborted,
			onFinished: this.#onFinished,
			unpipeEvent: this.#unpipeEvent,
		});
		this.#streamPromises.set(stream, streamPromise);

		stream.pipe(this, {end: false});
	}

	async remove(stream) {
		validateStream(stream);

		if (!this.#streams.has(stream)) {
			return false;
		}

		const streamPromise = this.#streamPromises.get(stream);
		if (streamPromise === undefined) {
			return false;
		}

		this.#streamPromises.delete(stream);

		stream.unpipe(this);
		await streamPromise;
		return true;
	}
}

const onMergedStreamFinished = async (passThroughStream, streams, unpipeEvent) => {
	updateMaxListeners(passThroughStream, PASSTHROUGH_LISTENERS_COUNT);
	const controller = new AbortController();

	try {
		await Promise.race([
			onMergedStreamEnd(passThroughStream, controller),
			onInputStreamsUnpipe(passThroughStream, streams, unpipeEvent, controller),
		]);
	} finally {
		controller.abort();
		updateMaxListeners(passThroughStream, -PASSTHROUGH_LISTENERS_COUNT);
	}
};

const onMergedStreamEnd = async (passThroughStream, {signal}) => {
	try {
		await finished(passThroughStream, {signal, cleanup: true});
	} catch (error) {
		errorOrAbortStream(passThroughStream, error);
		throw error;
	}
};

const onInputStreamsUnpipe = async (passThroughStream, streams, unpipeEvent, {signal}) => {
	for await (const [unpipedStream] of on(passThroughStream, 'unpipe', {signal})) {
		if (streams.has(unpipedStream)) {
			unpipedStream.emit(unpipeEvent);
		}
	}
};

const validateStream = stream => {
	if (typeof stream?.pipe !== 'function') {
		throw new TypeError(`Expected a readable stream, got: \`${typeof stream}\`.`);
	}
};

const endWhenStreamsDone = async ({passThroughStream, stream, streams, ended, aborted, onFinished, unpipeEvent}) => {
	updateMaxListeners(passThroughStream, PASSTHROUGH_LISTENERS_PER_STREAM);
	const controller = new AbortController();

	try {
		await Promise.race([
			afterMergedStreamFinished(onFinished, stream, controller),
			onInputStreamEnd({
				passThroughStream,
				stream,
				streams,
				ended,
				aborted,
				controller,
			}),
			onInputStreamUnpipe({
				stream,
				streams,
				ended,
				aborted,
				unpipeEvent,
				controller,
			}),
		]);
	} finally {
		controller.abort();
		updateMaxListeners(passThroughStream, -PASSTHROUGH_LISTENERS_PER_STREAM);
	}

	if (streams.size > 0 && streams.size === ended.size + aborted.size) {
		if (ended.size === 0 && aborted.size > 0) {
			abortStream(passThroughStream);
		} else {
			endStream(passThroughStream);
		}
	}
};

const afterMergedStreamFinished = async (onFinished, stream, {signal}) => {
	try {
		await onFinished;
		if (!signal.aborted) {
			abortStream(stream);
		}
	} catch (error) {
		if (!signal.aborted) {
			errorOrAbortStream(stream, error);
		}
	}
};

const onInputStreamEnd = async ({passThroughStream, stream, streams, ended, aborted, controller: {signal}}) => {
	try {
		await finished(stream, {
			signal,
			cleanup: true,
			readable: true,
			writable: false,
		});
		if (streams.has(stream)) {
			ended.add(stream);
		}
	} catch (error) {
		if (signal.aborted || !streams.has(stream)) {
			return;
		}

		if (isAbortError(error)) {
			aborted.add(stream);
		} else {
			errorStream(passThroughStream, error);
		}
	}
};

const onInputStreamUnpipe = async ({stream, streams, ended, aborted, unpipeEvent, controller: {signal}}) => {
	await once(stream, unpipeEvent, {signal});

	if (!stream.readable) {
		return once(signal, 'abort', {signal});
	}

	streams.delete(stream);
	ended.delete(stream);
	aborted.delete(stream);
};

const endStream = stream => {
	if (stream.writable) {
		stream.end();
	}
};

const errorOrAbortStream = (stream, error) => {
	if (isAbortError(error)) {
		abortStream(stream);
	} else {
		errorStream(stream, error);
	}
};

// This is the error thrown by `finished()` on `stream.destroy()`
const isAbortError = error => error?.code === 'ERR_STREAM_PREMATURE_CLOSE';

const abortStream = stream => {
	if (stream.readable || stream.writable) {
		stream.destroy();
	}
};

// `stream.destroy(error)` crashes the process with `uncaughtException` if no `error` event listener exists on `stream`.
// We take care of error handling on user behalf, so we do not want this to happen.
const errorStream = (stream, error) => {
	if (!stream.destroyed) {
		stream.once('error', noop$2);
		stream.destroy(error);
	}
};

const noop$2 = () => {};

const updateMaxListeners = (passThroughStream, increment) => {
	const maxListeners = passThroughStream.getMaxListeners();
	if (maxListeners !== 0 && maxListeners !== Number.POSITIVE_INFINITY) {
		passThroughStream.setMaxListeners(maxListeners + increment);
	}
};

// Number of times `passThroughStream.on()` is called regardless of streams:
//  - once due to `finished(passThroughStream)`
//  - once due to `on(passThroughStream)`
const PASSTHROUGH_LISTENERS_COUNT = 2;

// Number of times `passThroughStream.on()` is called per stream:
//  - once due to `stream.pipe(passThroughStream)`
const PASSTHROUGH_LISTENERS_PER_STREAM = 1;

// Similar to `Stream.pipeline(source, destination)`, but does not destroy standard streams.
// `onDestinationEnd` runs when the destination ends, before the source is aborted.
const pipeStreams = (source, destination, onDestinationEnd) => {
	source.pipe(destination);
	onSourceFinish(source, destination);
	onDestinationFinish(source, destination, onDestinationEnd);
};

// `source.pipe(destination)` makes `destination` end when `source` ends.
// But it does not propagate aborts or errors. This function does it.
const onSourceFinish = async (source, destination) => {
	if (isStandardStream(source) || isStandardStream(destination)) {
		return;
	}

	try {
		await finished(source, {cleanup: true, readable: true, writable: false});
	} catch {}

	endDestinationStream(destination);
};

const endDestinationStream = destination => {
	if (destination.writable) {
		destination.end();
	}
};

// We do the same thing in the other direction as well.
const onDestinationFinish = async (source, destination, onDestinationEnd) => {
	if (isStandardStream(source) || isStandardStream(destination)) {
		return;
	}

	try {
		await finished(destination, {cleanup: true, readable: false, writable: true});
	} catch {}

	if (onDestinationEnd !== undefined) {
		await onDestinationEnd();
	}

	abortSourceStream(source);
};

const abortSourceStream = source => {
	if (source.readable) {
		source.destroy();
	}
};

// Handle `input`, `inputFile`, `stdin`, `stdout` and `stderr` options, after spawning, in async mode
// When multiple input streams are used, we merge them to ensure the output stream ends only once each input stream has ended
const pipeOutputAsync = (subprocess, fileDescriptors, controller) => {
	const pipeGroups = new Map();

	for (const [fdNumber, {stdioItems, direction}] of Object.entries(fileDescriptors)) {
		const transformItems = stdioItems.filter(({type}) => TRANSFORM_TYPES.has(type));
		for (const {stream} of transformItems) {
			pipeTransform(subprocess, stream, direction, fdNumber);
		}

		const nonTransformItems = stdioItems.filter(({type}) => !TRANSFORM_TYPES.has(type));
		for (const {stream} of nonTransformItems) {
			pipeStdioItem({
				subprocess,
				stream,
				direction,
				fdNumber,
				pipeGroups,
				controller,
			});
		}
	}

	for (const [outputStream, inputStreams] of pipeGroups) {
		const inputStream = inputStreams.length === 1 ? inputStreams[0] : mergeStreams(inputStreams);
		pipeStreams(inputStream, outputStream);
	}
};

// When using transforms, `subprocess.stdin|stdout|stderr|stdio` is directly mutated
const pipeTransform = (subprocess, stream, direction, fdNumber) => {
	if (direction === 'output') {
		pipeStreams(subprocess.stdio[fdNumber], stream);
	} else {
		pipeStreams(stream, subprocess.stdio[fdNumber]);
	}

	const streamProperty = SUBPROCESS_STREAM_PROPERTIES[fdNumber];
	if (streamProperty !== undefined) {
		subprocess[streamProperty] = stream;
	}

	subprocess.stdio[fdNumber] = stream;
};

const SUBPROCESS_STREAM_PROPERTIES = ['stdin', 'stdout', 'stderr'];

// Most `std*` option values involve piping `subprocess.std*` to a stream.
// The stream is either passed by the user or created internally.
const pipeStdioItem = ({subprocess, stream, direction, fdNumber, pipeGroups, controller}) => {
	if (stream === undefined) {
		return;
	}

	setStandardStreamMaxListeners(stream, controller);

	const [inputStream, outputStream] = direction === 'output'
		? [stream, subprocess.stdio[fdNumber]]
		: [subprocess.stdio[fdNumber], stream];
	const outputStreams = pipeGroups.get(inputStream) ?? [];
	pipeGroups.set(inputStream, [...outputStreams, outputStream]);
};

// Multiple subprocesses might be piping from/to `process.std*` at the same time.
// This is not necessarily an error and should not print a `maxListeners` warning.
const setStandardStreamMaxListeners = (stream, {signal}) => {
	if (isStandardStream(stream)) {
		incrementMaxListeners(stream, MAX_LISTENERS_INCREMENT, signal);
	}
};

// `source.pipe(destination)` adds at most 1 listener for each event.
// If `stdin` option is an array, the values might be combined with `merge-streams`.
// That library also listens for `source` end, which adds 1 more listener.
const MAX_LISTENERS_INCREMENT = 2;

const isWindows = process$2.platform === 'win32';

// The `killDescendants` option terminates the whole process tree, not just the direct child.
// On Unix, this requires spawning the subprocess in its own process group, so we override the
// `detached` argument passed to `child_process.spawn()`.
// This is kept separate from the user-facing `detached` option, which must keep its own value,
// so the `cleanup` behavior is not affected.
const getSpawnOptions = options => !isWindows && options.killDescendants
	? {...options, detached: true}
	: options;

// Returns the low-level function used to send a signal to the subprocess.
// With the `killDescendants` option, the signal is sent to the whole process tree.
const getKillFunction = (subprocess, {killDescendants}) => {
	if (!killDescendants) {
		/*
		`subprocess.pid` is `undefined` when the subprocess failed to spawn, e.g. with `ENOENT`.
		`ChildProcess.prototype.kill()` then signals the current process group, since its process id is `0`.
		So, like `ChildProcess.prototype.kill()` does for a subprocess which already exited, nothing is signaled and `false` is returned.
		*/
		const killSubprocess = subprocess.kill.bind(subprocess);
		return (...arguments_) => (subprocess.pid === undefined ? false : killSubprocess(...arguments_));
	}

	const killDescendantsFunction = isWindows ? killDescendantsWindows : killDescendantsUnix;
	return killDescendantsFunction.bind(undefined, subprocess);
};

/*
Unlike `subprocess.kill()`, signaling a process group or a process tree uses the PID directly, without going through the subprocess' handle. The OS can re-assign that PID once it is not used anymore, and signaling it then would terminate an unrelated process, and on Windows its own descendants too.
On Unix, a PID stays reserved as long as it is the process group ID of a running process, i.e. as long as any descendant in that group is still running. So signaling the process group after the subprocess exited is still useful, e.g. for the `forceKillAfterDelay` escalation while Execa is waiting on a descendant, or with `subprocess.kill()` to terminate background descendants once the subprocess has settled. Once the whole group exited, the PID can only be signaled if the OS re-assigned it to a new process group leader, which is unlikely, so this is a documented limitation.
On Windows, there are no process groups: a PID is only reserved while a handle to the process is open, which Node.js closes once the subprocess exits. `taskkill /T` also cannot find the descendants of a process which exited. So the process tree is never signaled once the subprocess exited.
*/
const isAbandoned = subprocess => subprocess.pid === undefined
	|| (isWindows && hasExited(subprocess));

const hasExited = ({exitCode, signalCode}) => exitCode !== null || signalCode !== null;

// On Unix, the subprocess is its own process group leader (its PGID equals its PID), since it
// was spawned with `detached: true`. Sending the signal to `-pid` targets the whole group.
const killDescendantsUnix = (subprocess, signal) => {
	if (isAbandoned(subprocess)) {
		return false;
	}

	try {
		return process$2.kill(-subprocess.pid, signal);
	} catch {
		// The process group might already be gone, or signaling it might not be permitted, so we
		// fall back to the direct child. Like `ChildProcess.kill()`, this returns `false` instead of throwing.
		return subprocess.kill(signal);
	}
};

// Windows has no process groups. Instead, `taskkill /T` recursively terminates the process tree.
// It must run while the tree is still intact, so direct subprocess termination is only used as a
// fallback: killing the direct subprocess first would orphan its descendants before `taskkill` could enumerate them.
// If `taskkill` is unavailable or fails, the fallback only terminates the direct subprocess.
// `taskkill` ignores the signal (`/F` terminates the tree).
const killDescendantsWindows = (subprocess, signal) => {
	if (isAbandoned(subprocess)) {
		return false;
	}

	const taskkillFile = getTaskkillFile();
	if (taskkillFile === undefined) {
		return subprocess.kill(signal);
	}

	// This is best-effort: if `taskkill` fails, still try the direct subprocess.
	execFile(taskkillFile, ['/pid', `${subprocess.pid}`, '/T', '/F'], error => {
		if (error) {
			subprocess.kill(signal);
		}
	});
	return true;
};

const getTaskkillFile = () => {
	const windowsDirectory = [process$2.env.SystemRoot, process$2.env.windir]
		.find(directory => directory && isWindowsDriveAbsolutePath(directory));

	return windowsDirectory === undefined
		? undefined
		: path$1.join(windowsDirectory, 'System32', 'taskkill.exe');
};

const isWindowsDriveAbsolutePath = directory => {
	const {root} = path$1.parse(directory);
	return /^[a-z]:[/\\]/i.test(root);
};

/**
 * This is not the set of all possible signals.
 *
 * It IS, however, the set of all signals that trigger
 * an exit on either Linux or BSD systems.  Linux is a
 * superset of the signal names supported on BSD, and
 * the unknown signals just fail to register, so we can
 * catch that easily enough.
 *
 * Windows signals are a different set, since there are
 * signals that terminate Windows processes, but don't
 * terminate (or don't even exist) on Posix systems.
 *
 * Don't bother with SIGKILL.  It's uncatchable, which
 * means that we can't fire any callbacks anyway.
 *
 * If a user does happen to register a handler on a non-
 * fatal signal like SIGWINCH or something, and then
 * exit, it'll end up firing `process.emit('exit')`, so
 * the handler will be fired anyway.
 *
 * SIGBUS, SIGFPE, SIGSEGV and SIGILL, when not raised
 * artificially, inherently leave the process in a
 * state from which it is not safe to try and enter JS
 * listeners.
 */
const signals = [];
signals.push('SIGHUP', 'SIGINT', 'SIGTERM');
if (process.platform !== 'win32') {
    signals.push('SIGALRM', 'SIGABRT', 'SIGVTALRM', 'SIGXCPU', 'SIGXFSZ', 'SIGUSR2', 'SIGTRAP', 'SIGSYS', 'SIGQUIT', 'SIGIOT'
    // should detect profiler and enable/disable accordingly.
    // see #21
    // 'SIGPROF'
    );
}
if (process.platform === 'linux') {
    signals.push('SIGIO', 'SIGPOLL', 'SIGPWR', 'SIGSTKFLT');
}

// Note: since nyc uses this module to output coverage, any lines
// that are in the direct sync flow of nyc's outputCoverage are
// ignored, since we can never get coverage for them.
// grab a reference to node's real process object right away
const processOk = (process) => !!process &&
    typeof process === 'object' &&
    typeof process.removeListener === 'function' &&
    typeof process.emit === 'function' &&
    typeof process.reallyExit === 'function' &&
    typeof process.listeners === 'function' &&
    typeof process.kill === 'function' &&
    typeof process.pid === 'number' &&
    typeof process.on === 'function';
const kExitEmitter = Symbol.for('signal-exit emitter');
const global = globalThis;
const ObjectDefineProperty = Object.defineProperty.bind(Object);
// teeny special purpose ee
class Emitter {
    emitted = {
        afterExit: false,
        exit: false,
    };
    listeners = {
        afterExit: [],
        exit: [],
    };
    count = 0;
    id = Math.random();
    constructor() {
        if (global[kExitEmitter]) {
            return global[kExitEmitter];
        }
        ObjectDefineProperty(global, kExitEmitter, {
            value: this,
            writable: false,
            enumerable: false,
            configurable: false,
        });
    }
    on(ev, fn) {
        this.listeners[ev].push(fn);
    }
    removeListener(ev, fn) {
        const list = this.listeners[ev];
        const i = list.indexOf(fn);
        /* c8 ignore start */
        if (i === -1) {
            return;
        }
        /* c8 ignore stop */
        if (i === 0 && list.length === 1) {
            list.length = 0;
        }
        else {
            list.splice(i, 1);
        }
    }
    emit(ev, code, signal) {
        if (this.emitted[ev]) {
            return false;
        }
        this.emitted[ev] = true;
        let ret = false;
        for (const fn of this.listeners[ev]) {
            ret = fn(code, signal) === true || ret;
        }
        if (ev === 'exit') {
            ret = this.emit('afterExit', code, signal) || ret;
        }
        return ret;
    }
}
class SignalExitBase {
}
const signalExitWrap = (handler) => {
    return {
        onExit(cb, opts) {
            return handler.onExit(cb, opts);
        },
        load() {
            return handler.load();
        },
        unload() {
            return handler.unload();
        },
    };
};
class SignalExitFallback extends SignalExitBase {
    onExit() {
        return () => { };
    }
    load() { }
    unload() { }
}
class SignalExit extends SignalExitBase {
    // "SIGHUP" throws an `ENOSYS` error on Windows,
    // so use a supported signal instead
    /* c8 ignore start */
    #hupSig = process$1.platform === 'win32' ? 'SIGINT' : 'SIGHUP';
    /* c8 ignore stop */
    #emitter = new Emitter();
    #process;
    #originalProcessEmit;
    #originalProcessReallyExit;
    #sigListeners = {};
    #loaded = false;
    constructor(process) {
        super();
        this.#process = process;
        // { <signal>: <listener fn>, ... }
        this.#sigListeners = {};
        for (const sig of signals) {
            this.#sigListeners[sig] = () => {
                // If there are no other listeners, an exit is coming!
                // Simplest way: remove us and then re-send the signal.
                // We know that this will kill the process, so we can
                // safely emit now.
                const listeners = this.#process.listeners(sig);
                let { count } = this.#emitter;
                // This is a workaround for the fact that signal-exit v3 and signal
                // exit v4 are not aware of each other, and each will attempt to let
                // the other handle it, so neither of them do. To correct this, we
                // detect if we're the only handler *except* for previous versions
                // of signal-exit, and increment by the count of listeners it has
                // created.
                /* c8 ignore start */
                const p = process;
                if (typeof p.__signal_exit_emitter__ === 'object' &&
                    typeof p.__signal_exit_emitter__.count === 'number') {
                    count += p.__signal_exit_emitter__.count;
                }
                /* c8 ignore stop */
                if (listeners.length === count) {
                    this.unload();
                    const ret = this.#emitter.emit('exit', null, sig);
                    /* c8 ignore start */
                    const s = sig === 'SIGHUP' ? this.#hupSig : sig;
                    if (!ret)
                        process.kill(process.pid, s);
                    /* c8 ignore stop */
                }
            };
        }
        this.#originalProcessReallyExit = process.reallyExit;
        this.#originalProcessEmit = process.emit;
    }
    onExit(cb, opts) {
        /* c8 ignore start */
        if (!processOk(this.#process)) {
            return () => { };
        }
        /* c8 ignore stop */
        if (this.#loaded === false) {
            this.load();
        }
        const ev = opts?.alwaysLast ? 'afterExit' : 'exit';
        this.#emitter.on(ev, cb);
        return () => {
            this.#emitter.removeListener(ev, cb);
            if (this.#emitter.listeners['exit'].length === 0 &&
                this.#emitter.listeners['afterExit'].length === 0) {
                this.unload();
            }
        };
    }
    load() {
        if (this.#loaded) {
            return;
        }
        this.#loaded = true;
        // This is the number of onSignalExit's that are in play.
        // It's important so that we can count the correct number of
        // listeners on signals, and don't wait for the other one to
        // handle it instead of us.
        this.#emitter.count += 1;
        for (const sig of signals) {
            try {
                const fn = this.#sigListeners[sig];
                if (fn)
                    this.#process.on(sig, fn);
            }
            catch (_) { }
        }
        this.#process.emit = (ev, ...a) => {
            return this.#processEmit(ev, ...a);
        };
        this.#process.reallyExit = (code) => {
            return this.#processReallyExit(code);
        };
    }
    unload() {
        if (!this.#loaded) {
            return;
        }
        this.#loaded = false;
        signals.forEach(sig => {
            const listener = this.#sigListeners[sig];
            /* c8 ignore start */
            if (!listener) {
                throw new Error('Listener not defined for signal: ' + sig);
            }
            /* c8 ignore stop */
            try {
                this.#process.removeListener(sig, listener);
                /* c8 ignore start */
            }
            catch (_) { }
            /* c8 ignore stop */
        });
        this.#process.emit = this.#originalProcessEmit;
        this.#process.reallyExit = this.#originalProcessReallyExit;
        this.#emitter.count -= 1;
    }
    #processReallyExit(code) {
        /* c8 ignore start */
        if (!processOk(this.#process)) {
            return 0;
        }
        this.#process.exitCode = code || 0;
        /* c8 ignore stop */
        this.#emitter.emit('exit', this.#process.exitCode, null);
        return this.#originalProcessReallyExit.call(this.#process, this.#process.exitCode);
    }
    #processEmit(ev, ...args) {
        const og = this.#originalProcessEmit;
        if (ev === 'exit' && processOk(this.#process)) {
            if (typeof args[0] === 'number') {
                this.#process.exitCode = args[0];
                /* c8 ignore start */
            }
            /* c8 ignore start */
            const ret = og.call(this.#process, ev, ...args);
            /* c8 ignore start */
            this.#emitter.emit('exit', this.#process.exitCode, null);
            /* c8 ignore stop */
            return ret;
        }
        else {
            return og.call(this.#process, ev, ...args);
        }
    }
}
const process$1 = globalThis.process;
// wrap so that we call the method on the actual handler, without
// exporting it directly.
const { 
/**
 * Called when the process is exiting, whether via signal, explicit
 * exit, or running out of stuff to do.
 *
 * If the global process object is not suitable for instrumentation,
 * then this will be a no-op.
 *
 * Returns a function that may be used to unload signal-exit.
 */
onExit} = signalExitWrap(processOk(process$1) ? new SignalExit(process$1) : new SignalExitFallback());

// If the `cleanup` option is used, call `subprocess.kill()` when the parent process exits
const cleanupOnExit = (kill, {cleanup, detached}, {signal}) => {
	if (!cleanup || detached) {
		return;
	}

	const removeExitHandler = onExit(() => {
		kill();
	});
	addAbortListener(signal, () => {
		removeExitHandler();
	});
};

// When using multiple `.readable()`/`.writable()`/`.duplex()`, `final` and `destroy` should wait for other streams
const initializeConcurrentStreams = () => ({
	readableDestroy: new WeakMap(),
	writableFinal: new WeakMap(),
	writableDestroy: new WeakMap(),
});

// Each file descriptor + `waitName` has its own array of promises.
// Each promise is a single `.readable()`/`.writable()`/`.duplex()` call.
const addConcurrentStream = (concurrentStreams, stream, waitName) => {
	const weakMap = concurrentStreams[waitName];
	if (!weakMap.has(stream)) {
		weakMap.set(stream, []);
	}

	const promises = weakMap.get(stream);
	const promise = createDeferred();
	promises.push(promise);
	const resolve = promise.resolve.bind(promise);
	return {resolve, promises};
};

// Wait for other streams, but stop waiting when subprocess ends
const waitForConcurrentStreams = async ({resolve, promises}, subprocess) => {
	resolve();
	const [isSubprocessExit] = await Promise.race([
		Promise.allSettled([true, subprocess]),
		Promise.all([false, ...promises]),
	]);
	return !isSubprocessExit;
};

// Iterate over lines of `subprocess.stdout`, used by `subprocess.readable|duplex|iterable()`
const iterateOnSubprocessStream = ({subprocessStdout, subprocess, binary, shouldEncode, encoding, preserveNewlines}) => {
	const controller = new AbortController();
	stopReadingOnExit(subprocess, controller);
	return iterateOnStream({
		stream: subprocessStdout,
		controller,
		binary,
		shouldEncode: !subprocessStdout.readableObjectMode && shouldEncode,
		encoding,
		shouldSplit: !subprocessStdout.readableObjectMode,
		preserveNewlines,
	});
};

const stopReadingOnExit = async (subprocess, controller) => {
	try {
		await subprocess;
	} catch {} finally {
		controller.abort();
	}
};

// Iterate over lines of `subprocess.stdout`, used by `result.stdout` and the `verbose: 'full'` option.
// Applies the `lines` and `encoding` options.
const iterateForResult = ({stream, onStreamEnd, lines, encoding, stripFinalNewline, allMixed}) => {
	const controller = new AbortController();
	stopReadingOnStreamEnd(onStreamEnd, controller, stream);
	const objectMode = stream.readableObjectMode && !allMixed;
	return iterateOnStream({
		stream,
		controller,
		binary: encoding === 'buffer',
		shouldEncode: !objectMode,
		encoding,
		shouldSplit: !objectMode && lines,
		preserveNewlines: !stripFinalNewline,
	});
};

const stopReadingOnStreamEnd = async (onStreamEnd, controller, stream) => {
	try {
		await onStreamEnd;
	} catch {
		stream.destroy();
	} finally {
		controller.abort();
	}
};

const iterateOnStream = ({stream, controller, binary, shouldEncode, encoding, shouldSplit, preserveNewlines}) => {
	const onStdoutChunk = on(stream, 'data', {
		signal: controller.signal,
		highWaterMark: HIGH_WATER_MARK,
		// Backward compatibility with older name for this option
		// See https://github.com/nodejs/node/pull/52080#discussion_r1525227861
		// @todo Remove after removing support for Node 21
		highWatermark: HIGH_WATER_MARK,
	});
	return iterateOnData({
		onStdoutChunk,
		controller,
		binary,
		shouldEncode,
		encoding,
		shouldSplit,
		preserveNewlines,
	});
};

const DEFAULT_OBJECT_HIGH_WATER_MARK = getDefaultHighWaterMark(true);

// The `highWaterMark` of `events.on()` is measured in number of events, not in bytes.
// Not knowing the average amount of bytes per `data` event, we use the same heuristic as streams in objectMode, since they have the same issue.
// Therefore, we use the value of `getDefaultHighWaterMark(true)`.
// Note: this option does not exist on Node 18, but this is ok since the logic works without it. It just consumes more memory.
const HIGH_WATER_MARK = DEFAULT_OBJECT_HIGH_WATER_MARK;

const iterateOnData = async function * ({onStdoutChunk, controller, binary, shouldEncode, encoding, shouldSplit, preserveNewlines}) {
	const generators = getGenerators({
		binary,
		shouldEncode,
		encoding,
		shouldSplit,
		preserveNewlines,
	});

	try {
		for await (const [chunk] of onStdoutChunk) {
			yield * transformChunkSync(chunk, generators, 0);
		}
	} catch (error) {
		if (!controller.signal.aborted) {
			throw error;
		}
	} finally {
		yield * finalChunksSync(generators);
	}
};

const getGenerators = ({binary, shouldEncode, encoding, shouldSplit, preserveNewlines}) => [
	getEncodingTransformGenerator(binary, encoding, !shouldEncode),
	getSplitLinesGenerator(binary, preserveNewlines, !shouldSplit, {}),
].filter(Boolean);

// Convert the subprocess to an async iterable
const createIterable = (subprocess, encoding, options) => {
	const {from, binary: binaryOption = false, preserveNewlines = false} = copyOptions(options);
	checkBooleanOption(binaryOption, 'binary');
	checkBooleanOption(preserveNewlines, 'preserveNewlines');
	const binary = binaryOption || BINARY_ENCODINGS.has(encoding);
	const subprocessStdout = getFromStream(subprocess, from);
	const onStdoutData = iterateOnSubprocessStream({
		subprocessStdout,
		subprocess,
		binary,
		shouldEncode: true,
		encoding,
		preserveNewlines,
	});
	return iterateOnStdoutData(onStdoutData, subprocessStdout, subprocess);
};

const iterateOnStdoutData = async function * (onStdoutData, subprocessStdout, subprocess) {
	try {
		yield * onStdoutData;
	} finally {
		if (subprocessStdout.readable) {
			subprocessStdout.destroy();
		}

		await subprocess;
	}
};

// Wraps `finished(stream)` to handle the following case:
//  - When the subprocess exits, Node.js automatically calls `subprocess.stdin.destroy()`, which we need to ignore.
//  - However, we still need to throw if `subprocess.stdin.destroy()` is called before subprocess exit.
const waitForStream = async (stream, fdNumber, streamInfo, {
	isSameDirection,
	stopOnExit = false,
	readable = true,
	writable = true,
} = {}) => {
	const state = handleStdinDestroy(stream, streamInfo);
	const abortController = new AbortController();
	try {
		await Promise.race([
			...(stopOnExit ? [streamInfo.exitPromise] : []),
			finished(stream, {
				cleanup: true,
				signal: abortController.signal,
				readable,
				writable,
			}),
		]);
	} catch (error) {
		if (!state.stdinCleanedUp && !isInputTransformAbort(stream, fdNumber, streamInfo, error)) {
			handleStreamError(error, fdNumber, streamInfo, isSameDirection);
		}
	} finally {
		abortController.abort();
	}
};

// If `subprocess.stdin` is destroyed before being fully written to, it is considered aborted and should throw an error.
// This can happen for example when user called `subprocess.stdin.destroy()` before `subprocess.stdin.end()`.
// However, Node.js calls `subprocess.stdin.destroy()` on exit for cleanup purposes.
// https://github.com/nodejs/node/blob/0b4cdb4b42956cbd7019058e409e06700a199e11/lib/internal/child_process.js#L278
// This is normal and should not throw an error.
// Therefore, we need to differentiate between both situations to know whether to throw an error.
// Unfortunately, events (`close`, `error`, `end`, `exit`) cannot be used because `.destroy()` can take an arbitrary amount of time.
// For example, `stdin: 'pipe'` is implemented as a TCP socket, and its `.destroy()` method waits for TCP disconnection.
// Therefore `.destroy()` might end before or after subprocess exit, based on OS speed and load.
// The only way to detect this is to spy on `subprocess.stdin._destroy()` by wrapping it.
// If `subprocess.exitCode` or `subprocess.signalCode` is set, it means `.destroy()` is being called by Node.js itself.
// With an input transform, `subprocess.stdin` is that transform, so Node.js destroys it instead of the original `stdin`.
const handleStdinDestroy = (stream, {originalStreams, subprocess}) => {
	const [originalStdin] = originalStreams;
	const state = {stdinCleanedUp: false};
	if (stream === originalStdin || stream === subprocess.stdin) {
		spyOnStdinDestroy(stream, subprocess, state);
	}

	return state;
};

const spyOnStdinDestroy = (subprocessStdin, subprocess, state) => {
	const {_destroy} = subprocessStdin;
	subprocessStdin._destroy = (...destroyArguments) => {
		setStdinCleanedUp(subprocess, state);
		_destroy.call(subprocessStdin, ...destroyArguments);
	};
};

const setStdinCleanedUp = ({exitCode, signalCode}, state) => {
	if (exitCode !== null || signalCode !== null) {
		state.stdinCleanedUp = true;
	}
};

// An input transform writes to the subprocess' original stream. When that stream is closed first, e.g. when the subprocess exits without reading its input, the transform is aborted.
// Like `EPIPE` with `stdin: 'pipe'`, this is not an error.
// When the transform is aborted by the user instead, the original stream is still open at that point.
const isInputTransformAbort = (stream, fdNumber, streamInfo, error) => {
	const originalStream = streamInfo.originalStreams[fdNumber];
	return stream !== originalStream
		&& isInputFileDescriptor(streamInfo, fdNumber)
		&& originalStream?.destroyed === true
		// A `TransformStream` is aborted with an `AbortError`
		&& (isStreamAbort(error) || error?.code === 'ABORT_ERR');
};

// We ignore EPIPEs on writable streams and aborts on readable streams since those can happen normally.
// When one stream errors, the error is propagated to the other streams on the same file descriptor.
// Those other streams might have a different direction due to the above.
// When this happens, the direction of both the initial stream and the others should then be taken into account.
// Therefore, we keep track of whether a stream error is currently propagating.
const handleStreamError = (error, fdNumber, streamInfo, isSameDirection) => {
	if (!shouldIgnoreStreamError(error, fdNumber, streamInfo, isSameDirection)) {
		throw error;
	}
};

const shouldIgnoreStreamError = (error, fdNumber, streamInfo, isSameDirection = true) => {
	if (streamInfo.propagating) {
		return isStreamEpipe(error) || isStreamAbort(error);
	}

	streamInfo.propagating = true;
	return isInputFileDescriptor(streamInfo, fdNumber) === isSameDirection
		? isStreamEpipe(error)
		: isStreamAbort(error);
};

// Unfortunately, we cannot use the stream's class or properties to know whether it is readable or writable.
// For example, `subprocess.stdin` is technically a Duplex, but can only be used as a writable.
// Therefore, we need to use the file descriptor's direction (`stdin` is input, `stdout` is output, etc.).
// However, while `subprocess.std*` and transforms follow that direction, any stream passed the `std*` option has the opposite direction.
// For example, `subprocess.stdin` is a writable, but the `stdin` option is a readable.
const isInputFileDescriptor = ({fileDescriptors}, fdNumber) => fdNumber !== 'all' && fileDescriptors[fdNumber].direction === 'input';

// When `stream.destroy()` is called without an `error` argument, stream is aborted.
// This is the only way to abort a readable stream, which can be useful in some instances.
// Therefore, we ignore this error on readable streams.
// Breaking out of `for await (const chunk of subprocess.stdout)` aborts it with an `AbortError` instead. However, that error cannot be told apart from an `AbortError` thrown by a transform, which must make the subprocess fail. So this is not supported, and `subprocess.readable()` or `subprocess.iterable()` should be used instead.
const isStreamAbort = error => error?.code === 'ERR_STREAM_PREMATURE_CLOSE';

// When `stream.write()` is called but the underlying source has been closed, `EPIPE` is emitted.
// When piping subprocesses, the source subprocess usually decides when to stop piping.
// However, there are some instances when the destination does instead, such as `... | head -n1`.
// It notifies the source by using `EPIPE`.
// Therefore, we ignore this error on writable streams.
const isStreamEpipe = error => error?.code === 'EPIPE';

const safeWaitForSubprocessStdin = async subprocessStdin => {
	if (subprocessStdin === undefined) {
		return;
	}

	try {
		await waitForSubprocessStdin(subprocessStdin);
	} catch {}
};

const safeWaitForSubprocessStdout = async subprocessStdout => {
	if (subprocessStdout === undefined) {
		return;
	}

	try {
		await waitForSubprocessStdout(subprocessStdout);
	} catch {}
};

const waitForSubprocessStdin = async subprocessStdin => {
	await finished(subprocessStdin, {cleanup: true, readable: false, writable: true});
};

const waitForSubprocessStdout = async subprocessStdout => {
	await finished(subprocessStdout, {cleanup: true, readable: true, writable: false});
};

// When `readable` or `writable` aborts/errors, the caller awaits the subprocess, for the reason mentioned above.
// Then, the error passed to `stream.destroy(error)` is emitted, including when the subprocess has already ended.
const throwDestroyError = error => {
	if (error) {
		throw error;
	}
};

const destroyOtherStream = (stream, isOpen, error) => {
	if (error && !isStreamAbort(error)) {
		stream.destroy(error);
	} else if (isOpen) {
		stream.destroy();
	}
};

// Create a `Readable` stream that forwards from `stdout` and awaits the subprocess
const createReadable = ({subprocess, concurrentStreams, encoding}, options) => {
	const {from, binary: binaryOption = true, preserveNewlines = true} = copyOptions(options);
	checkBooleanOption(binaryOption, 'binary');
	checkBooleanOption(preserveNewlines, 'preserveNewlines');
	const binary = binaryOption || BINARY_ENCODINGS.has(encoding);
	const {subprocessStdout, waitReadableDestroy} = getSubprocessStdout(subprocess, from, concurrentStreams);
	const {readableEncoding, readableObjectMode, readableHighWaterMark} = getReadableOptions(subprocessStdout, binary);
	const {read, onStdoutDataDone} = getReadableMethods({
		subprocessStdout,
		subprocess,
		binary,
		encoding,
		preserveNewlines,
	});
	const readable = new Readable({
		read,
		destroy: callbackify(onReadableDestroy.bind(undefined, {subprocessStdout, subprocess, waitReadableDestroy})),
		highWaterMark: readableHighWaterMark,
		objectMode: readableObjectMode,
		encoding: readableEncoding,
	});
	onStdoutFinished({
		subprocessStdout,
		onStdoutDataDone,
		readable,
		subprocess,
	});
	return readable;
};

// Retrieve `stdout` (or other stream depending on `from`)
const getSubprocessStdout = (subprocess, from, concurrentStreams) => {
	const subprocessStdout = getFromStream(subprocess, from);
	const waitReadableDestroy = addConcurrentStream(concurrentStreams, subprocessStdout, 'readableDestroy');
	return {subprocessStdout, waitReadableDestroy};
};

const getReadableOptions = ({readableEncoding, readableObjectMode, readableHighWaterMark}, binary) => binary
	? {readableEncoding, readableObjectMode, readableHighWaterMark}
	: {readableEncoding, readableObjectMode: true, readableHighWaterMark: DEFAULT_OBJECT_HIGH_WATER_MARK};

const getReadableMethods = ({subprocessStdout, subprocess, binary, encoding, preserveNewlines}) => {
	const onStdoutDataDone = createDeferred();
	const onStdoutData = iterateOnSubprocessStream({
		subprocessStdout,
		subprocess,
		binary,
		shouldEncode: !binary,
		encoding,
		preserveNewlines,
	});

	return {
		read() {
			onRead(this, onStdoutData, onStdoutDataDone);
		},
		onStdoutDataDone,
	};
};

// Forwards data from `stdout` to `readable`
const onRead = async (readable, onStdoutData, onStdoutDataDone) => {
	try {
		const {value, done} = await onStdoutData.next();
		if (done) {
			onStdoutDataDone.resolve();
		} else {
			readable.push(value);
		}
	} catch {}
};

// When `subprocess.stdout` ends/aborts/errors, do the same on `readable`.
// Await the subprocess, for the same reason as above.
const onStdoutFinished = async ({subprocessStdout, onStdoutDataDone, readable, subprocess, subprocessStdin}) => {
	try {
		await waitForSubprocessStdout(subprocessStdout);
		await subprocess;
		await safeWaitForSubprocessStdin(subprocessStdin);
		await onStdoutDataDone;

		if (readable.readable) {
			readable.push(null);
		}
	} catch (error) {
		await safeWaitForSubprocessStdin(subprocessStdin);
		destroyOtherReadable(readable, await getPrematureCloseError(subprocess, error));
	}
};

const getPrematureCloseError = async (subprocess, error) => {
	if (error.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
		return error;
	}

	try {
		await subprocess;
	} catch (subprocessError) {
		return subprocessError;
	}

	return error;
};

// When `readable` aborts/errors, do the same on `subprocess.stdout`
const onReadableDestroy = async ({subprocessStdout, subprocess, waitReadableDestroy}, error) => {
	if (await waitForConcurrentStreams(waitReadableDestroy, subprocess)) {
		// Breaking out of `for await (const chunk of readable)` or cancelling `subprocess.readableStream()` destroys `readable` with an `AbortError`. Like `readable.destroy()`, this aborts `subprocess.stdout` instead of making it fail.
		destroyOtherReadable(subprocessStdout, error?.code === 'ABORT_ERR' ? undefined : error);
		await subprocess;
	}

	throwDestroyError(error);
};

const destroyOtherReadable = (stream, error) => {
	destroyOtherStream(stream, stream.readable, error);
};

// Web stream versions of `subprocess.readable()`, `subprocess.writable()` and `subprocess.duplex()`.
// They wrap those methods with `.toWeb()`, so all error and abort handling is reused as is.
const createReadableStream = (subprocess, readableOptions) => Readable.toWeb(subprocess.readable(readableOptions));
const createWritableStream = (subprocess, writableOptions) => Writable.toWeb(subprocess.writable(writableOptions));
const createTransformStream = (subprocess, duplexOptions) => Duplex.toWeb(subprocess.duplex(duplexOptions));

// Normalize and validate arguments passed to `source.pipe(destination)`
const normalizePipeArguments = ({source, sourcePromise, boundOptions, createNested}, ...pipeArguments) => {
	const startTime = getStartTime();
	const {
		destination,
		destinationStream,
		destinationError,
		from,
		unpipeSignal,
	} = getDestinationStream(boundOptions, createNested, pipeArguments);
	const {sourceStream, sourceError} = getSourceStream(source, from);
	const {options: sourceOptions, fileDescriptors} = SUBPROCESS_OPTIONS.get(source);
	return {
		source,
		sourcePromise,
		sourceStream,
		sourceOptions,
		sourceError,
		destination,
		destinationStream,
		destinationError,
		unpipeSignal,
		fileDescriptors,
		startTime,
	};
};

const getDestinationStream = (boundOptions, createNested, pipeArguments) => {
	let destinationInfo;

	try {
		destinationInfo = getDestination(boundOptions, createNested, ...pipeArguments);
	} catch (error) {
		return {destinationError: error};
	}

	const {destination, pipeOptions} = destinationInfo;

	try {
		const {from, to, unpipeSignal} = copyOptions(pipeOptions);
		validateAbortSignal(unpipeSignal, 'unpipeSignal');
		const destinationStream = getToStream(destination, to);
		return {
			destination,
			destinationStream,
			from,
			unpipeSignal,
		};
	} catch (error) {
		abortDestination(destination, pipeArguments[0]);
		return {destinationError: error};
	}
};

/*
The destination has already been spawned at that point, since the `to` option is validated afterwards.
When Execa spawned it, nothing else can end it, so it is terminated. Otherwise, it would keep running, e.g. waiting for its `stdin` forever, which also prevents the current process from exiting. A destination passed by the user is left to them.
In both cases, its error must be handled, otherwise it becomes an unhandled rejection which crashes the current process, e.g. with `source.pipe(execa('command'), {to: 'fd9'})`. This does not prevent the user from awaiting it.
*/
const abortDestination = (destination, firstArgument) => {
	if (destination !== firstArgument) {
		destination.kill();
	}

	destination.catch(() => {});
};

// Piping subprocesses can use three syntaxes:
//  - source.pipe('command', commandArguments, pipeOptionsOrDestinationOptions)
//  - source.pipe`command commandArgument` or source.pipe(pipeOptionsOrDestinationOptions)`command commandArgument`
//  - source.pipe(execa(...), pipeOptions)
const getDestination = (boundOptions, createNested, firstArgument, ...pipeArguments) => {
	if (Array.isArray(firstArgument)) {
		const destination = createNested(mapDestinationArguments, boundOptions)(firstArgument, ...pipeArguments);
		return {destination, pipeOptions: boundOptions};
	}

	if (typeof firstArgument === 'string' || firstArgument instanceof URL || isDenoExecPath(firstArgument)) {
		if (Object.keys(boundOptions).length > 0) {
			throw new TypeError('Please use .pipe("file", ..., options) or .pipe(execa("file", ..., options)) instead of .pipe(options)("file", ...).');
		}

		const [rawFile, rawArguments, rawOptions] = normalizeParameters(firstArgument, ...pipeArguments);
		const destination = createNested(mapDestinationArguments)(rawFile, rawArguments, rawOptions);
		return {destination, pipeOptions: rawOptions};
	}

	if (SUBPROCESS_OPTIONS.has(firstArgument)) {
		if (Object.keys(boundOptions).length > 0) {
			throw new TypeError('Please use .pipe(options)`command` or .pipe($(options)`command`) instead of .pipe(options)($`command`).');
		}

		return {destination: firstArgument, pipeOptions: pipeArguments[0]};
	}

	throw new TypeError(`The first argument must be a template string, an options object, or an Execa subprocess: ${firstArgument}`);
};

// Force `stdin: 'pipe'` with the destination subprocess
const mapDestinationArguments = ({options}) => ({
	options: {
		__proto__: null,
		...options,
		// Piping writes to the destination's `stdin`, which is forced even if the user set another value.
		// However, `stdin` cannot be combined with `stdio`, which is needed for `to: 'fd3'` and higher.
		...((options.stdio === undefined) && {stdin: 'pipe'}),
		piped: true,
	},
});

const getSourceStream = (source, from) => {
	try {
		const sourceStream = getFromStream(source, from);
		return {sourceStream};
	} catch (error) {
		return {sourceError: error};
	}
};

// When passing invalid arguments to `source.pipe()`, throw asynchronously.
// We also abort both subprocesses.
const handlePipeArgumentsError = ({
	sourceStream,
	sourceError,
	destinationStream,
	destinationError,
	fileDescriptors,
	sourceOptions,
	startTime,
}) => {
	const error = getPipeArgumentsError({
		sourceStream,
		sourceError,
		destinationStream,
		destinationError,
	});
	if (error !== undefined) {
		throw createNonCommandError({
			error,
			fileDescriptors,
			sourceOptions,
			startTime,
		});
	}
};

const getPipeArgumentsError = ({sourceStream, sourceError, destinationStream, destinationError}) => {
	if (sourceError !== undefined && destinationError !== undefined) {
		return destinationError;
	}

	if (destinationError !== undefined) {
		abortSourceStream(sourceStream);
		return destinationError;
	}

	if (sourceError === undefined) {
		return;
	}

	endDestinationStream(destinationStream);
	return sourceError;
};

// Specific error return value when passing invalid arguments to `subprocess.pipe()` or when using `unpipeSignal`
const createNonCommandError = ({error, fileDescriptors, sourceOptions, startTime}) => makeEarlyError({
	error,
	command: PIPE_COMMAND_MESSAGE,
	escapedCommand: PIPE_COMMAND_MESSAGE,
	fileDescriptors,
	options: sourceOptions,
	startTime,
	isSync: false,
});

const PIPE_COMMAND_MESSAGE = 'source.pipe(destination)';

// Like Bash, we await both subprocesses. This is unlike some other shells which only await the destination subprocess.
// Like Bash with the `pipefail` option, if either subprocess fails, the whole pipe fails.
// Like Bash, if both subprocesses fail, we return the failure of the destination.
// This ensures both subprocesses' errors are present, using `error.pipedFrom`.
const waitForBothSubprocesses = async subprocessPromises => {
	const [
		{status: sourceStatus, reason: sourceReason, value: sourceResult = sourceReason},
		{status: destinationStatus, reason: destinationReason, value: destinationResult = destinationReason},
		{reason: pipedFromReason, value: pipedFromResult = pipedFromReason},
	] = await subprocessPromises;

	if (!destinationResult.pipedFrom.includes(pipedFromResult)) {
		destinationResult.pipedFrom.push(pipedFromResult);
	}

	if (destinationStatus === 'rejected') {
		throw destinationResult;
	}

	if (sourceStatus === 'rejected') {
		throw sourceResult;
	}

	return destinationResult;
};

// The piping behavior is like Bash.
// In particular, when one subprocess exits, the other is not terminated by a signal.
// Instead, its stdout (for the source) or stdin (for the destination) closes.
// If the subprocess uses it, it will make it error with SIGPIPE or EPIPE (for the source) or end (for the destination).
// If it does not use it, it will continue running.
// This allows for subprocesses to gracefully exit and lower the coupling between subprocesses.
const pipeSubprocessStream = (sourceStream, destinationStream, maxListenersController) => {
	const mergedStream = MERGED_STREAMS.has(destinationStream)
		? pipeMoreSubprocessStream(sourceStream, destinationStream)
		: pipeFirstSubprocessStream(sourceStream, destinationStream);
	incrementMaxListeners(sourceStream, SOURCE_LISTENERS_PER_PIPE, maxListenersController.signal);
	incrementMaxListeners(destinationStream, DESTINATION_LISTENERS_PER_PIPE, maxListenersController.signal);
	cleanupMergedStreamsMap(destinationStream);
	return mergedStream;
};

// We use `merge-streams` to allow for multiple sources to pipe to the same destination.
const pipeFirstSubprocessStream = (sourceStream, destinationStream) => {
	const mergedStream = mergeStreams([sourceStream]);
	addMergeSource(mergedStream, sourceStream);
	pipeStreams(mergedStream, destinationStream, () => unpipeSourcesOnDestinationEnd(mergedStream));
	MERGED_STREAMS.set(destinationStream, mergedStream);
	return mergedStream;
};

const pipeMoreSubprocessStream = (sourceStream, destinationStream) => {
	const mergedStream = MERGED_STREAMS.get(destinationStream);
	mergedStream.add(sourceStream);
	addMergeSource(mergedStream, sourceStream);
	return mergedStream;
};

/*
One source stream can be piped to multiple destinations, e.g. `subprocess.pipe(first)` and `subprocess.pipe(second)`.
When a destination ends, its source must only be aborted when it is its last destination, so the other destinations keep receiving data.
Sources with several destinations are then only removed from the ended destination's merge stream, instead of being aborted.
This also holds when a source has been piped to one destination and unpiped from another one.
*/
const MERGE_SOURCES = new WeakMap();
const SOURCE_MERGES = new WeakMap();

const addMergeSource = (mergedStream, sourceStream) => {
	addToSetMap(MERGE_SOURCES, mergedStream, sourceStream);
	addToSetMap(SOURCE_MERGES, sourceStream, mergedStream);
};

const addToSetMap = (map, key, value) => {
	if (!map.has(key)) {
		map.set(key, new Set());
	}

	map.get(key).add(value);
};

// Stop piping a source into a destination, e.g. with the `unpipeSignal` option
const unpipeSubprocessStream = async (mergedStream, sourceStream) => {
	MERGE_SOURCES.get(mergedStream)?.delete(sourceStream);
	SOURCE_MERGES.get(sourceStream)?.delete(mergedStream);
	await mergedStream.remove(sourceStream);
};

const unpipeSourcesOnDestinationEnd = async mergedStream => {
	for (const sourceStream of MERGE_SOURCES.get(mergedStream)) {
		const mergedStreams = SOURCE_MERGES.get(sourceStream);
		mergedStreams.delete(mergedStream);
		if (mergedStreams.size > 0) {
			// Other destinations still need that source, so it must not be aborted
			// eslint-disable-next-line no-await-in-loop
			await mergedStream.remove(sourceStream);
		}
	}
};

const cleanupMergedStreamsMap = async destinationStream => {
	try {
		await finished(destinationStream, {cleanup: true, readable: false, writable: true});
	} catch {}

	MERGED_STREAMS.delete(destinationStream);
};

const MERGED_STREAMS = new WeakMap();

// Number of listeners set up on `sourceStream` by each `sourceStream.pipe(destinationStream)`
// Those are added by `merge-streams`
const SOURCE_LISTENERS_PER_PIPE = 2;
// Number of listeners set up on `destinationStream` by each `sourceStream.pipe(destinationStream)`
// Those are added by `finished()` in `cleanupMergedStreamsMap()`
const DESTINATION_LISTENERS_PER_PIPE = 1;

// When passing an `unpipeSignal` option, abort piping when the signal is aborted.
// However, do not terminate the subprocesses.
const unpipeOnAbort = (unpipeSignal, unpipeContext) => unpipeSignal === undefined
	? []
	: [unpipeOnSignalAbort(unpipeSignal, unpipeContext)];

const unpipeOnSignalAbort = async (unpipeSignal, {sourceStream, mergedStream, fileDescriptors, sourceOptions, startTime}) => {
	await aborted(unpipeSignal, sourceStream);
	await unpipeSubprocessStream(mergedStream, sourceStream);
	const error = new Error('Pipe canceled by `unpipeSignal` option.');
	throw createNonCommandError({
		error,
		fileDescriptors,
		sourceOptions,
		startTime,
	});
};

// Pipe a subprocess' `stdout`/`stderr`/`stdio` into another subprocess' `stdin`
const pipeToSubprocess = (sourceInfo, ...pipeArguments) => {
	if (isPlainObject$2(pipeArguments[0])) {
		return pipeToSubprocess.bind(undefined, {
			...sourceInfo,
			boundOptions: {...sourceInfo.boundOptions, ...pipeArguments[0]},
		});
	}

	// The previous `.pipe()` failed because of its arguments, so there is no source. Its promise rejects with that error.
	// The destination is not spawned, since nothing would end its `stdin`.
	if (sourceInfo.source === undefined) {
		return sourceInfo.sourcePromise;
	}

	const {destination, ...normalizedInfo} = normalizePipeArguments(sourceInfo, ...pipeArguments);
	const pipeFailureController = new AbortController();
	const promise = handlePipePromise({...normalizedInfo, destination, pipeFailureController});
	promise.pipe = pipeToSubprocess.bind(undefined, {
		...sourceInfo,
		source: destination,
		sourcePromise: promise,
		boundOptions: {},
	});
	forwardDestinationMethods(promise, destination, pipeFailureController.signal);
	return promise;
};

/*
The return value of `.pipe()` exposes the destination subprocess' output, but its iteration and stream conversion methods must await the whole pipe so source failures are propagated too. The destination is `undefined` when `.pipe()` was passed invalid arguments, in which case the promise rejects and there is nothing to forward.
*/
const forwardDestinationMethods = (promise, destination, pipeFailureSignal) => {
	if (destination === undefined) {
		return;
	}

	forwardReadableMethods(promise, destination);
	forwardIpcMethods(promise, destination, pipeFailureSignal);
};

const forwardReadableMethods = (promise, destination) => {
	const subprocessOptions = SUBPROCESS_OPTIONS.get(destination);
	SUBPROCESS_OPTIONS.set(promise, subprocessOptions);
	promise.stdio = destination.stdio;
	promise.all = destination.all;

	const {options: {encoding}} = subprocessOptions;
	const concurrentStreams = initializeConcurrentStreams();
	promise[Symbol.asyncIterator] = createIterable.bind(undefined, promise, encoding, {});
	promise.iterable = createIterable.bind(undefined, promise, encoding);
	promise.readable = createPipeReadable.bind(undefined, promise, {
		subprocess: promise,
		concurrentStreams,
		encoding,
	});
	promise.readableStream = createReadableStream.bind(undefined, promise);
	forwardAll(promise, destination);
};

const forwardAll = (promise, destination) => {
	if (destination.all === undefined) {
		promise.all = undefined;
		return;
	}

	Object.defineProperty(promise, 'all', {
		get() {
			setAllProperty(promise, destination.all);
			const all = promise.readable({from: 'all'});
			// Like `subprocess.all`, the pipe's failure is handled by awaiting it, not by listening to the stream's `error` event. Consumers like `text(all)` still fail.
			all.on('error', () => {});
			setAllProperty(promise, all);
			return all;
		},
		enumerable: true,
		configurable: true,
	});
};

const setAllProperty = (promise, value) => {
	Object.defineProperty(promise, 'all', {
		value,
		writable: true,
		enumerable: true,
		configurable: true,
	});
};

const createPipeReadable = (promise, readableOptions, ...arguments_) => {
	const readable = createReadable(readableOptions, ...arguments_);
	destroyOnPipeFailure(promise, readable);
	return readable;
};

const destroyOnPipeFailure = async (promise, readable) => {
	try {
		await promise;
	} catch (error) {
		readable.destroy(error);
	}
};

const forwardIpcMethods = (promise, destination, pipeFailureSignal) => {
	promise.sendMessage = destination.sendMessage;
	promise.getOneMessage = getOnePipeMessage.bind(undefined, destination, pipeFailureSignal);
	promise.getEachMessage = getEachPipeMessage.bind(undefined, promise, destination, pipeFailureSignal);
};

const getOnePipeMessage = (destination, pipeFailureSignal, ...arguments_) => {
	const controller = new AbortController();
	const messagePromise = destination.getOneMessage(...addPipeOptions(arguments_, controller.signal, internalGetOneMessageOptions));
	return waitForOnePipeMessage(pipeFailureSignal, messagePromise, controller);
};

const waitForOnePipeMessage = async (pipeFailureSignal, messagePromise, controller) => {
	try {
		return await Promise.race([messagePromise, getSignalRejection(pipeFailureSignal, controller.signal)]);
	} finally {
		controller.abort();
	}
};

const getSignalRejection = (signal, listenerSignal) => new Promise((_, reject) => {
	if (signal.aborted) {
		reject(signal.reason);
		return;
	}

	signal.addEventListener('abort', () => {
		reject(signal.reason);
	}, {once: true, signal: listenerSignal});
});

const getEachPipeMessage = (promise, destination, pipeFailureSignal, ...arguments_) => {
	const controller = new AbortController();
	// Create the destination iterator before awaiting the pipe so option validation stays synchronous.
	const iterator = destination.getEachMessage(...addPipeOptions(arguments_, controller.signal, internalGetEachMessageOptions));
	abortOnSignal(pipeFailureSignal, controller);
	return iterateOnPipeMessages(promise, iterator, controller);
};

const iterateOnPipeMessages = async function * (promise, iterator, controller) {
	try {
		yield * iterator;
	} finally {
		controller.abort();
		await promise;
	}
};

const addPipeOptions = (arguments_, signal, internalOptionsSymbol) => {
	if (arguments_[0] === null) {
		// Preserve the public validation error from `getEachMessage(null)` instead of masking it with a pipe failure.
		return arguments_;
	}

	const [options] = arguments_;
	// The returned pipe promise is awaited by the forwarded IPC iterator, so the destination IPC iterator must not also await the destination subprocess on close.
	return [{...options, [internalOptionsSymbol]: {signal, shouldAwait: false}}];
};

const abortOnSignal = (signal, controller) => {
	if (signal.aborted) {
		controller.abort();
		return;
	}

	// Interrupt pending IPC reads when the pipe rejects, including `unpipeSignal` cancellation while the destination keeps running.
	signal.addEventListener('abort', () => {
		controller.abort();
	}, {once: true, signal: controller.signal});
};

// `writable()`, `duplex()`, `writableStream()` and `transformStream()` are intentionally not forwarded: they write to the destination's `stdin`, which is already being piped from the source.

// Asynchronous logic when piping subprocesses
const handlePipePromise = async ({
	source,
	sourcePromise,
	sourceStream,
	sourceOptions,
	sourceError,
	destination,
	destinationStream,
	destinationError,
	unpipeSignal,
	fileDescriptors,
	startTime,
	pipeFailureController,
}) => {
	const maxListenersController = new AbortController();
	try {
		const subprocessPromises = getSubprocessPromises(sourcePromise, destination, source);
		handlePipeArgumentsError({
			sourceStream,
			sourceError,
			destinationStream,
			destinationError,
			fileDescriptors,
			sourceOptions,
			startTime,
		});
		const mergedStream = pipeSubprocessStream(sourceStream, destinationStream, maxListenersController);
		return await Promise.race([
			waitForBothSubprocesses(subprocessPromises),
			...unpipeOnAbort(unpipeSignal, {
				sourceStream,
				mergedStream,
				sourceOptions,
				fileDescriptors,
				startTime,
			}),
		]);
	} catch (error) {
		pipeFailureController.abort(error);
		throw error;
	} finally {
		maxListenersController.abort();
	}
};

// `.pipe()` awaits the subprocess promises.
// When invalid arguments are passed to `.pipe()`, we throw an error, which prevents awaiting them.
// We need to ensure this does not create unhandled rejections.
// With `source.pipe(destination).pipe(secondDestination)`, the second `sourcePromise` is the first `.pipe()`, which rejects with the error of either subprocess. However, `secondDestination` is only piped from `destination`.
const getSubprocessPromises = (sourcePromise, destination, source) => Promise.allSettled([sourcePromise, destination, source]);

// Retrieve `result.stdout|stderr|all|stdio[*]`
const getStreamOutput = async ({stream, onStreamEnd, fdNumber, encoding, buffer, maxBuffer, lines, allMixed, stripFinalNewline, allFds, verboseInfo, streamInfo}) => {
	const logPromise = logOutputAsync({
		stream,
		onStreamEnd,
		fdNumber,
		encoding,
		allMixed,
		verboseInfo,
		streamInfo,
	});

	if (!buffer) {
		await Promise.all([resumeStream(stream), logPromise]);
		return;
	}

	const stripFinalNewlineValue = getStripFinalNewline(stripFinalNewline, fdNumber, allFds);
	const iterable = iterateForResult({
		stream,
		onStreamEnd,
		lines,
		encoding,
		stripFinalNewline: stripFinalNewlineValue,
		allMixed,
	});
	const [output] = await Promise.all([
		getStreamContents({
			stream,
			iterable,
			fdNumber,
			encoding,
			maxBuffer,
			lines,
		}),
		logPromise,
	]);
	return output;
};

const logOutputAsync = async ({stream, onStreamEnd, fdNumber, encoding, allMixed, verboseInfo, streamInfo: {fileDescriptors}}) => {
	if (!shouldLogOutput({
		stdioItems: fileDescriptors[fdNumber]?.stdioItems,
		encoding,
		verboseInfo,
		fdNumber,
	})) {
		return;
	}

	const linesIterable = iterateForResult({
		stream,
		onStreamEnd,
		lines: true,
		encoding,
		stripFinalNewline: true,
		allMixed,
	});
	await logLines(linesIterable, stream, fdNumber, verboseInfo);
};

// When using `buffer: false`, users need to read `subprocess.stdout|stderr|all` right away
// See https://github.com/sindresorhus/execa/issues/730 and https://github.com/sindresorhus/execa/pull/729#discussion_r1465496310
const resumeStream = async stream => {
	await setImmediate();
	if (stream.readableFlowing === null) {
		stream.resume();
	}
};

const getStreamContents = async ({stream, stream: {readableObjectMode}, iterable, fdNumber, encoding, maxBuffer, lines}) => {
	try {
		if (readableObjectMode || lines) {
			return await getStreamAsArray(iterable, {maxBuffer});
		}

		return encoding === 'buffer' ? new Uint8Array(await getStreamAsArrayBuffer(iterable, {maxBuffer})) : (await getStreamAsString(iterable, {maxBuffer}));
	} catch (error) {
		return handleBufferedData(handleMaxBuffer({
			error,
			stream,
			readableObjectMode,
			lines,
			encoding,
			fdNumber,
		}));
	}
};

// On failure, `result.stdout|stderr|all` should contain the currently buffered stream
// They are automatically closed and flushed by Node.js when the subprocess exits
// When `buffer` is `false`, `streamPromise` is `undefined` and there is no buffered data to retrieve
const getBufferedData = async streamPromise => {
	try {
		return await streamPromise;
	} catch (error) {
		return handleBufferedData(error);
	}
};

// Ensure we are returning Uint8Arrays when using `encoding: 'buffer'`
// The error might be any value thrown by the `verbose` function, including `null` or `undefined`
const handleBufferedData = error => isArrayBuffer(error?.bufferedData)
	? new Uint8Array(error.bufferedData)
	: error?.bufferedData;

// Read the contents of `subprocess.std*` and|or wait for its completion
const waitForStdioStreams = ({subprocess, encoding, buffer, maxBuffer, lines, stripFinalNewline, verboseInfo, streamInfo}) => subprocess.stdio.map((stream, fdNumber) => waitForSubprocessStream({
	stream,
	fdNumber,
	encoding,
	buffer: buffer[fdNumber],
	maxBuffer: maxBuffer[fdNumber],
	lines: lines[fdNumber],
	allMixed: false,
	stripFinalNewline,
	verboseInfo,
	streamInfo,
}));

// Read the contents of `subprocess.std*` or `subprocess.all` and|or wait for its completion
const waitForSubprocessStream = async ({stream, fdNumber, encoding, buffer, maxBuffer, lines, allMixed, stripFinalNewline, allFds, verboseInfo, streamInfo}) => {
	if (!stream) {
		return;
	}

	const onStreamEnd = waitForStream(stream, fdNumber, streamInfo);
	if (isInputFileDescriptor(streamInfo, fdNumber)) {
		await onStreamEnd;
		return;
	}

	const [output] = await Promise.all([
		getStreamOutput({
			stream,
			onStreamEnd,
			fdNumber,
			encoding,
			buffer,
			maxBuffer,
			lines,
			allMixed,
			stripFinalNewline,
			allFds,
			verboseInfo,
			streamInfo,
		}),
		onStreamEnd,
	]);
	return output;
};

// `all` interleaves `stdout` and `stderr`
const makeAllStream = ({stdout, stderr}, {all}) => {
	if (!all) {
		return;
	}

	const fileDescriptors = [[stdout, 1], [stderr, 2]].filter(([stream]) => stream);
	return fileDescriptors.length === 0 ? undefined : setAllStream(mergeStreams(fileDescriptors.map(([stream]) => stream)));
};

// The file descriptors which `result.all` contains, which the `all` options must follow
// Unlike `subprocess.all`, this excludes the file descriptors which are not buffered, since their output is not included
const getAllFds = ({stdout, stderr}, [, bufferStdout, bufferStderr]) => [
	...(stdout !== null && bufferStdout ? [1] : []),
	...(stderr !== null && bufferStderr ? [2] : []),
];

// Read the contents of `subprocess.all` and|or wait for its completion
const waitForAllStream = ({subprocess, all, encoding, buffer, maxBuffer, lines, stripFinalNewline, verboseInfo, streamInfo}) => {
	const {stream, buffer: bufferAll, drainedStream} = getAllStream(subprocess, all, buffer);
	return Promise.all([
		waitForSubprocessStream({
			stream,
			buffer: bufferAll,
			fdNumber: 'all',
			encoding,
			maxBuffer: maxBuffer[1] + maxBuffer[2],
			lines: lines[1] || lines[2],
			allFds: getAllFds(subprocess, buffer),
			allMixed: getAllMixed(subprocess, all),
			stripFinalNewline,
			verboseInfo,
			streamInfo,
		}),
		// When only one of the two file descriptors is buffered, its stream is read instead of the merged one.
		// The merged one is then drained, otherwise it would apply backpressure on that file descriptor and hang the subprocess.
		drainedStream === undefined ? undefined : resumeStream(drainedStream),
	]).then(([output]) => output);
};

const getAllStream = ({stdout, stderr}, all, [, bufferStdout, bufferStderr]) => {
	const buffer = bufferStdout || bufferStderr;
	if (!buffer) {
		return {stream: all, buffer};
	}

	if (!all) {
		return {stream: undefined, buffer};
	}

	if (!bufferStdout) {
		return {stream: stderr, buffer, drainedStream: all};
	}

	return bufferStderr ? {stream: all, buffer} : {stream: stdout, buffer, drainedStream: all};
};

// When `subprocess.stdout` is in objectMode but not `subprocess.stderr` (or the opposite), we need to use both:
//  - `getStreamAsArray()` for the chunks in objectMode, to return as an array without changing each chunk
//  - `getStreamAsArrayBuffer()` or `getStream()` for the chunks not in objectMode, to convert them from Buffers to string or Uint8Array
// We do this by emulating the Buffer -> string|Uint8Array conversion performed by `get-stream` with our own, which is identical.
const getAllMixed = ({stdout, stderr}, all) => all
	&& stdout
	&& stderr
	&& stdout.readableObjectMode !== stderr.readableObjectMode;

// When `verbose` is `'full'`, print IPC messages from the subprocess
const shouldLogIpc = verboseInfo => isFullVerbose(verboseInfo, 'ipc');

const logIpcOutput = (message, verboseInfo) => {
	const verboseMessage = serializeVerboseMessage(message);
	verboseLog({
		type: 'ipc',
		verboseMessage,
		fdNumber: 'ipc',
		verboseInfo,
	});
};

// Iterate through IPC messages sent by the subprocess
const waitForIpcOutput = async ({
	subprocess,
	buffer: bufferArray,
	maxBuffer: maxBufferArray,
	ipc,
	ipcOutput,
	verboseInfo,
}) => {
	if (!ipc) {
		return ipcOutput;
	}

	const isVerbose = shouldLogIpc(verboseInfo);
	const buffer = getFdSpecificValue(bufferArray, 'ipc');
	const maxBuffer = getFdSpecificValue(maxBufferArray, 'ipc');
	// A throwing `verbose` function must not prevent the following messages from being buffered
	const logErrors = [];

	for await (const message of loopOnMessages({
		anyProcess: subprocess,
		channel: subprocess.channel,
		isSubprocess: false,
		ipc,
		shouldAwait: false,
		reference: true,
	})) {
		if (buffer) {
			checkIpcMaxBuffer(ipcOutput, maxBuffer);
			ipcOutput.push(message);
		}

		if (isVerbose) {
			try {
				logIpcOutput(message, verboseInfo);
			} catch (error) {
				logErrors.push(error);
			}
		}
	}

	if (logErrors.length > 0) {
		throw logErrors[0];
	}

	return ipcOutput;
};

const getBufferedIpcOutput = async (ipcOutputPromise, ipcOutput) => {
	await Promise.allSettled([ipcOutputPromise]);
	return ipcOutput;
};

// Retrieve result of subprocess: exit code, signal, error, streams (stdout/stderr/all)
const waitForSubprocessResult = async ({
	subprocess,
	kill,
	all,
	options: {
		encoding,
		buffer,
		maxBuffer,
		lines,
		timeoutDuration: timeout,
		cancelSignal,
		gracefulCancel,
		forceKillAfterDelay,
		stripFinalNewline,
		ipc,
		ipcInput,
	},
	context,
	verboseInfo,
	fileDescriptors,
	originalStreams,
	onInternalError,
	controller,
}) => {
	const exitPromise = waitForExit(subprocess, context);
	const streamInfo = {
		originalStreams,
		fileDescriptors,
		subprocess,
		exitPromise,
		propagating: false,
	};

	const stdioPromises = waitForStdioStreams({
		subprocess,
		encoding,
		buffer,
		maxBuffer,
		lines,
		stripFinalNewline,
		verboseInfo,
		streamInfo,
	});
	const allPromise = waitForAllStream({
		subprocess,
		all,
		encoding,
		buffer,
		maxBuffer,
		lines,
		stripFinalNewline,
		verboseInfo,
		streamInfo,
	});
	const ipcOutput = [];
	const ipcOutputPromise = waitForIpcOutput({
		subprocess,
		buffer,
		maxBuffer,
		ipc,
		ipcOutput,
		verboseInfo,
	});
	const originalPromises = waitForOriginalStreams(originalStreams, subprocess, streamInfo);
	const customStreamsEndPromises = waitForCustomStreamsEnd(fileDescriptors, streamInfo);

	try {
		return await Promise.race([
			Promise.all([
				{},
				waitForSuccessfulExit(exitPromise),
				Promise.all(stdioPromises),
				allPromise,
				ipcOutputPromise,
				sendIpcInput(subprocess, ipcInput, ipc),
				...originalPromises,
				...customStreamsEndPromises,
			]),
			onInternalError,
			throwOnSubprocessError(subprocess, controller),
			...throwOnTimeout(kill, timeout, context, controller),
			...throwOnCancel({
				kill,
				cancelSignal,
				gracefulCancel,
				context,
				controller,
			}),
			...throwOnGracefulCancel({
				subprocess,
				kill,
				cancelSignal,
				gracefulCancel,
				forceKillAfterDelay,
				context,
				controller,
			}),
		]);
	} catch (error) {
		context.terminationReason ??= 'other';
		return Promise.all([
			{error},
			exitPromise,
			Promise.all(stdioPromises.map(stdioPromise => getBufferedData(stdioPromise))),
			getBufferedData(allPromise),
			getBufferedIpcOutput(ipcOutputPromise, ipcOutput),
			Promise.allSettled(originalPromises),
			Promise.allSettled(customStreamsEndPromises),
		]);
	}
};

// Transforms replace `subprocess.std*`, which means they are not exposed to users.
// However, we still want to wait for their completion.
const waitForOriginalStreams = (originalStreams, subprocess, streamInfo) =>
	originalStreams.map((stream, fdNumber) => stream === subprocess.stdio[fdNumber]
		? undefined
		: waitForStream(stream, fdNumber, streamInfo));

// Some `stdin`/`stdout`/`stderr` options create a stream, e.g. when passing a file path.
// The `.pipe()` method automatically ends that stream when `subprocess` ends.
// This makes sure we wait for the completion of those streams, in order to catch any error.
const waitForCustomStreamsEnd = (fileDescriptors, streamInfo) => fileDescriptors.flatMap(({stdioItems, direction}, fdNumber) => stdioItems
	.filter(({value, stream = value}) => isStream(stream, {checkOpen: false}) && !isStandardStream(stream))
	.map(({type, value, stream = value}) => waitForStream(stream, fdNumber, streamInfo, {
		isSameDirection: TRANSFORM_TYPES.has(type),
		stopOnExit: type === 'native',
		// Output streams are destinations which are handed back to the user: waiting for their readable side would mean waiting for the user to consume them.
		readable: direction !== 'output' || TRANSFORM_TYPES.has(type),
	})));

// Fails when the subprocess emits an `error` event
const throwOnSubprocessError = async (subprocess, {signal}) => {
	const [error] = await once(subprocess, 'error', {signal});
	throw error;
};

// Create a `Writable` stream that forwards to `stdin` and awaits the subprocess
const createWritable = ({subprocess, concurrentStreams}, options) => {
	const {to} = copyOptions(options);
	const {subprocessStdin, waitWritableFinal, waitWritableDestroy} = getSubprocessStdin(subprocess, to, concurrentStreams);
	const writable = new Writable({
		...getWritableMethods(subprocessStdin, subprocess, waitWritableFinal),
		destroy: callbackify(onWritableDestroy.bind(undefined, {
			subprocessStdin,
			subprocess,
			waitWritableFinal,
			waitWritableDestroy,
		})),
		highWaterMark: subprocessStdin.writableHighWaterMark,
		objectMode: subprocessStdin.writableObjectMode,
	});
	onStdinFinished(subprocessStdin, writable, undefined, subprocess);
	return writable;
};

// Retrieve `stdin` (or other stream depending on `to`)
const getSubprocessStdin = (subprocess, to, concurrentStreams) => {
	const subprocessStdin = getToStream(subprocess, to);
	const waitWritableFinal = addConcurrentStream(concurrentStreams, subprocessStdin, 'writableFinal');
	const waitWritableDestroy = addConcurrentStream(concurrentStreams, subprocessStdin, 'writableDestroy');
	return {subprocessStdin, waitWritableFinal, waitWritableDestroy};
};

const getWritableMethods = (subprocessStdin, subprocess, waitWritableFinal) => ({
	write: onWrite.bind(undefined, subprocessStdin),
	final: callbackify(onWritableFinal.bind(undefined, subprocessStdin, subprocess, waitWritableFinal)),
});

// Forwards data from `writable` to `stdin`
const onWrite = (subprocessStdin, chunk, encoding, done) => {
	if (subprocessStdin.write(chunk, encoding)) {
		done();
	} else {
		subprocessStdin.once('drain', done);
	}
};

// Ensures that the writable `final` and readable `end` events awaits the subprocess.
// Like this, any subprocess failure is propagated as a stream `error` event, instead of being lost.
// The user does not need to `await` the subprocess anymore, but now needs to await the stream completion or error.
// When multiple writables are targeting the same stream, they wait for each other, unless the subprocess ends first.
const onWritableFinal = async (subprocessStdin, subprocess, waitWritableFinal) => {
	if (!await waitForConcurrentStreams(waitWritableFinal, subprocess)) {
		return;
	}

	if (subprocessStdin.writable) {
		subprocessStdin.end();
	}

	await subprocess;
};

// When `subprocess.stdin` ends/aborts/errors, do the same on `writable`.
const onStdinFinished = async (subprocessStdin, writable, subprocessStdout, subprocess) => {
	try {
		await waitForSubprocessStdin(subprocessStdin);
		await subprocess;
		if (writable.writable) {
			writable.end();
		}
	} catch (error) {
		await safeWaitForSubprocessStdout(subprocessStdout);
		// `writable.writable` is already `false` once `writable.end()` is called, although its pending writes still wait for `stdin`
		destroyOtherStream(writable, !writable.writableFinished, await getSubprocessError(subprocess, error));
	}
};

const getSubprocessError = async (subprocess, error) => {
	if (!shouldUseSubprocessError(error)) {
		return error;
	}

	try {
		await subprocess;
	} catch (subprocessError) {
		return subprocessError;
	}

	return error;
};

// `EPIPE` happens when the subprocess fails without reading its `stdin`, e.g. when it fails to spawn
const shouldUseSubprocessError = error => error === undefined
	|| isStreamAbort(error)
	|| error.code === 'EPIPE';

// When `writable` aborts/errors, do the same on `subprocess.stdin`
const onWritableDestroy = async ({subprocessStdin, subprocess, waitWritableFinal, waitWritableDestroy}, error) => {
	await waitForConcurrentStreams(waitWritableFinal, subprocess);
	if (await waitForConcurrentStreams(waitWritableDestroy, subprocess)) {
		destroyOtherWritable(subprocessStdin, error);
		await subprocess;
	}

	throwDestroyError(error);
};

const destroyOtherWritable = (stream, error) => {
	destroyOtherStream(stream, stream.writable, error);
};

// Create a `Duplex` stream combining both `subprocess.readable()` and `subprocess.writable()`
const createDuplex = ({subprocess, concurrentStreams, encoding}, options) => {
	const {from, to, binary: binaryOption = true, preserveNewlines = true} = copyOptions(options);
	checkBooleanOption(binaryOption, 'binary');
	checkBooleanOption(preserveNewlines, 'preserveNewlines');
	const binary = binaryOption || BINARY_ENCODINGS.has(encoding);
	const {subprocessStdout, waitReadableDestroy} = getSubprocessStdout(subprocess, from, concurrentStreams);
	const {subprocessStdin, waitWritableFinal, waitWritableDestroy} = getSubprocessStdin(subprocess, to, concurrentStreams);
	const {readableEncoding, readableObjectMode, readableHighWaterMark} = getReadableOptions(subprocessStdout, binary);
	const {read, onStdoutDataDone} = getReadableMethods({
		subprocessStdout,
		subprocess,
		binary,
		encoding,
		preserveNewlines,
	});
	const duplex = new Duplex({
		read,
		...getWritableMethods(subprocessStdin, subprocess, waitWritableFinal),
		destroy: callbackify(onDuplexDestroy.bind(undefined, {
			subprocessStdout,
			subprocessStdin,
			subprocess,
			waitReadableDestroy,
			waitWritableFinal,
			waitWritableDestroy,
		})),
		readableHighWaterMark,
		writableHighWaterMark: subprocessStdin.writableHighWaterMark,
		readableObjectMode,
		writableObjectMode: subprocessStdin.writableObjectMode,
		encoding: readableEncoding,
	});
	onStdoutFinished({
		subprocessStdout,
		onStdoutDataDone,
		readable: duplex,
		subprocess,
		subprocessStdin,
	});
	onStdinFinished(subprocessStdin, duplex, subprocessStdout, subprocess);
	return duplex;
};

const onDuplexDestroy = async ({subprocessStdout, subprocessStdin, subprocess, waitReadableDestroy, waitWritableFinal, waitWritableDestroy}, error) => {
	await Promise.all([
		onReadableDestroy({subprocessStdout, subprocess, waitReadableDestroy}, error),
		onWritableDestroy({
			subprocessStdin,
			subprocess,
			waitWritableFinal,
			waitWritableDestroy,
		}, error),
	]);
};

// Add methods to convert the subprocess to a stream or iterable
const addConvertedStreams = (subprocess, {encoding}) => {
	const concurrentStreams = initializeConcurrentStreams();
	subprocess.readable = createReadable.bind(undefined, {subprocess, concurrentStreams, encoding});
	subprocess.writable = createWritable.bind(undefined, {subprocess, concurrentStreams});
	subprocess.duplex = createDuplex.bind(undefined, {subprocess, concurrentStreams, encoding});
	subprocess.readableStream = createReadableStream.bind(undefined, subprocess);
	subprocess.writableStream = createWritableStream.bind(undefined, subprocess);
	subprocess.transformStream = createTransformStream.bind(undefined, subprocess);
	subprocess.iterable = createIterable.bind(undefined, subprocess, encoding);
	subprocess[Symbol.asyncIterator] = createIterable.bind(undefined, subprocess, encoding, {});
};

// The return value is a native Promise with Execa-specific properties.
const mergePromise = (promise, properties) => Object.assign(promise, properties);

// Main shared logic for all async methods: `execa()`, `$`, `execaNode()`
const execaCoreAsync = (rawFile, rawArguments, rawOptions, createNested) => {
	const {file, commandArguments, command, escapedCommand, startTime, verboseInfo, options, fileDescriptors} = handleAsyncArguments(rawFile, rawArguments, rawOptions);
	const {subprocess: nodeChildProcess, promise, kill, all, convertedStreams} = spawnSubprocessAsync({
		file,
		commandArguments,
		options,
		startTime,
		verboseInfo,
		command,
		escapedCommand,
		fileDescriptors,
	});
	const subprocess = getSubprocessPromise({
		promise,
		nodeChildProcess,
		kill,
		all,
		convertedStreams,
		options,
	});
	subprocess.pipe = pipeToSubprocess.bind(undefined, {
		source: subprocess,
		sourcePromise: promise,
		boundOptions: {},
		createNested,
	});
	SUBPROCESS_OPTIONS.set(subprocess, {options, fileDescriptors});
	return subprocess;
};

// Compute arguments to pass to `child_process.spawn()`
const handleAsyncArguments = (rawFile, rawArguments, rawOptions) => {
	const {command, escapedCommand, startTime, verboseInfo} = handleCommand(rawFile, rawArguments, rawOptions);
	const {file, commandArguments, options: normalizedOptions} = normalizeOptions(rawFile, rawArguments, rawOptions);
	const options = handleAsyncOptions(normalizedOptions);
	const fileDescriptors = handleStdioAsync(options, verboseInfo);
	return {
		file,
		commandArguments,
		command,
		escapedCommand,
		startTime,
		verboseInfo,
		options,
		fileDescriptors,
	};
};

// Options normalization logic specific to async methods.
// Prevent passing the `timeout` option directly to `child_process.spawn()`.
// The `signal` option is not passed either: it has been renamed and is validated in `normalizeOptions()`, for both synchronous and asynchronous methods.
// The returned object keeps a null prototype, otherwise a polluted `Object.prototype` would inject option values.
const handleAsyncOptions = ({timeout, signal, ...options}) => ({__proto__: null, ...options, timeoutDuration: timeout});

const spawnSubprocessAsync = ({file, commandArguments, options, startTime, verboseInfo, command, escapedCommand, fileDescriptors}) => {
	let subprocess;
	try {
		subprocess = spawn(...concatenateShell(file, commandArguments, getSpawnOptions(options)));
	} catch (error) {
		return handleEarlyError({
			error,
			command,
			escapedCommand,
			fileDescriptors,
			options,
			startTime,
			verboseInfo,
		});
	}

	// With EMFILE or ENFILE, Node.js does not create the subprocess' streams, and emits the error on the next tick instead of throwing it.
	// The listener must be added right away, or that error would crash the current process.
	if (subprocess.stdio === undefined) {
		return handleEarlyError({
			error: once(subprocess, 'error').then(([error]) => error),
			command,
			escapedCommand,
			fileDescriptors,
			options,
			startTime,
			verboseInfo,
		});
	}

	const controller = new AbortController();
	setMaxListeners(Infinity, controller.signal);

	const originalStreams = [...subprocess.stdio];
	pipeOutputAsync(subprocess, fileDescriptors, controller);
	setIpcSubprocessOptions(subprocess, options);

	const context = {};
	const onInternalError = createDeferred();
	const kill = subprocessKill.bind(undefined, {
		kill: getKillFunction(subprocess, options),
		options,
		onInternalError,
		context,
		controller,
	});
	cleanupOnExit(kill, options, controller);
	const all = makeAllStream(subprocess, options);

	const promise = handlePromise({
		subprocess,
		kill,
		all,
		options,
		startTime,
		verboseInfo,
		fileDescriptors,
		originalStreams,
		command,
		escapedCommand,
		context,
		onInternalError,
		controller,
	});
	return {
		subprocess,
		promise,
		kill,
		all,
	};
};

const getSubprocessPromise = ({promise, nodeChildProcess, kill, all, convertedStreams, options}) => {
	const subprocess = mergePromise(promise, getSubprocessProperties(nodeChildProcess, all));
	subprocess.kill = kill ?? subprocess.kill;
	if (convertedStreams === undefined) {
		addConvertedStreams(subprocess, options);
	} else {
		Object.assign(subprocess, convertedStreams);
	}

	addIpcMethods(subprocess, nodeChildProcess, options);
	return subprocess;
};

const getSubprocessProperties = (nodeChildProcess, all) => ({
	nodeChildProcess,
	pid: nodeChildProcess.pid,
	stdin: nodeChildProcess.stdin,
	stdout: nodeChildProcess.stdout,
	stderr: nodeChildProcess.stderr,
	stdio: nodeChildProcess.stdio,
	all,
	kill: nodeChildProcess.kill.bind(nodeChildProcess),
});

// Asynchronous logic, as opposed to the previous logic which can be run synchronously, i.e. can be returned to user right away
const handlePromise = async ({subprocess, kill, all: allStream, options, startTime, verboseInfo, fileDescriptors, originalStreams, command, escapedCommand, context, onInternalError, controller}) => {
	const [
		errorInfo,
		[exitCode, signal],
		stdioResults,
		allResult,
		ipcOutput,
	] = await waitForSubprocessResult({
		subprocess,
		kill,
		all: allStream,
		options,
		context,
		verboseInfo,
		fileDescriptors,
		originalStreams,
		onInternalError,
		controller,
	});
	controller.abort();
	onInternalError.resolve();

	const stdio = stdioResults.map((stdioResult, fdNumber) => stripNewline(stdioResult, options, fdNumber));
	const all = stripNewline(allResult, options, 'all', getAllFds(subprocess, options.buffer));
	const result = getAsyncResult({
		errorInfo,
		exitCode,
		signal,
		stdio,
		all,
		ipcOutput,
		context,
		options,
		command,
		escapedCommand,
		startTime,
	});
	return handleResult(result, verboseInfo, options);
};

const getAsyncResult = ({errorInfo, exitCode, signal, stdio, all, ipcOutput, context, options, command, escapedCommand, startTime}) => Object.hasOwn(errorInfo, 'error')
	? makeError({
		error: errorInfo.error,
		command,
		escapedCommand,
		timedOut: context.terminationReason === 'timeout',
		isCanceled: context.terminationReason === 'cancel' || context.terminationReason === 'gracefulCancel',
		isGracefullyCanceled: context.terminationReason === 'gracefulCancel',
		isMaxBuffer: errorInfo.error instanceof MaxBufferError,
		isForcefullyTerminated: context.isForcefullyTerminated,
		exitCode,
		signal,
		stdio,
		all,
		ipcOutput,
		options,
		startTime,
		isSync: false,
	})
	: makeSuccessResult({
		command,
		escapedCommand,
		stdio,
		all,
		ipcOutput,
		options,
		startTime,
	});

// Deep merge specific options like `env`. Shallow merge the other ones.
// Use spread (which only copies own properties) to safely read from boundOptions without prototype pollution
const mergeOptions = (boundOptions, options) => {
	const safeBoundOptions = {__proto__: null, ...boundOptions};
	const mergedOptions = Object.fromEntries(Object.entries(options).map(([optionName, optionValue]) => [
		optionName,
		mergeOption(optionName, safeBoundOptions[optionName], optionValue),
	]));
	return {__proto__: null, ...safeBoundOptions, ...mergedOptions};
};

const mergeOption = (optionName, boundOptionValue, optionValue) => DEEP_OPTIONS.has(optionName) && isPlainObject$2(boundOptionValue) && isPlainObject$2(optionValue) ? {...boundOptionValue, ...optionValue} : optionValue;

const DEEP_OPTIONS = new Set(['env', ...FD_SPECIFIC_OPTIONS]);

// Wraps every exported methods to provide the following features:
//  - template string syntax: execa`command argument`
//  - options binding: boundExeca = execa(options)
//  - optional argument/options: execa(file), execa(file, args), execa(file, options), execa(file, args, options)
// `mapArguments()` and `setBoundExeca()` allows for method-specific logic.
const createExeca = (mapArguments, boundOptions, deepOptions, setBoundExeca) => {
	const createNested = (mapArguments, boundOptions, setBoundExeca) => createExeca(mapArguments, boundOptions, deepOptions, setBoundExeca);
	const boundExeca = (...execaArguments) => callBoundExeca({
		mapArguments,
		deepOptions,
		boundOptions,
		setBoundExeca,
		createNested,
	}, ...execaArguments);

	if (setBoundExeca !== undefined) {
		setBoundExeca(boundExeca, createNested, boundOptions);
	}

	return boundExeca;
};

const callBoundExeca = ({mapArguments, deepOptions = {}, boundOptions = {}, setBoundExeca, createNested}, firstArgument, ...nextArguments) => {
	if (isPlainObject$2(firstArgument)) {
		return createNested(mapArguments, mergeOptions(boundOptions, firstArgument), setBoundExeca);
	}

	const {file, commandArguments, options, isSync} = parseArguments({
		mapArguments,
		firstArgument,
		nextArguments,
		deepOptions,
		boundOptions,
	});
	return isSync
		? execaCoreSync(file, commandArguments, options)
		: execaCoreAsync(file, commandArguments, options, createNested);
};

const parseArguments = ({mapArguments, firstArgument, nextArguments, deepOptions, boundOptions}) => {
	const callArguments = isTemplateString(firstArgument)
		? parseTemplates(firstArgument, nextArguments)
		: [firstArgument, ...nextArguments];
	const [initialFile, initialArguments, initialOptions] = normalizeParameters(...callArguments);
	const mergedOptions = mergeOptions(mergeOptions(deepOptions, boundOptions), initialOptions);
	// `mapArguments()` returns a plain object, so its result is read from a null-prototype copy.
	// Otherwise a polluted `Object.prototype` would replace every option, or switch the execution mode.
	const {
		options = mergedOptions,
		isSync = false,
	} = {__proto__: null, ...mapArguments({options: mergedOptions})};
	return {
		file: initialFile,
		commandArguments: initialArguments,
		options,
		isSync,
	};
};

// Sets `$.sync` and `$.s`
const setScriptSync = (boundExeca, createNested, boundOptions) => {
	boundExeca.sync = createNested(mapScriptSync, boundOptions);
	boundExeca.s = boundExeca.sync;
};

// Main logic for `$`
const mapScriptAsync = ({options}) => getScriptOptions(options);

// Main logic for `$.sync`
const mapScriptSync = ({options}) => ({...getScriptOptions(options), isSync: true});

// `$` is like `execa` but with script-friendly options: `{stdin: 'inherit', preferLocal: true}`
const getScriptOptions = options => ({options: {__proto__: null, ...getScriptStdinOption(options), ...options}});

const getScriptStdinOption = ({input, inputFile, stdio}) => input === undefined && inputFile === undefined && stdio === undefined
	? {stdin: 'inherit'}
	: {};

// When using $(...).pipe(...), most script-friendly options should apply to both commands.
// However, some options (like `stdin: 'inherit'`) would create issues with piping, i.e. cannot be deep.
const deepScriptOptions = {preferLocal: true};

const execa = createExeca(() => ({}));
createExeca(() => ({isSync: true}));
createExeca(mapNode);
createExeca(mapScriptAsync, {}, deepScriptOptions, setScriptSync);

getIpcExport();

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
const TEST_GATE = '#[cfg(test)]';
/** What `path` breaks of `rules` that it did not already break at `before`. */
function violations(rules, path, before, after) {
    return rules
        .filter((r) => r.applies(path) && r.count(after) > (before === undefined ? 0 : r.count(before)))
        .map((r) => `${path} ${r.message}`);
}
/**
 * The code outside `#[cfg(test)]` items. Each gated item ends at its first top-level `;` or at the
 * `}` closing its first `{`; production code may follow it.
 */
function nonTest(text) {
    let kept = '';
    let rest = text;
    for (let at = rest.indexOf(TEST_GATE); at !== -1; at = rest.indexOf(TEST_GATE)) {
        kept += rest.slice(0, at);
        let depth = 0;
        let end = rest.length;
        // Strings, char literals and comments are skipped whole, so their braces do not count.
        const skip = /b?r(#*)"[\s\S]*?"\1|"(\\.|[^"\\])*"|'(\\.|[^'\\])'|\/\/[^\n]*|\/\*[\s\S]*?\*\//y;
        for (let i = at + TEST_GATE.length; i < rest.length; i++) {
            skip.lastIndex = i;
            if (skip.test(rest)) {
                i = skip.lastIndex - 1;
                continue;
            }
            if (rest[i] === '{')
                depth++;
            else if (rest[i] === '}' && --depth === 0)
                end = i + 1;
            else if (rest[i] === ';' && depth === 0)
                end = i + 1;
            if (end !== rest.length)
                break;
        }
        rest = rest.slice(end);
    }
    return kept + rest;
}
const matches = (text, pattern) => text.match(pattern)?.length ?? 0;
/**
 * Whether a test is named for the artifact it belongs to rather than the behavior it asserts,
 * judged by whole words, so `testGeoFilterABI` counts and `capabilities` does not.
 */
function namedForArtifact(name) {
    const words = name
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
        .toLowerCase()
        .split('_');
    return words.some((w) => ['contract', 'boundary', 'abi', 'compat'].includes(w));
}
/** Tests named for an artifact: Rust test functions, Python flow tests and C++ tests. */
function artifactTests(text) {
    const names = [
        ...text.matchAll(/#\[(\w+::)*(test|rstest)\b[^\]]*\]\s*(#\[[^\]]*\]\s*)*(async\s+)?fn\s+(\w+)/g),
    ].map((m) => m[5]);
    for (const m of text.matchAll(/\bdef\s+(test\w*)\s*\(/g))
        names.push(m[1]);
    for (const m of text.matchAll(/\bTEST(_F)?\s*\(\s*\w+\s*,\s*(\w+)\s*\)/g))
        names.push(m[2]);
    return names.filter(namedForArtifact).length;
}
/** `debug_assert!`s outside the named assertion helpers that are meant to wrap them. */
function bareAsserts(text) {
    let helper = false;
    let count = 0;
    for (const line of text.split('\n')) {
        const fn = line.match(/\bfn\s+(\w+)/)?.[1];
        if (fn)
            helper = /^(debug_)?assert_/.test(fn);
        if (/\bdebug_assert(_eq|_ne)?!\s*\(/.test(line) && !helper)
            count++;
    }
    return count;
}
/** `# Safety` sections whose pre-conditions are not a numbered list. */
function unnumbered(text) {
    const sections = text.split(/^\s*\/\/\/\s*# Safety\s*$/m).slice(1);
    return sections.filter((rest) => {
        const docs = rest.split('\n').filter(Boolean);
        const body = docs.slice(0, docs.findIndex((l) => !l.trim().startsWith('///')));
        return !body.some((l) => /^\s*\/\/\/\s*\d+\./.test(l));
    }).length;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/** The Cargo workspace the Rust lives in. */
const WORKSPACE = 'src/redisearch_rs/';
/** C headers cbindgen generates from the FFI crates. */
const HEADERS = `${WORKSPACE}headers/`;
const FFI = `${WORKSPACE}c_entrypoint/`;
const isTest = (path) => /(^|\/)(tests|benches)\//.test(path);
const isGenerated = (path) => path.startsWith(HEADERS) || path.endsWith('Cargo.lock');
const rust = (path) => path.startsWith(WORKSPACE) && path.endsWith('.rs');
/** Rust outside the FFI crates and the C bindings: what a port's pure crate is. */
const isPure = (path) => rust(path) && !/^src\/redisearch_rs\/(c_entrypoint|ffi|c_wrappers|redis_mock)\//.test(path);
const production = (path) => rust(path) && !isTest(path);
const ffi = (path) => production(path) && path.startsWith(FFI);
/** Rule files: a reviewer finding that cites one blocks. */
const isRule = (path) => /^(\.claude\/)?\.?skills\//.test(path) || /(^|\/)(CLAUDE|AGENTS)\.md$/.test(path);
const RULE_FILES = '`.skills/`, CLAUDE.md or AGENTS.md';
/** What the Idiomatic Rust reviewer goes through rule by rule. */
const GUIDES = 'the generated-code smells and the house rules in .skills/rust-review row by row, and ' +
    '.skills/rust-tests-guidelines and .skills/rust-docs-guidelines';
// Miri runs in every stage, because nothing tells reliably whether a change reaches `unsafe` code.
// cargo mutants skips `unsafe extern` functions, so only a swap that adds other Rust needs it.
const LINT = ['make lint', 'cargo hakari generate --diff', 'cargo hakari manage-deps --dry-run'];
/** The checks each stage's plan must have, by kind, and what each must run. */
const CHECKS = {
    rust: {
        test: [],
        miri: ['miri'],
        mutants: ['cargo mutants'],
        lint: LINT,
        bench: ['cargo bench'],
    },
    swap: { test: [], miri: ['miri'], lint: LINT },
};
/** What a reproduction may run: a `*` stands for a word. */
const RUNNERS = ['cargo test', 'cargo nextest run', './build.sh RUN_*'];
/** The check that aggregates the others: the only CI result that decides. */
const CI_GATE = 'pr-validation';
/** Checks that never block, even before the gate reports. */
const ADVISORY = /sonar|codecov/i;
/** Lints a new crate enables for itself until the workspace does (.skills/port-c-module). */
const CRATE_LINTS = [
    'clippy::unwrap_used',
    'clippy::dbg_macro',
    'clippy::todo',
    'clippy::unimplemented',
    'clippy::print_stdout',
    'clippy::panic',
    'clippy::unnecessary_safety_comment',
    'clippy::ptr_as_ptr',
    'clippy::borrow_as_ptr',
    'clippy::ref_as_ptr',
    'clippy::indexing_slicing',
];
/** The house rules maintainers raise on every port (.skills/rust-review § 3k). */
const RULES = [
    {
        applies: production,
        count: (text) => matches(nonTest(text), /\.unwrap\(\)/g),
        message: 'adds `unwrap()` outside tests; use `expect("<the invariant it relies on>")`',
    },
    {
        applies: ffi,
        count: (text) => matches(text, /unsafe\s*\{\s*(&\s*(mut\s+)?\*|\(\s*\*)/g),
        message: 'dereferences a raw pointer with `*`; use `as_ref()`, `as_mut()` or `write()`',
    },
    {
        applies: ffi,
        count: (text) => bareAsserts(nonTest(text)),
        message: 'adds a bare `debug_assert!`; check FFI pre-conditions with a named assertion helper',
    },
    {
        applies: production,
        count: (text) => unnumbered(nonTest(text)),
        message: 'adds a `# Safety` section whose pre-conditions are not numbered',
    },
    {
        applies: production,
        count: (text) => matches(text, /RedisModule_(Alloc|Calloc|Realloc)\b/g),
        message: 'calls the Redis allocator directly; allocate with `Box`, the global allocator',
    },
    {
        applies: production,
        count: (text) => matches(text, /Result<[^;{]*,\s*&'static str\s*>/g),
        message: "returns a `&'static str` error; use an error enum deriving `thiserror::Error`",
    },
    {
        applies: (path) => rust(path) && path !== `${FFI}redisearch_rs/src/lib.rs`,
        count: (text) => matches(text, /#\[global_allocator\]/g),
        message: 'adds an allocator; Miri already catches leaks and double frees',
    },
    {
        applies: (path) => rust(path) || /^tests\/(pytests|cpptests|ctests)\//.test(path),
        count: artifactTests,
        message: 'adds a test named for its artifact (contract, ABI, boundary); name the behavior',
    },
    {
        applies: (path) => isPure(path) && !isTest(path),
        count: (text) => matches(text, /\bextern\s+"C"|#\[(unsafe\()?no_mangle\b/g),
        message: 'adds C ABI items to a pure crate; they belong in its `c_entrypoint/*_ffi` crate',
    },
    {
        // Declarations only: tests may still define stubs for C symbols they do not exercise.
        applies: (path) => isPure(path) && isTest(path),
        count: (text) => matches(text, /\bextern\s+"C"\s*\{/g),
        message: "declares C functions in a pure crate's tests; test the C boundary in the FFI crate",
    },
    {
        // An existing crate rolls the lints out on its own, but none may drop one.
        applies: (path) => /^src\/redisearch_rs\/(.+\/)?[^/]+\/src\/lib\.rs$/.test(path),
        count: (text) => CRATE_LINTS.filter((lint) => !text.includes(lint)).length,
        message: `lacks lints of the crate lint block: ${CRATE_LINTS.join(', ')}`,
    },
];

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
const env = { GIT_EDITOR: 'true', GIT_TERMINAL_PROMPT: '0', NO_COLOR: '1' };
/** How much slower than C, in percent, Rust may be in any benchmark. */
const SLOWDOWN_PERCENT = 5;
const ESTIMATES = /^(.+)[\\/](?:c|rust)[\\/]new[\\/]estimates\.json$/;
async function git(cwd, args, input = '') {
    return (await execa('git', args, { cwd, env, input })).stdout;
}
/**
 * Runs checks one at a time, because builds share the cargo lock, and stops at the first failure,
 * which already rejects the candidate. Each result carries its `problem`, if it has one.
 */
async function runChecks(cwd, dir, checks, signal, debug, vars = {}) {
    const results = [];
    for (const [index, check] of checks.entries()) {
        const log = join(dir, `check-${index}-${check.kind}.log`);
        const started = Date.now();
        const exitCode = signal.aborted ? null : await run(cwd, check.command, log, signal, vars);
        const result = { ...check, exitCode, started, ms: Date.now() - started, log };
        result.problem =
            exitCode === null
                ? 'timed out'
                : exitCode !== 0
                    ? `exited ${exitCode}`
                    : await judge(result, cwd);
        debug(`check \`${check.command}\` ${result.problem ?? 'passed'} in ${result.ms}ms: ${log}`);
        results.push(result);
        if (result.problem)
            break;
    }
    return results;
}
async function run(cwd, command, log, signal, vars) {
    // Both streams append to one log, so their output interleaves as it happened.
    await writeFile(log, '');
    // A process group of its own, so aborting also kills the redis-servers and compilers it starts.
    const child = execa('bash', ['-o', 'pipefail', '-c', command], {
        cwd,
        env: { ...env, ...vars },
        detached: true,
        reject: false,
        stdin: 'ignore',
        stdout: { file: log, append: true },
        stderr: { file: log, append: true },
    });
    const kill = () => {
        try {
            process.kill(-child.pid, 'SIGKILL');
        }
        catch { }
    };
    signal.addEventListener('abort', kill, { once: true });
    // An abort before the listener existed would otherwise leave the check running.
    if (signal.aborted)
        kill();
    const { exitCode } = await child;
    signal.removeEventListener('abort', kill);
    return signal.aborted ? null : (exitCode ?? null);
}
/**
 * Whether checks' logs show the test `name` ran: a line naming it, module path included, that
 * does not report it ignored or skipped. Another check may still skip it, say under Miri.
 */
function ran(logs, name) {
    const named = new RegExp(`(?<!\\w)${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?!\\w)`);
    return logs.split('\n').some((line) => named.test(line) && !/\b(ignored|SKIP)\b/.test(line));
}
/** Why a check that exited 0 still failed, judged from its log or its output files. */
async function judge(check, root) {
    if (check.kind === 'bench')
        return judgeBenchmarks(await criterion(root), check.started);
    const log = stripVTControlCharacters(await readFile(check.log, 'utf8').catch(() => ''));
    if (check.kind === 'miri')
        return judgeMiri(log);
    if (check.kind === 'mutants')
        return judgeMutants(log);
}
/**
 * Fails a Miri run that ran nothing, such as `true # miri`, or had a test nextest marked slow:
 * CI's runners are slower and time it out.
 */
function judgeMiri(log) {
    if (!Number(log.match(/Summary \[[^\]]*\] (\d+) tests? run/)?.[1]))
        return 'ran no tests under Miri: nextest reported no summary of tests run';
    const slow = new Set([...log.matchAll(/ SLOW \[[^\]]*\] \([^)]*\) (.+)$/gm)].map((m) => m[1].trim()));
    if (slow.size)
        return (`has tests nextest marked slow, which CI's slower runners time out: ${[...slow].join(', ')}. ` +
            'Shrink their inputs; never raise the timeout.');
}
/** Fails a mutation run that found nothing to mutate, because it then covers nothing. */
function judgeMutants(log) {
    if (!/Found [1-9]\d* mutants? to test/.test(log))
        return ('found no mutants in the stage diff, so it shows no test coverage. Point `-p` and ' +
            '`--in-diff` at the Rust the stage adds; a swap that changes only `unsafe extern` exports, ' +
            'which cargo mutants skips, needs no mutants check.');
}
/**
 * Where Criterion may write: `criterion` in the Cargo target directory. Cargo run from the
 * repository root and from the workspace disagree on it, so both are asked, one after the other
 * because Cargo commands contend for the workspace lock.
 */
async function criterion(root) {
    const manifest = join(root, WORKSPACE, 'Cargo.toml');
    const args = ['metadata', '--format-version', '1', '--no-deps', '--manifest-path', manifest];
    const targets = [];
    for (const cwd of [root, join(root, WORKSPACE)]) {
        const { stdout } = await execa('cargo', args, { cwd });
        targets.push(join(JSON.parse(stdout).target_directory, 'criterion'));
    }
    return [...new Set(targets)];
}
/** The median Criterion estimated in `file`, unless it was written before `since`. */
async function median(file, since) {
    const written = await stat(file).catch(() => undefined);
    if (!written || written.mtimeMs < since)
        return;
    const estimate = await readFile(file, 'utf8')
        .then((text) => JSON.parse(text).median.point_estimate)
        .catch(() => NaN);
    return estimate > 0 ? estimate : NaN;
}
/**
 * Judges each Criterion group measured since `since` by the ratio of its `rust` and `c` medians.
 * A group with neither side fresh is an earlier run's, left in the shared target directory.
 */
async function judgeBenchmarks(dirs, since, slowdownPercent = SLOWDOWN_PERCENT) {
    const ratios = [];
    const halves = [];
    for (const dir of dirs) {
        const files = await readdir(dir, { recursive: true }).catch(() => []);
        const names = new Set(files.map((f) => f.match(ESTIMATES)?.[1]).filter((n) => n !== undefined));
        for (const name of names) {
            const [c, rust] = await Promise.all(['c', 'rust'].map((impl) => median(join(dir, name, impl, 'new', 'estimates.json'), since)));
            if (c !== undefined && rust !== undefined)
                ratios.push({ name, ratio: rust / c });
            else if (c !== undefined || rust !== undefined)
                halves.push(name);
        }
    }
    if (halves.length)
        return `measured only one of \`c\` and \`rust\` for: ${halves.sort().join(', ')}`;
    if (!ratios.length)
        return `measured no Criterion group with \`c\` and \`rust\` functions in ${dirs.join(' or ')}`;
    const unreadable = ratios.filter((r) => Number.isNaN(r.ratio)).map((r) => r.name);
    if (unreadable.length)
        return `left unreadable Criterion estimates for: ${unreadable.join(', ')}`;
    const limit = 1 + slowdownPercent / 100;
    const slow = ratios.filter((r) => r.ratio > limit).map((r) => `${r.name} ${r.ratio.toFixed(2)}x`);
    if (slow.length)
        return `is slower than C beyond ${limit.toFixed(2)}x: ${slow.join(', ')}`;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/** A port lands as two stacked PRs: the Rust, then the swap that deletes the C. */
const STAGES = ['rust', 'swap'];
const POLICY = '.amp/plugins/rust-port/policy.md';
/** The tests the plan cites up to `stage`: a stage's candidate must contain them and run them. */
const cited = (plan, stage) => [...plan.contract.flatMap((c) => c.tests), ...plan.divergences.map((d) => d.test)].filter((t) => STAGES.indexOf(t.stage) <= STAGES.indexOf(stage));
const blank = (text) => typeof text !== 'string' || !text.trim();
function testErrors(t, what) {
    if (blank(t?.path) || blank(t?.name) || !STAGES.includes(t?.stage))
        return [`${what} needs a test: { path, name, stage }, stage one of ${STAGES.join(', ')}`];
    if (namedForArtifact(t.name))
        return [`${what} names ${t.name} for its artifact; name it for the behavior it asserts`];
    return [];
}
function validate(plan) {
    const errors = ['remote', 'baseBranch', 'branchPrefix', 'structure', 'interfaces']
        .filter((field) => blank(plan?.[field]))
        .map((field) => `${field} is required`);
    if (!/^[\w.-]+\/[\w.-]+$/.test(plan?.repository))
        errors.push('repository must be owner/name');
    if (!plan.module?.some((p) => p?.endsWith?.('.c')) || !plan.module.some((p) => p.endsWith('.h')))
        errors.push('module needs the C files being ported: sources and headers');
    if (!plan.contract?.length)
        errors.push('contract needs at least one item');
    for (const [i, c] of (plan.contract ?? []).entries()) {
        if (blank(c?.behavior))
            errors.push(`contract[${i}] needs a behavior`);
        if (!c?.tests?.length)
            errors.push(`contract[${i}] needs the tests that pin it`);
        for (const t of c?.tests ?? [])
            errors.push(...testErrors(t, `contract[${i}]`));
    }
    if (!Array.isArray(plan.divergences))
        errors.push('divergences is required, even if empty');
    for (const [i, d] of (plan.divergences ?? []).entries()) {
        if ([d?.behavior, d?.c, d?.rust, d?.why].some(blank))
            errors.push(`divergences[${i}] needs behavior, c, rust and why`);
        errors.push(...testErrors(d?.test, `divergences[${i}]`));
    }
    if (!plan.symbols?.length)
        errors.push('symbols needs every function the headers declare');
    for (const [i, s] of (plan.symbols ?? []).entries())
        if ([s?.c, s?.rust].some(blank))
            errors.push(`symbols[${i}] needs c and rust`);
    for (const stage of STAGES) {
        const s = plan.stages?.[stage];
        if (blank(s?.deliverable))
            errors.push(`${stage} needs a deliverable`);
        if (!s?.paths?.length || s.paths.some(blank))
            errors.push(`${stage} needs the paths it may change`);
        if (blank(s?.title) || blank(s?.body))
            errors.push(`${stage} needs a PR title and body`);
        const filter = benchFilter(s?.checks?.find((c) => c.kind === 'bench')?.command ?? '');
        if (filter)
            errors.push(`${stage} bench check filters the benchmarks (${filter}); run them all`);
        // A check passes only if every command in it does.
        for (const c of s?.checks ?? [])
            if (/\|\||;|(^|[^&])&(?!&)/.test(c?.command ?? ''))
                errors.push(`${stage} ${c.kind} check chains commands with something other than \`&&\``);
        for (const [kind, runs] of Object.entries(CHECKS[stage])) {
            const command = s?.checks?.find((c) => c.kind === kind)?.command ?? '';
            if (blank(command) || runs.some((r) => !command.includes(r)))
                errors.push(`${stage} needs a ${kind} check${runs.map((r) => ` running \`${r}\``).join(' and')}`);
        }
    }
    return errors;
}
/**
 * What in a `cargo bench` command selects only some benchmarks: a bench target, or a filter
 * passed to Criterion. Benchmarks a run skips leave stale results the judge ignores.
 */
function benchFilter(command) {
    const args = command
        .match(/cargo bench\b([^;&|]*)/)?.[1]
        .trim()
        .split(/\s+/) ?? [];
    const valued = ['-p', '--package', '--manifest-path', '--features', '-F', '--profile'];
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--' || args[i] === '--bench' || args[i].startsWith('--bench='))
            return args[i];
        if (valued.includes(args[i]) || args[i] === '--target-dir')
            i++;
        else if (args[i] && !args[i].startsWith('-'))
            return args[i];
    }
}
/**
 * Functions a C header declares: a name with a return type before it, either type words or a
 * macro call on the same line such as `arrayof(char**)`. Column-zero macro calls such as
 * `KHASH_MAP_INIT_INT64(...)` have none, and comments, typedefs and static functions are skipped.
 */
function declarations(header) {
    // Comments become blank space, keeping lines, so prose in them never reads as a prototype.
    const code = header
        .replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, ' '))
        .replace(/\/\/[^\n]*/g, '');
    const pattern = /^(?!typedef\b|static\b|#|\/\/|\/?\*|\s)(?:\w+\([^()]*\)(?:[ \t]*|[ \t]*\n[ \t]+)|(?:[\w*]+[\s*]+)+)(\w+)\s*\(/gm;
    return [...code.matchAll(pattern)].map((m) => m[1]);
}
/**
 * What the plan gets wrong about the repository and its tools: a remote that is not the
 * repository, where the PRs open from its branches; stage branches git refuses,
 * missing tools, headers not on the base branch, functions the symbols leave out or invent, and
 * paths spanning several crates.
 */
async function inspect(plan, root, git) {
    const problems = [];
    const url = await git(['remote', 'get-url', plan.remote]).catch(() => '');
    if (!new RegExp(`[/@]github\\.com[/:]${plan.repository.replace(/\./g, '\\.')}(\\.git)?/?$`, 'i').test(url))
        problems.push(`remote ${plan.remote} is not github.com/${plan.repository}, where the PRs open`);
    for (const stage of STAGES) {
        const branch = `${plan.branchPrefix}-${stage}`;
        await git(['check-ref-format', '--branch', branch]).catch(() => problems.push(`${branch} is not a valid branch name`));
    }
    for (const tool of ['mutants', 'nextest'])
        await execa('cargo', [tool, '--version'], { cwd: root }).catch(() => problems.push(`cargo ${tool} is missing: cargo install --locked cargo-${tool}`));
    await criterion(root).catch((error) => problems.push(`cargo metadata fails, so benchmarks cannot be read: ${error.shortMessage}`));
    await git(['fetch', plan.remote, plan.baseBranch]);
    const declared = new Set();
    for (const path of plan.module.filter((p) => p.endsWith('.h'))) {
        const header = await git(['show', `FETCH_HEAD:${path}`]).catch(() => undefined);
        if (header === undefined)
            problems.push(`${path} is not on ${plan.baseBranch}`);
        else
            for (const name of declarations(header))
                declared.add(name);
    }
    const mapped = new Set(plan.symbols.map((s) => s.c));
    const unmapped = [...declared].filter((name) => !mapped.has(name));
    const unknown = [...mapped].filter((name) => !declared.has(name));
    if (unmapped.length)
        problems.push(`symbols leaves out ${unmapped.join(', ')}`);
    if (unknown.length)
        problems.push(`symbols names undeclared ${unknown.join(', ')}`);
    const files = (await git(['ls-tree', '-r', '--name-only', 'FETCH_HEAD'])).split('\n');
    for (const stage of STAGES)
        problems.push(...crates(plan.stages[stage].paths, files));
    return problems;
}
/** Paths holding several crates: a stage names each crate it changes, so spreading shows. */
function crates(paths, files) {
    return paths.flatMap((path) => {
        const dir = path.replace(/\/*$/, '/');
        const count = files.filter((f) => f.startsWith(dir) && f.endsWith('/Cargo.toml')).length;
        return count > 1 ? [`${path} holds ${count} crates; list each one the stage changes`] : [];
    });
}
const testName = (t) => `\`${t.path}::${t.name}\``;
const contract = (plan) => plan.contract.map((c) => `- ${c.behavior} Tests: ${c.tests.map(testName).join(', ')}`);
function divergences(plan) {
    if (!plan.divergences.length)
        return 'None: the Rust behaves like the C everywhere.';
    return plan.divergences
        .map((d) => `- **${d.behavior}.** C: ${d.c} Rust: ${d.rust} Why: ${d.why} Test: ${testName(d.test)}`)
        .join('\n');
}
/**
 * The stage's PR. Every PR lists the divergences, so whoever reviews it can reject one, then, folded
 * away, the plan it was admitted under, so an amendment that widens a stage or weakens its checks
 * shows, and the `record` of the run.
 */
function pr(plan, stage, record) {
    const { title, body } = plan.stages[stage];
    return {
        title,
        body: `${body}

#### Behavior changes from C

${divergences(plan)}

<details><summary>How this port ran</summary>

${describe(plan, false)}

${record}
</details>
`,
    };
}
/** The plan as reviewers read it; a PR shows the divergences on their own. */
function describe(plan, withDivergences = true) {
    const stages = STAGES.map((s) => {
        const { paths, checks } = plan.stages[s];
        return `- \`${s}\` may change ${paths.map((p) => `\`${p}\``).join(', ')} and must pass ${checks.map((c) => `\`${c.command}\``).join(', ')}.`;
    });
    return `### Contract

${contract(plan).join('\n')}
${withDivergences ? `\n### Divergences from C\n\n${divergences(plan)}\n` : ''}
### Stages

${stages.join('\n')}

### What replaces each C function

${plan.symbols.map((s) => `- \`${s.c}\`: \`${s.rust}\``).join('\n')}

### Structure, dependencies and handoffs

${plan.structure}

### Interfaces later stages rely on

${plan.interfaces}`;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Why each reference does not hold in the workspace at `root`: a missing file, a line past its
 * end, a test it does not define, or an unknown finding. Empty when every reference holds.
 */
async function verify(root, refs, findings = {}) {
    if (!Array.isArray(refs))
        return ['evidence must be a list of references'];
    const problems = [];
    for (const ref of refs) {
        if (typeof ref !== 'object' || ref === null) {
            problems.push(`${JSON.stringify(ref)} is not a reference`);
            continue;
        }
        if ('finding' in ref) {
            if (!findings[ref.finding])
                problems.push(`there is no finding ${ref.finding}`);
            continue;
        }
        const path = typeof ref?.path === 'string' ? normalize(ref.path) : '';
        if (!path || isAbsolute(path) || path.startsWith('..')) {
            problems.push(`${JSON.stringify(ref)} needs a path inside the workspace`);
            continue;
        }
        if (ref.line !== undefined && typeof ref.line !== 'number') {
            problems.push(`${path}: line must be a number`);
            continue;
        }
        if (ref.test !== undefined && typeof ref.test !== 'string') {
            problems.push(`${path}: test must be a name`);
            continue;
        }
        const text = await readFile(join(root, path), 'utf8').catch(() => undefined);
        if (text === undefined)
            problems.push(`${path} does not exist`);
        else if (ref.line && !(ref.line >= 1 && ref.line <= text.split('\n').length))
            problems.push(`${path} has no line ${ref.line}`);
        else if (ref.test && !defines(text, ref.test))
            problems.push(`${path} defines no ${ref.test}`);
    }
    return problems;
}
/** Whether `text` defines the test `name`: its last path segment, as a whole word. */
function defines(text, name) {
    const word = name.split(/::|\./).at(-1).replace(/[^\w]/g, '');
    return !!word && new RegExp(`\\b${word}\\b`).test(text);
}
/**
 * A finding's location as `path:line`, whether it names the line, a range starting
 * there, or a symbol defined on it. A location that resolves to neither is returned as given.
 */
async function anchor(root, location) {
    const at = location.indexOf(':');
    const [path, spot] = at < 0 ? [location, ''] : [location.slice(0, at), location.slice(at + 1)];
    const line = /^\d+/.exec(spot)?.[0];
    if (line)
        return `${path}:${line}`;
    const file = normalize(path);
    if (isAbsolute(file) || file.startsWith('..'))
        return location;
    const text = await readFile(join(root, file), 'utf8').catch(() => '');
    const name = spot.split(/::|\./).at(-1).replace(/[^\w]/g, '');
    const lines = text.split('\n');
    const kinds = 'fn|struct|enum|union|trait|type|const|static|def|class';
    const definition = new RegExp(`\\b(${kinds})\\s+${name}\\b`);
    const c = new RegExp(`^\\S.*\\b${name}\\s*\\(`);
    let found = name ? lines.findIndex((l) => definition.test(l)) : -1;
    if (found < 0 && name)
        found = lines.findIndex((l) => c.test(l));
    return found < 0 ? location : `${path}:${found + 1}`;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/** Assessments a job may run before an operator grants more. */
const BUDGET = 30;
const TARGETS = ['plugin', 'policy', 'skills', 'checks', 'reviewers'];
/** Each retro list and the text fields its items need, besides their evidence. */
const RETRO = {
    mistakes: ['what', 'cost', 'prevention'],
    cuts: ['what', 'lines'],
    improvements: ['problem', 'proposal', 'target'],
};
/** A short digest of `parts`, independent of the order of keys in any object among them. */
function hash(...parts) {
    const sorted = (_, value) => value && typeof value === 'object' && !Array.isArray(value)
        ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)))
        : value;
    return createHash('sha256').update(JSON.stringify(parts, sorted)).digest('hex').slice(0, 16);
}
function newState(owner, plan) {
    const job = {
        ...{ owner, plan, planHash: hash(plan), started: new Date().toISOString() },
        ...{ admitted: {}, prs: {}, used: 0, grants: [], created: 0, delivered: 0, posted: [] },
    };
    return { job, evidence: {}, findings: {} };
}
const budget = (job) => job.grants.reduce((sum, g) => sum + g.amount, BUDGET);
/** Why the job may not go on without a human, if it may not. */
function stopped(job) {
    if (job.blocked)
        return `blocked: ${job.blocked}`;
    if (job.used >= budget(job))
        return `the assessment budget (${budget(job)}) is used up`;
}
/** Changes whenever a turn moved the job: an assessment, a finding raised or closed, an admission. */
function progress(state) {
    const closed = Object.values(state.findings).filter((f) => f.closed).length;
    const { used, created, admitted, retro, planHash } = state.job;
    return hash(used, created, closed, admitted, !!retro, planHash);
}
const previous = (stage) => STAGES[STAGES.indexOf(stage) - 1];
/** A stage stays admitted while the plan and the admitted stage it was built on are unchanged. */
function isAdmitted(job, stage) {
    const admission = job.admitted[stage];
    const prev = previous(stage);
    if (admission?.plan !== job.planHash)
        return false;
    return !prev || (isAdmitted(job, prev) && admission.base === job.admitted[prev].candidate);
}
const passed = (evidence) => evidence.checks.every((c) => !c.problem);
/** The stage's reviewed evidence under the current plan, oldest first. */
const reviewed = (state, stage) => Object.values(state.evidence).filter((e) => e.stage === stage && e.plan === state.job.planHash && e.reviewers.length);
/**
 * The replies due on review threads: a no-change answer at once, a fix once its stage's PR moved
 * past the revision the finding was raised on, quoting the head that carries it.
 */
function due(state) {
    return Object.values(state.findings).flatMap((finding) => {
        const { closed, replies, stage, revision } = finding;
        if (!closed || !replies?.length)
            return [];
        if (closed.resolution !== 'fixed')
            return [{ finding, reply: closed.reason }];
        const head = state.job.prs[stage]?.head;
        return head && head !== revision
            ? [{ finding, reply: `Fixed in ${head.slice(0, 10)}: ${closed.reason}` }]
            : [];
    });
}
function outstanding(state) {
    return Object.values(state.findings)
        .filter((f) => !f.closed)
        .sort((a, b) => STAGES.indexOf(a.stage) - STAGES.indexOf(b.stage) || seq(a) - seq(b));
}
/** CI runs only after publication, so its findings cannot hold back the admission that publishes. */
const holds = (f, stage) => f.stage === stage && f.blocking && f.source !== 'ci';
/** Every stage admitted, nothing outstanding and every reply posted: the retrospective is next. */
function done(state) {
    return (STAGES.every((s) => isAdmitted(state.job, s)) &&
        !outstanding(state).length &&
        !Object.values(state.findings).some((f) => f.closed && f.replies?.length));
}
const seq = (f) => Number(f.id.slice(1));
function insert(state, finding) {
    const id = `F${++state.job.created}`;
    state.findings[id] = { id, ...finding };
}
/**
 * Adds findings whose key is new. A duplicate merges into the existing finding: an open one gains
 * its links, and either gains its replies, so a comment on two stacked PRs is answered on both.
 */
function receive(state, findings) {
    for (const finding of findings) {
        const existing = Object.values(state.findings).find((f) => f.key === finding.key);
        if (!existing) {
            insert(state, finding);
            continue;
        }
        if (!existing.closed)
            existing.links = [...new Set([...existing.links, ...finding.links])];
        for (const reply of finding.replies ?? [])
            if (!existing.replies?.some((r) => r.comment === reply.comment))
                existing.replies = [...(existing.replies ?? []), reply];
    }
}
/**
 * Replaces the open findings of one stage and source with the current failures. A failure reported
 * again on another revision supersedes the open one with a new finding, so the worker hears of it.
 */
function reconcile(state, stage, source, failures) {
    const open = outstanding(state).filter((f) => f.stage === stage && f.source === source);
    for (const finding of open)
        if (!failures.some((f) => f.key === finding.key))
            finding.closed = { resolution: 'passed' };
    for (const failure of failures) {
        const existing = open.find((f) => f.key === failure.key);
        if (existing?.revision === failure.revision)
            continue;
        if (existing)
            existing.closed = { resolution: 'superseded' };
        insert(state, failure);
    }
}
/** Closes a finding the worker may close. On a review thread the reason becomes the reply. */
function close(state, id, resolution, reason, evidence) {
    const finding = state.findings[id];
    if (!finding || finding.closed)
        throw new Error(`${id} is not an outstanding finding`);
    if (finding.source === 'check' || finding.source === 'ci')
        throw new Error(`${id} closes itself once it passes`);
    if (!evidence.length)
        throw new Error('Cite evidence: the files, lines or tests that show it');
    if (resolution === 'no-change' && !reason?.trim())
        throw new Error('A no-change resolution needs the reason it does not apply');
    if (finding.replies?.length &&
        !(reason?.trim() && reason.length <= 280 && !reason.includes('\n')))
        throw new Error(`${id} is a review thread: give a one-line reason of at most 280 characters`);
    finding.closed = { resolution, reason, evidence };
    return finding;
}
/** Outstanding findings the worker has not been shown yet; marks them shown. */
function undelivered(state) {
    const fresh = outstanding(state).filter((f) => seq(f) > state.job.delivered);
    state.job.delivered = state.job.created;
    return fresh;
}
/** The findings as a numbered to-do list, then how to close them with `closeTool`. */
function render(findings, closeTool) {
    if (!findings.length)
        return '';
    const indent = (text) => text.replace(/^/gm, '   ');
    const items = findings.map((f, i) => [
        `${i + 1}. ${f.id} · ${f.stage} · ${f.title}${f.blocking ? '' : ' · suggestion'}`,
        indent(f.body),
        indent(f.links.join(' ')),
    ]
        .filter((line) => line.trim())
        .join('\n'));
    const any = (test) => findings.some(test);
    const how = [
        any((f) => f.source === 'review' || f.source === 'comment') &&
            `Close reviewer findings and comments with ${closeTool}: fixed, or no-change with the reason, citing the files, lines or tests that show it.`,
        any((f) => f.repro) &&
            "Every later claim re-runs a reproduced finding's test; commit it only if it is a regression test that belongs in the suite.",
        any((f) => f.replies?.length) &&
            'For a PR comment the reason is posted as the reply: one line of at most 280 characters.',
        any((f) => f.source === 'check' || f.source === 'ci') &&
            'Failed checks and CI close themselves once they pass.',
        any((f) => !f.blocking) && 'Suggestions do not hold admission but need closing.',
    ];
    return [...items, how.filter(Boolean).join(' ')].join('\n\n');
}
/** The run as a human reviewing its PRs reads it: its cost, its findings and its retrospective. */
function report(state) {
    const { job } = state;
    const findings = Object.values(state.findings);
    const tally = (key, keys) => keys.map((k) => `${findings.filter((f) => key(f) === k).length} ${k}`).join(', ');
    const stages = STAGES.map((s) => {
        const claims = Object.values(state.evidence).filter((e) => e.stage === s).length;
        const at = job.admitted[s]?.candidate.slice(0, 10);
        return `\`${s}\` assessed ${claims} times${at ? `, admitted at ${at}` : ''}`;
    });
    const confirmed = findings.filter((f) => f.source === 'review' && f.blocking).length;
    const reviewed = findings.filter((f) => f.source === 'review').length;
    const lines = [
        '### Run',
        '',
        `- Started ${job.started}; ${job.used} of ${budget(job)} assessments used, ${job.grants.length} grants.`,
        `- ${stages.join('; ')}.`,
        `- ${findings.length} findings: ${tally((f) => f.source, ['review', 'comment', 'check', 'ci'])}. ` +
            `${confirmed} of ${reviewed} reviewer findings confirmed or citing a rule.`,
        `- Closed: ${tally((f) => f.closed?.resolution, ['fixed', 'no-change', 'passed', 'superseded'])}.`,
    ];
    const { retro } = job;
    if (retro)
        lines.push('', '### Retrospective', '', ...retro.mistakes.map((m) => `- Mistake: ${m.what} Cost: ${m.cost} Prevention: ${m.prevention}`), ...retro.cuts.map((c) => `- Cut: ${c.what} (${c.lines} lines)`), ...retro.improvements.map((i) => `- Improve ${i.target}: ${i.problem} Proposal: ${i.proposal}`));
    return lines.join('\n');
}
/** Every list has an item, every item its fields and evidence, every improvement a target. */
function validateRetro(retro) {
    const errors = [];
    for (const [list, fields] of Object.entries(RETRO)) {
        const items = Array.isArray(retro?.[list]) ? retro[list] : [];
        if (!items.length)
            errors.push(`${list} needs at least one item`);
        for (const [i, item] of items.entries()) {
            for (const field of fields)
                if (typeof item?.[field] !== 'string' || !item[field].trim())
                    errors.push(`${list}[${i}] needs ${field}`);
            if (!item?.evidence?.length)
                errors.push(`${list}[${i}] needs evidence`);
        }
    }
    for (const [i, item] of (retro?.improvements ?? []).entries())
        if (!TARGETS.includes(item?.target))
            errors.push(`improvements[${i}].target must be one of ${TARGETS.join(', ')}`);
    return errors;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * The files a unified diff changes, with the lines it adds and removes in each. A file starts at
 * its `---`/`+++` pair; hunk line counts are ignored, because a model's are often wrong. In git's
 * own output the pair follows an `index` line, so hunk content that looks like one is not one.
 */
function parseDiff(diff) {
    const files = [];
    const lines = diff.split('\n');
    const fromGit = lines.some((line) => line.startsWith('diff --git '));
    let file;
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const header = !fromGit || lines[i - 1]?.startsWith('index ');
        if (header && line.startsWith('--- ') && lines[i + 1]?.startsWith('+++ ')) {
            const [from, to] = [line, lines[++i]].map((l) => l.slice(4).split('\t')[0].trim());
            const path = to === '/dev/null' ? from : to;
            file = { path: path.replace(/^[ab]\//, ''), added: [], removed: [] };
            files.push(file);
        }
        else if (file && line[0] === '+')
            file.added.push(line.slice(1));
        else if (file && line[0] === '-')
            file.removed.push(line.slice(1));
    }
    return files;
}
function measure(files) {
    const size = { lines: 0, code: 0, files: 0 };
    const code = (text = '') => nonTest(text)
        .split('\n')
        .filter((line) => line.trim()).length;
    for (const { path, added, removed, texts } of files) {
        if (path.endsWith('.md') || path.endsWith('.gitignore'))
            continue;
        size.lines += added.length;
        size.files++;
        if (isTest(path) || isGenerated(path))
            continue;
        size.code +=
            texts && path.endsWith('.rs')
                ? code(texts[1]) - code(texts[0])
                : added.length - removed.length;
    }
    return size;
}
/**
 * Pushback by lines added, which grows with them; nothing up to 250. The classes end where the PR
 * size label's do, and XL blocks. Deletions do not count: the classes counter bloat.
 */
function sizeClass(lines) {
    if (lines <= 250)
        return;
    if (lines <= 500)
        return {
            label: 'M',
            note: 'Every line costs a reviewer attention; before claiming again, delete what the port does not need.',
        };
    if (lines <= 1000)
        return {
            label: 'L',
            note: 'Reviewers will push back on a stage this big. Stop adding: cut first, and question every file the stage touches.',
        };
    return {
        label: 'XL',
        note: 'Too large to review: the stage cannot be admitted until it adds at most 1000 lines.',
    };
}
/** Files changed only in blank lines; other whitespace may matter, in a string or in Python. */
function cosmetic(files) {
    return files
        .filter((f) => {
        const lines = [...f.added, ...f.removed];
        return lines.length && lines.every((line) => !line.trim());
    })
        .map((f) => f.path);
}
/** The files that lie outside every one of `paths`, each a file or a directory. */
function outside(files, paths) {
    const dirs = paths.map((p) => p.replace(/\/*$/, '/'));
    return files.filter((f) => !paths.includes(f) && !dirs.some((d) => f.startsWith(d)));
}
/**
 * Why the candidate cannot be the stage, and its size: everything that rejects a claim before
 * anything runs, so a rejection costs no assessment.
 */
async function gates(git, root, state, stage, base, candidate) {
    const { plan } = state.job;
    // Without rename detection a moved file names both its paths, so neither escapes the scope.
    const diff = (...args) => git(['diff', '--no-renames', ...args, base, candidate]);
    const { paths } = plan.stages[stage];
    const stray = outside((await diff('--name-only')).split('\n').filter((f) => f && !isGenerated(f)), paths);
    if (stray.length)
        return {
            problems: [`it changes ${stray.join(', ')}, outside ${paths.join(', ')}; revert or amend`],
            size: { lines: 0, code: 0, files: 0 },
        };
    const show = (rev, path) => git(['show', `${rev}:${path}`]).catch(() => undefined);
    const files = parseDiff(await diff('--no-ext-diff'));
    for (const f of files)
        f.texts = [await show(base, f.path), await show(candidate, f.path)];
    const size = measure(files);
    const problems = cosmetic(files.filter((f) => !isGenerated(f.path))).map((path) => `${path} changes only blank lines; restore it`);
    const tooBig = sizeClass(size.lines);
    if (tooBig?.label === 'XL')
        problems.push(`it adds ${size.lines} lines: ${tooBig.note}`);
    // Once reviewed, a stage's code may not grow: a fix pays for itself in deleted code.
    const floor = Math.min(...reviewed(state, stage).map((e) => e.size.code));
    if (size.code > floor)
        problems.push(`it adds ${size.code} net lines of code (tests aside), its reviewed version ${floor}: ` +
            'a fix must delete as much code as it adds');
    for (const { path, texts } of files) {
        const [before, after] = texts;
        if (after !== undefined)
            problems.push(...violations(RULES, path, before, after));
    }
    problems.push(...crates(paths, (await git(['ls-tree', '-r', '--name-only', candidate])).split('\n')));
    const tests = cited(plan, stage).map((t) => ({ path: t.path, test: t.name }));
    problems.push(...(await verify(root, tests)), ...(await unfixed(git, state, stage, candidate)));
    if (stage === 'swap')
        problems.push(...(await swapped(git, plan, candidate, files)));
    return { problems, size };
}
/**
 * The stage's findings closed as fixed whose fix the candidate lacks: it changes none of the files
 * the fix cites since the revision the finding was raised on.
 */
async function unfixed(git, state, stage, candidate) {
    const problems = [];
    for (const f of Object.values(state.findings)) {
        if (f.stage !== stage || f.closed?.resolution !== 'fixed')
            continue;
        const paths = (f.closed.evidence ?? []).map((r) => r.path);
        // A revision this checkout never had cannot be compared, and is not held against it.
        const changed = await git(['diff', '--name-only', f.revision, candidate, '--', ...paths]).catch(() => '?');
        if (!changed)
            problems.push(`${f.id} was closed as fixed, but nothing it cites changed since ${f.revision.slice(0, 10)}`);
    }
    return problems;
}
/**
 * What the swap leaves undone: a Rust replacement or generated export missing, a module file
 * left, an export no production code calls, or Rust outside the FFI crate without a mutants check.
 * A C definition left elsewhere fails to link against the Rust export of its name.
 */
async function swapped(git, plan, candidate, files) {
    const problems = [];
    const found = (args) => git(args).then(() => true, () => false);
    const grep = (flags, pattern, ...paths) => found(['grep', flags, pattern, candidate, '--', ...paths]);
    // Exports are not mutation candidates, but other Rust the swap adds is.
    const pure = files.map((f) => f.path).filter((f) => isPure(f) && !isTest(f));
    if (pure.length && !plan.stages.swap.checks.some((c) => c.kind === 'mutants'))
        problems.push(`swap changes Rust outside its FFI crate (${pure.join(', ')}): add a mutants check`);
    for (const s of plan.symbols) {
        if (!(await definesRust(git, candidate, s.rust)))
            problems.push(`${s.rust}, which replaces ${s.c}, is not defined`);
        if (!(await grep('-qw', s.c, HEADERS)))
            problems.push(`no generated header exports ${s.c}`);
    }
    for (const path of plan.module)
        if (await found(['cat-file', '-e', `${candidate}:${path}`]))
            problems.push(`${path} still exists; the swap deletes the module, headers included`);
    for (const name of await orphans(git, files, candidate))
        problems.push(`${name} is exported, but no production code calls it: delete it, and point any test ` +
            'that calls it at the Rust API');
    return problems;
}
/**
 * Whether the crate a Rust path starts with defines each item after it, so `geo::RadiusFilter::new`
 * needs `fn new` in a file of the `geo` crate that implements `RadiusFilter`, not any `fn new`.
 */
async function definesRust(git, candidate, path) {
    const [crate, ...items] = path.split('::');
    const manifests = await git([
        'grep',
        '-l',
        `^name = "${crate}"`,
        candidate,
        '--',
        `${WORKSPACE}**Cargo.toml`,
    ]).catch(() => '');
    const manifest = manifests.split('\n')[0]?.split(':').at(-1);
    if (!manifest || !items.length)
        return false;
    const dir = manifest.replace(/Cargo\.toml$/, '');
    const files = async (pattern) => (await git(['grep', '-lwE', pattern, candidate, '--', dir]).catch(() => ''))
        .split('\n')
        .filter(Boolean);
    for (const item of items.slice(0, -1))
        if (!(await files(`(struct|enum|trait|type|mod) ${item}`)).length)
            return false;
    const [owner, item] = [items.at(-2), items.at(-1)];
    const defs = await files(`(fn|struct|enum|trait|type|const|static|mod) ${item}`);
    if (!owner)
        return defs.length > 0;
    // A method lives in a file that implements its type, or in its module.
    const owners = new Set(await files(`impl[^{]* ${owner}|mod ${owner}`));
    return defs.some((f) => owners.has(f));
}
/**
 * C ABI exports without a production caller: those the stage adds, and those whose caller it
 * deleted, that no C or Rust code outside tests, generated headers and comments mentions. An
 * export only tests call keeps C ABI alive for nothing; its tests belong on the Rust API.
 */
async function orphans(git, files, candidate) {
    const grep = (...args) => git(['grep', '-h', ...args]).catch(() => '');
    const touched = new Set(files.flatMap((f) => f.removed.join('\n').match(/\w+/g) ?? []));
    for (const f of files)
        for (const m of f.added.join('\n').matchAll(/#\[unsafe\(no_mangle\)\][^{;]*?\bfn\s+(\w+)/g))
            touched.add(m[1]);
    const exports = await grep('-A4', '#\\[unsafe(no_mangle)\\]', candidate, '--', '*.rs');
    const names = new Set([...exports.matchAll(/\bfn\s+(\w+)/g)].map((m) => m[1]).filter((n) => touched.has(n)));
    const sources = ['*.c', '*.h', '*.cpp', '*.cc', '*.hpp', '*.rs', `:!${HEADERS}`];
    const skip = [':!tests', ':!**/tests/**', ':!**/benches/**'];
    const result = [];
    for (const name of names) {
        const uses = (await grep('-w', name, candidate, '--', ...sources, ...skip))
            .split('\n')
            .map((line) => line.trim())
            .filter((line) => line && !/^(\/\/|\/?\*)/.test(line) && !new RegExp(`fn\\s+${name}\\b`).test(line));
        if (!uses.length)
            result.push(name);
    }
    return result;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/** Confirming reviewer proposals by trying them: running their failing test, or their deletion. */
/** The runners a reproduction may use, as the reviewers read them. */
const RUNS = RUNNERS.map((r) => `\`${r}\``).join(', ');
/** Deletions tried per assessment; each runs the stage's test checks. */
const MAX_DELETIONS = 3;
/**
 * Whether a reproduction command only runs tests: one of {@link RUNNERS}, whose arguments select
 * packages and tests, with no shell syntax and no option such as `--config` or `-Z` that makes
 * Cargo run something else. Reviewer output is model text that read untrusted code.
 */
function runnable(command) {
    const pattern = (r) => new RegExp(`^${r.replace(/[.]/g, '\\.').replace('*', '\\w+')}(?=\\s|$)`);
    const runner = RUNNERS.map((r) => command.match(pattern(r))?.[0]).find(Boolean);
    if (!runner)
        return false;
    const args = command.slice(runner.length).trim().split(/\s+/).filter(Boolean);
    const name = /^[\w:./-]+$/;
    // A script takes its options as variables.
    if (!runner.startsWith('cargo '))
        return args.every((a) => /^[A-Z_]+=[\w:./-]+$/.test(a));
    const valued = ['-p', '--package', '--test', '--features', '-F', '--manifest-path'];
    const flags = ['--lib', '--tests', '--exact', '--nocapture', '--no-fail-fast', '--release', '--'];
    for (let i = 0; i < args.length; i++)
        if (valued.includes(args[i])
            ? !name.test(args[++i] ?? '')
            : !flags.includes(args[i]) && (args[i].startsWith('-') || !name.test(args[i])))
            return false;
    return true;
}
/** The finding a checked proposal becomes; `verdict` says what the plugin confirmed. */
function toFinding(r, p, o) {
    return {
        // Identical findings from several reviewers share a key, so they merge into one.
        key: `review:${hash(o.stage, o.candidate, p.location, p.scenario, p.correction)}`,
        source: 'review',
        stage: o.stage,
        revision: o.candidate,
        blocking: o.blocking,
        title: `${r.label} · ${p.location}`,
        body: [`Problem: ${p.scenario}`, `Fix: ${p.correction}`, `Evidence: ${o.verdict}`, o.proof]
            .filter(Boolean)
            .join('\n'),
        repro: o.repro,
        links: o.link ? [o.link] : [],
    };
}
/** Applies a patch, runs `checks` with it, and takes it out again; a string if it does not apply. */
async function trial(run, patch, checks) {
    const text = patch.endsWith('\n') ? patch : `${patch}\n`;
    // Hunk line counts written by a model are often off; --recount ignores them.
    const applied = await git(run.root, ['apply', '--recount'], text).then(() => true, () => false);
    if (!applied)
        return 'its patch does not apply';
    try {
        return await runChecks(run.root, run.dir, checks, run.signal, run.debug, run.vars);
    }
    finally {
        await git(run.root, ['apply', '--recount', '-R'], text);
    }
}
/** Runs a reproduction's command, with its test patched in unless the candidate already has it. */
async function reproduction(run, repro, kind) {
    const check = [{ kind, command: repro.command }];
    const present = await readFile(join(run.root, repro.path), 'utf8').then((text) => defines(text, repro.test), () => false);
    return present
        ? (await runChecks(run.root, run.dir, check, run.signal, run.debug, run.vars))[0]
        : trial(run, repro.patch, check).then((r) => (typeof r === 'string' ? r : r[0]));
}
/**
 * Confirms a finding by its failing test: one test runner (see {@link runnable}) whose command
 * passes without the test and fails with it, naming it. Anything else may be the patch failing to
 * compile or an unrelated test.
 */
async function reproduce(run, repro, n) {
    const files = parseDiff(repro.patch).map((f) => f.path);
    if (!files.includes(repro.path) ||
        files.some((f) => !isTest(f) || /(^|\/)(Cargo\.toml|build\.rs)$/.test(f)))
        return {
            confirmed: false,
            outcome: `its patch must add ${repro.path} and touch only test sources`,
        };
    if (!runnable(repro.command))
        return { confirmed: false, outcome: `its command must be one of ${RUNS}` };
    const [before] = await runChecks(run.root, run.dir, [{ kind: `repro-${n}-before`, command: repro.command }], run.signal, run.debug, run.vars);
    if (before.exitCode !== 0)
        return { confirmed: false, outcome: `its command fails without the test (${before.log})` };
    const after = await reproduction(run, repro, `repro-${n}`);
    if (typeof after === 'string')
        return { confirmed: false, outcome: after };
    if (after.exitCode === null)
        return { confirmed: false, outcome: 'its command timed out' };
    if (after.exitCode === 0)
        return { confirmed: false, outcome: 'its test passes' };
    if (!(await readFile(after.log, 'utf8').catch(() => '')).includes(repro.test))
        return {
            confirmed: false,
            outcome: `its command fails, but its log never names ${repro.test}`,
        };
    return {
        confirmed: true,
        outcome: `reproduced: \`${repro.command}\` exits ${after.exitCode} (${after.log})`,
        proof: `Reproduction (${repro.command}):\n${repro.patch}`,
    };
}
/**
 * Whether a deletion removes tests, which proves nothing: a test file, or a line of a Rust source
 * that appears only inside its `#[cfg(test)]` items. `read` gives the candidate's text of a file.
 */
async function deletesTests(patch, read) {
    for (const f of parseDiff(patch)) {
        if (isTest(f.path))
            return true;
        if (!f.path.endsWith('.rs'))
            continue;
        const text = await read(f.path).catch(() => '');
        const production = nonTest(text);
        if (f.removed.some((line) => line.trim() && text.includes(line) && !production.includes(line)))
            return true;
    }
    return false;
}
/** Confirms that code is not needed: every test check passes without it. */
async function remove(run, patch, tests, n) {
    const net = parseDiff(patch).reduce((sum, f) => sum + f.removed.length - f.added.length, 0);
    if (net <= 0 || (await deletesTests(patch, (path) => readFile(join(run.root, path), 'utf8'))))
        return { confirmed: false, outcome: 'its patch must remove more than it adds, outside tests' };
    const results = await trial(run, patch, tests.map((c) => ({ ...c, kind: `${c.kind}-deletion-${n}` })));
    if (typeof results === 'string')
        return { confirmed: false, outcome: results };
    const failed = results.find((r) => r.problem);
    if (failed)
        return { confirmed: false, outcome: `\`${failed.command}\` fails without it` };
    return {
        confirmed: true,
        outcome: `every test check passes without it (−${net} lines)`,
        proof: `Deletion:\n${patch}`,
    };
}
/**
 * Turns proposals into findings. Those whose references do not hold are dropped; the rest block
 * only if their test fails, their deletion keeps the tests passing, or they cite a rule. Proposals
 * resolving to one line are one finding, and a confirmed one replaces a suggestion there.
 */
async function confirm(run, reviews, o) {
    const findings = [];
    const kept = new Map();
    let n = 0;
    let deletions = 0;
    for (const r of reviews)
        for (const p of r.proposals) {
            const problems = !(typeof p.location === 'string' && p.location && p.scenario)
                ? ['it has no location or scenario']
                : p.refs.length
                    ? await verify(run.root, p.refs)
                    : ['it cites nothing'];
            if (problems.length) {
                run.debug(`${r.label}: dropped ${p.location}: ${problems.join('; ')}`);
                continue;
            }
            const id = await anchor(run.root, p.location);
            const earlier = kept.get(id);
            if (earlier !== undefined && findings[earlier].blocking)
                continue;
            let result;
            if (p.repro)
                result = await reproduce(run, p.repro, ++n);
            else if (p.deletion && deletions++ < MAX_DELETIONS)
                result = await remove(run, p.deletion.patch, o.tests, ++n);
            const rule = p.refs.find((ref) => isRule(ref.path));
            const blocking = !!result?.confirmed || !!rule;
            const verdict = result?.confirmed
                ? result.outcome
                : rule
                    ? `breaks the rule at ${rule.path}${rule.line ? `:${rule.line}` : ''}`
                    : `unconfirmed${result ? `, ${result.outcome}` : ''}`;
            const finding = toFinding(r, p, {
                ...{ stage: o.stage, candidate: o.candidate, blocking, verdict },
                proof: result?.confirmed ? result.proof : undefined,
                repro: result?.confirmed ? p.repro : undefined,
                link: r.thread && o.threadURL(r.thread),
            });
            if (earlier === undefined)
                kept.set(id, findings.push(finding) - 1);
            else if (blocking)
                findings[earlier] = finding;
        }
    return findings;
}

function getUserAgent() {
  if (typeof navigator === "object" && "userAgent" in navigator) {
    return navigator.userAgent;
  }

  if (typeof process === "object" && process.version !== undefined) {
    return `Node.js/${process.version.substr(1)} (${process.platform}; ${
      process.arch
    })`;
  }

  return "<environment undetectable>";
}

// @ts-check

function register(state, name, method, options) {
  if (typeof method !== "function") {
    throw new Error("method for before hook must be a function");
  }

  if (!options) {
    options = {};
  }

  if (Array.isArray(name)) {
    return name.reverse().reduce((callback, name) => {
      return register.bind(null, state, name, callback, options);
    }, method)();
  }

  return Promise.resolve().then(() => {
    if (!state.registry[name]) {
      return method(options);
    }

    return state.registry[name].reduce((method, registered) => {
      return registered.hook.bind(null, method, options);
    }, method)();
  });
}

// @ts-check

function addHook(state, kind, name, hook) {
  const orig = hook;
  if (!state.registry[name]) {
    state.registry[name] = [];
  }

  if (kind === "before") {
    hook = (method, options) => {
      return Promise.resolve()
        .then(orig.bind(null, options))
        .then(method.bind(null, options));
    };
  }

  if (kind === "after") {
    hook = (method, options) => {
      let result;
      return Promise.resolve()
        .then(method.bind(null, options))
        .then((result_) => {
          result = result_;
          return orig(result, options);
        })
        .then(() => {
          return result;
        });
    };
  }

  if (kind === "error") {
    hook = (method, options) => {
      return Promise.resolve()
        .then(method.bind(null, options))
        .catch((error) => {
          return orig(error, options);
        });
    };
  }

  state.registry[name].push({
    hook: hook,
    orig: orig,
  });
}

// @ts-check

function removeHook(state, name, method) {
  if (!state.registry[name]) {
    return;
  }

  const index = state.registry[name]
    .map((registered) => {
      return registered.orig;
    })
    .indexOf(method);

  if (index === -1) {
    return;
  }

  state.registry[name].splice(index, 1);
}

// @ts-check


// bind with array of arguments: https://stackoverflow.com/a/21792913
const bind = Function.bind;
const bindable = bind.bind(bind);

function bindApi(hook, state, name) {
  const removeHookRef = bindable(removeHook, null).apply(
    null,
    [state]
  );
  hook.api = { remove: removeHookRef };
  hook.remove = removeHookRef;
  ["before", "error", "after", "wrap"].forEach((kind) => {
    const args = [state, kind];
    hook[kind] = hook.api[kind] = bindable(addHook, null).apply(null, args);
  });
}

function Collection() {
  const state = {
    registry: {},
  };

  const hook = register.bind(null, state);
  bindApi(hook, state);

  return hook;
}

var Hook = { Collection };

// pkg/dist-src/defaults.js

// pkg/dist-src/version.js
var VERSION$7 = "0.0.0-development";

// pkg/dist-src/defaults.js
var userAgent = `octokit-endpoint.js/${VERSION$7} ${getUserAgent()}`;
var DEFAULTS = {
  method: "GET",
  baseUrl: "https://api.github.com",
  headers: {
    accept: "application/vnd.github.v3+json",
    "user-agent": userAgent
  },
  mediaType: {
    format: ""
  }
};

// pkg/dist-src/util/lowercase-keys.js
function lowercaseKeys(object) {
  if (!object) {
    return {};
  }
  return Object.keys(object).reduce((newObj, key) => {
    newObj[key.toLowerCase()] = object[key];
    return newObj;
  }, {});
}

// pkg/dist-src/util/is-plain-object.js
function isPlainObject$1(value) {
  if (typeof value !== "object" || value === null) return false;
  if (Object.prototype.toString.call(value) !== "[object Object]") return false;
  const proto = Object.getPrototypeOf(value);
  if (proto === null) return true;
  const Ctor = Object.prototype.hasOwnProperty.call(proto, "constructor") && proto.constructor;
  return typeof Ctor === "function" && Ctor instanceof Ctor && Function.prototype.call(Ctor) === Function.prototype.call(value);
}

// pkg/dist-src/util/merge-deep.js
function mergeDeep(defaults, options) {
  const result = Object.assign({}, defaults);
  Object.keys(options).forEach((key) => {
    if (isPlainObject$1(options[key])) {
      if (!(key in defaults)) Object.assign(result, { [key]: options[key] });
      else result[key] = mergeDeep(defaults[key], options[key]);
    } else {
      Object.assign(result, { [key]: options[key] });
    }
  });
  return result;
}

// pkg/dist-src/util/remove-undefined-properties.js
function removeUndefinedProperties(obj) {
  for (const key in obj) {
    if (obj[key] === void 0) {
      delete obj[key];
    }
  }
  return obj;
}

// pkg/dist-src/merge.js
function merge(defaults, route, options) {
  if (typeof route === "string") {
    let [method, url] = route.split(" ");
    options = Object.assign(url ? { method, url } : { url: method }, options);
  } else {
    options = Object.assign({}, route);
  }
  options.headers = lowercaseKeys(options.headers);
  removeUndefinedProperties(options);
  removeUndefinedProperties(options.headers);
  const mergedOptions = mergeDeep(defaults || {}, options);
  if (options.url === "/graphql") {
    if (defaults && defaults.mediaType.previews?.length) {
      mergedOptions.mediaType.previews = defaults.mediaType.previews.filter(
        (preview) => !mergedOptions.mediaType.previews.includes(preview)
      ).concat(mergedOptions.mediaType.previews);
    }
    mergedOptions.mediaType.previews = (mergedOptions.mediaType.previews || []).map((preview) => preview.replace(/-preview/, ""));
  }
  return mergedOptions;
}

// pkg/dist-src/util/add-query-parameters.js
function addQueryParameters(url, parameters) {
  const separator = /\?/.test(url) ? "&" : "?";
  const names = Object.keys(parameters);
  if (names.length === 0) {
    return url;
  }
  return url + separator + names.map((name) => {
    if (name === "q") {
      return "q=" + parameters.q.split("+").map(encodeURIComponent).join("+");
    }
    return `${name}=${encodeURIComponent(parameters[name])}`;
  }).join("&");
}

// pkg/dist-src/util/extract-url-variable-names.js
var urlVariableRegex = /\{[^{}}]+\}/g;
function removeNonChars(variableName) {
  return variableName.replace(/(?:^\W+)|(?:(?<!\W)\W+$)/g, "").split(/,/);
}
function extractUrlVariableNames(url) {
  const matches = url.match(urlVariableRegex);
  if (!matches) {
    return [];
  }
  return matches.map(removeNonChars).reduce((a, b) => a.concat(b), []);
}

// pkg/dist-src/util/omit.js
function omit(object, keysToOmit) {
  const result = { __proto__: null };
  for (const key of Object.keys(object)) {
    if (keysToOmit.indexOf(key) === -1) {
      result[key] = object[key];
    }
  }
  return result;
}

// pkg/dist-src/util/url-template.js
function encodeReserved(str) {
  return str.split(/(%[0-9A-Fa-f]{2})/g).map(function(part) {
    if (!/%[0-9A-Fa-f]/.test(part)) {
      part = encodeURI(part).replace(/%5B/g, "[").replace(/%5D/g, "]");
    }
    return part;
  }).join("");
}
function encodeUnreserved(str) {
  return encodeURIComponent(str).replace(/[!'()*]/g, function(c) {
    return "%" + c.charCodeAt(0).toString(16).toUpperCase();
  });
}
function encodeValue(operator, value, key) {
  value = operator === "+" || operator === "#" ? encodeReserved(value) : encodeUnreserved(value);
  if (key) {
    return encodeUnreserved(key) + "=" + value;
  } else {
    return value;
  }
}
function isDefined(value) {
  return value !== void 0 && value !== null;
}
function isKeyOperator(operator) {
  return operator === ";" || operator === "&" || operator === "?";
}
function getValues(context, operator, key, modifier) {
  var value = context[key], result = [];
  if (isDefined(value) && value !== "") {
    if (typeof value === "string" || typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
      value = value.toString();
      if (modifier && modifier !== "*") {
        value = value.substring(0, parseInt(modifier, 10));
      }
      result.push(
        encodeValue(operator, value, isKeyOperator(operator) ? key : "")
      );
    } else {
      if (modifier === "*") {
        if (Array.isArray(value)) {
          value.filter(isDefined).forEach(function(value2) {
            result.push(
              encodeValue(operator, value2, isKeyOperator(operator) ? key : "")
            );
          });
        } else {
          Object.keys(value).forEach(function(k) {
            if (isDefined(value[k])) {
              result.push(encodeValue(operator, value[k], k));
            }
          });
        }
      } else {
        const tmp = [];
        if (Array.isArray(value)) {
          value.filter(isDefined).forEach(function(value2) {
            tmp.push(encodeValue(operator, value2));
          });
        } else {
          Object.keys(value).forEach(function(k) {
            if (isDefined(value[k])) {
              tmp.push(encodeUnreserved(k));
              tmp.push(encodeValue(operator, value[k].toString()));
            }
          });
        }
        if (isKeyOperator(operator)) {
          result.push(encodeUnreserved(key) + "=" + tmp.join(","));
        } else if (tmp.length !== 0) {
          result.push(tmp.join(","));
        }
      }
    }
  } else {
    if (operator === ";") {
      if (isDefined(value)) {
        result.push(encodeUnreserved(key));
      }
    } else if (value === "" && (operator === "&" || operator === "?")) {
      result.push(encodeUnreserved(key) + "=");
    } else if (value === "") {
      result.push("");
    }
  }
  return result;
}
function parseUrl(template) {
  return {
    expand: expand.bind(null, template)
  };
}
function expand(template, context) {
  var operators = ["+", "#", ".", "/", ";", "?", "&"];
  template = template.replace(
    /\{([^\{\}]+)\}|([^\{\}]+)/g,
    function(_, expression, literal) {
      if (expression) {
        let operator = "";
        const values = [];
        if (operators.indexOf(expression.charAt(0)) !== -1) {
          operator = expression.charAt(0);
          expression = expression.substr(1);
        }
        expression.split(/,/g).forEach(function(variable) {
          var tmp = /([^:\*]*)(?::(\d+)|(\*))?/.exec(variable);
          values.push(getValues(context, operator, tmp[1], tmp[2] || tmp[3]));
        });
        if (operator && operator !== "+") {
          var separator = ",";
          if (operator === "?") {
            separator = "&";
          } else if (operator !== "#") {
            separator = operator;
          }
          return (values.length !== 0 ? operator : "") + values.join(separator);
        } else {
          return values.join(",");
        }
      } else {
        return encodeReserved(literal);
      }
    }
  );
  if (template === "/") {
    return template;
  } else {
    return template.replace(/\/$/, "");
  }
}

// pkg/dist-src/parse.js
function parse$1(options) {
  let method = options.method.toUpperCase();
  let url = (options.url || "/").replace(/:([a-z]\w+)/g, "{$1}");
  let headers = Object.assign({}, options.headers);
  let body;
  let parameters = omit(options, [
    "method",
    "baseUrl",
    "url",
    "headers",
    "request",
    "mediaType"
  ]);
  const urlVariableNames = extractUrlVariableNames(url);
  url = parseUrl(url).expand(parameters);
  if (!/^http/.test(url)) {
    url = options.baseUrl + url;
  }
  const omittedParameters = Object.keys(options).filter((option) => urlVariableNames.includes(option)).concat("baseUrl");
  const remainingParameters = omit(parameters, omittedParameters);
  const isBinaryRequest = /application\/octet-stream/i.test(headers.accept);
  if (!isBinaryRequest) {
    if (options.mediaType.format) {
      headers.accept = headers.accept.split(/,/).map(
        (format) => format.replace(
          /application\/vnd(\.\w+)(\.v3)?(\.\w+)?(\+json)?$/,
          `application/vnd$1$2.${options.mediaType.format}`
        )
      ).join(",");
    }
    if (url.endsWith("/graphql")) {
      if (options.mediaType.previews?.length) {
        const previewsFromAcceptHeader = headers.accept.match(/(?<![\w-])[\w-]+(?=-preview)/g) || [];
        headers.accept = previewsFromAcceptHeader.concat(options.mediaType.previews).map((preview) => {
          const format = options.mediaType.format ? `.${options.mediaType.format}` : "+json";
          return `application/vnd.github.${preview}-preview${format}`;
        }).join(",");
      }
    }
  }
  if (["GET", "HEAD"].includes(method)) {
    url = addQueryParameters(url, remainingParameters);
  } else {
    if ("data" in remainingParameters) {
      body = remainingParameters.data;
    } else {
      if (Object.keys(remainingParameters).length) {
        body = remainingParameters;
      }
    }
  }
  if (!headers["content-type"] && typeof body !== "undefined") {
    headers["content-type"] = "application/json; charset=utf-8";
  }
  if (["PATCH", "PUT"].includes(method) && typeof body === "undefined") {
    body = "";
  }
  return Object.assign(
    { method, url, headers },
    typeof body !== "undefined" ? { body } : null,
    options.request ? { request: options.request } : null
  );
}

// pkg/dist-src/endpoint-with-defaults.js
function endpointWithDefaults(defaults, route, options) {
  return parse$1(merge(defaults, route, options));
}

// pkg/dist-src/with-defaults.js
function withDefaults$2(oldDefaults, newDefaults) {
  const DEFAULTS2 = merge(oldDefaults, newDefaults);
  const endpoint2 = endpointWithDefaults.bind(null, DEFAULTS2);
  return Object.assign(endpoint2, {
    DEFAULTS: DEFAULTS2,
    defaults: withDefaults$2.bind(null, DEFAULTS2),
    merge: merge.bind(null, DEFAULTS2),
    parse: parse$1
  });
}

// pkg/dist-src/index.js
var endpoint = withDefaults$2(null, DEFAULTS);

/*!
 * content-type
 * Copyright(c) 2015 Douglas Christopher Wilson
 * MIT Licensed
 */
const SP = 32; // " "
const HTAB = 9; // "\t"
const SEMI = 59; // ";"
const EQ = 61; // "="
const DQUOTE = 34; // '"'
const BSLASH = 92; // "\\"
const COMMA = 44; // ","
const LOWER_CASE = 1;
const OWS = 2;
const SEMI_FLAG = 4;
const COMMA_FLAG = 8;
const TOKEN_FLAG = 16;
const NON_ASCII = 0xff00;
const CASE_FLAGS = LOWER_CASE | NON_ASCII;
/**
 * Character flags used to normalize HTTP field values while scanning.
 * Out-of-range reads intentionally coerce to zero in bitwise expressions.
 */
const CHAR_MAP = new Uint8Array(0x100);
CHAR_MAP[HTAB] |= OWS;
CHAR_MAP[SP] |= OWS;
CHAR_MAP[SEMI] |= SEMI_FLAG;
CHAR_MAP[COMMA] |= COMMA_FLAG;
for (let code = 0x80 /* non-ASCII */; code <= 0xff; code++) {
    CHAR_MAP[code] |= LOWER_CASE;
}
for (const char of "!#$%&'*+-.^_`|~") {
    CHAR_MAP[char.charCodeAt(0)] |= TOKEN_FLAG;
}
for (let code = 0x30 /* 0 */; code <= 0x39 /* 9 */; code++) {
    CHAR_MAP[code] |= TOKEN_FLAG;
}
for (let code = 0x41 /* A */; code <= 0x5a /* Z */; code++) {
    CHAR_MAP[code] |= LOWER_CASE | TOKEN_FLAG;
}
for (let code = 0x61 /* a */; code <= 0x7a /* z */; code++) {
    CHAR_MAP[code] |= TOKEN_FLAG;
}
/**
 * Null object perf optimization. Faster than `Object.create(null)` and `{ __proto__: null }`.
 */
const NullObject = /* @__PURE__ */ (() => {
    const C = function () { };
    C.prototype = Object.create(null);
    return C;
})();
/**
 * Parse a `Content-Type` header.
 */
function parse(header, options) {
    const stopFlags = SEMI_FLAG | (0);
    const len = header.length;
    let valueStart = 0;
    while ((CHAR_MAP[header.charCodeAt(valueStart)] & OWS) !== 0) {
        valueStart++;
    }
    let index = valueStart;
    let typeFlags = 0;
    let whitespace = -1;
    let stop = 0;
    while (index < len) {
        const code = header.charCodeAt(index);
        const flags = CHAR_MAP[code];
        if ((flags & stopFlags) !== 0) {
            stop |= flags & COMMA_FLAG;
            break;
        }
        if ((flags & OWS) !== 0) {
            if (whitespace === -1)
                whitespace = index;
        }
        else {
            whitespace = -1;
        }
        typeFlags |= (code & NON_ASCII) | flags;
        index++;
    }
    const valueEnd = whitespace === -1 ? index : whitespace;
    const value = header.slice(valueStart, valueEnd);
    const type = (typeFlags & CASE_FLAGS) === 0 ? value : value.toLowerCase();
    if (index === len || stop !== 0) {
        return { type, index, parameters: new NullObject() };
    }
    return parseParameters(header, type, index, len, stopFlags);
}
/**
 * Parses the parameters of a `Content-Type` header starting at the given index.
 */
function parseParameters(header, type, index, len, stopFlags) {
    const parameters = new NullObject();
    parameter: while (index < len) {
        index++; // Skip over ;
        while ((CHAR_MAP[header.charCodeAt(index)] & OWS) !== 0) {
            index++;
        }
        const keyStart = index;
        let keyFlags = 0;
        let keyWhitespace = -1;
        while (index < len) {
            const code = header.charCodeAt(index);
            const flags = CHAR_MAP[code];
            if ((flags & stopFlags) !== 0) {
                if ((flags & COMMA_FLAG) !== 0)
                    break parameter;
                continue parameter;
            }
            if (code === EQ) {
                const keyEnd = keyWhitespace === -1 ? index : keyWhitespace;
                const value = header.slice(keyStart, keyEnd);
                const key = (keyFlags & CASE_FLAGS) === 0 ? value : value.toLowerCase();
                index++;
                while ((CHAR_MAP[header.charCodeAt(index)] & OWS) !== 0) {
                    index++;
                }
                if (index < len && header.charCodeAt(index) === DQUOTE) {
                    const quotedStart = ++index;
                    let escaped = false;
                    while (index < len) {
                        const code = header.charCodeAt(index);
                        if (code === DQUOTE) {
                            if (parameters[key] === undefined) {
                                parameters[key] = escaped
                                    ? unescapeQuotedPairs(header, quotedStart, index)
                                    : header.slice(quotedStart, index);
                            }
                            index++;
                            let stop = 0;
                            // Discard characters between quote and delimiter.
                            while (index < len) {
                                const code = header.charCodeAt(index);
                                const flags = CHAR_MAP[code];
                                if ((flags & stopFlags) !== 0) {
                                    stop = flags & COMMA_FLAG;
                                    break;
                                }
                                index++;
                            }
                            if (stop !== 0)
                                break parameter;
                            continue parameter;
                        }
                        if (code === BSLASH && index + 1 < len) {
                            escaped = true;
                            index += 2;
                            continue;
                        }
                        index++;
                    }
                    continue parameter;
                }
                const valueStart = index;
                let stop = 0;
                let valueWhitespace = -1;
                while (index < len) {
                    const code = header.charCodeAt(index);
                    const flags = CHAR_MAP[code];
                    if ((flags & stopFlags) !== 0) {
                        stop = flags & COMMA_FLAG;
                        break;
                    }
                    if ((flags & OWS) !== 0) {
                        if (valueWhitespace === -1)
                            valueWhitespace = index;
                    }
                    else {
                        valueWhitespace = -1;
                    }
                    index++;
                }
                if (parameters[key] === undefined) {
                    const valueEnd = valueWhitespace === -1 ? index : valueWhitespace;
                    parameters[key] = header.slice(valueStart, valueEnd);
                }
                if (stop !== 0)
                    break parameter;
                continue parameter;
            }
            if ((flags & OWS) !== 0) {
                if (keyWhitespace === -1)
                    keyWhitespace = index;
            }
            else {
                keyWhitespace = -1;
            }
            keyFlags |= (code & NON_ASCII) | flags;
            index++;
        }
    }
    return { type, index, parameters };
}
/**
 * Remove backslashes from quoted pairs in a known-terminated quoted string body.
 */
function unescapeQuotedPairs(str, start, end) {
    let result = "";
    for (let index = start; index < end; index++) {
        if (str.charCodeAt(index) === BSLASH) {
            result += str.slice(start, index);
            start = ++index;
        }
    }
    return result + str.slice(start, end);
}

const intRegex = /^-?\d+$/;
const noiseValue = /^-?\d+n+$/; // Noise - strings that match the custom format before being converted to it
const originalStringify = JSON.stringify;
const originalParse = JSON.parse;
const customFormat = /^-?\d+n$/;

const bigIntsStringify = /([\[:])?"(-?\d+)n"($|\s*[,\}\]])/g;
const noiseStringify = /([\[:])?("-?\d+n+)n("$|"\s*[,\}\]])/g;

/**
 * @typedef {(this: any, key: string | number | undefined, value: any) => any} Replacer
 * @typedef {(key: string | number | undefined, value: any, context?: { source: string }) => any} Reviver
 */

/**
 * Checks if a value is unstringifiable according to native JSON.stringify rules.
 *
 * @param {any} val The value to check.
 * @returns {boolean} True if the value is undefined, a function, or a symbol.
 */
const isUnstringifiable = (val) =>
  val === undefined || typeof val === "function" || typeof val === "symbol";

/**
 * Checks if a value is a native JSON.rawJSON object (Node.js 22+).
 *
 * @param {any} val The value to check.
 * @returns {boolean} True if the value is a RawJSON instance.
 */
const isRawJSON = (val) =>
  val !== null &&
  typeof val === "object" &&
  val.constructor &&
  val.constructor.name === "RawJSON";

/**
 * Iteratively converts a JS value to a JSON string.
 * Used as a fallback when the native JSON.stringify hits the Maximum Call Stack size.
 * Fully compliant with JSON formatting (space), replacers, and toJSON behaviors.
 *
 * @param {any} rootValue The value to stringify.
 * @param {Replacer | Array<string | number> | null} [replacer] User's custom replacer function.
 * @param {string | number} [spaceParam] Indentation for pretty-printing.
 * @returns {string | undefined} The generated JSON string.
 */
const stringifyIteratively = (rootValue, replacer, spaceParam) => {
  let space = "";
  const propertyList = Array.isArray(replacer)
    ? new Set(replacer.map(String))
    : null;

  /**
   * Prepares a value for stringification by resolving toJSON, handling BigInts,
   * applying custom replacers, and unwrapping primitive objects.
   *
   * @param {object|Array} parent The parent object or array holding the value.
   * @param {string} key The key associated with the value.
   * @param {any} val The raw value to process.
   * @returns {any} The processed value ready for stringification.
   */
  const prepareVal = (parent, key, val) => {
    const isObject = val !== null && typeof val === "object";
    const hasToJSON = isObject && typeof val.toJSON === "function";

    if (hasToJSON) {
      val = val.toJSON(key);
    }

    const isNoise = typeof val === "string" && noiseValue.test(val);

    if (isNoise) return val + "n";

    const isBigInt = typeof val === "bigint";

    if (isBigInt) {
      const supportsRawJSON = "rawJSON" in JSON;

      if (supportsRawJSON) return JSON.rawJSON(val.toString());

      return val.toString() + "n";
    }

    const isPostReplacerObject = val !== null && typeof val === "object";

    if (isPostReplacerObject) {
      const isPrimitiveWrapper =
        val instanceof Number ||
        val instanceof String ||
        val instanceof Boolean;

      if (isPrimitiveWrapper) {
        val = val.valueOf();
      }
    }

    return val;
  };

  const rootProcessed = prepareVal({ }, "", rootValue);

  if (isUnstringifiable(rootProcessed)) {
    return undefined;
  }

  const isRootPrimitive =
    rootProcessed === null || typeof rootProcessed !== "object";
  const isRootNativeRawJSON = isRawJSON(rootProcessed);

  if (isRootPrimitive || isRootNativeRawJSON) {
    return originalStringify(rootProcessed);
  }

  const chunks = [];

  const stack = [
    {
      parent: { "": rootProcessed },
      key: "",
      val: rootProcessed,
      isArray: Array.isArray(rootProcessed),
      keys: Array.isArray(rootProcessed) ? null : Object.keys(rootProcessed),
      index: 0,
      first: true,
    },
  ];

  const visited = new WeakSet([rootProcessed]);

  while (stack.length > 0) {
    const node = stack[stack.length - 1];

    if (node.index === 0) {
      chunks.push(node.isArray ? "[" : "{");
    }

    let isDone = false;

    if (node.isArray) {
      if (node.index < node.val.length) {
        if (!node.first) chunks.push(",");

        const childRaw = node.val[node.index];
        const childVal = prepareVal(node.val, String(node.index), childRaw);

        if (isUnstringifiable(childVal)) {
          chunks.push("null");
          node.first = false;
          node.index++;
        } else {
          const isComplexObject =
            childVal !== null && typeof childVal === "object";
          const isNativeRaw = isRawJSON(childVal);

          if (isComplexObject && !isNativeRaw) {
            if (visited.has(childVal)) {
              throw new TypeError("Converting circular structure to JSON");
            }

            visited.add(childVal);

            stack.push({
              parent: node.val,
              key: String(node.index),
              val: childVal,
              isArray: Array.isArray(childVal),
              keys: Array.isArray(childVal) ? null : Object.keys(childVal),
              index: 0,
              first: true,
            });

            node.first = false;
            node.index++;
          } else {
            chunks.push(originalStringify(childVal));
            node.first = false;
            node.index++;
          }
        }
      } else {
        isDone = true;
      }
    } else {
      while (node.index < node.keys.length) {
        const k = node.keys[node.index++];

        const isFilteredOutByArray = propertyList && !propertyList.has(k);

        if (isFilteredOutByArray) continue;

        const childRaw = node.val[k];
        const childVal = prepareVal(node.val, k, childRaw);

        if (isUnstringifiable(childVal)) continue;

        if (!node.first) chunks.push(",");

        {
          chunks.push(originalStringify(k) + ":");
        }

        const isComplexObject =
          childVal !== null && typeof childVal === "object";
        const isNativeRaw = isRawJSON(childVal);

        if (isComplexObject && !isNativeRaw) {
          if (visited.has(childVal)) {
            throw new TypeError("Converting circular structure to JSON");
          }

          visited.add(childVal);

          stack.push({
            parent: node.val,
            key: k,
            val: childVal,
            isArray: Array.isArray(childVal),
            keys: Array.isArray(childVal) ? null : Object.keys(childVal),
            index: 0,
            first: true,
          });

          node.first = false;

          break; // Stop current loop level to process the newly pushed stack node
        } else {
          chunks.push(originalStringify(childVal));
          node.first = false;
        }
      }

      const isNodeFullyProcessed =
        node.index >= node.keys.length && stack[stack.length - 1] === node;

      if (isNodeFullyProcessed) {
        isDone = true;
      }
    }

    if (isDone) {

      if (!node.first && space) ;

      chunks.push(node.isArray ? "]" : "}");
      visited.delete(node.val);
      stack.pop();
    }
  }

  return chunks.join("");
};

/**
 * Converts a JavaScript value to a JSON string.
 *
 * Supports serialization of BigInt values using two strategies:
 * 1. Custom format "123n" → "123" (universal fallback)
 * 2. Native JSON.rawJSON() (Node.js 22+, fastest) when available
 *
 * All other values are serialized exactly like native JSON.stringify().
 *
 * @param {*} value The value to convert to a JSON string.
 * @param {Replacer | Array<string | number> | null} [replacer]
 * A function that alters the behavior of the stringification process,
 * or an array of strings/numbers to indicate properties to exclude.
 * @param {string | number} [space]
 * A string or number to specify indentation or pretty-printing.
 * @returns {string} The JSON string representation.
 */
const JSONStringify = (value, replacer, space) => {
  try {
    const supportsRawJSON = "rawJSON" in JSON;

    if (supportsRawJSON) {
      return originalStringify(
        value,
        (key, val) => {
          if (typeof val === "bigint") return JSON.rawJSON(val.toString());

          const hasFunctionReplacer = typeof replacer === "function";

          if (hasFunctionReplacer) ;

          const isKeyInArrayReplacer =
            Array.isArray(replacer) && replacer.includes(key);

          if (isKeyInArrayReplacer) return val;

          return val;
        },
        space,
      );
    }

    if (!value) return originalStringify(value, replacer, space);

    const convertedToCustomJSON = originalStringify(
      value,
      (key, val) => {
        const isNoise = typeof val === "string" && noiseValue.test(val);

        if (isNoise) return val.toString() + "n"; // Mark noise values with additional "n" to offset the deletion of one "n" during the processing

        if (typeof val === "bigint") return val.toString() + "n";

        const hasFunctionReplacer = typeof replacer === "function";

        if (hasFunctionReplacer) ;

        const isKeyInArrayReplacer =
          Array.isArray(replacer) && replacer.includes(key);

        if (isKeyInArrayReplacer) return val;

        return val;
      },
      space,
    );

    const processedJSON = convertedToCustomJSON.replace(
      bigIntsStringify,
      "$1$2$3",
    ); // Delete one "n" off the end of every BigInt value

    const denoisedJSON = processedJSON.replace(noiseStringify, "$1$2$3"); // Remove one "n" off the end of every noisy string

    return denoisedJSON;
  } catch (error) {
    if (error instanceof RangeError) {
      const convertedJSON = stringifyIteratively(value, replacer);

      if (convertedJSON === undefined) return undefined;

      const supportsRawJSON = "rawJSON" in JSON;

      if (supportsRawJSON) return convertedJSON;

      const processedJSON = convertedJSON.replace(bigIntsStringify, "$1$2$3");

      return processedJSON.replace(noiseStringify, "$1$2$3");
    }

    throw error;
  }
};

const featureCache = new Map();

/**
 * Detects if the current JSON.parse implementation supports the context.source feature.
 *
 * Uses toString() fingerprinting to cache results and automatically detect runtime
 * replacements of JSON.parse (polyfills, mocks, etc.).
 *
 * @returns {boolean} true if context.source is supported, false otherwise.
 */
const isContextSourceSupported = () => {
  const parseFingerprint = JSON.parse.toString();

  if (featureCache.has(parseFingerprint)) {
    return featureCache.get(parseFingerprint);
  }

  try {
    const result = JSON.parse(
      "1",
      (_, __, context) => !!context?.source && context.source === "1",
    );
    featureCache.set(parseFingerprint, result);

    return result;
  } catch {
    featureCache.set(parseFingerprint, false);

    return false;
  }
};

/**
 * Reviver function that converts custom-format BigInt strings back to BigInt values.
 * Also handles "noise" strings that accidentally match the BigInt format.
 *
 * @param {string | number | undefined} key The object key.
 * @param {*} value The value being parsed.
 * @param {object} [context] Parse context (if supported by JSON.parse).
 * @param {Reviver} [userReviver] User's custom reviver function.
 * @returns {any} The transformed value.
 */
const convertMarkedBigIntsReviver = (key, value, context, userReviver) => {
  const isCustomFormatBigInt =
    typeof value === "string" && customFormat.test(value);

  if (isCustomFormatBigInt) return BigInt(value.slice(0, -1));

  const isNoiseValue = typeof value === "string" && noiseValue.test(value);
  if (isNoiseValue) return value.slice(0, -1);

  return value;
};

/**
 * Fast JSON.parse implementation (~2x faster than classic fallback).
 * Uses JSON.parse's context.source feature to detect integers and convert
 * large numbers directly to BigInt without string manipulation.
 *
 * Does not support legacy custom format from v1 of this library.
 *
 * @param {string} text JSON string to parse.
 * @param {Reviver} [reviver] Transform function to apply to each value.
 * @returns {any} Parsed JavaScript value.
 */
const JSONParseV2 = (text, reviver) => {
  return JSON.parse(text, (key, value, context) => {
    const isNumber = typeof value === "number";
    const isOutOfBounds =
      value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER;
    const isBigNumber = isNumber && isOutOfBounds;
    const isInt = context && intRegex.test(context.source);
    const isBigInt = isBigNumber && isInt;

    if (isBigInt) return BigInt(context.source);

    return value;
  });
};

const MAX_INT = Number.MAX_SAFE_INTEGER.toString();
const MAX_DIGITS = MAX_INT.length;
const stringsOrLargeNumbers =
  /"(?:[^"\\]|\\.)*"|-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?/g;
const noiseValueWithQuotes = /^"-?\d+n+"$/; // Noise - strings that match the custom format before being converted to it

/**
 * Iteratively traverses the parsed object bottom-up (post-order),
 * emulating the native JSON.parse reviver behavior.
 * This avoids Call Stack overflows (RangeError) on deeply nested structures.
 *
 * @param {any} parsed The natively parsed JSON object.
 * @param {Reviver} [userReviver] User's custom reviver function.
 * @returns {any} The fully processed object.
 */
const applyReviverIteratively = (parsed, userReviver) => {
  const rootHolder = { "": parsed };
  const stack = [{ parent: rootHolder, key: "", visited: false }];

  while (stack.length > 0) {
    const node = stack[stack.length - 1];

    if (!node.visited) {
      node.visited = true;

      const value = node.parent[node.key];
      const isComplexObject = value !== null && typeof value === "object";

      if (isComplexObject) {
        const keys = Object.keys(value);

        for (let i = keys.length - 1; i >= 0; i--) {
          stack.push({ parent: value, key: keys[i], visited: false });
        }
      }
    } else {
      const { parent, key } = node;
      let value = parent[key];

      if (typeof value === "string") {
        const isCustomFormatBigInt = customFormat.test(value);

        if (isCustomFormatBigInt) {
          value = BigInt(value.slice(0, -1));
        } else {
          const isNoise = noiseValue.test(value);

          if (isNoise) value = value.slice(0, -1);
        }
      }

      const isDeleted = value === undefined;

      if (isDeleted) {
        delete parent[key];
      } else {
        parent[key] = value;
      }

      stack.pop();
    }
  }

  return rootHolder[""];
};

/**
 * Pre-processes the JSON string to mark large numbers with an 'n' suffix.
 *
 * @param {string} text The raw JSON string.
 * @returns {string} The serialized string with marked BigInts.
 */
const serializeBigInts = (text) => {
  return text.replace(
    stringsOrLargeNumbers,
    (match, digits, fractional, exponential) => {
      const isString = match[0] === '"';
      const isNoise = isString && noiseValueWithQuotes.test(match);

      if (isNoise) return match.substring(0, match.length - 1) + 'n"'; // Mark noise values with additional "n" to offset the deletion of one "n" during the processing

      const hasFractionalOrExponential = fractional || exponential;

      // With a fixed number of digits, we can correctly use lexicographical comparison to do a numeric comparison
      const isLessThanMaxSafeInt =
        digits &&
        (digits.length < MAX_DIGITS ||
          (digits.length === MAX_DIGITS && digits <= MAX_INT));

      const isStandardValue =
        isString || hasFractionalOrExponential || isLessThanMaxSafeInt;

      if (isStandardValue) return match;

      return '"' + match + 'n"';
    },
  );
};

/**
 * Converts a JSON string into a JavaScript value.
 *
 * Supports parsing of large integers using two strategies:
 * 1. Classic fallback: Marks large numbers with "123n" format, then converts to BigInt
 * 2. Fast path (JSONParseV2): Uses context.source feature (~2x faster) when available
 *
 * All other JSON values are parsed exactly like native JSON.parse().
 *
 * @param {string} text A valid JSON string.
 * @param {Reviver} [reviver]
 * A function that transforms the results. This function is called for each member
 * of the object. If a member contains nested objects, the nested objects are
 * transformed before the parent object is.
 * @returns {any} The parsed JavaScript value.
 * @throws {SyntaxError} If text is not valid JSON.
 */
const JSONParse = (text, reviver) => {
  if (!text) return originalParse(text, reviver);

  try {
    if (isContextSourceSupported()) return JSONParseV2(text, reviver); // Shortcut to a faster (2x) and simpler version

    // Find and mark big numbers with "n"
    const serializedData = serializeBigInts(text);

    return originalParse(serializedData, (key, value, context) =>
      convertMarkedBigIntsReviver(key, value, context, reviver),
    );
  } catch (error) {
    if (error instanceof RangeError) {
      const serializedData = serializeBigInts(text);
      const parsed = originalParse(serializedData);

      return applyReviverIteratively(parsed);
    }

    throw error;
  }
};

class RequestError extends Error {
  name;
  /**
   * http status code
   */
  status;
  /**
   * Request options that lead to the error.
   */
  request;
  /**
   * Response object if a response was received
   */
  response;
  constructor(message, statusCode, options) {
    super(message, { cause: options.cause });
    this.name = "HttpError";
    this.status = Number.parseInt(statusCode);
    if (Number.isNaN(this.status)) {
      this.status = 0;
    }
    /* v8 ignore else -- @preserve -- Bug with vitest coverage where it sees an else branch that doesn't exist */
    if ("response" in options) {
      this.response = options.response;
    }
    const requestCopy = Object.assign({}, options.request);
    if (options.request.headers.authorization) {
      requestCopy.headers = Object.assign({}, options.request.headers, {
        authorization: options.request.headers.authorization.replace(
          /(?<! ) .*$/,
          " [REDACTED]"
        )
      });
    }
    requestCopy.url = requestCopy.url.replace(/\bclient_secret=\w+/g, "client_secret=[REDACTED]").replace(/\baccess_token=\w+/g, "access_token=[REDACTED]");
    this.request = requestCopy;
  }
}

// pkg/dist-src/index.js

// pkg/dist-src/version.js
var VERSION$6 = "10.0.16";

// pkg/dist-src/defaults.js
var defaults_default = {
  headers: {
    "user-agent": `octokit-request.js/${VERSION$6} ${getUserAgent()}`
  }
};

// pkg/dist-src/is-plain-object.js
function isPlainObject(value) {
  if (typeof value !== "object" || value === null) return false;
  if (Object.prototype.toString.call(value) !== "[object Object]") return false;
  const proto = Object.getPrototypeOf(value);
  if (proto === null) return true;
  const Ctor = Object.prototype.hasOwnProperty.call(proto, "constructor") && proto.constructor;
  return typeof Ctor === "function" && Ctor instanceof Ctor && Function.prototype.call(Ctor) === Function.prototype.call(value);
}
var noop$1 = () => "";
async function fetchWrapper(requestOptions) {
  const fetch = requestOptions.request?.fetch || globalThis.fetch;
  if (!fetch) {
    throw new Error(
      "fetch is not set. Please pass a fetch implementation as new Octokit({ request: { fetch }}). Learn more at https://github.com/octokit/octokit.js/#fetch-missing"
    );
  }
  const log = requestOptions.request?.log || console;
  const parseSuccessResponseBody = requestOptions.request?.parseSuccessResponseBody !== false;
  const body = isPlainObject(requestOptions.body) || Array.isArray(requestOptions.body) ? JSONStringify(requestOptions.body) : requestOptions.body;
  const requestHeaders = Object.fromEntries(
    Object.entries(requestOptions.headers).map(([name, value]) => [
      name,
      String(value)
    ])
  );
  let fetchResponse;
  try {
    fetchResponse = await fetch(requestOptions.url, {
      method: requestOptions.method,
      body,
      redirect: requestOptions.request?.redirect,
      headers: requestHeaders,
      signal: requestOptions.request?.signal,
      // duplex must be set if request.body is ReadableStream or Async Iterables.
      // See https://fetch.spec.whatwg.org/#dom-requestinit-duplex.
      ...requestOptions.body && { duplex: "half" }
    });
  } catch (error) {
    let message = "Unknown Error";
    if (error instanceof Error) {
      if (error.name === "AbortError") {
        error.status = 500;
        throw error;
      }
      message = error.message;
      if (error.name === "TypeError" && "cause" in error) {
        if (error.cause instanceof Error) {
          message = error.cause.message;
        } else if (typeof error.cause === "string") {
          message = error.cause;
        }
      }
    }
    const requestError = new RequestError(message, 500, {
      request: requestOptions
    });
    requestError.cause = error;
    throw requestError;
  }
  const status = fetchResponse.status;
  const url = fetchResponse.url;
  const responseHeaders = {};
  for (const [key, value] of fetchResponse.headers) {
    responseHeaders[key] = value;
  }
  const octokitResponse = {
    url,
    status,
    headers: responseHeaders,
    data: ""
  };
  if ("deprecation" in responseHeaders) {
    const matches = responseHeaders.link && responseHeaders.link.match(/<([^<>]+)>; rel="deprecation"/);
    const deprecationLink = matches && matches.pop();
    log.warn(
      `[@octokit/request] "${requestOptions.method} ${requestOptions.url}" is deprecated. It is scheduled to be removed on ${responseHeaders.sunset}${deprecationLink ? `. See ${deprecationLink}` : ""}`
    );
  }
  if (status === 204 || status === 205) {
    return octokitResponse;
  }
  if (requestOptions.method === "HEAD") {
    if (status < 400) {
      return octokitResponse;
    }
    throw new RequestError(fetchResponse.statusText, status, {
      response: octokitResponse,
      request: requestOptions
    });
  }
  if (status === 304) {
    octokitResponse.data = await getResponseData(fetchResponse);
    throw new RequestError("Not modified", status, {
      response: octokitResponse,
      request: requestOptions
    });
  }
  if (status >= 400) {
    octokitResponse.data = await getResponseData(fetchResponse);
    throw new RequestError(toErrorMessage(octokitResponse.data), status, {
      response: octokitResponse,
      request: requestOptions
    });
  }
  octokitResponse.data = parseSuccessResponseBody ? await getResponseData(fetchResponse) : fetchResponse.body;
  return octokitResponse;
}
async function getResponseData(response) {
  const contentType = response.headers.get("content-type");
  if (!contentType) {
    return response.text().catch(noop$1);
  }
  const mimetype = parse(contentType);
  if (isJSONResponse(mimetype)) {
    let text = "";
    try {
      text = await response.text();
      return JSONParse(text);
    } catch (err) {
      return text;
    }
  } else if (mimetype.type.startsWith("text/") || // `application/octet-stream` is the canonical "arbitrary binary" type
  // (RFC 2046) and must never be decoded as text, even when the response
  // carries a (misleading) `charset=utf-8` parameter — see #751.
  mimetype.parameters.charset?.toLowerCase() === "utf-8" && mimetype.type !== "application/octet-stream") {
    return response.text().catch(noop$1);
  } else {
    return response.arrayBuffer().catch(
      /* v8 ignore next -- @preserve */
      () => new ArrayBuffer(0)
    );
  }
}
function isJSONResponse(mimetype) {
  return mimetype.type === "application/json" || mimetype.type === "application/scim+json";
}
function toErrorMessage(data) {
  if (typeof data === "string") {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return "Unknown error";
  }
  if (typeof data === "object" && data !== null && "message" in data) {
    const objectData = data;
    const suffix = "documentation_url" in objectData ? ` - ${objectData.documentation_url}` : "";
    return Array.isArray(objectData.errors) ? `${objectData.message}: ${objectData.errors.map((v) => JSON.stringify(v)).join(", ")}${suffix}` : `${objectData.message}${suffix}`;
  }
  return `Unknown error: ${JSON.stringify(data)}`;
}

// pkg/dist-src/with-defaults.js
function withDefaults$1(oldEndpoint, newDefaults) {
  const endpoint2 = oldEndpoint.defaults(newDefaults);
  const newApi = function(route, parameters) {
    const endpointOptions = endpoint2.merge(route, parameters);
    if (!endpointOptions.request || !endpointOptions.request.hook) {
      return fetchWrapper(endpoint2.parse(endpointOptions));
    }
    const request2 = (route2, parameters2) => {
      return fetchWrapper(
        endpoint2.parse(endpoint2.merge(route2, parameters2))
      );
    };
    Object.assign(request2, {
      endpoint: endpoint2,
      defaults: withDefaults$1.bind(null, endpoint2)
    });
    return endpointOptions.request.hook(request2, endpointOptions);
  };
  return Object.assign(newApi, {
    endpoint: endpoint2,
    defaults: withDefaults$1.bind(null, endpoint2)
  });
}

// pkg/dist-src/index.js
var request = withDefaults$1(endpoint, defaults_default);
/* v8 ignore next -- @preserve */
/* v8 ignore else -- @preserve */

// pkg/dist-src/index.js

// pkg/dist-src/version.js
var VERSION$5 = "0.0.0-development";

// pkg/dist-src/error.js
function _buildMessageForResponseErrors(data) {
  return `Request failed due to following response errors:
` + data.errors.map((e) => ` - ${e.message}`).join("\n");
}
var GraphqlResponseError = class extends Error {
  constructor(request2, headers, response) {
    super(_buildMessageForResponseErrors(response));
    this.request = request2;
    this.headers = headers;
    this.response = response;
    this.errors = response.errors;
    this.data = response.data;
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
  request;
  headers;
  response;
  name = "GraphqlResponseError";
  errors;
  data;
};

// pkg/dist-src/graphql.js
var NON_VARIABLE_OPTIONS = [
  "method",
  "baseUrl",
  "url",
  "headers",
  "request",
  "query",
  "mediaType",
  "operationName"
];
var FORBIDDEN_VARIABLE_OPTIONS = ["query", "method", "url"];
var GHES_V3_SUFFIX_REGEX = /\/api\/v3\/?$/;
function graphql(request2, query, options) {
  if (options) {
    if (typeof query === "string" && "query" in options) {
      return Promise.reject(
        new Error(`[@octokit/graphql] "query" cannot be used as variable name`)
      );
    }
    for (const key in options) {
      if (!FORBIDDEN_VARIABLE_OPTIONS.includes(key)) continue;
      return Promise.reject(
        new Error(
          `[@octokit/graphql] "${key}" cannot be used as variable name`
        )
      );
    }
  }
  const parsedOptions = typeof query === "string" ? Object.assign({ query }, options) : query;
  const requestOptions = Object.keys(
    parsedOptions
  ).reduce((result, key) => {
    if (NON_VARIABLE_OPTIONS.includes(key)) {
      result[key] = parsedOptions[key];
      return result;
    }
    if (!result.variables) {
      result.variables = {};
    }
    result.variables[key] = parsedOptions[key];
    return result;
  }, {});
  const baseUrl = parsedOptions.baseUrl || request2.endpoint.DEFAULTS.baseUrl;
  if (GHES_V3_SUFFIX_REGEX.test(baseUrl)) {
    requestOptions.url = baseUrl.replace(GHES_V3_SUFFIX_REGEX, "/api/graphql");
  }
  return request2(requestOptions).then((response) => {
    if (response.data.errors) {
      const headers = {};
      for (const key of Object.keys(response.headers)) {
        headers[key] = response.headers[key];
      }
      throw new GraphqlResponseError(
        requestOptions,
        headers,
        response.data
      );
    }
    return response.data.data;
  });
}

// pkg/dist-src/with-defaults.js
function withDefaults(request2, newDefaults) {
  const newRequest = request2.defaults(newDefaults);
  const newApi = (query, options) => {
    return graphql(newRequest, query, options);
  };
  return Object.assign(newApi, {
    defaults: withDefaults.bind(null, newRequest),
    endpoint: newRequest.endpoint
  });
}

// pkg/dist-src/index.js
withDefaults(request, {
  headers: {
    "user-agent": `octokit-graphql.js/${VERSION$5} ${getUserAgent()}`
  },
  method: "POST",
  url: "/graphql"
});
function withCustomRequest(customRequest) {
  return withDefaults(customRequest, {
    method: "POST",
    url: "/graphql"
  });
}
/* v8 ignore if -- @preserve */

// pkg/dist-src/is-jwt.js
var b64url = "(?:[a-zA-Z0-9_-]+)";
var sep = "\\.";
var jwtRE = new RegExp(`^${b64url}${sep}${b64url}${sep}${b64url}$`);
var isJWT = jwtRE.test.bind(jwtRE);

// pkg/dist-src/auth.js
async function auth(token) {
  const isApp = isJWT(token);
  const isInstallation = token.startsWith("v1.") || token.startsWith("ghs_");
  const isUserToServer = token.startsWith("ghu_");
  const tokenType = isApp ? "app" : isInstallation ? "installation" : isUserToServer ? "user-to-server" : "oauth";
  return {
    type: "token",
    token,
    tokenType
  };
}

// pkg/dist-src/with-authorization-prefix.js
function withAuthorizationPrefix(token) {
  if (token.split(/\./).length === 3) {
    return `bearer ${token}`;
  }
  return `token ${token}`;
}

// pkg/dist-src/hook.js
async function hook(token, request, route, parameters) {
  const endpoint = request.endpoint.merge(
    route,
    parameters
  );
  endpoint.headers.authorization = withAuthorizationPrefix(token);
  return request(endpoint);
}

// pkg/dist-src/index.js
var createTokenAuth = function createTokenAuth2(token) {
  if (!token) {
    throw new Error("[@octokit/auth-token] No token passed to createTokenAuth");
  }
  if (typeof token !== "string") {
    throw new Error(
      "[@octokit/auth-token] Token passed to createTokenAuth is not a string"
    );
  }
  token = token.replace(/^(token|bearer) +/i, "");
  return Object.assign(auth.bind(null, token), {
    hook: hook.bind(null, token)
  });
};

const VERSION$4 = "7.0.8";

const noop = () => {
};
const consoleWarn = console.warn.bind(console);
const consoleError = console.error.bind(console);
function createLogger(logger = {}) {
  if (typeof logger.debug !== "function") {
    logger.debug = noop;
  }
  if (typeof logger.info !== "function") {
    logger.info = noop;
  }
  if (typeof logger.warn !== "function") {
    logger.warn = consoleWarn;
  }
  if (typeof logger.error !== "function") {
    logger.error = consoleError;
  }
  return logger;
}
const userAgentTrail = `octokit-core.js/${VERSION$4} ${getUserAgent()}`;
let Octokit$1 = class Octokit {
  static VERSION = VERSION$4;
  static defaults(defaults) {
    const OctokitWithDefaults = class extends this {
      constructor(...args) {
        const options = args[0] || {};
        if (typeof defaults === "function") {
          super(defaults(options));
          return;
        }
        super(
          Object.assign(
            {},
            defaults,
            options,
            options.userAgent && defaults.userAgent ? {
              userAgent: `${options.userAgent} ${defaults.userAgent}`
            } : null
          )
        );
      }
    };
    return OctokitWithDefaults;
  }
  static plugins = [];
  /**
   * Attach a plugin (or many) to your Octokit instance.
   *
   * @example
   * const API = Octokit.plugin(plugin1, plugin2, plugin3, ...)
   */
  static plugin(...newPlugins) {
    const currentPlugins = this.plugins;
    const NewOctokit = class extends this {
      static plugins = currentPlugins.concat(
        newPlugins.filter((plugin) => !currentPlugins.includes(plugin))
      );
    };
    return NewOctokit;
  }
  constructor(options = {}) {
    const hook = new Hook.Collection();
    const requestDefaults = {
      baseUrl: request.endpoint.DEFAULTS.baseUrl,
      headers: {},
      request: Object.assign({}, options.request, {
        // @ts-ignore internal usage only, no need to type
        hook: hook.bind(null, "request")
      }),
      mediaType: {
        previews: [],
        format: ""
      }
    };
    requestDefaults.headers["user-agent"] = options.userAgent ? `${options.userAgent} ${userAgentTrail}` : userAgentTrail;
    if (options.baseUrl) {
      requestDefaults.baseUrl = options.baseUrl;
    }
    if (options.previews) {
      requestDefaults.mediaType.previews = options.previews;
    }
    if (options.timeZone) {
      requestDefaults.headers["time-zone"] = options.timeZone;
    }
    this.request = request.defaults(requestDefaults);
    this.graphql = withCustomRequest(this.request).defaults(requestDefaults);
    this.log = createLogger(options.log);
    this.hook = hook;
    if (!options.authStrategy) {
      if (!options.auth) {
        this.auth = async () => ({
          type: "unauthenticated"
        });
      } else {
        const auth = createTokenAuth(options.auth);
        hook.wrap("request", auth.hook);
        this.auth = auth;
      }
    } else {
      const { authStrategy, ...otherOptions } = options;
      const auth = authStrategy(
        Object.assign(
          {
            request: this.request,
            log: this.log,
            // we pass the current octokit instance as well as its constructor options
            // to allow for authentication strategies that return a new octokit instance
            // that shares the same internal state as the current one. The original
            // requirement for this was the "event-octokit" authentication strategy
            // of https://github.com/probot/octokit-auth-probot.
            octokit: this,
            octokitOptions: otherOptions
          },
          options.auth
        )
      );
      hook.wrap("request", auth.hook);
      this.auth = auth;
    }
    const classConstructor = this.constructor;
    for (let i = 0; i < classConstructor.plugins.length; ++i) {
      Object.assign(this, classConstructor.plugins[i](this, options));
    }
  }
  // assigned during constructor
  request;
  graphql;
  log;
  hook;
  // TODO: type `octokit.auth` based on passed options.authStrategy
  auth;
};

const VERSION$3 = "6.0.0";

function requestLog(octokit) {
  octokit.hook.wrap("request", (request, options) => {
    octokit.log.debug("request", options);
    const start = Date.now();
    const requestOptions = octokit.request.endpoint.parse(options);
    const path = requestOptions.url.replace(options.baseUrl, "");
    return request(options).then((response) => {
      const requestId = response.headers["x-github-request-id"];
      octokit.log.info(
        `${requestOptions.method} ${path} - ${response.status} with id ${requestId} in ${Date.now() - start}ms`
      );
      return response;
    }).catch((error) => {
      const requestId = error.response?.headers["x-github-request-id"] || "UNKNOWN";
      octokit.log.error(
        `${requestOptions.method} ${path} - ${error.status} with id ${requestId} in ${Date.now() - start}ms`
      );
      throw error;
    });
  });
}
requestLog.VERSION = VERSION$3;

// pkg/dist-src/version.js
var VERSION$2 = "0.0.0-development";

// pkg/dist-src/normalize-paginated-list-response.js
function normalizePaginatedListResponse(response) {
  if (!response.data) {
    return {
      ...response,
      data: []
    };
  }
  const responseNeedsNormalization = ("total_count" in response.data || "total_commits" in response.data) && !("url" in response.data);
  if (!responseNeedsNormalization) return response;
  const incompleteResults = response.data.incomplete_results;
  const repositorySelection = response.data.repository_selection;
  const totalCount = response.data.total_count;
  const totalCommits = response.data.total_commits;
  delete response.data.incomplete_results;
  delete response.data.repository_selection;
  delete response.data.total_count;
  delete response.data.total_commits;
  const namespaceKey = Object.keys(response.data)[0];
  const data = response.data[namespaceKey];
  response.data = data;
  if (typeof incompleteResults !== "undefined") {
    response.data.incomplete_results = incompleteResults;
  }
  if (typeof repositorySelection !== "undefined") {
    response.data.repository_selection = repositorySelection;
  }
  response.data.total_count = totalCount;
  response.data.total_commits = totalCommits;
  return response;
}

// pkg/dist-src/iterator.js
function iterator(octokit, route, parameters) {
  const options = typeof route === "function" ? route.endpoint(parameters) : octokit.request.endpoint(route, parameters);
  const requestMethod = typeof route === "function" ? route : octokit.request;
  const method = options.method;
  const headers = options.headers;
  let url = options.url;
  return {
    [Symbol.asyncIterator]: () => ({
      async next() {
        if (!url) return { done: true };
        try {
          const response = await requestMethod({ method, url, headers });
          const normalizedResponse = normalizePaginatedListResponse(response);
          url = ((normalizedResponse.headers.link || "").match(
            /<([^<>]+)>;\s*rel="next"/
          ) || [])[1];
          if (!url && "total_commits" in normalizedResponse.data) {
            const parsedUrl = new URL(normalizedResponse.url);
            const params = parsedUrl.searchParams;
            const page = parseInt(params.get("page") || "1", 10);
            const per_page = parseInt(params.get("per_page") || "250", 10);
            if (page * per_page < normalizedResponse.data.total_commits) {
              params.set("page", String(page + 1));
              url = parsedUrl.toString();
            }
          }
          return { value: normalizedResponse };
        } catch (error) {
          if (error.status !== 409) throw error;
          url = "";
          return {
            value: {
              status: 200,
              headers: {},
              data: []
            }
          };
        }
      }
    })
  };
}

// pkg/dist-src/paginate.js
function paginate(octokit, route, parameters, mapFn) {
  if (typeof parameters === "function") {
    mapFn = parameters;
    parameters = void 0;
  }
  return gather(
    octokit,
    [],
    iterator(octokit, route, parameters)[Symbol.asyncIterator](),
    mapFn
  );
}
function gather(octokit, results, iterator2, mapFn) {
  return iterator2.next().then((result) => {
    if (result.done) {
      return results;
    }
    let earlyExit = false;
    function done() {
      earlyExit = true;
    }
    results = results.concat(
      mapFn ? mapFn(result.value, done) : result.value.data
    );
    if (earlyExit) {
      return results;
    }
    return gather(octokit, results, iterator2, mapFn);
  });
}

// pkg/dist-src/compose-paginate.js
Object.assign(paginate, {
  iterator
});

// pkg/dist-src/index.js
function paginateRest(octokit) {
  return {
    paginate: Object.assign(paginate.bind(null, octokit), {
      iterator: iterator.bind(null, octokit)
    })
  };
}
paginateRest.VERSION = VERSION$2;

const VERSION$1 = "17.0.0";

const Endpoints = {
  actions: {
    addCustomLabelsToSelfHostedRunnerForOrg: [
      "POST /orgs/{org}/actions/runners/{runner_id}/labels"
    ],
    addCustomLabelsToSelfHostedRunnerForRepo: [
      "POST /repos/{owner}/{repo}/actions/runners/{runner_id}/labels"
    ],
    addRepoAccessToSelfHostedRunnerGroupInOrg: [
      "PUT /orgs/{org}/actions/runner-groups/{runner_group_id}/repositories/{repository_id}"
    ],
    addSelectedRepoToOrgSecret: [
      "PUT /orgs/{org}/actions/secrets/{secret_name}/repositories/{repository_id}"
    ],
    addSelectedRepoToOrgVariable: [
      "PUT /orgs/{org}/actions/variables/{name}/repositories/{repository_id}"
    ],
    approveWorkflowRun: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve"
    ],
    cancelWorkflowRun: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/cancel"
    ],
    createEnvironmentVariable: [
      "POST /repos/{owner}/{repo}/environments/{environment_name}/variables"
    ],
    createHostedRunnerForOrg: ["POST /orgs/{org}/actions/hosted-runners"],
    createOrUpdateEnvironmentSecret: [
      "PUT /repos/{owner}/{repo}/environments/{environment_name}/secrets/{secret_name}"
    ],
    createOrUpdateOrgSecret: ["PUT /orgs/{org}/actions/secrets/{secret_name}"],
    createOrUpdateRepoSecret: [
      "PUT /repos/{owner}/{repo}/actions/secrets/{secret_name}"
    ],
    createOrgVariable: ["POST /orgs/{org}/actions/variables"],
    createRegistrationTokenForOrg: [
      "POST /orgs/{org}/actions/runners/registration-token"
    ],
    createRegistrationTokenForRepo: [
      "POST /repos/{owner}/{repo}/actions/runners/registration-token"
    ],
    createRemoveTokenForOrg: ["POST /orgs/{org}/actions/runners/remove-token"],
    createRemoveTokenForRepo: [
      "POST /repos/{owner}/{repo}/actions/runners/remove-token"
    ],
    createRepoVariable: ["POST /repos/{owner}/{repo}/actions/variables"],
    createWorkflowDispatch: [
      "POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches"
    ],
    deleteActionsCacheById: [
      "DELETE /repos/{owner}/{repo}/actions/caches/{cache_id}"
    ],
    deleteActionsCacheByKey: [
      "DELETE /repos/{owner}/{repo}/actions/caches{?key,ref}"
    ],
    deleteArtifact: [
      "DELETE /repos/{owner}/{repo}/actions/artifacts/{artifact_id}"
    ],
    deleteCustomImageFromOrg: [
      "DELETE /orgs/{org}/actions/hosted-runners/images/custom/{image_definition_id}"
    ],
    deleteCustomImageVersionFromOrg: [
      "DELETE /orgs/{org}/actions/hosted-runners/images/custom/{image_definition_id}/versions/{version}"
    ],
    deleteEnvironmentSecret: [
      "DELETE /repos/{owner}/{repo}/environments/{environment_name}/secrets/{secret_name}"
    ],
    deleteEnvironmentVariable: [
      "DELETE /repos/{owner}/{repo}/environments/{environment_name}/variables/{name}"
    ],
    deleteHostedRunnerForOrg: [
      "DELETE /orgs/{org}/actions/hosted-runners/{hosted_runner_id}"
    ],
    deleteOrgSecret: ["DELETE /orgs/{org}/actions/secrets/{secret_name}"],
    deleteOrgVariable: ["DELETE /orgs/{org}/actions/variables/{name}"],
    deleteRepoSecret: [
      "DELETE /repos/{owner}/{repo}/actions/secrets/{secret_name}"
    ],
    deleteRepoVariable: [
      "DELETE /repos/{owner}/{repo}/actions/variables/{name}"
    ],
    deleteSelfHostedRunnerFromOrg: [
      "DELETE /orgs/{org}/actions/runners/{runner_id}"
    ],
    deleteSelfHostedRunnerFromRepo: [
      "DELETE /repos/{owner}/{repo}/actions/runners/{runner_id}"
    ],
    deleteWorkflowRun: ["DELETE /repos/{owner}/{repo}/actions/runs/{run_id}"],
    deleteWorkflowRunLogs: [
      "DELETE /repos/{owner}/{repo}/actions/runs/{run_id}/logs"
    ],
    disableSelectedRepositoryGithubActionsOrganization: [
      "DELETE /orgs/{org}/actions/permissions/repositories/{repository_id}"
    ],
    disableWorkflow: [
      "PUT /repos/{owner}/{repo}/actions/workflows/{workflow_id}/disable"
    ],
    downloadArtifact: [
      "GET /repos/{owner}/{repo}/actions/artifacts/{artifact_id}/{archive_format}"
    ],
    downloadJobLogsForWorkflowRun: [
      "GET /repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
    ],
    downloadWorkflowRunAttemptLogs: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}/logs"
    ],
    downloadWorkflowRunLogs: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/logs"
    ],
    enableSelectedRepositoryGithubActionsOrganization: [
      "PUT /orgs/{org}/actions/permissions/repositories/{repository_id}"
    ],
    enableWorkflow: [
      "PUT /repos/{owner}/{repo}/actions/workflows/{workflow_id}/enable"
    ],
    forceCancelWorkflowRun: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/force-cancel"
    ],
    generateRunnerJitconfigForOrg: [
      "POST /orgs/{org}/actions/runners/generate-jitconfig"
    ],
    generateRunnerJitconfigForRepo: [
      "POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig"
    ],
    getActionsCacheList: ["GET /repos/{owner}/{repo}/actions/caches"],
    getActionsCacheUsage: ["GET /repos/{owner}/{repo}/actions/cache/usage"],
    getActionsCacheUsageByRepoForOrg: [
      "GET /orgs/{org}/actions/cache/usage-by-repository"
    ],
    getActionsCacheUsageForOrg: ["GET /orgs/{org}/actions/cache/usage"],
    getAllowedActionsOrganization: [
      "GET /orgs/{org}/actions/permissions/selected-actions"
    ],
    getAllowedActionsRepository: [
      "GET /repos/{owner}/{repo}/actions/permissions/selected-actions"
    ],
    getArtifact: ["GET /repos/{owner}/{repo}/actions/artifacts/{artifact_id}"],
    getCustomImageForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/custom/{image_definition_id}"
    ],
    getCustomImageVersionForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/custom/{image_definition_id}/versions/{version}"
    ],
    getCustomOidcSubClaimForRepo: [
      "GET /repos/{owner}/{repo}/actions/oidc/customization/sub"
    ],
    getEnvironmentPublicKey: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/secrets/public-key"
    ],
    getEnvironmentSecret: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/secrets/{secret_name}"
    ],
    getEnvironmentVariable: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/variables/{name}"
    ],
    getGithubActionsDefaultWorkflowPermissionsOrganization: [
      "GET /orgs/{org}/actions/permissions/workflow"
    ],
    getGithubActionsDefaultWorkflowPermissionsRepository: [
      "GET /repos/{owner}/{repo}/actions/permissions/workflow"
    ],
    getGithubActionsPermissionsOrganization: [
      "GET /orgs/{org}/actions/permissions"
    ],
    getGithubActionsPermissionsRepository: [
      "GET /repos/{owner}/{repo}/actions/permissions"
    ],
    getHostedRunnerForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/{hosted_runner_id}"
    ],
    getHostedRunnersGithubOwnedImagesForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/github-owned"
    ],
    getHostedRunnersLimitsForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/limits"
    ],
    getHostedRunnersMachineSpecsForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/machine-sizes"
    ],
    getHostedRunnersPartnerImagesForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/partner"
    ],
    getHostedRunnersPlatformsForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/platforms"
    ],
    getJobForWorkflowRun: ["GET /repos/{owner}/{repo}/actions/jobs/{job_id}"],
    getOrgPublicKey: ["GET /orgs/{org}/actions/secrets/public-key"],
    getOrgSecret: ["GET /orgs/{org}/actions/secrets/{secret_name}"],
    getOrgVariable: ["GET /orgs/{org}/actions/variables/{name}"],
    getPendingDeploymentsForRun: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/pending_deployments"
    ],
    getRepoPermissions: [
      "GET /repos/{owner}/{repo}/actions/permissions",
      {},
      { renamed: ["actions", "getGithubActionsPermissionsRepository"] }
    ],
    getRepoPublicKey: ["GET /repos/{owner}/{repo}/actions/secrets/public-key"],
    getRepoSecret: ["GET /repos/{owner}/{repo}/actions/secrets/{secret_name}"],
    getRepoVariable: ["GET /repos/{owner}/{repo}/actions/variables/{name}"],
    getReviewsForRun: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/approvals"
    ],
    getSelfHostedRunnerForOrg: ["GET /orgs/{org}/actions/runners/{runner_id}"],
    getSelfHostedRunnerForRepo: [
      "GET /repos/{owner}/{repo}/actions/runners/{runner_id}"
    ],
    getWorkflow: ["GET /repos/{owner}/{repo}/actions/workflows/{workflow_id}"],
    getWorkflowAccessToRepository: [
      "GET /repos/{owner}/{repo}/actions/permissions/access"
    ],
    getWorkflowRun: ["GET /repos/{owner}/{repo}/actions/runs/{run_id}"],
    getWorkflowRunAttempt: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}"
    ],
    getWorkflowRunUsage: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/timing"
    ],
    getWorkflowUsage: [
      "GET /repos/{owner}/{repo}/actions/workflows/{workflow_id}/timing"
    ],
    listArtifactsForRepo: ["GET /repos/{owner}/{repo}/actions/artifacts"],
    listCustomImageVersionsForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/custom/{image_definition_id}/versions"
    ],
    listCustomImagesForOrg: [
      "GET /orgs/{org}/actions/hosted-runners/images/custom"
    ],
    listEnvironmentSecrets: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/secrets"
    ],
    listEnvironmentVariables: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/variables"
    ],
    listGithubHostedRunnersInGroupForOrg: [
      "GET /orgs/{org}/actions/runner-groups/{runner_group_id}/hosted-runners"
    ],
    listHostedRunnersForOrg: ["GET /orgs/{org}/actions/hosted-runners"],
    listJobsForWorkflowRun: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
    ],
    listJobsForWorkflowRunAttempt: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}/jobs"
    ],
    listLabelsForSelfHostedRunnerForOrg: [
      "GET /orgs/{org}/actions/runners/{runner_id}/labels"
    ],
    listLabelsForSelfHostedRunnerForRepo: [
      "GET /repos/{owner}/{repo}/actions/runners/{runner_id}/labels"
    ],
    listOrgSecrets: ["GET /orgs/{org}/actions/secrets"],
    listOrgVariables: ["GET /orgs/{org}/actions/variables"],
    listRepoOrganizationSecrets: [
      "GET /repos/{owner}/{repo}/actions/organization-secrets"
    ],
    listRepoOrganizationVariables: [
      "GET /repos/{owner}/{repo}/actions/organization-variables"
    ],
    listRepoSecrets: ["GET /repos/{owner}/{repo}/actions/secrets"],
    listRepoVariables: ["GET /repos/{owner}/{repo}/actions/variables"],
    listRepoWorkflows: ["GET /repos/{owner}/{repo}/actions/workflows"],
    listRunnerApplicationsForOrg: ["GET /orgs/{org}/actions/runners/downloads"],
    listRunnerApplicationsForRepo: [
      "GET /repos/{owner}/{repo}/actions/runners/downloads"
    ],
    listSelectedReposForOrgSecret: [
      "GET /orgs/{org}/actions/secrets/{secret_name}/repositories"
    ],
    listSelectedReposForOrgVariable: [
      "GET /orgs/{org}/actions/variables/{name}/repositories"
    ],
    listSelectedRepositoriesEnabledGithubActionsOrganization: [
      "GET /orgs/{org}/actions/permissions/repositories"
    ],
    listSelfHostedRunnersForOrg: ["GET /orgs/{org}/actions/runners"],
    listSelfHostedRunnersForRepo: ["GET /repos/{owner}/{repo}/actions/runners"],
    listWorkflowRunArtifacts: [
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/artifacts"
    ],
    listWorkflowRuns: [
      "GET /repos/{owner}/{repo}/actions/workflows/{workflow_id}/runs"
    ],
    listWorkflowRunsForRepo: ["GET /repos/{owner}/{repo}/actions/runs"],
    reRunJobForWorkflowRun: [
      "POST /repos/{owner}/{repo}/actions/jobs/{job_id}/rerun"
    ],
    reRunWorkflow: ["POST /repos/{owner}/{repo}/actions/runs/{run_id}/rerun"],
    reRunWorkflowFailedJobs: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs"
    ],
    removeAllCustomLabelsFromSelfHostedRunnerForOrg: [
      "DELETE /orgs/{org}/actions/runners/{runner_id}/labels"
    ],
    removeAllCustomLabelsFromSelfHostedRunnerForRepo: [
      "DELETE /repos/{owner}/{repo}/actions/runners/{runner_id}/labels"
    ],
    removeCustomLabelFromSelfHostedRunnerForOrg: [
      "DELETE /orgs/{org}/actions/runners/{runner_id}/labels/{name}"
    ],
    removeCustomLabelFromSelfHostedRunnerForRepo: [
      "DELETE /repos/{owner}/{repo}/actions/runners/{runner_id}/labels/{name}"
    ],
    removeSelectedRepoFromOrgSecret: [
      "DELETE /orgs/{org}/actions/secrets/{secret_name}/repositories/{repository_id}"
    ],
    removeSelectedRepoFromOrgVariable: [
      "DELETE /orgs/{org}/actions/variables/{name}/repositories/{repository_id}"
    ],
    reviewCustomGatesForRun: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/deployment_protection_rule"
    ],
    reviewPendingDeploymentsForRun: [
      "POST /repos/{owner}/{repo}/actions/runs/{run_id}/pending_deployments"
    ],
    setAllowedActionsOrganization: [
      "PUT /orgs/{org}/actions/permissions/selected-actions"
    ],
    setAllowedActionsRepository: [
      "PUT /repos/{owner}/{repo}/actions/permissions/selected-actions"
    ],
    setCustomLabelsForSelfHostedRunnerForOrg: [
      "PUT /orgs/{org}/actions/runners/{runner_id}/labels"
    ],
    setCustomLabelsForSelfHostedRunnerForRepo: [
      "PUT /repos/{owner}/{repo}/actions/runners/{runner_id}/labels"
    ],
    setCustomOidcSubClaimForRepo: [
      "PUT /repos/{owner}/{repo}/actions/oidc/customization/sub"
    ],
    setGithubActionsDefaultWorkflowPermissionsOrganization: [
      "PUT /orgs/{org}/actions/permissions/workflow"
    ],
    setGithubActionsDefaultWorkflowPermissionsRepository: [
      "PUT /repos/{owner}/{repo}/actions/permissions/workflow"
    ],
    setGithubActionsPermissionsOrganization: [
      "PUT /orgs/{org}/actions/permissions"
    ],
    setGithubActionsPermissionsRepository: [
      "PUT /repos/{owner}/{repo}/actions/permissions"
    ],
    setSelectedReposForOrgSecret: [
      "PUT /orgs/{org}/actions/secrets/{secret_name}/repositories"
    ],
    setSelectedReposForOrgVariable: [
      "PUT /orgs/{org}/actions/variables/{name}/repositories"
    ],
    setSelectedRepositoriesEnabledGithubActionsOrganization: [
      "PUT /orgs/{org}/actions/permissions/repositories"
    ],
    setWorkflowAccessToRepository: [
      "PUT /repos/{owner}/{repo}/actions/permissions/access"
    ],
    updateEnvironmentVariable: [
      "PATCH /repos/{owner}/{repo}/environments/{environment_name}/variables/{name}"
    ],
    updateHostedRunnerForOrg: [
      "PATCH /orgs/{org}/actions/hosted-runners/{hosted_runner_id}"
    ],
    updateOrgVariable: ["PATCH /orgs/{org}/actions/variables/{name}"],
    updateRepoVariable: [
      "PATCH /repos/{owner}/{repo}/actions/variables/{name}"
    ]
  },
  activity: {
    checkRepoIsStarredByAuthenticatedUser: ["GET /user/starred/{owner}/{repo}"],
    deleteRepoSubscription: ["DELETE /repos/{owner}/{repo}/subscription"],
    deleteThreadSubscription: [
      "DELETE /notifications/threads/{thread_id}/subscription"
    ],
    getFeeds: ["GET /feeds"],
    getRepoSubscription: ["GET /repos/{owner}/{repo}/subscription"],
    getThread: ["GET /notifications/threads/{thread_id}"],
    getThreadSubscriptionForAuthenticatedUser: [
      "GET /notifications/threads/{thread_id}/subscription"
    ],
    listEventsForAuthenticatedUser: ["GET /users/{username}/events"],
    listNotificationsForAuthenticatedUser: ["GET /notifications"],
    listOrgEventsForAuthenticatedUser: [
      "GET /users/{username}/events/orgs/{org}"
    ],
    listPublicEvents: ["GET /events"],
    listPublicEventsForRepoNetwork: ["GET /networks/{owner}/{repo}/events"],
    listPublicEventsForUser: ["GET /users/{username}/events/public"],
    listPublicOrgEvents: ["GET /orgs/{org}/events"],
    listReceivedEventsForUser: ["GET /users/{username}/received_events"],
    listReceivedPublicEventsForUser: [
      "GET /users/{username}/received_events/public"
    ],
    listRepoEvents: ["GET /repos/{owner}/{repo}/events"],
    listRepoNotificationsForAuthenticatedUser: [
      "GET /repos/{owner}/{repo}/notifications"
    ],
    listReposStarredByAuthenticatedUser: ["GET /user/starred"],
    listReposStarredByUser: ["GET /users/{username}/starred"],
    listReposWatchedByUser: ["GET /users/{username}/subscriptions"],
    listStargazersForRepo: ["GET /repos/{owner}/{repo}/stargazers"],
    listWatchedReposForAuthenticatedUser: ["GET /user/subscriptions"],
    listWatchersForRepo: ["GET /repos/{owner}/{repo}/subscribers"],
    markNotificationsAsRead: ["PUT /notifications"],
    markRepoNotificationsAsRead: ["PUT /repos/{owner}/{repo}/notifications"],
    markThreadAsDone: ["DELETE /notifications/threads/{thread_id}"],
    markThreadAsRead: ["PATCH /notifications/threads/{thread_id}"],
    setRepoSubscription: ["PUT /repos/{owner}/{repo}/subscription"],
    setThreadSubscription: [
      "PUT /notifications/threads/{thread_id}/subscription"
    ],
    starRepoForAuthenticatedUser: ["PUT /user/starred/{owner}/{repo}"],
    unstarRepoForAuthenticatedUser: ["DELETE /user/starred/{owner}/{repo}"]
  },
  apps: {
    addRepoToInstallation: [
      "PUT /user/installations/{installation_id}/repositories/{repository_id}",
      {},
      { renamed: ["apps", "addRepoToInstallationForAuthenticatedUser"] }
    ],
    addRepoToInstallationForAuthenticatedUser: [
      "PUT /user/installations/{installation_id}/repositories/{repository_id}"
    ],
    checkToken: ["POST /applications/{client_id}/token"],
    createFromManifest: ["POST /app-manifests/{code}/conversions"],
    createInstallationAccessToken: [
      "POST /app/installations/{installation_id}/access_tokens"
    ],
    deleteAuthorization: ["DELETE /applications/{client_id}/grant"],
    deleteInstallation: ["DELETE /app/installations/{installation_id}"],
    deleteToken: ["DELETE /applications/{client_id}/token"],
    getAuthenticated: ["GET /app"],
    getBySlug: ["GET /apps/{app_slug}"],
    getInstallation: ["GET /app/installations/{installation_id}"],
    getOrgInstallation: ["GET /orgs/{org}/installation"],
    getRepoInstallation: ["GET /repos/{owner}/{repo}/installation"],
    getSubscriptionPlanForAccount: [
      "GET /marketplace_listing/accounts/{account_id}"
    ],
    getSubscriptionPlanForAccountStubbed: [
      "GET /marketplace_listing/stubbed/accounts/{account_id}"
    ],
    getUserInstallation: ["GET /users/{username}/installation"],
    getWebhookConfigForApp: ["GET /app/hook/config"],
    getWebhookDelivery: ["GET /app/hook/deliveries/{delivery_id}"],
    listAccountsForPlan: ["GET /marketplace_listing/plans/{plan_id}/accounts"],
    listAccountsForPlanStubbed: [
      "GET /marketplace_listing/stubbed/plans/{plan_id}/accounts"
    ],
    listInstallationReposForAuthenticatedUser: [
      "GET /user/installations/{installation_id}/repositories"
    ],
    listInstallationRequestsForAuthenticatedApp: [
      "GET /app/installation-requests"
    ],
    listInstallations: ["GET /app/installations"],
    listInstallationsForAuthenticatedUser: ["GET /user/installations"],
    listPlans: ["GET /marketplace_listing/plans"],
    listPlansStubbed: ["GET /marketplace_listing/stubbed/plans"],
    listReposAccessibleToInstallation: ["GET /installation/repositories"],
    listSubscriptionsForAuthenticatedUser: ["GET /user/marketplace_purchases"],
    listSubscriptionsForAuthenticatedUserStubbed: [
      "GET /user/marketplace_purchases/stubbed"
    ],
    listWebhookDeliveries: ["GET /app/hook/deliveries"],
    redeliverWebhookDelivery: [
      "POST /app/hook/deliveries/{delivery_id}/attempts"
    ],
    removeRepoFromInstallation: [
      "DELETE /user/installations/{installation_id}/repositories/{repository_id}",
      {},
      { renamed: ["apps", "removeRepoFromInstallationForAuthenticatedUser"] }
    ],
    removeRepoFromInstallationForAuthenticatedUser: [
      "DELETE /user/installations/{installation_id}/repositories/{repository_id}"
    ],
    resetToken: ["PATCH /applications/{client_id}/token"],
    revokeInstallationAccessToken: ["DELETE /installation/token"],
    scopeToken: ["POST /applications/{client_id}/token/scoped"],
    suspendInstallation: ["PUT /app/installations/{installation_id}/suspended"],
    unsuspendInstallation: [
      "DELETE /app/installations/{installation_id}/suspended"
    ],
    updateWebhookConfigForApp: ["PATCH /app/hook/config"]
  },
  billing: {
    getGithubActionsBillingOrg: ["GET /orgs/{org}/settings/billing/actions"],
    getGithubActionsBillingUser: [
      "GET /users/{username}/settings/billing/actions"
    ],
    getGithubBillingPremiumRequestUsageReportOrg: [
      "GET /organizations/{org}/settings/billing/premium_request/usage"
    ],
    getGithubBillingPremiumRequestUsageReportUser: [
      "GET /users/{username}/settings/billing/premium_request/usage"
    ],
    getGithubBillingUsageReportOrg: [
      "GET /organizations/{org}/settings/billing/usage"
    ],
    getGithubBillingUsageReportUser: [
      "GET /users/{username}/settings/billing/usage"
    ],
    getGithubPackagesBillingOrg: ["GET /orgs/{org}/settings/billing/packages"],
    getGithubPackagesBillingUser: [
      "GET /users/{username}/settings/billing/packages"
    ],
    getSharedStorageBillingOrg: [
      "GET /orgs/{org}/settings/billing/shared-storage"
    ],
    getSharedStorageBillingUser: [
      "GET /users/{username}/settings/billing/shared-storage"
    ]
  },
  campaigns: {
    createCampaign: ["POST /orgs/{org}/campaigns"],
    deleteCampaign: ["DELETE /orgs/{org}/campaigns/{campaign_number}"],
    getCampaignSummary: ["GET /orgs/{org}/campaigns/{campaign_number}"],
    listOrgCampaigns: ["GET /orgs/{org}/campaigns"],
    updateCampaign: ["PATCH /orgs/{org}/campaigns/{campaign_number}"]
  },
  checks: {
    create: ["POST /repos/{owner}/{repo}/check-runs"],
    createSuite: ["POST /repos/{owner}/{repo}/check-suites"],
    get: ["GET /repos/{owner}/{repo}/check-runs/{check_run_id}"],
    getSuite: ["GET /repos/{owner}/{repo}/check-suites/{check_suite_id}"],
    listAnnotations: [
      "GET /repos/{owner}/{repo}/check-runs/{check_run_id}/annotations"
    ],
    listForRef: ["GET /repos/{owner}/{repo}/commits/{ref}/check-runs"],
    listForSuite: [
      "GET /repos/{owner}/{repo}/check-suites/{check_suite_id}/check-runs"
    ],
    listSuitesForRef: ["GET /repos/{owner}/{repo}/commits/{ref}/check-suites"],
    rerequestRun: [
      "POST /repos/{owner}/{repo}/check-runs/{check_run_id}/rerequest"
    ],
    rerequestSuite: [
      "POST /repos/{owner}/{repo}/check-suites/{check_suite_id}/rerequest"
    ],
    setSuitesPreferences: [
      "PATCH /repos/{owner}/{repo}/check-suites/preferences"
    ],
    update: ["PATCH /repos/{owner}/{repo}/check-runs/{check_run_id}"]
  },
  codeScanning: {
    commitAutofix: [
      "POST /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}/autofix/commits"
    ],
    createAutofix: [
      "POST /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}/autofix"
    ],
    createVariantAnalysis: [
      "POST /repos/{owner}/{repo}/code-scanning/codeql/variant-analyses"
    ],
    deleteAnalysis: [
      "DELETE /repos/{owner}/{repo}/code-scanning/analyses/{analysis_id}{?confirm_delete}"
    ],
    deleteCodeqlDatabase: [
      "DELETE /repos/{owner}/{repo}/code-scanning/codeql/databases/{language}"
    ],
    getAlert: [
      "GET /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}",
      {},
      { renamedParameters: { alert_id: "alert_number" } }
    ],
    getAnalysis: [
      "GET /repos/{owner}/{repo}/code-scanning/analyses/{analysis_id}"
    ],
    getAutofix: [
      "GET /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}/autofix"
    ],
    getCodeqlDatabase: [
      "GET /repos/{owner}/{repo}/code-scanning/codeql/databases/{language}"
    ],
    getDefaultSetup: ["GET /repos/{owner}/{repo}/code-scanning/default-setup"],
    getSarif: ["GET /repos/{owner}/{repo}/code-scanning/sarifs/{sarif_id}"],
    getVariantAnalysis: [
      "GET /repos/{owner}/{repo}/code-scanning/codeql/variant-analyses/{codeql_variant_analysis_id}"
    ],
    getVariantAnalysisRepoTask: [
      "GET /repos/{owner}/{repo}/code-scanning/codeql/variant-analyses/{codeql_variant_analysis_id}/repos/{repo_owner}/{repo_name}"
    ],
    listAlertInstances: [
      "GET /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}/instances"
    ],
    listAlertsForOrg: ["GET /orgs/{org}/code-scanning/alerts"],
    listAlertsForRepo: ["GET /repos/{owner}/{repo}/code-scanning/alerts"],
    listAlertsInstances: [
      "GET /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}/instances",
      {},
      { renamed: ["codeScanning", "listAlertInstances"] }
    ],
    listCodeqlDatabases: [
      "GET /repos/{owner}/{repo}/code-scanning/codeql/databases"
    ],
    listRecentAnalyses: ["GET /repos/{owner}/{repo}/code-scanning/analyses"],
    updateAlert: [
      "PATCH /repos/{owner}/{repo}/code-scanning/alerts/{alert_number}"
    ],
    updateDefaultSetup: [
      "PATCH /repos/{owner}/{repo}/code-scanning/default-setup"
    ],
    uploadSarif: ["POST /repos/{owner}/{repo}/code-scanning/sarifs"]
  },
  codeSecurity: {
    attachConfiguration: [
      "POST /orgs/{org}/code-security/configurations/{configuration_id}/attach"
    ],
    attachEnterpriseConfiguration: [
      "POST /enterprises/{enterprise}/code-security/configurations/{configuration_id}/attach"
    ],
    createConfiguration: ["POST /orgs/{org}/code-security/configurations"],
    createConfigurationForEnterprise: [
      "POST /enterprises/{enterprise}/code-security/configurations"
    ],
    deleteConfiguration: [
      "DELETE /orgs/{org}/code-security/configurations/{configuration_id}"
    ],
    deleteConfigurationForEnterprise: [
      "DELETE /enterprises/{enterprise}/code-security/configurations/{configuration_id}"
    ],
    detachConfiguration: [
      "DELETE /orgs/{org}/code-security/configurations/detach"
    ],
    getConfiguration: [
      "GET /orgs/{org}/code-security/configurations/{configuration_id}"
    ],
    getConfigurationForRepository: [
      "GET /repos/{owner}/{repo}/code-security-configuration"
    ],
    getConfigurationsForEnterprise: [
      "GET /enterprises/{enterprise}/code-security/configurations"
    ],
    getConfigurationsForOrg: ["GET /orgs/{org}/code-security/configurations"],
    getDefaultConfigurations: [
      "GET /orgs/{org}/code-security/configurations/defaults"
    ],
    getDefaultConfigurationsForEnterprise: [
      "GET /enterprises/{enterprise}/code-security/configurations/defaults"
    ],
    getRepositoriesForConfiguration: [
      "GET /orgs/{org}/code-security/configurations/{configuration_id}/repositories"
    ],
    getRepositoriesForEnterpriseConfiguration: [
      "GET /enterprises/{enterprise}/code-security/configurations/{configuration_id}/repositories"
    ],
    getSingleConfigurationForEnterprise: [
      "GET /enterprises/{enterprise}/code-security/configurations/{configuration_id}"
    ],
    setConfigurationAsDefault: [
      "PUT /orgs/{org}/code-security/configurations/{configuration_id}/defaults"
    ],
    setConfigurationAsDefaultForEnterprise: [
      "PUT /enterprises/{enterprise}/code-security/configurations/{configuration_id}/defaults"
    ],
    updateConfiguration: [
      "PATCH /orgs/{org}/code-security/configurations/{configuration_id}"
    ],
    updateEnterpriseConfiguration: [
      "PATCH /enterprises/{enterprise}/code-security/configurations/{configuration_id}"
    ]
  },
  codesOfConduct: {
    getAllCodesOfConduct: ["GET /codes_of_conduct"],
    getConductCode: ["GET /codes_of_conduct/{key}"]
  },
  codespaces: {
    addRepositoryForSecretForAuthenticatedUser: [
      "PUT /user/codespaces/secrets/{secret_name}/repositories/{repository_id}"
    ],
    addSelectedRepoToOrgSecret: [
      "PUT /orgs/{org}/codespaces/secrets/{secret_name}/repositories/{repository_id}"
    ],
    checkPermissionsForDevcontainer: [
      "GET /repos/{owner}/{repo}/codespaces/permissions_check"
    ],
    codespaceMachinesForAuthenticatedUser: [
      "GET /user/codespaces/{codespace_name}/machines"
    ],
    createForAuthenticatedUser: ["POST /user/codespaces"],
    createOrUpdateOrgSecret: [
      "PUT /orgs/{org}/codespaces/secrets/{secret_name}"
    ],
    createOrUpdateRepoSecret: [
      "PUT /repos/{owner}/{repo}/codespaces/secrets/{secret_name}"
    ],
    createOrUpdateSecretForAuthenticatedUser: [
      "PUT /user/codespaces/secrets/{secret_name}"
    ],
    createWithPrForAuthenticatedUser: [
      "POST /repos/{owner}/{repo}/pulls/{pull_number}/codespaces"
    ],
    createWithRepoForAuthenticatedUser: [
      "POST /repos/{owner}/{repo}/codespaces"
    ],
    deleteForAuthenticatedUser: ["DELETE /user/codespaces/{codespace_name}"],
    deleteFromOrganization: [
      "DELETE /orgs/{org}/members/{username}/codespaces/{codespace_name}"
    ],
    deleteOrgSecret: ["DELETE /orgs/{org}/codespaces/secrets/{secret_name}"],
    deleteRepoSecret: [
      "DELETE /repos/{owner}/{repo}/codespaces/secrets/{secret_name}"
    ],
    deleteSecretForAuthenticatedUser: [
      "DELETE /user/codespaces/secrets/{secret_name}"
    ],
    exportForAuthenticatedUser: [
      "POST /user/codespaces/{codespace_name}/exports"
    ],
    getCodespacesForUserInOrg: [
      "GET /orgs/{org}/members/{username}/codespaces"
    ],
    getExportDetailsForAuthenticatedUser: [
      "GET /user/codespaces/{codespace_name}/exports/{export_id}"
    ],
    getForAuthenticatedUser: ["GET /user/codespaces/{codespace_name}"],
    getOrgPublicKey: ["GET /orgs/{org}/codespaces/secrets/public-key"],
    getOrgSecret: ["GET /orgs/{org}/codespaces/secrets/{secret_name}"],
    getPublicKeyForAuthenticatedUser: [
      "GET /user/codespaces/secrets/public-key"
    ],
    getRepoPublicKey: [
      "GET /repos/{owner}/{repo}/codespaces/secrets/public-key"
    ],
    getRepoSecret: [
      "GET /repos/{owner}/{repo}/codespaces/secrets/{secret_name}"
    ],
    getSecretForAuthenticatedUser: [
      "GET /user/codespaces/secrets/{secret_name}"
    ],
    listDevcontainersInRepositoryForAuthenticatedUser: [
      "GET /repos/{owner}/{repo}/codespaces/devcontainers"
    ],
    listForAuthenticatedUser: ["GET /user/codespaces"],
    listInOrganization: [
      "GET /orgs/{org}/codespaces",
      {},
      { renamedParameters: { org_id: "org" } }
    ],
    listInRepositoryForAuthenticatedUser: [
      "GET /repos/{owner}/{repo}/codespaces"
    ],
    listOrgSecrets: ["GET /orgs/{org}/codespaces/secrets"],
    listRepoSecrets: ["GET /repos/{owner}/{repo}/codespaces/secrets"],
    listRepositoriesForSecretForAuthenticatedUser: [
      "GET /user/codespaces/secrets/{secret_name}/repositories"
    ],
    listSecretsForAuthenticatedUser: ["GET /user/codespaces/secrets"],
    listSelectedReposForOrgSecret: [
      "GET /orgs/{org}/codespaces/secrets/{secret_name}/repositories"
    ],
    preFlightWithRepoForAuthenticatedUser: [
      "GET /repos/{owner}/{repo}/codespaces/new"
    ],
    publishForAuthenticatedUser: [
      "POST /user/codespaces/{codespace_name}/publish"
    ],
    removeRepositoryForSecretForAuthenticatedUser: [
      "DELETE /user/codespaces/secrets/{secret_name}/repositories/{repository_id}"
    ],
    removeSelectedRepoFromOrgSecret: [
      "DELETE /orgs/{org}/codespaces/secrets/{secret_name}/repositories/{repository_id}"
    ],
    repoMachinesForAuthenticatedUser: [
      "GET /repos/{owner}/{repo}/codespaces/machines"
    ],
    setRepositoriesForSecretForAuthenticatedUser: [
      "PUT /user/codespaces/secrets/{secret_name}/repositories"
    ],
    setSelectedReposForOrgSecret: [
      "PUT /orgs/{org}/codespaces/secrets/{secret_name}/repositories"
    ],
    startForAuthenticatedUser: ["POST /user/codespaces/{codespace_name}/start"],
    stopForAuthenticatedUser: ["POST /user/codespaces/{codespace_name}/stop"],
    stopInOrganization: [
      "POST /orgs/{org}/members/{username}/codespaces/{codespace_name}/stop"
    ],
    updateForAuthenticatedUser: ["PATCH /user/codespaces/{codespace_name}"]
  },
  copilot: {
    addCopilotSeatsForTeams: [
      "POST /orgs/{org}/copilot/billing/selected_teams"
    ],
    addCopilotSeatsForUsers: [
      "POST /orgs/{org}/copilot/billing/selected_users"
    ],
    cancelCopilotSeatAssignmentForTeams: [
      "DELETE /orgs/{org}/copilot/billing/selected_teams"
    ],
    cancelCopilotSeatAssignmentForUsers: [
      "DELETE /orgs/{org}/copilot/billing/selected_users"
    ],
    copilotMetricsForOrganization: ["GET /orgs/{org}/copilot/metrics"],
    copilotMetricsForTeam: ["GET /orgs/{org}/team/{team_slug}/copilot/metrics"],
    getCopilotOrganizationDetails: ["GET /orgs/{org}/copilot/billing"],
    getCopilotSeatDetailsForUser: [
      "GET /orgs/{org}/members/{username}/copilot"
    ],
    listCopilotSeats: ["GET /orgs/{org}/copilot/billing/seats"]
  },
  credentials: { revoke: ["POST /credentials/revoke"] },
  dependabot: {
    addSelectedRepoToOrgSecret: [
      "PUT /orgs/{org}/dependabot/secrets/{secret_name}/repositories/{repository_id}"
    ],
    createOrUpdateOrgSecret: [
      "PUT /orgs/{org}/dependabot/secrets/{secret_name}"
    ],
    createOrUpdateRepoSecret: [
      "PUT /repos/{owner}/{repo}/dependabot/secrets/{secret_name}"
    ],
    deleteOrgSecret: ["DELETE /orgs/{org}/dependabot/secrets/{secret_name}"],
    deleteRepoSecret: [
      "DELETE /repos/{owner}/{repo}/dependabot/secrets/{secret_name}"
    ],
    getAlert: ["GET /repos/{owner}/{repo}/dependabot/alerts/{alert_number}"],
    getOrgPublicKey: ["GET /orgs/{org}/dependabot/secrets/public-key"],
    getOrgSecret: ["GET /orgs/{org}/dependabot/secrets/{secret_name}"],
    getRepoPublicKey: [
      "GET /repos/{owner}/{repo}/dependabot/secrets/public-key"
    ],
    getRepoSecret: [
      "GET /repos/{owner}/{repo}/dependabot/secrets/{secret_name}"
    ],
    listAlertsForEnterprise: [
      "GET /enterprises/{enterprise}/dependabot/alerts"
    ],
    listAlertsForOrg: ["GET /orgs/{org}/dependabot/alerts"],
    listAlertsForRepo: ["GET /repos/{owner}/{repo}/dependabot/alerts"],
    listOrgSecrets: ["GET /orgs/{org}/dependabot/secrets"],
    listRepoSecrets: ["GET /repos/{owner}/{repo}/dependabot/secrets"],
    listSelectedReposForOrgSecret: [
      "GET /orgs/{org}/dependabot/secrets/{secret_name}/repositories"
    ],
    removeSelectedRepoFromOrgSecret: [
      "DELETE /orgs/{org}/dependabot/secrets/{secret_name}/repositories/{repository_id}"
    ],
    repositoryAccessForOrg: [
      "GET /organizations/{org}/dependabot/repository-access"
    ],
    setRepositoryAccessDefaultLevel: [
      "PUT /organizations/{org}/dependabot/repository-access/default-level"
    ],
    setSelectedReposForOrgSecret: [
      "PUT /orgs/{org}/dependabot/secrets/{secret_name}/repositories"
    ],
    updateAlert: [
      "PATCH /repos/{owner}/{repo}/dependabot/alerts/{alert_number}"
    ],
    updateRepositoryAccessForOrg: [
      "PATCH /organizations/{org}/dependabot/repository-access"
    ]
  },
  dependencyGraph: {
    createRepositorySnapshot: [
      "POST /repos/{owner}/{repo}/dependency-graph/snapshots"
    ],
    diffRange: [
      "GET /repos/{owner}/{repo}/dependency-graph/compare/{basehead}"
    ],
    exportSbom: ["GET /repos/{owner}/{repo}/dependency-graph/sbom"]
  },
  emojis: { get: ["GET /emojis"] },
  enterpriseTeamMemberships: {
    add: [
      "PUT /enterprises/{enterprise}/teams/{enterprise-team}/memberships/{username}"
    ],
    bulkAdd: [
      "POST /enterprises/{enterprise}/teams/{enterprise-team}/memberships/add"
    ],
    bulkRemove: [
      "POST /enterprises/{enterprise}/teams/{enterprise-team}/memberships/remove"
    ],
    get: [
      "GET /enterprises/{enterprise}/teams/{enterprise-team}/memberships/{username}"
    ],
    list: ["GET /enterprises/{enterprise}/teams/{enterprise-team}/memberships"],
    remove: [
      "DELETE /enterprises/{enterprise}/teams/{enterprise-team}/memberships/{username}"
    ]
  },
  enterpriseTeamOrganizations: {
    add: [
      "PUT /enterprises/{enterprise}/teams/{enterprise-team}/organizations/{org}"
    ],
    bulkAdd: [
      "POST /enterprises/{enterprise}/teams/{enterprise-team}/organizations/add"
    ],
    bulkRemove: [
      "POST /enterprises/{enterprise}/teams/{enterprise-team}/organizations/remove"
    ],
    delete: [
      "DELETE /enterprises/{enterprise}/teams/{enterprise-team}/organizations/{org}"
    ],
    getAssignment: [
      "GET /enterprises/{enterprise}/teams/{enterprise-team}/organizations/{org}"
    ],
    getAssignments: [
      "GET /enterprises/{enterprise}/teams/{enterprise-team}/organizations"
    ]
  },
  enterpriseTeams: {
    create: ["POST /enterprises/{enterprise}/teams"],
    delete: ["DELETE /enterprises/{enterprise}/teams/{team_slug}"],
    get: ["GET /enterprises/{enterprise}/teams/{team_slug}"],
    list: ["GET /enterprises/{enterprise}/teams"],
    update: ["PATCH /enterprises/{enterprise}/teams/{team_slug}"]
  },
  gists: {
    checkIsStarred: ["GET /gists/{gist_id}/star"],
    create: ["POST /gists"],
    createComment: ["POST /gists/{gist_id}/comments"],
    delete: ["DELETE /gists/{gist_id}"],
    deleteComment: ["DELETE /gists/{gist_id}/comments/{comment_id}"],
    fork: ["POST /gists/{gist_id}/forks"],
    get: ["GET /gists/{gist_id}"],
    getComment: ["GET /gists/{gist_id}/comments/{comment_id}"],
    getRevision: ["GET /gists/{gist_id}/{sha}"],
    list: ["GET /gists"],
    listComments: ["GET /gists/{gist_id}/comments"],
    listCommits: ["GET /gists/{gist_id}/commits"],
    listForUser: ["GET /users/{username}/gists"],
    listForks: ["GET /gists/{gist_id}/forks"],
    listPublic: ["GET /gists/public"],
    listStarred: ["GET /gists/starred"],
    star: ["PUT /gists/{gist_id}/star"],
    unstar: ["DELETE /gists/{gist_id}/star"],
    update: ["PATCH /gists/{gist_id}"],
    updateComment: ["PATCH /gists/{gist_id}/comments/{comment_id}"]
  },
  git: {
    createBlob: ["POST /repos/{owner}/{repo}/git/blobs"],
    createCommit: ["POST /repos/{owner}/{repo}/git/commits"],
    createRef: ["POST /repos/{owner}/{repo}/git/refs"],
    createTag: ["POST /repos/{owner}/{repo}/git/tags"],
    createTree: ["POST /repos/{owner}/{repo}/git/trees"],
    deleteRef: ["DELETE /repos/{owner}/{repo}/git/refs/{ref}"],
    getBlob: ["GET /repos/{owner}/{repo}/git/blobs/{file_sha}"],
    getCommit: ["GET /repos/{owner}/{repo}/git/commits/{commit_sha}"],
    getRef: ["GET /repos/{owner}/{repo}/git/ref/{ref}"],
    getTag: ["GET /repos/{owner}/{repo}/git/tags/{tag_sha}"],
    getTree: ["GET /repos/{owner}/{repo}/git/trees/{tree_sha}"],
    listMatchingRefs: ["GET /repos/{owner}/{repo}/git/matching-refs/{ref}"],
    updateRef: ["PATCH /repos/{owner}/{repo}/git/refs/{ref}"]
  },
  gitignore: {
    getAllTemplates: ["GET /gitignore/templates"],
    getTemplate: ["GET /gitignore/templates/{name}"]
  },
  hostedCompute: {
    createNetworkConfigurationForOrg: [
      "POST /orgs/{org}/settings/network-configurations"
    ],
    deleteNetworkConfigurationFromOrg: [
      "DELETE /orgs/{org}/settings/network-configurations/{network_configuration_id}"
    ],
    getNetworkConfigurationForOrg: [
      "GET /orgs/{org}/settings/network-configurations/{network_configuration_id}"
    ],
    getNetworkSettingsForOrg: [
      "GET /orgs/{org}/settings/network-settings/{network_settings_id}"
    ],
    listNetworkConfigurationsForOrg: [
      "GET /orgs/{org}/settings/network-configurations"
    ],
    updateNetworkConfigurationForOrg: [
      "PATCH /orgs/{org}/settings/network-configurations/{network_configuration_id}"
    ]
  },
  interactions: {
    getRestrictionsForAuthenticatedUser: ["GET /user/interaction-limits"],
    getRestrictionsForOrg: ["GET /orgs/{org}/interaction-limits"],
    getRestrictionsForRepo: ["GET /repos/{owner}/{repo}/interaction-limits"],
    getRestrictionsForYourPublicRepos: [
      "GET /user/interaction-limits",
      {},
      { renamed: ["interactions", "getRestrictionsForAuthenticatedUser"] }
    ],
    removeRestrictionsForAuthenticatedUser: ["DELETE /user/interaction-limits"],
    removeRestrictionsForOrg: ["DELETE /orgs/{org}/interaction-limits"],
    removeRestrictionsForRepo: [
      "DELETE /repos/{owner}/{repo}/interaction-limits"
    ],
    removeRestrictionsForYourPublicRepos: [
      "DELETE /user/interaction-limits",
      {},
      { renamed: ["interactions", "removeRestrictionsForAuthenticatedUser"] }
    ],
    setRestrictionsForAuthenticatedUser: ["PUT /user/interaction-limits"],
    setRestrictionsForOrg: ["PUT /orgs/{org}/interaction-limits"],
    setRestrictionsForRepo: ["PUT /repos/{owner}/{repo}/interaction-limits"],
    setRestrictionsForYourPublicRepos: [
      "PUT /user/interaction-limits",
      {},
      { renamed: ["interactions", "setRestrictionsForAuthenticatedUser"] }
    ]
  },
  issues: {
    addAssignees: [
      "POST /repos/{owner}/{repo}/issues/{issue_number}/assignees"
    ],
    addBlockedByDependency: [
      "POST /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by"
    ],
    addLabels: ["POST /repos/{owner}/{repo}/issues/{issue_number}/labels"],
    addSubIssue: [
      "POST /repos/{owner}/{repo}/issues/{issue_number}/sub_issues"
    ],
    checkUserCanBeAssigned: ["GET /repos/{owner}/{repo}/assignees/{assignee}"],
    checkUserCanBeAssignedToIssue: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/assignees/{assignee}"
    ],
    create: ["POST /repos/{owner}/{repo}/issues"],
    createComment: [
      "POST /repos/{owner}/{repo}/issues/{issue_number}/comments"
    ],
    createLabel: ["POST /repos/{owner}/{repo}/labels"],
    createMilestone: ["POST /repos/{owner}/{repo}/milestones"],
    deleteComment: [
      "DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}"
    ],
    deleteLabel: ["DELETE /repos/{owner}/{repo}/labels/{name}"],
    deleteMilestone: [
      "DELETE /repos/{owner}/{repo}/milestones/{milestone_number}"
    ],
    get: ["GET /repos/{owner}/{repo}/issues/{issue_number}"],
    getComment: ["GET /repos/{owner}/{repo}/issues/comments/{comment_id}"],
    getEvent: ["GET /repos/{owner}/{repo}/issues/events/{event_id}"],
    getLabel: ["GET /repos/{owner}/{repo}/labels/{name}"],
    getMilestone: ["GET /repos/{owner}/{repo}/milestones/{milestone_number}"],
    getParent: ["GET /repos/{owner}/{repo}/issues/{issue_number}/parent"],
    list: ["GET /issues"],
    listAssignees: ["GET /repos/{owner}/{repo}/assignees"],
    listComments: ["GET /repos/{owner}/{repo}/issues/{issue_number}/comments"],
    listCommentsForRepo: ["GET /repos/{owner}/{repo}/issues/comments"],
    listDependenciesBlockedBy: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by"
    ],
    listDependenciesBlocking: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocking"
    ],
    listEvents: ["GET /repos/{owner}/{repo}/issues/{issue_number}/events"],
    listEventsForRepo: ["GET /repos/{owner}/{repo}/issues/events"],
    listEventsForTimeline: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/timeline"
    ],
    listForAuthenticatedUser: ["GET /user/issues"],
    listForOrg: ["GET /orgs/{org}/issues"],
    listForRepo: ["GET /repos/{owner}/{repo}/issues"],
    listLabelsForMilestone: [
      "GET /repos/{owner}/{repo}/milestones/{milestone_number}/labels"
    ],
    listLabelsForRepo: ["GET /repos/{owner}/{repo}/labels"],
    listLabelsOnIssue: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/labels"
    ],
    listMilestones: ["GET /repos/{owner}/{repo}/milestones"],
    listSubIssues: [
      "GET /repos/{owner}/{repo}/issues/{issue_number}/sub_issues"
    ],
    lock: ["PUT /repos/{owner}/{repo}/issues/{issue_number}/lock"],
    removeAllLabels: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/labels"
    ],
    removeAssignees: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/assignees"
    ],
    removeDependencyBlockedBy: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by/{issue_id}"
    ],
    removeLabel: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/labels/{name}"
    ],
    removeSubIssue: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/sub_issue"
    ],
    reprioritizeSubIssue: [
      "PATCH /repos/{owner}/{repo}/issues/{issue_number}/sub_issues/priority"
    ],
    setLabels: ["PUT /repos/{owner}/{repo}/issues/{issue_number}/labels"],
    unlock: ["DELETE /repos/{owner}/{repo}/issues/{issue_number}/lock"],
    update: ["PATCH /repos/{owner}/{repo}/issues/{issue_number}"],
    updateComment: ["PATCH /repos/{owner}/{repo}/issues/comments/{comment_id}"],
    updateLabel: ["PATCH /repos/{owner}/{repo}/labels/{name}"],
    updateMilestone: [
      "PATCH /repos/{owner}/{repo}/milestones/{milestone_number}"
    ]
  },
  licenses: {
    get: ["GET /licenses/{license}"],
    getAllCommonlyUsed: ["GET /licenses"],
    getForRepo: ["GET /repos/{owner}/{repo}/license"]
  },
  markdown: {
    render: ["POST /markdown"],
    renderRaw: [
      "POST /markdown/raw",
      { headers: { "content-type": "text/plain; charset=utf-8" } }
    ]
  },
  meta: {
    get: ["GET /meta"],
    getAllVersions: ["GET /versions"],
    getOctocat: ["GET /octocat"],
    getZen: ["GET /zen"],
    root: ["GET /"]
  },
  migrations: {
    deleteArchiveForAuthenticatedUser: [
      "DELETE /user/migrations/{migration_id}/archive"
    ],
    deleteArchiveForOrg: [
      "DELETE /orgs/{org}/migrations/{migration_id}/archive"
    ],
    downloadArchiveForOrg: [
      "GET /orgs/{org}/migrations/{migration_id}/archive"
    ],
    getArchiveForAuthenticatedUser: [
      "GET /user/migrations/{migration_id}/archive"
    ],
    getStatusForAuthenticatedUser: ["GET /user/migrations/{migration_id}"],
    getStatusForOrg: ["GET /orgs/{org}/migrations/{migration_id}"],
    listForAuthenticatedUser: ["GET /user/migrations"],
    listForOrg: ["GET /orgs/{org}/migrations"],
    listReposForAuthenticatedUser: [
      "GET /user/migrations/{migration_id}/repositories"
    ],
    listReposForOrg: ["GET /orgs/{org}/migrations/{migration_id}/repositories"],
    listReposForUser: [
      "GET /user/migrations/{migration_id}/repositories",
      {},
      { renamed: ["migrations", "listReposForAuthenticatedUser"] }
    ],
    startForAuthenticatedUser: ["POST /user/migrations"],
    startForOrg: ["POST /orgs/{org}/migrations"],
    unlockRepoForAuthenticatedUser: [
      "DELETE /user/migrations/{migration_id}/repos/{repo_name}/lock"
    ],
    unlockRepoForOrg: [
      "DELETE /orgs/{org}/migrations/{migration_id}/repos/{repo_name}/lock"
    ]
  },
  oidc: {
    getOidcCustomSubTemplateForOrg: [
      "GET /orgs/{org}/actions/oidc/customization/sub"
    ],
    updateOidcCustomSubTemplateForOrg: [
      "PUT /orgs/{org}/actions/oidc/customization/sub"
    ]
  },
  orgs: {
    addSecurityManagerTeam: [
      "PUT /orgs/{org}/security-managers/teams/{team_slug}",
      {},
      {
        deprecated: "octokit.rest.orgs.addSecurityManagerTeam() is deprecated, see https://docs.github.com/rest/orgs/security-managers#add-a-security-manager-team"
      }
    ],
    assignTeamToOrgRole: [
      "PUT /orgs/{org}/organization-roles/teams/{team_slug}/{role_id}"
    ],
    assignUserToOrgRole: [
      "PUT /orgs/{org}/organization-roles/users/{username}/{role_id}"
    ],
    blockUser: ["PUT /orgs/{org}/blocks/{username}"],
    cancelInvitation: ["DELETE /orgs/{org}/invitations/{invitation_id}"],
    checkBlockedUser: ["GET /orgs/{org}/blocks/{username}"],
    checkMembershipForUser: ["GET /orgs/{org}/members/{username}"],
    checkPublicMembershipForUser: ["GET /orgs/{org}/public_members/{username}"],
    convertMemberToOutsideCollaborator: [
      "PUT /orgs/{org}/outside_collaborators/{username}"
    ],
    createArtifactStorageRecord: [
      "POST /orgs/{org}/artifacts/metadata/storage-record"
    ],
    createInvitation: ["POST /orgs/{org}/invitations"],
    createIssueType: ["POST /orgs/{org}/issue-types"],
    createWebhook: ["POST /orgs/{org}/hooks"],
    customPropertiesForOrgsCreateOrUpdateOrganizationValues: [
      "PATCH /organizations/{org}/org-properties/values"
    ],
    customPropertiesForOrgsGetOrganizationValues: [
      "GET /organizations/{org}/org-properties/values"
    ],
    customPropertiesForReposCreateOrUpdateOrganizationDefinition: [
      "PUT /orgs/{org}/properties/schema/{custom_property_name}"
    ],
    customPropertiesForReposCreateOrUpdateOrganizationDefinitions: [
      "PATCH /orgs/{org}/properties/schema"
    ],
    customPropertiesForReposCreateOrUpdateOrganizationValues: [
      "PATCH /orgs/{org}/properties/values"
    ],
    customPropertiesForReposDeleteOrganizationDefinition: [
      "DELETE /orgs/{org}/properties/schema/{custom_property_name}"
    ],
    customPropertiesForReposGetOrganizationDefinition: [
      "GET /orgs/{org}/properties/schema/{custom_property_name}"
    ],
    customPropertiesForReposGetOrganizationDefinitions: [
      "GET /orgs/{org}/properties/schema"
    ],
    customPropertiesForReposGetOrganizationValues: [
      "GET /orgs/{org}/properties/values"
    ],
    delete: ["DELETE /orgs/{org}"],
    deleteAttestationsBulk: ["POST /orgs/{org}/attestations/delete-request"],
    deleteAttestationsById: [
      "DELETE /orgs/{org}/attestations/{attestation_id}"
    ],
    deleteAttestationsBySubjectDigest: [
      "DELETE /orgs/{org}/attestations/digest/{subject_digest}"
    ],
    deleteIssueType: ["DELETE /orgs/{org}/issue-types/{issue_type_id}"],
    deleteWebhook: ["DELETE /orgs/{org}/hooks/{hook_id}"],
    disableSelectedRepositoryImmutableReleasesOrganization: [
      "DELETE /orgs/{org}/settings/immutable-releases/repositories/{repository_id}"
    ],
    enableSelectedRepositoryImmutableReleasesOrganization: [
      "PUT /orgs/{org}/settings/immutable-releases/repositories/{repository_id}"
    ],
    get: ["GET /orgs/{org}"],
    getImmutableReleasesSettings: [
      "GET /orgs/{org}/settings/immutable-releases"
    ],
    getImmutableReleasesSettingsRepositories: [
      "GET /orgs/{org}/settings/immutable-releases/repositories"
    ],
    getMembershipForAuthenticatedUser: ["GET /user/memberships/orgs/{org}"],
    getMembershipForUser: ["GET /orgs/{org}/memberships/{username}"],
    getOrgRole: ["GET /orgs/{org}/organization-roles/{role_id}"],
    getOrgRulesetHistory: ["GET /orgs/{org}/rulesets/{ruleset_id}/history"],
    getOrgRulesetVersion: [
      "GET /orgs/{org}/rulesets/{ruleset_id}/history/{version_id}"
    ],
    getWebhook: ["GET /orgs/{org}/hooks/{hook_id}"],
    getWebhookConfigForOrg: ["GET /orgs/{org}/hooks/{hook_id}/config"],
    getWebhookDelivery: [
      "GET /orgs/{org}/hooks/{hook_id}/deliveries/{delivery_id}"
    ],
    list: ["GET /organizations"],
    listAppInstallations: ["GET /orgs/{org}/installations"],
    listArtifactStorageRecords: [
      "GET /orgs/{org}/artifacts/{subject_digest}/metadata/storage-records"
    ],
    listAttestationRepositories: ["GET /orgs/{org}/attestations/repositories"],
    listAttestations: ["GET /orgs/{org}/attestations/{subject_digest}"],
    listAttestationsBulk: [
      "POST /orgs/{org}/attestations/bulk-list{?per_page,before,after}"
    ],
    listBlockedUsers: ["GET /orgs/{org}/blocks"],
    listFailedInvitations: ["GET /orgs/{org}/failed_invitations"],
    listForAuthenticatedUser: ["GET /user/orgs"],
    listForUser: ["GET /users/{username}/orgs"],
    listInvitationTeams: ["GET /orgs/{org}/invitations/{invitation_id}/teams"],
    listIssueTypes: ["GET /orgs/{org}/issue-types"],
    listMembers: ["GET /orgs/{org}/members"],
    listMembershipsForAuthenticatedUser: ["GET /user/memberships/orgs"],
    listOrgRoleTeams: ["GET /orgs/{org}/organization-roles/{role_id}/teams"],
    listOrgRoleUsers: ["GET /orgs/{org}/organization-roles/{role_id}/users"],
    listOrgRoles: ["GET /orgs/{org}/organization-roles"],
    listOrganizationFineGrainedPermissions: [
      "GET /orgs/{org}/organization-fine-grained-permissions"
    ],
    listOutsideCollaborators: ["GET /orgs/{org}/outside_collaborators"],
    listPatGrantRepositories: [
      "GET /orgs/{org}/personal-access-tokens/{pat_id}/repositories"
    ],
    listPatGrantRequestRepositories: [
      "GET /orgs/{org}/personal-access-token-requests/{pat_request_id}/repositories"
    ],
    listPatGrantRequests: ["GET /orgs/{org}/personal-access-token-requests"],
    listPatGrants: ["GET /orgs/{org}/personal-access-tokens"],
    listPendingInvitations: ["GET /orgs/{org}/invitations"],
    listPublicMembers: ["GET /orgs/{org}/public_members"],
    listSecurityManagerTeams: [
      "GET /orgs/{org}/security-managers",
      {},
      {
        deprecated: "octokit.rest.orgs.listSecurityManagerTeams() is deprecated, see https://docs.github.com/rest/orgs/security-managers#list-security-manager-teams"
      }
    ],
    listWebhookDeliveries: ["GET /orgs/{org}/hooks/{hook_id}/deliveries"],
    listWebhooks: ["GET /orgs/{org}/hooks"],
    pingWebhook: ["POST /orgs/{org}/hooks/{hook_id}/pings"],
    redeliverWebhookDelivery: [
      "POST /orgs/{org}/hooks/{hook_id}/deliveries/{delivery_id}/attempts"
    ],
    removeMember: ["DELETE /orgs/{org}/members/{username}"],
    removeMembershipForUser: ["DELETE /orgs/{org}/memberships/{username}"],
    removeOutsideCollaborator: [
      "DELETE /orgs/{org}/outside_collaborators/{username}"
    ],
    removePublicMembershipForAuthenticatedUser: [
      "DELETE /orgs/{org}/public_members/{username}"
    ],
    removeSecurityManagerTeam: [
      "DELETE /orgs/{org}/security-managers/teams/{team_slug}",
      {},
      {
        deprecated: "octokit.rest.orgs.removeSecurityManagerTeam() is deprecated, see https://docs.github.com/rest/orgs/security-managers#remove-a-security-manager-team"
      }
    ],
    reviewPatGrantRequest: [
      "POST /orgs/{org}/personal-access-token-requests/{pat_request_id}"
    ],
    reviewPatGrantRequestsInBulk: [
      "POST /orgs/{org}/personal-access-token-requests"
    ],
    revokeAllOrgRolesTeam: [
      "DELETE /orgs/{org}/organization-roles/teams/{team_slug}"
    ],
    revokeAllOrgRolesUser: [
      "DELETE /orgs/{org}/organization-roles/users/{username}"
    ],
    revokeOrgRoleTeam: [
      "DELETE /orgs/{org}/organization-roles/teams/{team_slug}/{role_id}"
    ],
    revokeOrgRoleUser: [
      "DELETE /orgs/{org}/organization-roles/users/{username}/{role_id}"
    ],
    setImmutableReleasesSettings: [
      "PUT /orgs/{org}/settings/immutable-releases"
    ],
    setImmutableReleasesSettingsRepositories: [
      "PUT /orgs/{org}/settings/immutable-releases/repositories"
    ],
    setMembershipForUser: ["PUT /orgs/{org}/memberships/{username}"],
    setPublicMembershipForAuthenticatedUser: [
      "PUT /orgs/{org}/public_members/{username}"
    ],
    unblockUser: ["DELETE /orgs/{org}/blocks/{username}"],
    update: ["PATCH /orgs/{org}"],
    updateIssueType: ["PUT /orgs/{org}/issue-types/{issue_type_id}"],
    updateMembershipForAuthenticatedUser: [
      "PATCH /user/memberships/orgs/{org}"
    ],
    updatePatAccess: ["POST /orgs/{org}/personal-access-tokens/{pat_id}"],
    updatePatAccesses: ["POST /orgs/{org}/personal-access-tokens"],
    updateWebhook: ["PATCH /orgs/{org}/hooks/{hook_id}"],
    updateWebhookConfigForOrg: ["PATCH /orgs/{org}/hooks/{hook_id}/config"]
  },
  packages: {
    deletePackageForAuthenticatedUser: [
      "DELETE /user/packages/{package_type}/{package_name}"
    ],
    deletePackageForOrg: [
      "DELETE /orgs/{org}/packages/{package_type}/{package_name}"
    ],
    deletePackageForUser: [
      "DELETE /users/{username}/packages/{package_type}/{package_name}"
    ],
    deletePackageVersionForAuthenticatedUser: [
      "DELETE /user/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    deletePackageVersionForOrg: [
      "DELETE /orgs/{org}/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    deletePackageVersionForUser: [
      "DELETE /users/{username}/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    getAllPackageVersionsForAPackageOwnedByAnOrg: [
      "GET /orgs/{org}/packages/{package_type}/{package_name}/versions",
      {},
      { renamed: ["packages", "getAllPackageVersionsForPackageOwnedByOrg"] }
    ],
    getAllPackageVersionsForAPackageOwnedByTheAuthenticatedUser: [
      "GET /user/packages/{package_type}/{package_name}/versions",
      {},
      {
        renamed: [
          "packages",
          "getAllPackageVersionsForPackageOwnedByAuthenticatedUser"
        ]
      }
    ],
    getAllPackageVersionsForPackageOwnedByAuthenticatedUser: [
      "GET /user/packages/{package_type}/{package_name}/versions"
    ],
    getAllPackageVersionsForPackageOwnedByOrg: [
      "GET /orgs/{org}/packages/{package_type}/{package_name}/versions"
    ],
    getAllPackageVersionsForPackageOwnedByUser: [
      "GET /users/{username}/packages/{package_type}/{package_name}/versions"
    ],
    getPackageForAuthenticatedUser: [
      "GET /user/packages/{package_type}/{package_name}"
    ],
    getPackageForOrganization: [
      "GET /orgs/{org}/packages/{package_type}/{package_name}"
    ],
    getPackageForUser: [
      "GET /users/{username}/packages/{package_type}/{package_name}"
    ],
    getPackageVersionForAuthenticatedUser: [
      "GET /user/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    getPackageVersionForOrganization: [
      "GET /orgs/{org}/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    getPackageVersionForUser: [
      "GET /users/{username}/packages/{package_type}/{package_name}/versions/{package_version_id}"
    ],
    listDockerMigrationConflictingPackagesForAuthenticatedUser: [
      "GET /user/docker/conflicts"
    ],
    listDockerMigrationConflictingPackagesForOrganization: [
      "GET /orgs/{org}/docker/conflicts"
    ],
    listDockerMigrationConflictingPackagesForUser: [
      "GET /users/{username}/docker/conflicts"
    ],
    listPackagesForAuthenticatedUser: ["GET /user/packages"],
    listPackagesForOrganization: ["GET /orgs/{org}/packages"],
    listPackagesForUser: ["GET /users/{username}/packages"],
    restorePackageForAuthenticatedUser: [
      "POST /user/packages/{package_type}/{package_name}/restore{?token}"
    ],
    restorePackageForOrg: [
      "POST /orgs/{org}/packages/{package_type}/{package_name}/restore{?token}"
    ],
    restorePackageForUser: [
      "POST /users/{username}/packages/{package_type}/{package_name}/restore{?token}"
    ],
    restorePackageVersionForAuthenticatedUser: [
      "POST /user/packages/{package_type}/{package_name}/versions/{package_version_id}/restore"
    ],
    restorePackageVersionForOrg: [
      "POST /orgs/{org}/packages/{package_type}/{package_name}/versions/{package_version_id}/restore"
    ],
    restorePackageVersionForUser: [
      "POST /users/{username}/packages/{package_type}/{package_name}/versions/{package_version_id}/restore"
    ]
  },
  privateRegistries: {
    createOrgPrivateRegistry: ["POST /orgs/{org}/private-registries"],
    deleteOrgPrivateRegistry: [
      "DELETE /orgs/{org}/private-registries/{secret_name}"
    ],
    getOrgPrivateRegistry: ["GET /orgs/{org}/private-registries/{secret_name}"],
    getOrgPublicKey: ["GET /orgs/{org}/private-registries/public-key"],
    listOrgPrivateRegistries: ["GET /orgs/{org}/private-registries"],
    updateOrgPrivateRegistry: [
      "PATCH /orgs/{org}/private-registries/{secret_name}"
    ]
  },
  projects: {
    addItemForOrg: ["POST /orgs/{org}/projectsV2/{project_number}/items"],
    addItemForUser: [
      "POST /users/{username}/projectsV2/{project_number}/items"
    ],
    deleteItemForOrg: [
      "DELETE /orgs/{org}/projectsV2/{project_number}/items/{item_id}"
    ],
    deleteItemForUser: [
      "DELETE /users/{username}/projectsV2/{project_number}/items/{item_id}"
    ],
    getFieldForOrg: [
      "GET /orgs/{org}/projectsV2/{project_number}/fields/{field_id}"
    ],
    getFieldForUser: [
      "GET /users/{username}/projectsV2/{project_number}/fields/{field_id}"
    ],
    getForOrg: ["GET /orgs/{org}/projectsV2/{project_number}"],
    getForUser: ["GET /users/{username}/projectsV2/{project_number}"],
    getOrgItem: ["GET /orgs/{org}/projectsV2/{project_number}/items/{item_id}"],
    getUserItem: [
      "GET /users/{username}/projectsV2/{project_number}/items/{item_id}"
    ],
    listFieldsForOrg: ["GET /orgs/{org}/projectsV2/{project_number}/fields"],
    listFieldsForUser: [
      "GET /users/{username}/projectsV2/{project_number}/fields"
    ],
    listForOrg: ["GET /orgs/{org}/projectsV2"],
    listForUser: ["GET /users/{username}/projectsV2"],
    listItemsForOrg: ["GET /orgs/{org}/projectsV2/{project_number}/items"],
    listItemsForUser: [
      "GET /users/{username}/projectsV2/{project_number}/items"
    ],
    updateItemForOrg: [
      "PATCH /orgs/{org}/projectsV2/{project_number}/items/{item_id}"
    ],
    updateItemForUser: [
      "PATCH /users/{username}/projectsV2/{project_number}/items/{item_id}"
    ]
  },
  pulls: {
    checkIfMerged: ["GET /repos/{owner}/{repo}/pulls/{pull_number}/merge"],
    create: ["POST /repos/{owner}/{repo}/pulls"],
    createReplyForReviewComment: [
      "POST /repos/{owner}/{repo}/pulls/{pull_number}/comments/{comment_id}/replies"
    ],
    createReview: ["POST /repos/{owner}/{repo}/pulls/{pull_number}/reviews"],
    createReviewComment: [
      "POST /repos/{owner}/{repo}/pulls/{pull_number}/comments"
    ],
    deletePendingReview: [
      "DELETE /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}"
    ],
    deleteReviewComment: [
      "DELETE /repos/{owner}/{repo}/pulls/comments/{comment_id}"
    ],
    dismissReview: [
      "PUT /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}/dismissals"
    ],
    get: ["GET /repos/{owner}/{repo}/pulls/{pull_number}"],
    getReview: [
      "GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}"
    ],
    getReviewComment: ["GET /repos/{owner}/{repo}/pulls/comments/{comment_id}"],
    list: ["GET /repos/{owner}/{repo}/pulls"],
    listCommentsForReview: [
      "GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}/comments"
    ],
    listCommits: ["GET /repos/{owner}/{repo}/pulls/{pull_number}/commits"],
    listFiles: ["GET /repos/{owner}/{repo}/pulls/{pull_number}/files"],
    listRequestedReviewers: [
      "GET /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers"
    ],
    listReviewComments: [
      "GET /repos/{owner}/{repo}/pulls/{pull_number}/comments"
    ],
    listReviewCommentsForRepo: ["GET /repos/{owner}/{repo}/pulls/comments"],
    listReviews: ["GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews"],
    merge: ["PUT /repos/{owner}/{repo}/pulls/{pull_number}/merge"],
    removeRequestedReviewers: [
      "DELETE /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers"
    ],
    requestReviewers: [
      "POST /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers"
    ],
    submitReview: [
      "POST /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}/events"
    ],
    update: ["PATCH /repos/{owner}/{repo}/pulls/{pull_number}"],
    updateBranch: [
      "PUT /repos/{owner}/{repo}/pulls/{pull_number}/update-branch"
    ],
    updateReview: [
      "PUT /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}"
    ],
    updateReviewComment: [
      "PATCH /repos/{owner}/{repo}/pulls/comments/{comment_id}"
    ]
  },
  rateLimit: { get: ["GET /rate_limit"] },
  reactions: {
    createForCommitComment: [
      "POST /repos/{owner}/{repo}/comments/{comment_id}/reactions"
    ],
    createForIssue: [
      "POST /repos/{owner}/{repo}/issues/{issue_number}/reactions"
    ],
    createForIssueComment: [
      "POST /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
    ],
    createForPullRequestReviewComment: [
      "POST /repos/{owner}/{repo}/pulls/comments/{comment_id}/reactions"
    ],
    createForRelease: [
      "POST /repos/{owner}/{repo}/releases/{release_id}/reactions"
    ],
    createForTeamDiscussionCommentInOrg: [
      "POST /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}/reactions"
    ],
    createForTeamDiscussionInOrg: [
      "POST /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/reactions"
    ],
    deleteForCommitComment: [
      "DELETE /repos/{owner}/{repo}/comments/{comment_id}/reactions/{reaction_id}"
    ],
    deleteForIssue: [
      "DELETE /repos/{owner}/{repo}/issues/{issue_number}/reactions/{reaction_id}"
    ],
    deleteForIssueComment: [
      "DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions/{reaction_id}"
    ],
    deleteForPullRequestComment: [
      "DELETE /repos/{owner}/{repo}/pulls/comments/{comment_id}/reactions/{reaction_id}"
    ],
    deleteForRelease: [
      "DELETE /repos/{owner}/{repo}/releases/{release_id}/reactions/{reaction_id}"
    ],
    deleteForTeamDiscussion: [
      "DELETE /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/reactions/{reaction_id}"
    ],
    deleteForTeamDiscussionComment: [
      "DELETE /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}/reactions/{reaction_id}"
    ],
    listForCommitComment: [
      "GET /repos/{owner}/{repo}/comments/{comment_id}/reactions"
    ],
    listForIssue: ["GET /repos/{owner}/{repo}/issues/{issue_number}/reactions"],
    listForIssueComment: [
      "GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions"
    ],
    listForPullRequestReviewComment: [
      "GET /repos/{owner}/{repo}/pulls/comments/{comment_id}/reactions"
    ],
    listForRelease: [
      "GET /repos/{owner}/{repo}/releases/{release_id}/reactions"
    ],
    listForTeamDiscussionCommentInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}/reactions"
    ],
    listForTeamDiscussionInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/reactions"
    ]
  },
  repos: {
    acceptInvitation: [
      "PATCH /user/repository_invitations/{invitation_id}",
      {},
      { renamed: ["repos", "acceptInvitationForAuthenticatedUser"] }
    ],
    acceptInvitationForAuthenticatedUser: [
      "PATCH /user/repository_invitations/{invitation_id}"
    ],
    addAppAccessRestrictions: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/apps",
      {},
      { mapToData: "apps" }
    ],
    addCollaborator: ["PUT /repos/{owner}/{repo}/collaborators/{username}"],
    addStatusCheckContexts: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks/contexts",
      {},
      { mapToData: "contexts" }
    ],
    addTeamAccessRestrictions: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/teams",
      {},
      { mapToData: "teams" }
    ],
    addUserAccessRestrictions: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/users",
      {},
      { mapToData: "users" }
    ],
    cancelPagesDeployment: [
      "POST /repos/{owner}/{repo}/pages/deployments/{pages_deployment_id}/cancel"
    ],
    checkAutomatedSecurityFixes: [
      "GET /repos/{owner}/{repo}/automated-security-fixes"
    ],
    checkCollaborator: ["GET /repos/{owner}/{repo}/collaborators/{username}"],
    checkImmutableReleases: ["GET /repos/{owner}/{repo}/immutable-releases"],
    checkPrivateVulnerabilityReporting: [
      "GET /repos/{owner}/{repo}/private-vulnerability-reporting"
    ],
    checkVulnerabilityAlerts: [
      "GET /repos/{owner}/{repo}/vulnerability-alerts"
    ],
    codeownersErrors: ["GET /repos/{owner}/{repo}/codeowners/errors"],
    compareCommits: ["GET /repos/{owner}/{repo}/compare/{base}...{head}"],
    compareCommitsWithBasehead: [
      "GET /repos/{owner}/{repo}/compare/{basehead}"
    ],
    createAttestation: ["POST /repos/{owner}/{repo}/attestations"],
    createAutolink: ["POST /repos/{owner}/{repo}/autolinks"],
    createCommitComment: [
      "POST /repos/{owner}/{repo}/commits/{commit_sha}/comments"
    ],
    createCommitSignatureProtection: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/required_signatures"
    ],
    createCommitStatus: ["POST /repos/{owner}/{repo}/statuses/{sha}"],
    createDeployKey: ["POST /repos/{owner}/{repo}/keys"],
    createDeployment: ["POST /repos/{owner}/{repo}/deployments"],
    createDeploymentBranchPolicy: [
      "POST /repos/{owner}/{repo}/environments/{environment_name}/deployment-branch-policies"
    ],
    createDeploymentProtectionRule: [
      "POST /repos/{owner}/{repo}/environments/{environment_name}/deployment_protection_rules"
    ],
    createDeploymentStatus: [
      "POST /repos/{owner}/{repo}/deployments/{deployment_id}/statuses"
    ],
    createDispatchEvent: ["POST /repos/{owner}/{repo}/dispatches"],
    createForAuthenticatedUser: ["POST /user/repos"],
    createFork: ["POST /repos/{owner}/{repo}/forks"],
    createInOrg: ["POST /orgs/{org}/repos"],
    createOrUpdateEnvironment: [
      "PUT /repos/{owner}/{repo}/environments/{environment_name}"
    ],
    createOrUpdateFileContents: ["PUT /repos/{owner}/{repo}/contents/{path}"],
    createOrgRuleset: ["POST /orgs/{org}/rulesets"],
    createPagesDeployment: ["POST /repos/{owner}/{repo}/pages/deployments"],
    createPagesSite: ["POST /repos/{owner}/{repo}/pages"],
    createRelease: ["POST /repos/{owner}/{repo}/releases"],
    createRepoRuleset: ["POST /repos/{owner}/{repo}/rulesets"],
    createUsingTemplate: [
      "POST /repos/{template_owner}/{template_repo}/generate"
    ],
    createWebhook: ["POST /repos/{owner}/{repo}/hooks"],
    customPropertiesForReposCreateOrUpdateRepositoryValues: [
      "PATCH /repos/{owner}/{repo}/properties/values"
    ],
    customPropertiesForReposGetRepositoryValues: [
      "GET /repos/{owner}/{repo}/properties/values"
    ],
    declineInvitation: [
      "DELETE /user/repository_invitations/{invitation_id}",
      {},
      { renamed: ["repos", "declineInvitationForAuthenticatedUser"] }
    ],
    declineInvitationForAuthenticatedUser: [
      "DELETE /user/repository_invitations/{invitation_id}"
    ],
    delete: ["DELETE /repos/{owner}/{repo}"],
    deleteAccessRestrictions: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/restrictions"
    ],
    deleteAdminBranchProtection: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/enforce_admins"
    ],
    deleteAnEnvironment: [
      "DELETE /repos/{owner}/{repo}/environments/{environment_name}"
    ],
    deleteAutolink: ["DELETE /repos/{owner}/{repo}/autolinks/{autolink_id}"],
    deleteBranchProtection: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection"
    ],
    deleteCommitComment: ["DELETE /repos/{owner}/{repo}/comments/{comment_id}"],
    deleteCommitSignatureProtection: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/required_signatures"
    ],
    deleteDeployKey: ["DELETE /repos/{owner}/{repo}/keys/{key_id}"],
    deleteDeployment: [
      "DELETE /repos/{owner}/{repo}/deployments/{deployment_id}"
    ],
    deleteDeploymentBranchPolicy: [
      "DELETE /repos/{owner}/{repo}/environments/{environment_name}/deployment-branch-policies/{branch_policy_id}"
    ],
    deleteFile: ["DELETE /repos/{owner}/{repo}/contents/{path}"],
    deleteInvitation: [
      "DELETE /repos/{owner}/{repo}/invitations/{invitation_id}"
    ],
    deleteOrgRuleset: ["DELETE /orgs/{org}/rulesets/{ruleset_id}"],
    deletePagesSite: ["DELETE /repos/{owner}/{repo}/pages"],
    deletePullRequestReviewProtection: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/required_pull_request_reviews"
    ],
    deleteRelease: ["DELETE /repos/{owner}/{repo}/releases/{release_id}"],
    deleteReleaseAsset: [
      "DELETE /repos/{owner}/{repo}/releases/assets/{asset_id}"
    ],
    deleteRepoRuleset: ["DELETE /repos/{owner}/{repo}/rulesets/{ruleset_id}"],
    deleteWebhook: ["DELETE /repos/{owner}/{repo}/hooks/{hook_id}"],
    disableAutomatedSecurityFixes: [
      "DELETE /repos/{owner}/{repo}/automated-security-fixes"
    ],
    disableDeploymentProtectionRule: [
      "DELETE /repos/{owner}/{repo}/environments/{environment_name}/deployment_protection_rules/{protection_rule_id}"
    ],
    disableImmutableReleases: [
      "DELETE /repos/{owner}/{repo}/immutable-releases"
    ],
    disablePrivateVulnerabilityReporting: [
      "DELETE /repos/{owner}/{repo}/private-vulnerability-reporting"
    ],
    disableVulnerabilityAlerts: [
      "DELETE /repos/{owner}/{repo}/vulnerability-alerts"
    ],
    downloadArchive: [
      "GET /repos/{owner}/{repo}/zipball/{ref}",
      {},
      { renamed: ["repos", "downloadZipballArchive"] }
    ],
    downloadTarballArchive: ["GET /repos/{owner}/{repo}/tarball/{ref}"],
    downloadZipballArchive: ["GET /repos/{owner}/{repo}/zipball/{ref}"],
    enableAutomatedSecurityFixes: [
      "PUT /repos/{owner}/{repo}/automated-security-fixes"
    ],
    enableImmutableReleases: ["PUT /repos/{owner}/{repo}/immutable-releases"],
    enablePrivateVulnerabilityReporting: [
      "PUT /repos/{owner}/{repo}/private-vulnerability-reporting"
    ],
    enableVulnerabilityAlerts: [
      "PUT /repos/{owner}/{repo}/vulnerability-alerts"
    ],
    generateReleaseNotes: [
      "POST /repos/{owner}/{repo}/releases/generate-notes"
    ],
    get: ["GET /repos/{owner}/{repo}"],
    getAccessRestrictions: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/restrictions"
    ],
    getAdminBranchProtection: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/enforce_admins"
    ],
    getAllDeploymentProtectionRules: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/deployment_protection_rules"
    ],
    getAllEnvironments: ["GET /repos/{owner}/{repo}/environments"],
    getAllStatusCheckContexts: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks/contexts"
    ],
    getAllTopics: ["GET /repos/{owner}/{repo}/topics"],
    getAppsWithAccessToProtectedBranch: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/apps"
    ],
    getAutolink: ["GET /repos/{owner}/{repo}/autolinks/{autolink_id}"],
    getBranch: ["GET /repos/{owner}/{repo}/branches/{branch}"],
    getBranchProtection: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection"
    ],
    getBranchRules: ["GET /repos/{owner}/{repo}/rules/branches/{branch}"],
    getClones: ["GET /repos/{owner}/{repo}/traffic/clones"],
    getCodeFrequencyStats: ["GET /repos/{owner}/{repo}/stats/code_frequency"],
    getCollaboratorPermissionLevel: [
      "GET /repos/{owner}/{repo}/collaborators/{username}/permission"
    ],
    getCombinedStatusForRef: ["GET /repos/{owner}/{repo}/commits/{ref}/status"],
    getCommit: ["GET /repos/{owner}/{repo}/commits/{ref}"],
    getCommitActivityStats: ["GET /repos/{owner}/{repo}/stats/commit_activity"],
    getCommitComment: ["GET /repos/{owner}/{repo}/comments/{comment_id}"],
    getCommitSignatureProtection: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/required_signatures"
    ],
    getCommunityProfileMetrics: ["GET /repos/{owner}/{repo}/community/profile"],
    getContent: ["GET /repos/{owner}/{repo}/contents/{path}"],
    getContributorsStats: ["GET /repos/{owner}/{repo}/stats/contributors"],
    getCustomDeploymentProtectionRule: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/deployment_protection_rules/{protection_rule_id}"
    ],
    getDeployKey: ["GET /repos/{owner}/{repo}/keys/{key_id}"],
    getDeployment: ["GET /repos/{owner}/{repo}/deployments/{deployment_id}"],
    getDeploymentBranchPolicy: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/deployment-branch-policies/{branch_policy_id}"
    ],
    getDeploymentStatus: [
      "GET /repos/{owner}/{repo}/deployments/{deployment_id}/statuses/{status_id}"
    ],
    getEnvironment: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}"
    ],
    getLatestPagesBuild: ["GET /repos/{owner}/{repo}/pages/builds/latest"],
    getLatestRelease: ["GET /repos/{owner}/{repo}/releases/latest"],
    getOrgRuleSuite: ["GET /orgs/{org}/rulesets/rule-suites/{rule_suite_id}"],
    getOrgRuleSuites: ["GET /orgs/{org}/rulesets/rule-suites"],
    getOrgRuleset: ["GET /orgs/{org}/rulesets/{ruleset_id}"],
    getOrgRulesets: ["GET /orgs/{org}/rulesets"],
    getPages: ["GET /repos/{owner}/{repo}/pages"],
    getPagesBuild: ["GET /repos/{owner}/{repo}/pages/builds/{build_id}"],
    getPagesDeployment: [
      "GET /repos/{owner}/{repo}/pages/deployments/{pages_deployment_id}"
    ],
    getPagesHealthCheck: ["GET /repos/{owner}/{repo}/pages/health"],
    getParticipationStats: ["GET /repos/{owner}/{repo}/stats/participation"],
    getPullRequestReviewProtection: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/required_pull_request_reviews"
    ],
    getPunchCardStats: ["GET /repos/{owner}/{repo}/stats/punch_card"],
    getReadme: ["GET /repos/{owner}/{repo}/readme"],
    getReadmeInDirectory: ["GET /repos/{owner}/{repo}/readme/{dir}"],
    getRelease: ["GET /repos/{owner}/{repo}/releases/{release_id}"],
    getReleaseAsset: ["GET /repos/{owner}/{repo}/releases/assets/{asset_id}"],
    getReleaseByTag: ["GET /repos/{owner}/{repo}/releases/tags/{tag}"],
    getRepoRuleSuite: [
      "GET /repos/{owner}/{repo}/rulesets/rule-suites/{rule_suite_id}"
    ],
    getRepoRuleSuites: ["GET /repos/{owner}/{repo}/rulesets/rule-suites"],
    getRepoRuleset: ["GET /repos/{owner}/{repo}/rulesets/{ruleset_id}"],
    getRepoRulesetHistory: [
      "GET /repos/{owner}/{repo}/rulesets/{ruleset_id}/history"
    ],
    getRepoRulesetVersion: [
      "GET /repos/{owner}/{repo}/rulesets/{ruleset_id}/history/{version_id}"
    ],
    getRepoRulesets: ["GET /repos/{owner}/{repo}/rulesets"],
    getStatusChecksProtection: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks"
    ],
    getTeamsWithAccessToProtectedBranch: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/teams"
    ],
    getTopPaths: ["GET /repos/{owner}/{repo}/traffic/popular/paths"],
    getTopReferrers: ["GET /repos/{owner}/{repo}/traffic/popular/referrers"],
    getUsersWithAccessToProtectedBranch: [
      "GET /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/users"
    ],
    getViews: ["GET /repos/{owner}/{repo}/traffic/views"],
    getWebhook: ["GET /repos/{owner}/{repo}/hooks/{hook_id}"],
    getWebhookConfigForRepo: [
      "GET /repos/{owner}/{repo}/hooks/{hook_id}/config"
    ],
    getWebhookDelivery: [
      "GET /repos/{owner}/{repo}/hooks/{hook_id}/deliveries/{delivery_id}"
    ],
    listActivities: ["GET /repos/{owner}/{repo}/activity"],
    listAttestations: [
      "GET /repos/{owner}/{repo}/attestations/{subject_digest}"
    ],
    listAutolinks: ["GET /repos/{owner}/{repo}/autolinks"],
    listBranches: ["GET /repos/{owner}/{repo}/branches"],
    listBranchesForHeadCommit: [
      "GET /repos/{owner}/{repo}/commits/{commit_sha}/branches-where-head"
    ],
    listCollaborators: ["GET /repos/{owner}/{repo}/collaborators"],
    listCommentsForCommit: [
      "GET /repos/{owner}/{repo}/commits/{commit_sha}/comments"
    ],
    listCommitCommentsForRepo: ["GET /repos/{owner}/{repo}/comments"],
    listCommitStatusesForRef: [
      "GET /repos/{owner}/{repo}/commits/{ref}/statuses"
    ],
    listCommits: ["GET /repos/{owner}/{repo}/commits"],
    listContributors: ["GET /repos/{owner}/{repo}/contributors"],
    listCustomDeploymentRuleIntegrations: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/deployment_protection_rules/apps"
    ],
    listDeployKeys: ["GET /repos/{owner}/{repo}/keys"],
    listDeploymentBranchPolicies: [
      "GET /repos/{owner}/{repo}/environments/{environment_name}/deployment-branch-policies"
    ],
    listDeploymentStatuses: [
      "GET /repos/{owner}/{repo}/deployments/{deployment_id}/statuses"
    ],
    listDeployments: ["GET /repos/{owner}/{repo}/deployments"],
    listForAuthenticatedUser: ["GET /user/repos"],
    listForOrg: ["GET /orgs/{org}/repos"],
    listForUser: ["GET /users/{username}/repos"],
    listForks: ["GET /repos/{owner}/{repo}/forks"],
    listInvitations: ["GET /repos/{owner}/{repo}/invitations"],
    listInvitationsForAuthenticatedUser: ["GET /user/repository_invitations"],
    listLanguages: ["GET /repos/{owner}/{repo}/languages"],
    listPagesBuilds: ["GET /repos/{owner}/{repo}/pages/builds"],
    listPublic: ["GET /repositories"],
    listPullRequestsAssociatedWithCommit: [
      "GET /repos/{owner}/{repo}/commits/{commit_sha}/pulls"
    ],
    listReleaseAssets: [
      "GET /repos/{owner}/{repo}/releases/{release_id}/assets"
    ],
    listReleases: ["GET /repos/{owner}/{repo}/releases"],
    listTags: ["GET /repos/{owner}/{repo}/tags"],
    listTeams: ["GET /repos/{owner}/{repo}/teams"],
    listWebhookDeliveries: [
      "GET /repos/{owner}/{repo}/hooks/{hook_id}/deliveries"
    ],
    listWebhooks: ["GET /repos/{owner}/{repo}/hooks"],
    merge: ["POST /repos/{owner}/{repo}/merges"],
    mergeUpstream: ["POST /repos/{owner}/{repo}/merge-upstream"],
    pingWebhook: ["POST /repos/{owner}/{repo}/hooks/{hook_id}/pings"],
    redeliverWebhookDelivery: [
      "POST /repos/{owner}/{repo}/hooks/{hook_id}/deliveries/{delivery_id}/attempts"
    ],
    removeAppAccessRestrictions: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/apps",
      {},
      { mapToData: "apps" }
    ],
    removeCollaborator: [
      "DELETE /repos/{owner}/{repo}/collaborators/{username}"
    ],
    removeStatusCheckContexts: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks/contexts",
      {},
      { mapToData: "contexts" }
    ],
    removeStatusCheckProtection: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks"
    ],
    removeTeamAccessRestrictions: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/teams",
      {},
      { mapToData: "teams" }
    ],
    removeUserAccessRestrictions: [
      "DELETE /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/users",
      {},
      { mapToData: "users" }
    ],
    renameBranch: ["POST /repos/{owner}/{repo}/branches/{branch}/rename"],
    replaceAllTopics: ["PUT /repos/{owner}/{repo}/topics"],
    requestPagesBuild: ["POST /repos/{owner}/{repo}/pages/builds"],
    setAdminBranchProtection: [
      "POST /repos/{owner}/{repo}/branches/{branch}/protection/enforce_admins"
    ],
    setAppAccessRestrictions: [
      "PUT /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/apps",
      {},
      { mapToData: "apps" }
    ],
    setStatusCheckContexts: [
      "PUT /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks/contexts",
      {},
      { mapToData: "contexts" }
    ],
    setTeamAccessRestrictions: [
      "PUT /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/teams",
      {},
      { mapToData: "teams" }
    ],
    setUserAccessRestrictions: [
      "PUT /repos/{owner}/{repo}/branches/{branch}/protection/restrictions/users",
      {},
      { mapToData: "users" }
    ],
    testPushWebhook: ["POST /repos/{owner}/{repo}/hooks/{hook_id}/tests"],
    transfer: ["POST /repos/{owner}/{repo}/transfer"],
    update: ["PATCH /repos/{owner}/{repo}"],
    updateBranchProtection: [
      "PUT /repos/{owner}/{repo}/branches/{branch}/protection"
    ],
    updateCommitComment: ["PATCH /repos/{owner}/{repo}/comments/{comment_id}"],
    updateDeploymentBranchPolicy: [
      "PUT /repos/{owner}/{repo}/environments/{environment_name}/deployment-branch-policies/{branch_policy_id}"
    ],
    updateInformationAboutPagesSite: ["PUT /repos/{owner}/{repo}/pages"],
    updateInvitation: [
      "PATCH /repos/{owner}/{repo}/invitations/{invitation_id}"
    ],
    updateOrgRuleset: ["PUT /orgs/{org}/rulesets/{ruleset_id}"],
    updatePullRequestReviewProtection: [
      "PATCH /repos/{owner}/{repo}/branches/{branch}/protection/required_pull_request_reviews"
    ],
    updateRelease: ["PATCH /repos/{owner}/{repo}/releases/{release_id}"],
    updateReleaseAsset: [
      "PATCH /repos/{owner}/{repo}/releases/assets/{asset_id}"
    ],
    updateRepoRuleset: ["PUT /repos/{owner}/{repo}/rulesets/{ruleset_id}"],
    updateStatusCheckPotection: [
      "PATCH /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks",
      {},
      { renamed: ["repos", "updateStatusCheckProtection"] }
    ],
    updateStatusCheckProtection: [
      "PATCH /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks"
    ],
    updateWebhook: ["PATCH /repos/{owner}/{repo}/hooks/{hook_id}"],
    updateWebhookConfigForRepo: [
      "PATCH /repos/{owner}/{repo}/hooks/{hook_id}/config"
    ],
    uploadReleaseAsset: [
      "POST /repos/{owner}/{repo}/releases/{release_id}/assets{?name,label}",
      { baseUrl: "https://uploads.github.com" }
    ]
  },
  search: {
    code: ["GET /search/code"],
    commits: ["GET /search/commits"],
    issuesAndPullRequests: ["GET /search/issues"],
    labels: ["GET /search/labels"],
    repos: ["GET /search/repositories"],
    topics: ["GET /search/topics"],
    users: ["GET /search/users"]
  },
  secretScanning: {
    createPushProtectionBypass: [
      "POST /repos/{owner}/{repo}/secret-scanning/push-protection-bypasses"
    ],
    getAlert: [
      "GET /repos/{owner}/{repo}/secret-scanning/alerts/{alert_number}"
    ],
    getScanHistory: ["GET /repos/{owner}/{repo}/secret-scanning/scan-history"],
    listAlertsForOrg: ["GET /orgs/{org}/secret-scanning/alerts"],
    listAlertsForRepo: ["GET /repos/{owner}/{repo}/secret-scanning/alerts"],
    listLocationsForAlert: [
      "GET /repos/{owner}/{repo}/secret-scanning/alerts/{alert_number}/locations"
    ],
    listOrgPatternConfigs: [
      "GET /orgs/{org}/secret-scanning/pattern-configurations"
    ],
    updateAlert: [
      "PATCH /repos/{owner}/{repo}/secret-scanning/alerts/{alert_number}"
    ],
    updateOrgPatternConfigs: [
      "PATCH /orgs/{org}/secret-scanning/pattern-configurations"
    ]
  },
  securityAdvisories: {
    createFork: [
      "POST /repos/{owner}/{repo}/security-advisories/{ghsa_id}/forks"
    ],
    createPrivateVulnerabilityReport: [
      "POST /repos/{owner}/{repo}/security-advisories/reports"
    ],
    createRepositoryAdvisory: [
      "POST /repos/{owner}/{repo}/security-advisories"
    ],
    createRepositoryAdvisoryCveRequest: [
      "POST /repos/{owner}/{repo}/security-advisories/{ghsa_id}/cve"
    ],
    getGlobalAdvisory: ["GET /advisories/{ghsa_id}"],
    getRepositoryAdvisory: [
      "GET /repos/{owner}/{repo}/security-advisories/{ghsa_id}"
    ],
    listGlobalAdvisories: ["GET /advisories"],
    listOrgRepositoryAdvisories: ["GET /orgs/{org}/security-advisories"],
    listRepositoryAdvisories: ["GET /repos/{owner}/{repo}/security-advisories"],
    updateRepositoryAdvisory: [
      "PATCH /repos/{owner}/{repo}/security-advisories/{ghsa_id}"
    ]
  },
  teams: {
    addOrUpdateMembershipForUserInOrg: [
      "PUT /orgs/{org}/teams/{team_slug}/memberships/{username}"
    ],
    addOrUpdateRepoPermissionsInOrg: [
      "PUT /orgs/{org}/teams/{team_slug}/repos/{owner}/{repo}"
    ],
    checkPermissionsForRepoInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/repos/{owner}/{repo}"
    ],
    create: ["POST /orgs/{org}/teams"],
    createDiscussionCommentInOrg: [
      "POST /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments"
    ],
    createDiscussionInOrg: ["POST /orgs/{org}/teams/{team_slug}/discussions"],
    deleteDiscussionCommentInOrg: [
      "DELETE /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}"
    ],
    deleteDiscussionInOrg: [
      "DELETE /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}"
    ],
    deleteInOrg: ["DELETE /orgs/{org}/teams/{team_slug}"],
    getByName: ["GET /orgs/{org}/teams/{team_slug}"],
    getDiscussionCommentInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}"
    ],
    getDiscussionInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}"
    ],
    getMembershipForUserInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/memberships/{username}"
    ],
    list: ["GET /orgs/{org}/teams"],
    listChildInOrg: ["GET /orgs/{org}/teams/{team_slug}/teams"],
    listDiscussionCommentsInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments"
    ],
    listDiscussionsInOrg: ["GET /orgs/{org}/teams/{team_slug}/discussions"],
    listForAuthenticatedUser: ["GET /user/teams"],
    listMembersInOrg: ["GET /orgs/{org}/teams/{team_slug}/members"],
    listPendingInvitationsInOrg: [
      "GET /orgs/{org}/teams/{team_slug}/invitations"
    ],
    listReposInOrg: ["GET /orgs/{org}/teams/{team_slug}/repos"],
    removeMembershipForUserInOrg: [
      "DELETE /orgs/{org}/teams/{team_slug}/memberships/{username}"
    ],
    removeRepoInOrg: [
      "DELETE /orgs/{org}/teams/{team_slug}/repos/{owner}/{repo}"
    ],
    updateDiscussionCommentInOrg: [
      "PATCH /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}/comments/{comment_number}"
    ],
    updateDiscussionInOrg: [
      "PATCH /orgs/{org}/teams/{team_slug}/discussions/{discussion_number}"
    ],
    updateInOrg: ["PATCH /orgs/{org}/teams/{team_slug}"]
  },
  users: {
    addEmailForAuthenticated: [
      "POST /user/emails",
      {},
      { renamed: ["users", "addEmailForAuthenticatedUser"] }
    ],
    addEmailForAuthenticatedUser: ["POST /user/emails"],
    addSocialAccountForAuthenticatedUser: ["POST /user/social_accounts"],
    block: ["PUT /user/blocks/{username}"],
    checkBlocked: ["GET /user/blocks/{username}"],
    checkFollowingForUser: ["GET /users/{username}/following/{target_user}"],
    checkPersonIsFollowedByAuthenticated: ["GET /user/following/{username}"],
    createGpgKeyForAuthenticated: [
      "POST /user/gpg_keys",
      {},
      { renamed: ["users", "createGpgKeyForAuthenticatedUser"] }
    ],
    createGpgKeyForAuthenticatedUser: ["POST /user/gpg_keys"],
    createPublicSshKeyForAuthenticated: [
      "POST /user/keys",
      {},
      { renamed: ["users", "createPublicSshKeyForAuthenticatedUser"] }
    ],
    createPublicSshKeyForAuthenticatedUser: ["POST /user/keys"],
    createSshSigningKeyForAuthenticatedUser: ["POST /user/ssh_signing_keys"],
    deleteAttestationsBulk: [
      "POST /users/{username}/attestations/delete-request"
    ],
    deleteAttestationsById: [
      "DELETE /users/{username}/attestations/{attestation_id}"
    ],
    deleteAttestationsBySubjectDigest: [
      "DELETE /users/{username}/attestations/digest/{subject_digest}"
    ],
    deleteEmailForAuthenticated: [
      "DELETE /user/emails",
      {},
      { renamed: ["users", "deleteEmailForAuthenticatedUser"] }
    ],
    deleteEmailForAuthenticatedUser: ["DELETE /user/emails"],
    deleteGpgKeyForAuthenticated: [
      "DELETE /user/gpg_keys/{gpg_key_id}",
      {},
      { renamed: ["users", "deleteGpgKeyForAuthenticatedUser"] }
    ],
    deleteGpgKeyForAuthenticatedUser: ["DELETE /user/gpg_keys/{gpg_key_id}"],
    deletePublicSshKeyForAuthenticated: [
      "DELETE /user/keys/{key_id}",
      {},
      { renamed: ["users", "deletePublicSshKeyForAuthenticatedUser"] }
    ],
    deletePublicSshKeyForAuthenticatedUser: ["DELETE /user/keys/{key_id}"],
    deleteSocialAccountForAuthenticatedUser: ["DELETE /user/social_accounts"],
    deleteSshSigningKeyForAuthenticatedUser: [
      "DELETE /user/ssh_signing_keys/{ssh_signing_key_id}"
    ],
    follow: ["PUT /user/following/{username}"],
    getAuthenticated: ["GET /user"],
    getById: ["GET /user/{account_id}"],
    getByUsername: ["GET /users/{username}"],
    getContextForUser: ["GET /users/{username}/hovercard"],
    getGpgKeyForAuthenticated: [
      "GET /user/gpg_keys/{gpg_key_id}",
      {},
      { renamed: ["users", "getGpgKeyForAuthenticatedUser"] }
    ],
    getGpgKeyForAuthenticatedUser: ["GET /user/gpg_keys/{gpg_key_id}"],
    getPublicSshKeyForAuthenticated: [
      "GET /user/keys/{key_id}",
      {},
      { renamed: ["users", "getPublicSshKeyForAuthenticatedUser"] }
    ],
    getPublicSshKeyForAuthenticatedUser: ["GET /user/keys/{key_id}"],
    getSshSigningKeyForAuthenticatedUser: [
      "GET /user/ssh_signing_keys/{ssh_signing_key_id}"
    ],
    list: ["GET /users"],
    listAttestations: ["GET /users/{username}/attestations/{subject_digest}"],
    listAttestationsBulk: [
      "POST /users/{username}/attestations/bulk-list{?per_page,before,after}"
    ],
    listBlockedByAuthenticated: [
      "GET /user/blocks",
      {},
      { renamed: ["users", "listBlockedByAuthenticatedUser"] }
    ],
    listBlockedByAuthenticatedUser: ["GET /user/blocks"],
    listEmailsForAuthenticated: [
      "GET /user/emails",
      {},
      { renamed: ["users", "listEmailsForAuthenticatedUser"] }
    ],
    listEmailsForAuthenticatedUser: ["GET /user/emails"],
    listFollowedByAuthenticated: [
      "GET /user/following",
      {},
      { renamed: ["users", "listFollowedByAuthenticatedUser"] }
    ],
    listFollowedByAuthenticatedUser: ["GET /user/following"],
    listFollowersForAuthenticatedUser: ["GET /user/followers"],
    listFollowersForUser: ["GET /users/{username}/followers"],
    listFollowingForUser: ["GET /users/{username}/following"],
    listGpgKeysForAuthenticated: [
      "GET /user/gpg_keys",
      {},
      { renamed: ["users", "listGpgKeysForAuthenticatedUser"] }
    ],
    listGpgKeysForAuthenticatedUser: ["GET /user/gpg_keys"],
    listGpgKeysForUser: ["GET /users/{username}/gpg_keys"],
    listPublicEmailsForAuthenticated: [
      "GET /user/public_emails",
      {},
      { renamed: ["users", "listPublicEmailsForAuthenticatedUser"] }
    ],
    listPublicEmailsForAuthenticatedUser: ["GET /user/public_emails"],
    listPublicKeysForUser: ["GET /users/{username}/keys"],
    listPublicSshKeysForAuthenticated: [
      "GET /user/keys",
      {},
      { renamed: ["users", "listPublicSshKeysForAuthenticatedUser"] }
    ],
    listPublicSshKeysForAuthenticatedUser: ["GET /user/keys"],
    listSocialAccountsForAuthenticatedUser: ["GET /user/social_accounts"],
    listSocialAccountsForUser: ["GET /users/{username}/social_accounts"],
    listSshSigningKeysForAuthenticatedUser: ["GET /user/ssh_signing_keys"],
    listSshSigningKeysForUser: ["GET /users/{username}/ssh_signing_keys"],
    setPrimaryEmailVisibilityForAuthenticated: [
      "PATCH /user/email/visibility",
      {},
      { renamed: ["users", "setPrimaryEmailVisibilityForAuthenticatedUser"] }
    ],
    setPrimaryEmailVisibilityForAuthenticatedUser: [
      "PATCH /user/email/visibility"
    ],
    unblock: ["DELETE /user/blocks/{username}"],
    unfollow: ["DELETE /user/following/{username}"],
    updateAuthenticated: ["PATCH /user"]
  }
};
var endpoints_default = Endpoints;

const endpointMethodsMap = /* @__PURE__ */ new Map();
for (const [scope, endpoints] of Object.entries(endpoints_default)) {
  for (const [methodName, endpoint] of Object.entries(endpoints)) {
    const [route, defaults, decorations] = endpoint;
    const [method, url] = route.split(/ /);
    const endpointDefaults = Object.assign(
      {
        method,
        url
      },
      defaults
    );
    if (!endpointMethodsMap.has(scope)) {
      endpointMethodsMap.set(scope, /* @__PURE__ */ new Map());
    }
    endpointMethodsMap.get(scope).set(methodName, {
      scope,
      methodName,
      endpointDefaults,
      decorations
    });
  }
}
const handler = {
  has({ scope }, methodName) {
    return endpointMethodsMap.get(scope).has(methodName);
  },
  getOwnPropertyDescriptor(target, methodName) {
    return {
      value: this.get(target, methodName),
      // ensures method is in the cache
      configurable: true,
      writable: true,
      enumerable: true
    };
  },
  defineProperty(target, methodName, descriptor) {
    Object.defineProperty(target.cache, methodName, descriptor);
    return true;
  },
  deleteProperty(target, methodName) {
    delete target.cache[methodName];
    return true;
  },
  ownKeys({ scope }) {
    return [...endpointMethodsMap.get(scope).keys()];
  },
  set(target, methodName, value) {
    return target.cache[methodName] = value;
  },
  get({ octokit, scope, cache }, methodName) {
    if (cache[methodName]) {
      return cache[methodName];
    }
    const method = endpointMethodsMap.get(scope).get(methodName);
    if (!method) {
      return void 0;
    }
    const { endpointDefaults, decorations } = method;
    if (decorations) {
      cache[methodName] = decorate(
        octokit,
        scope,
        methodName,
        endpointDefaults,
        decorations
      );
    } else {
      cache[methodName] = octokit.request.defaults(endpointDefaults);
    }
    return cache[methodName];
  }
};
function endpointsToMethods(octokit) {
  const newMethods = {};
  for (const scope of endpointMethodsMap.keys()) {
    newMethods[scope] = new Proxy({ octokit, scope, cache: {} }, handler);
  }
  return newMethods;
}
function decorate(octokit, scope, methodName, defaults, decorations) {
  const requestWithDefaults = octokit.request.defaults(defaults);
  function withDecorations(...args) {
    let options = requestWithDefaults.endpoint.merge(...args);
    if (decorations.mapToData) {
      options = Object.assign({}, options, {
        data: options[decorations.mapToData],
        [decorations.mapToData]: void 0
      });
      return requestWithDefaults(options);
    }
    if (decorations.renamed) {
      const [newScope, newMethodName] = decorations.renamed;
      octokit.log.warn(
        `octokit.${scope}.${methodName}() has been renamed to octokit.${newScope}.${newMethodName}()`
      );
    }
    if (decorations.deprecated) {
      octokit.log.warn(decorations.deprecated);
    }
    if (decorations.renamedParameters) {
      const options2 = requestWithDefaults.endpoint.merge(...args);
      for (const [name, alias] of Object.entries(
        decorations.renamedParameters
      )) {
        if (name in options2) {
          octokit.log.warn(
            `"${name}" parameter is deprecated for "octokit.${scope}.${methodName}()". Use "${alias}" instead`
          );
          if (!(alias in options2)) {
            options2[alias] = options2[name];
          }
          delete options2[name];
        }
      }
      return requestWithDefaults(options2);
    }
    return requestWithDefaults(...args);
  }
  return Object.assign(withDecorations, requestWithDefaults);
}

function legacyRestEndpointMethods(octokit) {
  const api = endpointsToMethods(octokit);
  return {
    ...api,
    rest: api
  };
}
legacyRestEndpointMethods.VERSION = VERSION$1;

const VERSION = "22.0.1";

const Octokit = Octokit$1.plugin(requestLog, legacyRestEndpointMethods, paginateRest).defaults(
  {
    userAgent: `octokit-rest.js/${VERSION}`
  }
);

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
// STALE is a check GitHub gave up on: it never passed.
const FAILED = [
    'FAILURE',
    'ERROR',
    'TIMED_OUT',
    'CANCELLED',
    'ACTION_REQUIRED',
    'STARTUP_FAILURE',
    'STALE',
];
const FEEDBACK = `query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) { pullRequest(number: $number) {
    state headRefOid url
    reviewThreads(first: 100, after: $after) { pageInfo { hasNextPage endCursor }
      nodes { id isResolved path line
      comments(last: 20) { nodes { databaseId author { login } body url } } } }
    reviews(last: 50) { nodes { databaseId author { login __typename } body url
      comments { totalCount } } }
    comments(last: 50) { nodes { databaseId author { login __typename } body url } }
    commits(last: 1) { nodes { commit { statusCheckRollup { contexts(first: 100) { nodes {
      ... on CheckRun { name conclusion detailsUrl }
      ... on StatusContext { context state targetUrl } } } } } } }
  } }
}`;
const RESOLVE = `mutation($id: ID!) {
  resolveReviewThread(input: { threadId: $id }) { clientMutationId }
}`;
/** Bots wrap their comments in HTML comments and collapsed sections the worker never needs. */
function trim(body, max = 1500) {
    const text = body
        .replace(/<!--[\s\S]*?-->/g, '')
        .replace(/<details>[\s\S]*?<\/details>/g, '')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
    return text.length > max ? `${text.slice(0, max)}… (full text at the link)` : text;
}
/** Amp configures git's credential helper in Orbs, so this needs nothing beyond git itself. */
async function token(cwd) {
    const env = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
    if (env)
        return env;
    const fill = await git(cwd, ['credential', 'fill'], 'protocol=https\nhost=github.com\n\n');
    const password = fill.match(/^password=(.*)$/m)?.[1];
    if (!password)
        throw new Error('git has no credentials for github.com');
    return password;
}
/**
 * The review comments and the CI verdict of a stage PR, from the {@link FEEDBACK} query; nothing
 * once it merged. `posted` are the comments the plugin wrote; `head` is the revision it published.
 */
function readFeedback(data, stage, number, posted = [], head) {
    const pr = data.repository.pullRequest;
    // The plugin posts with the operator's token, so only the IDs it recorded tell its replies apart.
    const ours = (comment) => posted.includes(comment.databaseId);
    const finding = (source, key, title, body, url) => ({
        ...{ source, key, title, body, stage, revision: pr.headRefOid },
        ...{ blocking: true, links: [url] },
    });
    // A PR closed unmerged, or one whose head moved past what the job published, no longer carries
    // the assessed candidate, so its CI says nothing about it. Its comments are still heard.
    const ci = (title, body) => finding('ci', `ci:${stage}`, title, body, pr.url);
    const stale = pr.state === 'CLOSED'
        ? ci(`PR #${number} was closed without merging`, 'Reopen it or publish the stage again.')
        : head && pr.headRefOid !== head
            ? ci(`PR #${number} head is ${pr.headRefOid.slice(0, 10)}, not the published ${head.slice(0, 10)}`, 'Something else pushed to its branch; claim the stage again.')
            : undefined;
    const comments = [];
    const keys = new Set();
    for (const t of pr.reviewThreads.nodes) {
        const thread = t.comments.nodes;
        const last = thread.at(-1);
        if (t.isResolved || !last || ours(last))
            continue;
        const body = thread.map((c) => `${c.author?.login}: ${trim(c.body)}`).join('\n');
        // Keyed by content, not ID: a stacked PR repeats its ancestors' comments, on other lines. Two
        // threads on one PR with the same text are still two.
        let key = `comment:${hash(t.path, thread.filter((c) => !ours(c)).map((c) => c.body))}`;
        if (keys.has(key))
            key = `${key}:${t.id}`;
        keys.add(key);
        comments.push({
            ...finding('comment', key, `review comment on PR #${number} · ${t.path}:${t.line}`, body, last.url),
            replies: [{ pr: number, comment: last.databaseId, thread: t.id }],
        });
    }
    // People are always heard. Bots are heard in review threads only: their review bodies summarize
    // those threads, and their conversation comments are reports CI already covers. The body is part
    // of the key, so an edited comment is a new finding.
    const human = (c) => c.body.trim() && !ours(c) && c.author?.__typename !== 'Bot';
    for (const [kind, nodes] of [
        ['review', pr.reviews.nodes],
        ['comment', pr.comments.nodes],
    ])
        for (const c of nodes.filter(human))
            comments.push(finding('comment', `${kind}:${c.databaseId}:${hash(c.body)}`, `${kind} by ${c.author?.login} on PR #${number}`, trim(c.body), c.url));
    if (stale)
        return { comments, pending: false, failures: [stale] };
    // The gate reports after the slowest check, so until then a failed check is reported at once
    // under the gate; once it reports, only an explicit success passes.
    const runs = (pr.commits.nodes[0]?.commit.statusCheckRollup?.contexts.nodes ?? []).map((c) => ({
        name: c.name ?? c.context,
        result: c.conclusion ?? c.state,
        url: c.detailsUrl ?? c.targetUrl,
    }));
    const gate = runs.find((r) => r.name === CI_GATE);
    const pending = !gate || gate.result == null || ['PENDING', 'EXPECTED'].includes(gate.result);
    const failed = runs.filter((r) => r !== gate && FAILED.includes(r.result) && !ADVISORY.test(r.name));
    const failing = pending ? failed.length > 0 : gate.result !== 'SUCCESS';
    const list = failed.map((r) => `- ${r.name}: ${r.url}`).join('\n') || 'No failed check is listed.';
    const failures = failing
        ? [
            finding('ci', `ci:${stage}`, `CI ${CI_GATE} failed on PR #${number}`, `${list}\nRead the run log to find which stage the fix belongs in.`, gate?.url ?? failed[0]?.url ?? pr.url),
        ]
        : [];
    return { comments, pending, failures };
}
async function github(cwd, repository) {
    const [owner, repo] = repository.split('/');
    const octokit = new Octokit({ auth: await token(cwd) });
    return {
        async feedback(stage, number, posted, head) {
            const query = (after) => octokit.graphql(FEEDBACK, { owner, name: repo, number, after });
            const data = await query();
            // Only review threads are paged in full: they are where unresolved feedback lives.
            const threads = data.repository.pullRequest.reviewThreads;
            for (let page = threads; page.pageInfo.hasNextPage;) {
                page = (await query(page.pageInfo.endCursor)).repository.pullRequest.reviewThreads;
                threads.nodes.push(...page.nodes);
            }
            return readFeedback(data, stage, number, posted, head);
        },
        /**
         * Pushes `candidate` to `branch` and opens the PR from it into `base`, or updates it. An
         * existing PR keeps its base: GitHub refuses to change it within a stack.
         */
        async publish(o) {
            const fetched = o.existing
                ? (await octokit.rest.pulls.get({ owner, repo, pull_number: o.existing.number })).data
                : undefined;
            // A merged or closed PR cannot carry a new candidate: a new one replaces it.
            const current = fetched?.state === 'open' ? fetched : undefined;
            // The lease refuses to overwrite anything but this job's last publication, or, with none, a
            // branch that exists at all. A retry after a successful push, including one whose PR
            // creation then failed, needs only the unfinished metadata update.
            const pushed = current
                ? current.head.sha
                : (await git(cwd, ['ls-remote', o.remote, `refs/heads/${o.branch}`])).split('\t')[0];
            if (pushed !== o.candidate) {
                const expected = pushed ? o.existing?.head : '';
                if (expected === undefined)
                    throw new Error(`${o.branch} has commits this job did not push: choose another branchPrefix or delete it`);
                const lease = `--force-with-lease=${o.branch}:${expected}`;
                await git(cwd, ['push', lease, o.remote, `${o.candidate}:refs/heads/${o.branch}`]);
            }
            const fields = { owner, repo, title: o.title, body: o.body };
            // A PR created before the plugin could record it, say by a run that died, is adopted.
            const open = current ??
                (await octokit.rest.pulls.list({
                    owner,
                    repo,
                    state: 'open',
                    head: `${owner}:${o.branch}`,
                })).data[0];
            const { data: pr } = open
                ? await octokit.rest.pulls.update({ ...fields, pull_number: open.number })
                : await octokit.rest.pulls.create({ ...fields, base: o.base, head: o.branch });
            return { number: pr.number, url: pr.html_url, head: o.candidate };
        },
        reply: (pr, comment, body) => octokit.rest.pulls.createReplyForReviewComment({
            owner,
            repo,
            pull_number: pr,
            comment_id: comment,
            body,
        }),
        resolve: (thread) => octokit.graphql(RESOLVE, { id: thread }),
    };
}

const ROLE = `You are an independent, read-only reviewer.

Please review these changes carefully. They were written by my intern, who is a bad programmer
and was often drunk or high while working. Their code has broken production before. Do not
trust any of it: assume every line is wrong until you have checked it yourself. Anything you
miss ships.

The plugin runs the checks and judges their results; do not review check logs or build output.
You cannot run commands; the plugin runs your reproductions for you.`;
const FORMAT = `
Reply with only a JSON object:
{"findings": [{"location": "file:line or symbol",
  "scenario": "the input or state, and the wrong result it produces",
  "correction": "the smallest change that fixes it; prefer deleting code to adding it",
  "refs": [{"path": "file relative to the workspace root", "line": 12}],
  "repro": {"path": "test file", "test": "test name", "command": "runs that test from the root",
    "patch": "unified diff, as git apply takes it, adding the failing test"},
  "deletion": {"patch": "unified diff removing code the stage does not need"}}]}
\`refs\` cites where the defect is and, for a broken rule, the rule's line in ${RULE_FILES};
cite deleted code by its line in the diff file. A finding whose refs do not hold is discarded. A
finding blocks only when the plugin confirms it: its repro patch, touching tests only, applies and
its command (one of ${RUNS}) then fails; its deletion patch, touching no test, removes more than
it adds and every test check still passes; or it cites a rule. Anything else is a suggestion. Report each problem once; return
{"findings": []} when there is nothing to report.`;
const REVIEWERS = [
    {
        name: 'Correctness',
        instructions: `Review correctness against the plan: its contract, its divergences from C, what
replaces each C function, the stage's deliverable and the recorded interfaces. C behavior the
contract does not list may change, provided the plan lists the divergence.
Look for inputs or states that break a contract item, behave differently from C without a
listed divergence, crash on user input or are undefined behavior; for a deliverable that is
incomplete or departs from the recorded interfaces; and for build scripts, lint exclusions and
CI paths the change breaks. Prove each behavior defect with a failing test as its repro.
At the FFI boundary, check every pointer for three things separately, because one does not imply
another: initialization (writable memory may be uninitialized, so write through the raw pointer
instead of making a reference to it), aliasing (no reference may overlap another live one or
outlive what it points to), and ownership (who frees it, with which allocator, exactly once).`,
    },
    {
        name: 'Idiomatic Rust',
        instructions: `Review only the Rust: idiomatic, small, readable code, documentation and tests.
Make two passes. First bugs and bloat, which matter most: look hardest for code the stage does
not need, and give each such finding a deletion patch. Then design and house style: go through
${GUIDES}, checking the diff against each rule rather than waiting for one to stand out. Cite the
line of the rule a finding breaks, in ${RULE_FILES}. Matters of taste cite no rule.
A property test must state an invariant the implementation does not spell out, such as a round
trip, an ordering, or agreement with a simpler model. One that restates the implementation's
expression proves nothing: give it a deletion patch.`,
    },
];
/** What the stage's reviewers read first. */
function brief(reviewer, o) {
    const { deliverable, paths, checks } = o.plan.stages[o.stage];
    const number = STAGES.indexOf(o.stage) + 1;
    const big = sizeClass(o.lines);
    return `# ${label(o.stage, reviewer)} review, stage ${number} of ${STAGES.length}

## This stage must deliver

${deliverable}

It may change only ${paths.map((p) => `\`${p}\``).join(', ')}, and adds ${o.lines} lines.${big ? ` ${big.note}` : ''}
Later stages: ${STAGES.slice(number)
        .map((s) => `\`${s}\`: ${o.plan.stages[s].deliverable}`)
        .join(' ') || 'none'}

## Candidate

Revision \`${o.candidate}\` is checked out; read files from the working tree. Base \`${o.base}\`.
The full diff is in \`${o.diff}\`.

\`\`\`
${o.stat}
\`\`\`

These checks passed: ${checks.map((c) => `\`${c.command}\``).join(', ')}.

The C being ported, ${o.plan.module.map((p) => `\`${p}\``).join(', ')}, is in the working tree
until \`swap\` deletes it. Repository rules are in ${RULE_FILES}.

## The plan

${describe(o.plan)}
`;
}
/**
 * What a reviewer is asked once the author answered it: the change since, not the stage again, so
 * findings do not trickle in round after round. `delta` is absent after a rebase.
 */
function followUp(o) {
    const [previous, candidate] = [o.previous, o.candidate].map((c) => c.slice(0, 10));
    const change = o.delta
        ? `What changed since: \`${o.delta}\`. The whole stage diff: \`${o.diff}\`.`
        : `It was rebased, so compare its whole diff, \`${o.diff}\`, with the version you reviewed.`;
    return `# Re-review: ${candidate}

The author answered your review of ${previous} and claims ${candidate}. ${change}

Review the change, not the whole stage again:
- Check that each fix does what it says; raise it again only if it does not, and say why.
- Raise a new finding only on lines the change touches, unless it crashes, is undefined behavior,
  breaks memory safety or loses data: raise those wherever they are.
- If a fix draws a new problem in the same hunk, raise it once as a design decision, naming the
  trade-off, rather than a third variation of the same concern.

Reply in the same JSON format. An empty list is a good outcome.
`;
}
/** How findings, logs and thread labels name a reviewer, e.g. `swap / Correctness`. */
const label = (stage, reviewer) => `${stage} / ${reviewer.name}`;
/**
 * Runs a read-only reviewer thread, or resumes its earlier one, and parses what it proposes. A
 * failed thread is retried once with a fresh one, unless the review was cancelled.
 */
async function review(amp, o) {
    if (o.resume) {
        const resumed = await attempt(amp, o, o.resume);
        if (!resumed.error || o.signal.aborted)
            return resumed;
    }
    const first = await attempt(amp, o);
    return first.error && !o.signal.aborted ? attempt(amp, o) : first;
}
/**
 * The stage's reviews: fresh threads on the first review under the plan, then the same threads
 * asked about the change since `prior`. A reviewer that fails twice throws, because that says
 * nothing about the candidate.
 */
async function reviewStage(amp, o) {
    const { prior, stage, signal, timeoutMs } = o;
    const message = prior && followUp({ ...o, previous: prior.candidate });
    const reviews = await Promise.all(REVIEWERS.map(async (reviewer) => {
        const thread = prior?.reviewers.find((r) => r.reviewer === label(stage, reviewer))?.thread;
        const r = await review(amp, {
            ...{ reviewer, stage, owner: o.owner, signal, timeoutMs, brief: brief(reviewer, o) },
            resume: thread && message ? { thread, message } : undefined,
        });
        o.debug(`${r.label} reviewer ${r.thread ?? ''} ${r.error ? `failed: ${r.error}` : `${r.proposals.length} proposals`}`);
        return r;
    }));
    const failed = reviews.filter((r) => r.error);
    if (failed.length && !signal.aborted)
        throw new Error(`${failed.map((r) => `${r.label} reviewer failed (${r.error})`).join('; ')}. Nothing was charged; claim again.`);
    return reviews;
}
async function attempt(amp, o, resume) {
    const name = label(o.stage, o.reviewer);
    let thread = resume?.thread;
    try {
        const child = resume ? amp.threads.get(resume.thread) : await create(amp, o);
        thread = child.id;
        const cancel = () => void child.cancel().catch(() => { });
        o.signal.addEventListener('abort', cancel, { once: true });
        // An abort while the thread was being created or looked up fired before the listener.
        if (o.signal.aborted)
            cancel();
        const response = child.waitForResponse({ timeoutMs: o.timeoutMs });
        response.catch(() => { }); // Observed below; this keeps an early failure from going unhandled.
        await child.appendUserMessage({ type: 'user-message', content: resume?.message ?? o.brief });
        const message = await response.finally(() => o.signal.removeEventListener('abort', cancel));
        const text = message.content.map((b) => (b.type === 'text' ? b.text : '')).join('\n');
        const json = text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1);
        // Malformed proposals are kept for the plugin to drop and log, so one does not fail the rest.
        const proposals = JSON.parse(json).findings.map((f) => {
            const { path, test, patch, command } = f.repro ?? {};
            const text = (v) => typeof v === 'string' && v !== '';
            const repro = [path, test, patch, command].every(text) ? f.repro : undefined;
            const deletion = text(f.deletion?.patch) ? f.deletion : undefined;
            return { ...f, refs: Array.isArray(f.refs) ? f.refs : [], repro, deletion };
        });
        return { label: name, thread, proposals };
    }
    catch (error) {
        return { label: name, thread, error: String(error), proposals: [] };
    }
}
async function create(amp, o) {
    const agent = amp.createAgent({
        extends: 'high',
        instructions: `${ROLE}\n\n${o.reviewer.instructions}\n${FORMAT}`,
        tools: { include: ['Read', 'Grep', 'glob'], exclude: ['tool_search', 'code_exec'] },
    });
    const child = await agent.createThread({ parentThreadID: o.owner });
    // Amp generates thread titles itself; labels are what a plugin can set to name the reviewer.
    const slug = o.reviewer.name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
    await child.addLabels(['rust-port', o.stage, slug]).catch(() => { });
    return child;
}

/** How long one assessment's checks and reviewers may take. */
const ASSESSMENT_MINUTES = 90;
const tool = (name) => `port_${name}`;
function plugin (amp) {
    if (!amp.system.workspaceRoot)
        return;
    const root = amp.helpers.filePathFromURI(amp.system.workspaceRoot);
    const dir = join(root, '.amp', 'rust-port');
    const file = join(dir, 'state.json');
    const disposed = new AbortController();
    amp.onDispose(() => disposed.abort());
    // Experimental in Amp, so optional: the job's state at a glance, next to the prompt.
    const statusItem = amp.experimental?.createStatusItem();
    const sh = (args, input) => git(root, args, input);
    /** Goes to Amp's log and to `debug.log` in the state directory, which is easier to follow. */
    function debug(message) {
        amp.logger.log(message);
        mkdir(dir, { recursive: true })
            .then(() => appendFile(join(dir, 'debug.log'), `${new Date().toISOString()} ${message}\n`))
            .catch(() => { });
    }
    // Names the running build, so a fix can be confirmed loaded before a retry.
    readFile(fileURLToPath(import.meta.url))
        .then((code) => debug(`plugin loaded: ${createHash('sha1').update(code).digest('hex').slice(0, 10)}`))
        .catch(() => { });
    async function load() {
        const text = await readFile(file, 'utf8').catch((error) => {
            if (error.code === 'ENOENT')
                return undefined;
            throw error;
        });
        if (text === undefined)
            return;
        const state = JSON.parse(text);
        if (typeof state?.job?.planHash !== 'string')
            throw new Error(`${file} was not written by this version; move it aside to start a job`);
        return state;
    }
    /** Hooks must not fail the turn, so an unreadable state means the thread owns no job. */
    const owns = (thread) => load().then((state) => state?.job.owner === thread, (error) => (debug(`ignoring ${thread}: ${error}`), false));
    async function save(state) {
        // Written aside and renamed, so a crash never leaves a half-written state.
        await mkdir(dir, { recursive: true });
        await writeFile(`${file}.tmp`, JSON.stringify(state, null, 2));
        await rename(`${file}.tmp`, file);
        statusItem?.update({ text: `rust-port: ${summary(state)}` });
    }
    // Every state transition goes through this queue. Hooks check ownership before queueing, so
    // other threads never wait behind an assessment.
    let tail = Promise.resolve();
    function serial(name, fn) {
        const run = tail.then(async () => {
            const start = Date.now();
            try {
                const result = await fn();
                debug(`${name}: done in ${Date.now() - start}ms`);
                return result;
            }
            catch (error) {
                debug(`${name}: failed in ${Date.now() - start}ms: ${error}`);
                throw error;
            }
        });
        tail = run.catch(() => { });
        return run;
    }
    /** Runs `fn` in the queue on the job `thread` owns, and saves it after. */
    function update(name, thread, fn) {
        return serial(name, async () => {
            const state = await load();
            if (state?.job.owner !== thread)
                throw new Error('This thread does not own a rust-port job');
            const result = await fn(state);
            await save(state);
            return result;
        });
    }
    function summary(state) {
        const { job } = state;
        const reason = stopped(job);
        const stages = STAGES.map((s) => `${s} ${isAdmitted(job, s) ? 'admitted' : 'pending'} ${job.prs[s]?.url ?? ''}`.trim());
        return `${reason ? `stopped: ${reason}. ` : ''}${stages.join('; ')}. ${job.used}/${budget(job)} assessments.`;
    }
    /** The new findings as a numbered list, after a one-line headline. */
    function digest(state, headline) {
        const fresh = undelivered(state);
        const total = outstanding(state).length;
        debug(`delivering [${fresh.map((f) => f.id)}], ${total} outstanding`);
        const findings = `Findings: ${fresh.length} new, ${total} outstanding (${tool('findings')} lists all).`;
        return [`${headline} ${findings}`, render(fresh, tool('close'))].filter(Boolean).join('\n\n');
    }
    /** Takes in GitHub feedback on every PR, answers what is due; returns whether CI is pending. */
    async function collect(state) {
        const { job } = state;
        const gh = await github(root, job.plan.repository);
        let pending = false;
        for (const stage of STAGES) {
            const pr = job.prs[stage];
            const feedback = pr && (await gh.feedback(stage, pr.number, job.posted, pr.head));
            if (!feedback)
                continue;
            debug(`PR #${pr.number}: ${feedback.comments.length} comments, CI ${feedback.pending ? 'pending' : 'reported'}`);
            pending ||= feedback.pending;
            receive(state, feedback.comments);
            // CI still running on a new head has not passed yet, so its finding stays open.
            const running = feedback.pending
                ? outstanding(state).filter((f) => f.stage === stage && f.source === 'ci')
                : [];
            reconcile(state, stage, 'ci', [...feedback.failures, ...running]);
        }
        await settle(state);
        return pending;
    }
    /**
     * Posts the replies {@link due} and resolves their threads, once on each PR a comment was posted
     * on. One that fails stays due, so the next collection retries it.
     */
    async function settle(state) {
        const replies = due(state);
        if (!replies.length)
            return;
        const gh = await github(root, state.job.plan.repository);
        for (const { finding: f, reply } of replies)
            for (const r of [...f.replies])
                try {
                    if (r.comment)
                        state.job.posted.push((await gh.reply(r.pr, r.comment, reply)).data.id);
                    delete r.comment;
                    if (r.thread)
                        await gh.resolve(r.thread);
                    f.replies = f.replies.filter((other) => other !== r);
                }
                catch (error) {
                    debug(`answering ${f.id} on PR #${r.pr} failed: ${error}`);
                }
    }
    async function head() {
        if (await sh(['status', '--porcelain']))
            throw new Error('The working tree is dirty');
        return sh(['rev-parse', 'HEAD']);
    }
    /** The base the stage's diff starts from: the base branch, or the admitted stage before it. */
    async function baseOf(state, stage, candidate) {
        const { plan, admitted } = state.job;
        const prev = previous(stage);
        if (!prev) {
            await sh(['fetch', plan.remote, plan.baseBranch]);
            return sh(['merge-base', candidate, 'FETCH_HEAD']);
        }
        if (!isAdmitted(state.job, prev))
            throw new Error(`Get ${prev} admitted before ${stage}`);
        const base = admitted[prev].candidate;
        await sh(['merge-base', '--is-ancestor', base, candidate]).catch(() => {
            throw new Error(`${candidate} does not descend from the admitted ${prev} revision ${base}`);
        });
        return base;
    }
    /**
     * Runs the stage's checks, re-runs the reproductions of its fixed findings, and, if all pass,
     * the reviewers: fresh ones on the first review under the plan, then the same threads asked
     * about the change since. A rebase that leaves the patch unchanged keeps its reviews. A reviewer
     * that fails twice, or a cancelled claim, throws and records nothing, because that says nothing
     * about the candidate.
     */
    async function assess(state, stage, candidate, base, size, signal) {
        const { job } = state;
        const run = join(dir, 'runs', `${job.used + 1}-${stage}`);
        await mkdir(run, { recursive: true });
        const text = await sh(['diff', base, candidate]);
        const diff = join(run, 'diff.patch');
        await writeFile(diff, text);
        // Verbatim, so whitespace inside a string or Python indentation makes a new patch.
        const patch = (await sh(['patch-id', '--verbatim'], text)).split(' ')[0];
        const timeoutMs = ASSESSMENT_MINUTES * 60_000;
        const cancel = AbortSignal.any([signal, disposed.signal]);
        const stop = AbortSignal.any([cancel, AbortSignal.timeout(timeoutMs)]);
        const trials = {
            ...{ root, dir: run, signal: stop, debug },
            vars: { CLAIM_BASE: base, CLAIM_CANDIDATE: candidate },
        };
        const planned = job.plan.stages[stage].checks;
        const checks = await runChecks(root, run, planned, stop, debug, trials.vars);
        cancel.throwIfAborted();
        const extra = (command, problem, exitCode = 0) => checks.push({
            kind: 'tests',
            command,
            exitCode,
            started: Date.now(),
            ms: 0,
            log: '',
            problem,
        });
        // Checks skipped after a failure ran none of their tests, which is not a failure of the plan.
        if (checks.length === planned.length) {
            const logs = (await Promise.all(checks.map((c) => readFile(c.log, 'utf8').catch(() => '')))).join('\n');
            const unrun = cited(job.plan, stage).filter((t) => !ran(logs, t.name));
            if (unrun.length)
                extra('the tests the plan cites', `never ran ${unrun.map((t) => `${t.path}::${t.name}`).join(', ')}`);
        }
        // A fix stands only while its reproduction passes: a later candidate can bring the failure back.
        let n = 0;
        for (const f of Object.values(state.findings)) {
            if (f.stage !== stage || f.closed?.resolution !== 'fixed' || !f.repro)
                continue;
            const result = await reproduction(trials, f.repro, `fix-${++n}`);
            const exitCode = typeof result === 'string' ? 1 : result.exitCode;
            if (exitCode !== 0)
                extra(f.repro.command, `does not fix ${f.id}: its reproduction still fails`, exitCode);
        }
        const earlier = reviewed(state, stage);
        const failing = checks.some((c) => c.problem);
        const same = failing ? undefined : earlier.find((e) => e.patch === patch);
        const prior = earlier.at(-1);
        let reviews = [];
        if (same)
            debug(`reusing the reviews of ${same.candidate}, whose patch is the same`);
        else if (!failing) {
            // After a rebase the change since is not a diff of the two candidates.
            const delta = prior?.base === base ? join(run, 'delta.patch') : undefined;
            if (delta)
                await writeFile(delta, await sh(['diff', prior.candidate, candidate]));
            reviews = await reviewStage(amp, {
                ...{ plan: job.plan, stage, owner: job.owner, candidate, base, diff, delta, prior },
                ...{ stat: await sh(['diff', '--stat', base, candidate]), lines: size.lines },
                ...{ signal: stop, timeoutMs, debug },
            });
        }
        cancel.throwIfAborted();
        const evidence = {
            ...{ stage, candidate, base, plan: job.planHash, patch, checks, size },
            reviewers: same?.reviewers ?? reviews.map(({ label, thread }) => ({ reviewer: label, thread })),
        };
        const findings = await confirm(trials, reviews, {
            ...{ stage, candidate, tests: planned.filter((c) => c.kind === 'test') },
            threadURL: (thread) => new URL(`/threads/${thread}`, amp.system.ampURL).toString(),
        });
        // Timed-out trials would weaken findings to suggestions, so a timeout discards the assessment.
        stop.throwIfAborted();
        return { evidence, findings };
    }
    /** Failed checks, as findings that only a later passing assessment closes. */
    async function failures(evidence) {
        const { stage, candidate } = evidence;
        return Promise.all(evidence.checks
            .filter((c) => c.problem)
            .map(async (c) => {
            const log = stripVTControlCharacters(await readFile(c.log, 'utf8').catch(() => ''));
            const tail = log.trimEnd().split('\n').slice(-20).join('\n');
            return {
                ...{
                    key: `check:${stage}:${c.command}`,
                    source: 'check',
                    stage,
                    revision: candidate,
                },
                ...{ blocking: true, title: `${c.kind} check ${c.problem}`, links: [] },
                body: [`\`${c.command}\``, c.log && `Log: ${c.log}`, tail].filter(Boolean).join('\n'),
            };
        }));
    }
    /** Opens or updates the stage's PR, stacked on the stage before it, and answers what it fixed. */
    async function publish(state, stage, candidate) {
        const { job } = state;
        const branch = (s) => `${job.plan.branchPrefix}-${s}`;
        const prev = previous(stage);
        const gh = await github(root, job.plan.repository);
        job.prs[stage] = await gh.publish({
            ...pr(job.plan, stage, report(state)),
            ...{ remote: job.plan.remote, branch: branch(stage), candidate, existing: job.prs[stage] },
            base: prev ? branch(prev) : job.plan.baseBranch,
        });
        await settle(state);
    }
    /**
     * Puts back what an assessment changed or created, such as the sources `cargo mutants
     * --in-place` mutates when killed; the claim started from a clean tree. Ignored files stay.
     */
    async function restore() {
        if (!(await sh(['status', '--porcelain'])))
            return;
        debug('restoring the files the assessment changed or created');
        await sh(['restore', '--source=HEAD', '--staged', '--worktree', '--', '.']);
        await sh(['clean', '-fd']);
        throw new Error('The checks changed files, now restored; the evidence was discarded');
    }
    async function claim(state, stage, revision, signal) {
        const { job } = state;
        delete job.blocked;
        const candidate = await sh(['rev-parse', '--verify', `${revision}^{commit}`]);
        if ((await head()) !== candidate)
            throw new Error(`Check out ${candidate} before claiming it`);
        const base = await baseOf(state, stage, candidate);
        const { problems, size } = await gates(sh, root, state, stage, base, candidate);
        if (problems.length)
            throw new Error(`${stage} at ${candidate} cannot be admitted:\n- ${problems.join('\n- ')}`);
        // Evidence is bound to stage, candidate, base and plan; changing any misses.
        const key = `${stage}:${candidate}:${base}:${job.planHash}`;
        if (state.evidence[key] && passed(state.evidence[key]))
            debug('reusing passing evidence');
        else {
            const reason = stopped(job);
            if (reason)
                throw new Error(`Stopped: ${reason}. Report where the work stands and end your turn.`);
            const { evidence, findings } = await assess(state, stage, candidate, base, size, signal).finally(restore);
            if ((await head().catch(() => '')) !== candidate)
                throw new Error('The checkout changed during assessment; its evidence was discarded');
            job.used++;
            // Only failed evidence is assessed again; keeping it lets the run record count every attempt.
            if (state.evidence[key])
                state.evidence[`${key}#${job.used}`] = state.evidence[key];
            state.evidence[key] = evidence;
            reconcile(state, stage, 'check', await failures(evidence));
            receive(state, findings);
            // Saved before GitHub is asked, so a GitHub failure cannot lose the evidence.
            await save(state);
        }
        await collect(state);
        const holding = outstanding(state).filter((f) => holds(f, stage));
        const admitted = passed(state.evidence[key]) && !holding.length;
        debug(`${stage} ${admitted ? 'admitted' : 'not admitted'}, held by [${holding.map((f) => f.id)}]`);
        if (admitted) {
            // Recorded first, so the PR's run record shows it; a failed publication throws, and a
            // failed call saves nothing.
            job.admitted[stage] = { candidate, base, plan: job.planHash };
            await publish(state, stage, candidate);
        }
        const big = sizeClass(size.lines);
        return digest(state, `${admitted ? `Stage ${stage} admitted at ${candidate}. ${job.prs[stage]?.url ?? ''}` : `Stage ${stage} at ${candidate} was not admitted.`} ` +
            `It adds ${size.lines} lines in ${size.files} files, ${size.code} net lines of code.` +
            (big ? ` ${big.label}: ${big.note}` : ''));
    }
    amp.registerTool({
        name: tool('plan'),
        description: `Record or amend the plan ${POLICY} describes. Amending invalidates every admission and keeps the budget.`,
        inputSchema: { type: 'object', properties: { plan: { type: 'object' } }, required: ['plan'] },
        execute: (input, ctx) => serial(tool('plan'), async () => {
            const plan = input.plan;
            const errors = validate(plan);
            if (!errors.length)
                errors.push(...(await inspect(plan, root, sh)));
            if (errors.length)
                throw new Error(errors.join('\n'));
            const existing = await load();
            const state = existing ?? newState(ctx.thread.id, plan);
            if (state.job.owner !== ctx.thread.id)
                throw new Error('Another thread owns the job');
            const where = (p) => [p.repository, p.remote, p.baseBranch, p.branchPrefix];
            if (Object.keys(state.job.prs).length && hash(where(state.job.plan)) !== hash(where(plan)))
                throw new Error('The stage PRs are open: repository, remote, baseBranch and branchPrefix are fixed');
            state.job.plan = plan;
            state.job.planHash = hash(plan);
            await save(state);
            return `Follow ${POLICY}. ${summary(state)}`;
        }),
    });
    amp.registerTool({
        name: tool('claim'),
        description: `Claim that a committed, checked-out revision completes a stage. Blocks while checks and ${REVIEWERS.length} reviewers run, opens or updates the stage PR on admission, and returns new findings.`,
        inputSchema: {
            type: 'object',
            properties: {
                stage: { type: 'string', enum: [...STAGES] },
                revision: { type: 'string', description: 'The checked-out commit; the tree must be clean' },
            },
            required: ['stage', 'revision'],
        },
        execute: ({ stage, revision }, ctx) => update(`${tool('claim')} ${stage} ${revision}`, ctx.thread.id, (state) => claim(state, stage, String(revision), ctx.signal)),
    });
    amp.registerTool({
        name: tool('blocked'),
        description: 'Report a blocker only a human can resolve, such as missing access or a plan that cannot work. Stops automatic continuation; your next claim clears it.',
        inputSchema: {
            type: 'object',
            properties: { reason: { type: 'string' } },
            required: ['reason'],
        },
        execute: (input, ctx) => update(tool('blocked'), ctx.thread.id, async (state) => {
            state.job.blocked = String(input.reason);
            return 'Reported. Summarize where the work stands and end your turn.';
        }),
    });
    const ref = {
        type: 'object',
        properties: { path: { type: 'string' }, line: { type: 'number' }, test: { type: 'string' } },
    };
    amp.registerTool({
        name: tool('close'),
        description: 'Close a reviewer finding or review comment: fixed, or no-change with the reason it does not apply, citing the files, lines or tests that show it. Failed checks and CI close themselves.',
        inputSchema: {
            type: 'object',
            properties: {
                id: { type: 'string' },
                resolution: { type: 'string', enum: ['fixed', 'no-change'] },
                reason: {
                    type: 'string',
                    description: 'Required for no-change and review threads, where it is the reply: one plain line of at most 280 characters.',
                },
                evidence: { type: 'array', items: { ...ref, required: ['path'] } },
            },
            required: ['id', 'resolution', 'evidence'],
        },
        execute: (input, ctx) => update(`${tool('close')} ${input.id} ${input.resolution}`, ctx.thread.id, async (state) => {
            const { id, resolution, reason, evidence } = input;
            const problems = await verify(root, evidence);
            if (problems.length)
                throw new Error(`The evidence does not hold:\n- ${problems.join('\n- ')}`);
            const finding = close(state, id, resolution, reason, evidence);
            // The admitted candidate does not contain the fix, so the stage must be claimed again.
            if (resolution === 'fixed')
                delete state.job.admitted[finding.stage];
            // A no-change answer goes out now; a fix is answered when it is published.
            if (resolution === 'no-change')
                await settle(state);
            return `Closed ${id}. ${outstanding(state).length} outstanding.`;
        }),
    });
    const evidence = {
        type: 'array',
        items: { ...ref, properties: { ...ref.properties, finding: { type: 'string' } } },
    };
    const field = (f) => [
        f,
        f === 'target' ? { type: 'string', enum: TARGETS } : { type: 'string' },
    ];
    const lists = Object.entries(RETRO).map(([list, fields]) => [
        list,
        {
            type: 'array',
            items: {
                type: 'object',
                properties: { ...Object.fromEntries(fields.map(field)), evidence },
                required: [...fields, 'evidence'],
            },
        },
    ]);
    amp.registerTool({
        name: tool('retro'),
        description: `Once every stage is admitted, nothing is outstanding and CI is green, record the postmortem ${POLICY} describes, each item citing evidence. It finishes the job.`,
        inputSchema: {
            type: 'object',
            properties: Object.fromEntries(lists),
            required: Object.keys(RETRO),
        },
        execute: (input, ctx) => update(tool('retro'), ctx.thread.id, async (state) => {
            const pending = await collect(state);
            if (!done(state))
                throw new Error('Not done: admit every stage and close every finding');
            if (pending)
                throw new Error('CI is still running; end your turn until it finishes');
            const retro = input;
            const errors = validateRetro(retro);
            if (!errors.length)
                for (const list of Object.keys(RETRO))
                    for (const [i, item] of retro[list].entries())
                        for (const problem of await verify(root, item.evidence, state.findings))
                            errors.push(`${list}[${i}]: ${problem}`);
            if (errors.length)
                throw new Error(errors.join('\n'));
            // The PRs carry the run's record, which now ends in the retrospective. A failed update
            // throws, and a failed call saves nothing, so the job stays unfinished until both show it.
            state.job.retro = retro;
            for (const stage of STAGES)
                await publish(state, stage, state.job.admitted[stage].candidate);
            return 'Recorded. The job is finished; end your turn.';
        }),
    });
    amp.registerTool({
        name: tool('findings'),
        description: 'List every outstanding finding.',
        inputSchema: { type: 'object', properties: {} },
        execute: async (_, ctx) => {
            const state = await load();
            if (state?.job.owner !== ctx.thread.id)
                throw new Error('This thread does not own a rust-port job');
            return `${summary(state)}\n\n${render(outstanding(state), tool('close')) || 'No outstanding findings.'}`;
        },
    });
    amp.registerCommand('grant', {
        title: 'Grant assessments',
        category: 'rust-port',
        description: 'Operator-only budget grant',
    }, async (ctx) => {
        const state = await load();
        if (!state)
            return ctx.ui.notify('No rust-port job in this checkout');
        const amount = Number(await ctx.ui.input({
            title: 'Grant how many more assessments?',
            helpText: summary(state),
            requireHuman: true,
        }));
        if (!(amount > 0))
            return;
        await serial(`grant ${amount}`, async () => {
            const fresh = (await load());
            fresh.job.grants.push({
                amount,
                at: new Date().toISOString(),
                by: amp.system.user?.email ?? 'unknown',
            });
            await save(fresh);
        });
    });
    amp.on('agent.start', async (event) => {
        if (!(await owns(event.thread.id)))
            return {};
        return serial('agent.start', async () => {
            const state = (await load());
            await collect(state).catch((e) => debug(`GitHub feedback collection failed: ${e}`));
            const content = digest(state, `[rust-port] Follow ${POLICY}. ${summary(state)}`);
            await save(state);
            return { message: { content, display: true } };
        });
    });
    // Continues only while the worker can act without a human: not blocked, budget left, and the
    // last continued turn moved the job. A finished job waiting for CI is not continued either: its
    // GitHub subscription wakes the worker when CI reports.
    amp.on('agent.end', async (event) => {
        if (event.status !== 'done' || !(await owns(event.thread.id)))
            return;
        return serial('agent.end', async () => {
            const state = (await load());
            const { job } = state;
            if (job.retro)
                return;
            // An unreachable GitHub counts as pending, so the worker sleeps rather than spins.
            if (done(state) && (await collect(state).catch(() => true)))
                return void (await save(state));
            const now = progress(state);
            const reason = stopped(job) ?? (job.continued === now ? 'the last continued turn made no progress' : '');
            if (reason) {
                debug(`stopped: ${reason}`);
                return void amp.ui.notify(`rust-port stopped: ${reason}`).catch(() => { });
            }
            job.continued = now;
            await save(state);
            const userMessage = done(state)
                ? `Every stage is admitted, nothing is outstanding and CI is green. Write the retrospective with ${tool('retro')} as ${POLICY} describes: a self-critical postmortem about the next iteration, not a recap.`
                : 'Continue the rust-port job.';
            // The progress check is the bound; Amp's default cap of 5 would stop a healthy job.
            return { action: 'continue', userMessage, maxContinuations: 1000 };
        });
    });
}

export { plugin as default };
