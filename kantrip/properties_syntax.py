"""Read Java properties, librdkafka properties, and JAAS configuration as plain syntax.

These readers only tokenize: they return keys and values and never interpret a
connection. Errors name lines, keys, and rules, never values, because any value
may be a credential.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass

MAX_PROPERTIES = 4096
MAX_KEY_LENGTH = 256
_JAVA_WHITESPACE = " \t\f"
_JAVA_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}
_NATURAL_LINE = re.compile(r"\r\n|\r|\n")
_REPORTABLE_KEY = re.compile(r"[A-Za-z0-9._-]{1,256}")
_LIBRDKAFKA_KEY = re.compile(r"[A-Za-z0-9._-]+")
_LIBRDKAFKA_CONTROL = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")


class PropertiesSyntaxError(ValueError):
    """Raised when a document does not follow the syntax it is read as."""


def key_name(key: str) -> str:
    """Name a key for a message, or describe it when it could carry a value."""
    return f"key {key}" if _REPORTABLE_KEY.fullmatch(key) else "a key with unsupported characters"


def reportable_key(key: str) -> bool:
    """Return whether a key is safe to print by name."""
    return _REPORTABLE_KEY.fullmatch(key) is not None


def parse_java_properties(text: str) -> dict[str, str]:
    """Read text the way Java's ``Properties.load`` does, rejecting duplicate keys.

    Kafka tools load properties files as ISO-8859-1, so characters outside
    ASCII must be written as ``\\uXXXX`` escapes; a UTF-8 character would be
    read differently by the client.
    """
    properties: dict[str, str] = {}
    for number, line in _java_logical_lines(text):
        if not line.isascii():
            raise PropertiesSyntaxError(
                f"line {number} has a non-ASCII character; Java reads properties as "
                "ISO-8859-1, so write it as a \\uXXXX escape"
            )
        raw_key, raw_value = _split_java_line(line)
        key = _java_unescape(raw_key, number)
        _store(properties, key, _java_unescape(raw_value, number), number)
    return properties


def _java_logical_lines(text: str) -> Iterator[tuple[int, str]]:
    """Join continued natural lines, skipping blank and comment lines."""
    pending: str | None = None
    start = 0
    for number, natural in enumerate(_NATURAL_LINE.split(text), start=1):
        line = natural.lstrip(_JAVA_WHITESPACE)
        if pending is None:
            if not line or line[0] in "#!":
                continue
            start = number
        else:
            line = pending + line
        if _continues(line):
            pending = line[:-1]
            continue
        pending = None
        yield start, line
    if pending is not None:
        raise PropertiesSyntaxError(f"line {start} ends with a continuation and no next line")


def _continues(line: str) -> bool:
    """Return whether a line ends with an odd number of backslashes."""
    return (len(line) - len(line.rstrip("\\"))) % 2 == 1


def _split_java_line(line: str) -> tuple[str, str]:
    """Split at the first unescaped ``=``, ``:``, or whitespace, as Java does."""
    index = 0
    escaped = False
    while index < len(line):
        character = line[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character in "=:" or character in _JAVA_WHITESPACE:
            break
        index += 1
    rest = line[index:].lstrip(_JAVA_WHITESPACE)
    if rest[:1] in ("=", ":") and rest:
        rest = rest[1:].lstrip(_JAVA_WHITESPACE)
    return line[:index], rest


def _java_unescape(raw: str, number: int) -> str:
    characters: list[str] = []
    index = 0
    while index < len(raw):
        character = raw[index]
        index += 1
        if character != "\\":
            characters.append(character)
            continue
        escape = raw[index : index + 1]
        index += 1
        if escape != "u":
            characters.append(_JAVA_ESCAPES.get(escape, escape))
            continue
        digits = raw[index : index + 4]
        if len(digits) != 4 or any(digit not in "0123456789abcdefABCDEF" for digit in digits):
            raise PropertiesSyntaxError(f"line {number} has a malformed \\uXXXX escape")
        characters.append(chr(int(digits, 16)))
        index += 4
    return _joined_surrogates("".join(characters), number)


def _joined_surrogates(value: str, number: int) -> str:
    """Combine UTF-16 surrogate pairs written as two ``\\u`` escapes."""
    if not any("\ud800" <= character <= "\udfff" for character in value):
        return value
    try:
        return value.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
    except UnicodeDecodeError as error:
        raise PropertiesSyntaxError(
            f"line {number} has an unpaired \\u surrogate escape"
        ) from error


def parse_librdkafka_properties(text: str) -> dict[str, str]:
    """Read ``key=value`` lines with ``#`` comments, the format librdkafka tools load."""
    properties: dict[str, str] = {}
    for number, natural in enumerate(text.split("\n"), start=1):
        line = natural.removesuffix("\r").lstrip(" \t")
        if not line or line.startswith("#"):
            continue
        if _LIBRDKAFKA_CONTROL.search(line):
            raise PropertiesSyntaxError(f"line {number} contains a control character")
        key, separator, value = line.partition("=")
        if not separator or not _LIBRDKAFKA_KEY.fullmatch(key):
            raise PropertiesSyntaxError(f"line {number} is not a key=value line")
        _store(properties, key, value, number)
    return properties


def _store(properties: dict[str, str], key: str, value: str, number: int) -> None:
    if not key:
        raise PropertiesSyntaxError(f"line {number} has an empty key")
    if len(key) > MAX_KEY_LENGTH:
        raise PropertiesSyntaxError(f"line {number} has a key longer than {MAX_KEY_LENGTH}")
    if key in properties:
        raise PropertiesSyntaxError(f"line {number} repeats {key_name(key)}")
    if len(properties) >= MAX_PROPERTIES:
        raise PropertiesSyntaxError(f"has more than {MAX_PROPERTIES} properties")
    properties[key] = value


@dataclass(frozen=True)
class JaasEntry:
    """One JAAS login module entry: its class, control flag, and options."""

    login_module: str
    control_flag: str
    options: dict[str, str]


@dataclass(frozen=True)
class _Token:
    kind: str  # "word", "quoted", "number", or "symbol"
    text: str


# Kafka reads `sasl.jaas.config` with a `java.io.StreamTokenizer` in its default
# mode plus `//` and `/* */` comments and `-`, `_`, and `$` as word characters.
_JAAS_ESCAPES = {"a": "\a", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}
_OCTAL = "01234567"


def _word_start(character: str) -> bool:
    code = ord(character)
    return character.isascii() and (character.isalpha() or character in "_$") or code >= 160


def _word_part(character: str) -> bool:
    return _word_start(character) or character.isdigit() or character in ".-"


def parse_jaas_entries(text: str) -> tuple[JaasEntry, ...]:
    """Parse JAAS configuration into entries, rejecting duplicate options."""
    tokens = list(_jaas_tokens(text))
    entries: list[JaasEntry] = []
    index = 0
    while index < len(tokens):
        entry, index = _jaas_entry(tokens, index)
        entries.append(entry)
    return tuple(entries)


def _jaas_entry(tokens: list[_Token], index: int) -> tuple[JaasEntry, int]:
    header = tokens[index : index + 2]
    if len(header) != 2 or any(token.kind != "word" for token in header):
        raise PropertiesSyntaxError("needs a login module and a control flag")
    index += 2
    options: dict[str, str] = {}
    while index < len(tokens) and tokens[index] != _Token("symbol", ";"):
        name, separator, value = (tokens[index : index + 3] + [_Token("end", "")] * 3)[:3]
        if name.kind != "word" or separator != _Token("symbol", "="):
            raise PropertiesSyntaxError("has an option that is not name=value")
        if value.kind not in ("word", "quoted"):
            raise PropertiesSyntaxError(f"option {name.text} needs a quoted value")
        if name.text in options:
            raise PropertiesSyntaxError(f"repeats option {name.text}")
        options[name.text] = value.text
        index += 3
    if index >= len(tokens):
        raise PropertiesSyntaxError("entry is not terminated by a semicolon")
    return JaasEntry(header[0].text, header[1].text, options), index + 1


def _jaas_tokens(text: str) -> Iterator[_Token]:
    index = 0
    while index < len(text):
        character = text[index]
        if ord(character) <= 32:
            index += 1
        elif character == "/":
            index = _skip_comment(text, index)
        elif character in "\"'":
            value, index = _quoted(text, index)
            yield _Token("quoted", value)
        elif _word_start(character):
            end = index + 1
            while end < len(text) and _word_part(text[end]):
                end += 1
            yield _Token("word", text[index:end])
            index = end
        elif character.isdigit() or character in ".-":
            end = index + 1
            while end < len(text) and (text[end].isdigit() or text[end] == "."):
                end += 1
            yield _Token("number", "")
            index = end
        else:
            yield _Token("symbol", character)
            index += 1


def _skip_comment(text: str, index: int) -> int:
    """Skip a ``/* */`` comment, or a ``//`` or lone ``/`` comment to the end of the line."""
    if text.startswith("/*", index):
        end = text.find("*/", index + 2)
        return len(text) if end < 0 else end + 2
    end = min(
        (
            position
            for position in (text.find("\n", index), text.find("\r", index))
            if position >= 0
        ),
        default=len(text),
    )
    return end


def _quoted(text: str, index: int) -> tuple[str, int]:
    quote = text[index]
    characters: list[str] = []
    index += 1
    while index < len(text) and text[index] != quote:
        character = text[index]
        if character in "\r\n":
            break
        if character == "\\" and index + 1 < len(text):
            decoded, index = _jaas_escape(text, index + 1)
            characters.append(decoded)
            continue
        characters.append(character)
        index += 1
    if index >= len(text) or text[index] != quote:
        raise PropertiesSyntaxError("has an unterminated quoted value")
    return "".join(characters), index + 1


def _jaas_escape(text: str, index: int) -> tuple[str, int]:
    """Decode one StreamTokenizer escape starting after its backslash."""
    character = text[index]
    if character not in _OCTAL:
        return _JAAS_ESCAPES.get(character, character), index + 1
    limit = 3 if character <= "3" else 2
    end = index
    while end < len(text) and end - index < limit and text[end] in _OCTAL:
        end += 1
    return chr(int(text[index:end], 8)), end


__all__ = [
    "MAX_KEY_LENGTH",
    "MAX_PROPERTIES",
    "JaasEntry",
    "PropertiesSyntaxError",
    "key_name",
    "parse_jaas_entries",
    "parse_java_properties",
    "parse_librdkafka_properties",
    "reportable_key",
]
