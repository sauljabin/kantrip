"""Opaque in-memory secret values that never render themselves."""

from __future__ import annotations

import hmac
from typing import Any, NoReturn

_MASK = "Secret('***')"


class Secret:
    """Hold one credential so that it cannot be printed, formatted, or serialized.

    ``repr()`` returns a mask so tracebacks, debuggers, and dataclass
    representations stay safe. ``str()`` and ``format()`` raise instead of
    masking: a forgotten ``reveal()`` in an f-string must fail loudly rather
    than silently send a masked value to a client. Call ``reveal()`` only where
    the value leaves the process: a rendered client file, a native
    credential-store write, a network request, or validation.
    """

    __slots__ = ("_value",)
    _value: str

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("secret value must be text")
        object.__setattr__(self, "_value", value)

    def reveal(self) -> str:
        """Return the underlying value at an explicit trust boundary."""
        return self._value

    def __repr__(self) -> str:
        return _MASK

    def __str__(self) -> NoReturn:
        raise TypeError("Secret values cannot be converted to text; call reveal()")

    def __format__(self, format_spec: str) -> NoReturn:
        raise TypeError("Secret values cannot be formatted; call reveal()")

    def __bool__(self) -> bool:
        return bool(self._value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Secret):
            return NotImplemented
        return hmac.compare_digest(self._value.encode(), other._value.encode())

    def __hash__(self) -> int:
        return hash((Secret, self._value))

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        raise AttributeError("Secret values are immutable")

    def __delattr__(self, name: str) -> NoReturn:
        raise AttributeError("Secret values are immutable")

    def __copy__(self) -> Secret:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Secret:
        return self

    def __reduce__(self) -> NoReturn:
        raise TypeError("Secret values cannot be serialized")


def reveal_optional(value: Secret | None) -> str | None:
    """Reveal an optional secret at a trust boundary."""
    return value.reveal() if value is not None else None


__all__ = ["Secret", "reveal_optional"]
