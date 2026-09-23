"""Apply the same adapter argument policy inside interactive shell shims."""

from __future__ import annotations

import sys

from kantrip.adapters import (
    KAFKA_EXECUTABLE_OPTIONS,
    KCAT_EXECUTABLES,
    AdapterError,
    _reject_kafka_overrides,
    _reject_kaskade_overrides,
    _reject_kcat_overrides,
)


def main() -> int:
    if len(sys.argv) < 2:
        return 2
    executable, *arguments = sys.argv[1:]
    try:
        if executable in KAFKA_EXECUTABLE_OPTIONS:
            _reject_kafka_overrides(executable, arguments, KAFKA_EXECUTABLE_OPTIONS[executable])
        elif executable in KCAT_EXECUTABLES:
            _reject_kcat_overrides(executable, arguments)
        elif executable == "kaskade":
            _reject_kaskade_overrides(arguments)
        else:
            return 2
    except AdapterError as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
