"""Apply the same adapter argument policy inside interactive shell shims."""

from __future__ import annotations

import sys

from kantrip.adapters import AdapterError, client_adapter


def main() -> int:
    if len(sys.argv) < 2:
        return 2
    executable, *arguments = sys.argv[1:]
    adapter = client_adapter(executable)
    if adapter is None:
        return 2
    try:
        signal = adapter.check_arguments(executable, arguments)
    except AdapterError as error:
        print(error, file=sys.stderr)
        return 2
    if signal is not None:
        print(signal)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
