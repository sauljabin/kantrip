"""Describe the manual sandbox without starting external services implicitly."""

from pathlib import Path

SANDBOX_ROOT = Path(__file__).resolve().parent


def main() -> None:
    print("Kantrip manual sandbox")
    print("Start: docker compose --project-directory sandbox up -d")
    print("Stop:  docker compose --project-directory sandbox down -v")
    print("Kafka: localhost:19092, localhost:29092, localhost:39092")
    print("Confluent Registry: http://localhost:18081")
    print("Apicurio Registry:  http://localhost:18082/apis/registry/v3")
    print(f"Compose file: {SANDBOX_ROOT / 'compose.yml'}")


if __name__ == "__main__":
    main()
