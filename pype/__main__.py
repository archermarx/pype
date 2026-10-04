import argparse
import json
import os
from pathlib import Path

from . import NetworkClient, NetworkServer


def _token_from_environment(variable_name: str) -> str:
    try:
        return os.environ[variable_name]
    except KeyError as error:
        raise SystemExit(
            f"Set {variable_name} to the same secret on the server and client"
        ) from error


def _json_object(value: str) -> dict:
    try:
        result = json.loads(value)
    except json.JSONDecodeError as error:
        raise argparse.ArgumentTypeError(f"invalid JSON: {error}") from error
    if not isinstance(result, dict):
        raise argparse.ArgumentTypeError("payload must be a JSON object")
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pype",
        description="Test Pype over an SSH-forwarded TCP connection.",
    )
    parser.add_argument(
        "--token-env",
        default="PYPE_TOKEN",
        help="environment variable containing the shared token (default: PYPE_TOKEN)",
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    serve = subparsers.add_parser("serve", help="run a network server")
    serve.add_argument("--path", type=Path, default=Path("pype-state"))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    request = subparsers.add_parser("request", help="send one request")
    request.add_argument("command")
    request.add_argument("--payload", type=_json_object, default={})
    request.add_argument("--host", default="127.0.0.1")
    request.add_argument("--port", type=int, default=8765)
    request.add_argument("--timeout", type=float, default=30.0)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    token = _token_from_environment(args.token_env)

    if args.operation == "serve":
        server = NetworkServer(
            args.path,
            host=args.host,
            port=args.port,
            token=token,
        )
        try:
            server.listen()
        except KeyboardInterrupt:
            pass
        return

    client = NetworkClient(
        args.host,
        args.port,
        token=token,
        response_timeout_s=args.timeout,
    )
    payload, elapsed = client.request(args.command, args.payload)
    print(json.dumps({"payload": payload, "elapsed_s": elapsed}, indent=2))


if __name__ == "__main__":
    main()
