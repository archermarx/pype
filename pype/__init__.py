import hmac
from http import HTTPStatus
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Callable, cast
import uuid


_REQUEST_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
_COMPLETED_REQUEST_CACHE_SIZE = 10_000
_MAX_NETWORK_MESSAGE_BYTES = 128 * 1024 * 1024

def uuid7() -> str:
    ts = time.time_ns() // 1_000_000  # 48 bits
    rand_a = int.from_bytes(os.urandom(2), "big") & 0xFFF
    rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)

    value = (
        (ts << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )

    return uuid.UUID(int=value).hex


def _write_json_atomic(path: Path, value: object) -> None:
    """Write JSON without exposing the destination until the content is complete."""
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")

    try:
        with temporary_path.open("x", encoding="utf-8") as fp:
            json.dump(value, fp)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _validate_request_id(request_id: object) -> str:
    if not isinstance(request_id, str) or not _REQUEST_ID_PATTERN.fullmatch(
        request_id
    ):
        raise ValueError("Request id must be a 32-character lowercase hexadecimal UUID")
    return request_id


def _validate_token(token: object) -> str:
    if not isinstance(token, str) or not token:
        raise ValueError("Network authentication token must be a non-empty string")
    if "\r" in token or "\n" in token:
        raise ValueError("Network authentication token cannot contain newlines")
    return token


def _validate_response(
    response: object,
    request_id: str,
    command: str,
) -> dict:
    if not isinstance(response, dict):
        raise RuntimeError("Server response must be a JSON object")
    if response.get("id") != request_id:
        raise RuntimeError("Server response has an unexpected request id")
    if response.get("command") != command:
        acknowledged_command = response.get("command")
        raise RuntimeError(
            "Server response acknowledged the wrong command. "
            f"Expected {command}, got {acknowledged_command}"
        )
    if response.get("status") != "success":
        raise RuntimeError(
            f"Server returned an error: {response.get('status')}; "
            f"payload={response.get('payload')}"
        )
    response_payload = response.get("payload")
    if not isinstance(response_payload, dict):
        raise RuntimeError("Server response payload must be a JSON object")
    return response_payload


# Built-in actions
def _ping(path, request_id, payload):
    return {}

def _echo(path, request_id, payload):
    return payload


class Server:
    def __init__(
        self,
        path,
        poll_interval_s=0.1,
        request_decode_grace_s=1.0,
    ):
        if poll_interval_s < 0:
            raise ValueError("Poll interval must be non-negative")
        if request_decode_grace_s < 0:
            raise ValueError("Request decode grace period must be non-negative")

        self.path = Path(path)
        self.request_path = self.path / "request"
        self.response_path = self.path / "response"

        self.path.mkdir(parents=True, exist_ok=True)
        self.request_path.mkdir(exist_ok=True)
        self.response_path.mkdir(exist_ok=True)

        self.poll_interval_s = poll_interval_s
        self.request_decode_grace_s = request_decode_grace_s
        self._request_decode_failures: dict[Path, float] = {}
        self._completed_request_ids: dict[str, None] = {}
        self.log_path = self.path / "pype.log"
        self._log_lock = threading.Lock()

        self.actions: dict[str, Callable[[Path, str, dict], dict]] = {}
        self.register_action("ping", _ping)
        self.register_action("echo", _echo)

    def find_new_requests(self) -> list[Path]:
        # Writers publish by renaming a .tmp file, so every visible .json file
        # is complete. Keeping requests on disk until a response is published
        # also lets a restarted server pick up outstanding work.
        return sorted(self.request_path.glob("*.json"))

    def register_action(
        self,
        command: str,
        action: Callable[[Path, str, dict], dict],
    ) -> None:
        if not isinstance(command, str):
            raise TypeError("Action command must be a string")
        if not callable(action):
            raise TypeError("Action must be callable")
        if command in self.actions:
            raise ValueError(
                f"Command {command} has already been registered with this server."
            )
        self.actions[command] = action

    def _log(self, level: str, message: str) -> None:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        line = f"{timestamp} {level} {message}"
        with self._log_lock:
            print(line, flush=True)
            with self.log_path.open("a", encoding="utf-8") as fp:
                fp.write(f"{line}\n")

    def _quarantine_request(self, request_path: Path, error: Exception) -> None:
        self._request_decode_failures.pop(request_path, None)
        quarantine_path = request_path.with_name(
            f"{request_path.name}.{uuid.uuid4().hex}.invalid"
        )
        try:
            os.replace(request_path, quarantine_path)
        except FileNotFoundError:
            return
        self._log("WARNING", f"Rejected request {request_path.name!r}: {error}")

    def _should_retry_decode(
        self,
        request_path: Path,
        error: Exception,
    ) -> bool:
        now = time.monotonic()
        first_failure = self._request_decode_failures.setdefault(request_path, now)
        if now - first_failure >= self.request_decode_grace_s:
            return False
        if first_failure == now:
            self._log(
                "WARNING",
                f"Request {request_path.name!r} is not readable yet; will retry: "
                f"{error}",
            )
        return True

    def _remember_completed_request(self, request_id: str) -> None:
        self._completed_request_ids[request_id] = None
        if len(self._completed_request_ids) > _COMPLETED_REQUEST_CACHE_SIZE:
            oldest_request_id = next(iter(self._completed_request_ids))
            del self._completed_request_ids[oldest_request_id]

    def _discard_completed_request(self, request_path: Path) -> bool:
        request_id = request_path.stem
        if not _REQUEST_ID_PATTERN.fullmatch(request_id):
            return False

        response_exists = (self.response_path / request_path.name).exists()
        if request_id not in self._completed_request_ids and not response_exists:
            return False

        self._remember_completed_request(request_id)
        self._request_decode_failures.pop(request_path, None)
        request_path.unlink(missing_ok=True)
        self._log(
            "INFO",
            f"Discarded duplicate completed request request_id={request_id!r}",
        )
        return True

    def process_pending_requests(self) -> int:
        processed = 0
        for request_path in self.find_new_requests():
            if self._discard_completed_request(request_path):
                continue
            try:
                with request_path.open("r", encoding="utf-8") as fp:
                    request_content = json.load(fp)
                self._request_decode_failures.pop(request_path, None)
                request_id = self.handle_request(request_content)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                if self._should_retry_decode(request_path, error):
                    continue
                self._quarantine_request(request_path, error)
                continue
            except (
                TypeError,
                ValueError,
                KeyError,
            ) as error:
                self._quarantine_request(request_path, error)
                continue
            except OSError as error:
                self._request_decode_failures.pop(request_path, None)
                # A transient filesystem failure should not stop the server or
                # discard the request. Leave it in place for the next poll.
                self._log(
                    "WARNING",
                    f"Could not process request {request_path.name!r}: {error}",
                )
                continue

            self._remember_completed_request(request_id)
            request_path.unlink(missing_ok=True)
            processed += 1
        return processed

    def listen(self) -> None:
        while True:
            self.process_pending_requests()
            time.sleep(self.poll_interval_s)

    def _build_response(self, request: dict) -> dict:
        if not isinstance(request, dict):
            raise TypeError("Request must be a JSON object")

        request_id = _validate_request_id(request["id"])
        command = request["command"]
        if not isinstance(command, str):
            raise TypeError("Request command must be a string")
        self._log("INFO", f"request_id={request_id!r} command={command!r}")
        request_payload = request.get("payload", {})
        if not isinstance(request_payload, dict):
            raise TypeError("Request payload must be a JSON object")

        response: dict = dict(id=request_id, command=command)

        if command not in self.actions:
            response["status"] = "error_cmd_not_found"
            response["payload"] = {"message": f"Unknown command: {command}"}
        else:
            try:
                response_payload = self.actions[command](
                    self.response_path,
                    request_id,
                    request_payload,
                )
                if not isinstance(response_payload, dict):
                    raise TypeError("Action must return a JSON object")
            except Exception as error:
                response["status"] = "error_action_failed"
                response["payload"] = {"message": str(error)}
            else:
                response["status"] = "success"
                response["payload"] = response_payload

        try:
            # Validate here so every transport reports a bad action result in
            # the same way, before attempting to publish it.
            json.dumps(response)
        except (TypeError, ValueError) as error:
            if response["status"] != "success":
                raise
            response["status"] = "error_action_failed"
            response["payload"] = {"message": str(error)}
        return response

    def handle_request(self, request: dict) -> str:
        response = self._build_response(request)
        request_id = response["id"]
        response_path = self.response_path / f"{request_id}.json"
        _write_json_atomic(response_path, response)
        return request_id


class Client:
    def __init__(self, path, poll_interval_s=0.1, response_timeout_s=30.0):
        if poll_interval_s < 0:
            raise ValueError("Poll interval must be non-negative")
        if response_timeout_s is not None and response_timeout_s < 0:
            raise ValueError("Response timeout must be non-negative or None")

        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(
                "Communication directory has not been created yet. "
                "Server has not been started."
            )

        self.request_path = self.path / "request"
        self.response_path = self.path / "response"
        self.poll_interval_s = poll_interval_s
        self.response_timeout_s = response_timeout_s

        if not self.request_path.is_dir():
            raise FileNotFoundError(
                "Request directory does not exist. Server may be malfunctioning."
            )

        if not self.response_path.is_dir():
            raise FileNotFoundError(
                "Response directory does not exist. Server may be malfunctioning."
            )

    def wait_for_response(self, request_id, timeout_s: float | None = None):
        request_id = _validate_request_id(request_id)
        if timeout_s is None:
            timeout_s = self.response_timeout_s
        if timeout_s is not None and timeout_s < 0:
            raise ValueError("Response timeout must be non-negative or None")

        target_path = self.response_path / f"{request_id}.json"
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        last_decode_error = None

        while True:
            if target_path.exists():
                try:
                    with target_path.open("r", encoding="utf-8") as fp:
                        response = json.load(fp)
                except (json.JSONDecodeError, UnicodeDecodeError) as error:
                    last_decode_error = error
                except FileNotFoundError:
                    # The sharing layer may invalidate a cached directory entry
                    # between exists() and open(). Poll it again.
                    pass
                else:
                    target_path.unlink(missing_ok=True)
                    return response

            if deadline is not None and time.monotonic() >= deadline:
                if last_decode_error is not None:
                    raise TimeoutError(
                        "Timed out waiting for a readable response to request "
                        f"{request_id}"
                    ) from last_decode_error
                raise TimeoutError(
                    f"Timed out waiting for response to request {request_id}"
                )
            time.sleep(self.poll_interval_s)

    def request(
        self,
        command: str,
        payload: dict | None = None,
        *,
        timeout_s: float | None = None,
    ) -> tuple[dict, float]:
        if not isinstance(command, str):
            raise TypeError("Request command must be a string")
        if payload is None:
            payload = {}
        elif not isinstance(payload, dict):
            raise TypeError("Request payload must be a JSON object")

        request_id = uuid7()
        request = dict(id=request_id, command=command, payload=payload)

        start_time = time.monotonic_ns()
        filename = f"{request_id}.json"
        _write_json_atomic(self.request_path / filename, request)

        response = self.wait_for_response(request_id, timeout_s=timeout_s)
        end_time = time.monotonic_ns()
        elapsed = (end_time - start_time) / 1e9

        return _validate_response(response, request_id, command), elapsed

    def ping(self):
        _, elapsed = self.request("ping", {})
        print(f"Ping time: {elapsed:.3g} seconds")


class _NetworkRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, status: HTTPStatus, value: object) -> None:
        body = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # The action may complete after a client-side timeout or a closed
            # SSH tunnel. There is no response channel left in that case.
            pass

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json(
            status,
            {"status": "error_invalid_request", "payload": {"message": message}},
        )

    def do_POST(self) -> None:
        network_server = cast("_PypeHTTPServer", self.server).pype_server

        if self.path != "/request":
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return

        expected_authorization = f"Bearer {network_server.token}"
        authorization = self.headers.get("Authorization", "")
        if not hmac.compare_digest(authorization, expected_authorization):
            self._send_error(HTTPStatus.UNAUTHORIZED, "Authentication failed")
            return

        content_length_header = self.headers.get("Content-Length")
        if content_length_header is None:
            self._send_error(HTTPStatus.LENGTH_REQUIRED, "Content-Length is required")
            return
        try:
            content_length = int(content_length_header)
        except ValueError:
            self._send_error(HTTPStatus.BAD_REQUEST, "Content-Length must be an integer")
            return

        if content_length < 0 or content_length > _MAX_NETWORK_MESSAGE_BYTES:
            self._send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request is too large")
            return

        body = self.rfile.read(content_length)
        try:
            request = json.loads(body.decode("utf-8"))
            response = network_server._build_response(request)
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TypeError,
            ValueError,
            KeyError,
        ) as error:
            self._send_error(HTTPStatus.BAD_REQUEST, str(error))
            return

        self._send_json(HTTPStatus.OK, response)

    def log_message(self, format: str, *args: object) -> None:
        # Commands are logged by Server._build_response. Suppressing the
        # standard access log keeps noisy request metadata out of pype.log.
        return


class _PypeHTTPServer(HTTPServer):
    def __init__(self, server_address, pype_server):
        self.pype_server = pype_server
        super().__init__(server_address, _NetworkRequestHandler)


class NetworkServer(Server):
    """Serve Pype requests over TCP for forwarding through an SSH tunnel."""

    def __init__(
        self,
        path,
        host="127.0.0.1",
        port=8765,
        *,
        token,
        poll_interval_s=0.1,
    ):
        if not isinstance(host, str) or not host:
            raise ValueError("Network host must be a non-empty string")
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 0 <= port <= 65535
        ):
            raise ValueError("Network port must be an integer from 0 through 65535")

        super().__init__(path, poll_interval_s=poll_interval_s)
        self.token = _validate_token(token)
        self._http_server = _PypeHTTPServer((host, port), self)
        self.host = self._http_server.server_address[0]
        self.port = self._http_server.server_address[1]

    def listen(self) -> None:
        self._log("INFO", f"Listening on {self.host}:{self.port}")
        try:
            self._http_server.serve_forever(poll_interval=self.poll_interval_s)
        finally:
            self._http_server.server_close()

    def shutdown(self) -> None:
        self._http_server.shutdown()


class NetworkClient:
    """Send Pype requests to a localhost port forwarded by SSH."""

    def __init__(
        self,
        host="127.0.0.1",
        port=8765,
        *,
        token,
        response_timeout_s=30.0,
    ):
        if not isinstance(host, str) or not host:
            raise ValueError("Network host must be a non-empty string")
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("Network port must be an integer from 1 through 65535")
        if response_timeout_s is not None and response_timeout_s < 0:
            raise ValueError("Response timeout must be non-negative or None")

        self.host = host
        self.port = port
        self.token = _validate_token(token)
        self.response_timeout_s = response_timeout_s

    def request(
        self,
        command: str,
        payload: dict | None = None,
        *,
        timeout_s: float | None = None,
    ) -> tuple[dict, float]:
        if not isinstance(command, str):
            raise TypeError("Request command must be a string")
        if payload is None:
            payload = {}
        elif not isinstance(payload, dict):
            raise TypeError("Request payload must be a JSON object")
        if timeout_s is None:
            timeout_s = self.response_timeout_s
        if timeout_s is not None and timeout_s < 0:
            raise ValueError("Response timeout must be non-negative or None")

        request_id = uuid7()
        request = {"id": request_id, "command": command, "payload": payload}
        try:
            request_body = json.dumps(request, separators=(",", ":"))
        except (TypeError, ValueError) as error:
            raise TypeError(
                "Request must contain only JSON-serializable values"
            ) from error

        start_time = time.monotonic_ns()
        connection = HTTPConnection(self.host, self.port, timeout=timeout_s)
        try:
            connection.request(
                "POST",
                "/request",
                body=request_body.encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
            )
            http_response = connection.getresponse()
            response_body = http_response.read(_MAX_NETWORK_MESSAGE_BYTES + 1)
        finally:
            connection.close()
        end_time = time.monotonic_ns()

        if len(response_body) > _MAX_NETWORK_MESSAGE_BYTES:
            raise RuntimeError("Server response is too large")
        try:
            response = json.loads(response_body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise RuntimeError("Server returned an invalid JSON response") from error
        if http_response.status != HTTPStatus.OK:
            message = response.get("payload", {}).get("message")
            raise RuntimeError(
                f"Server returned HTTP {http_response.status}: {message}"
            )

        elapsed = (end_time - start_time) / 1e9
        return _validate_response(response, request_id, command), elapsed

    def ping(self) -> None:
        _, elapsed = self.request("ping", {})
        print(f"Ping time: {elapsed:.3g} seconds")
