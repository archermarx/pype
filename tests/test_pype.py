import json
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from pype import Client, Server, _write_json_atomic, uuid7


class AtomicWriteTests(unittest.TestCase):
    def test_destination_is_hidden_until_json_is_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "message.json"
            dump_started = threading.Event()
            finish_dump = threading.Event()
            original_dump = json.dump

            def slow_dump(value, fp):
                fp.write('{"partial":')
                fp.flush()
                dump_started.set()
                finish_dump.wait(timeout=2)
                fp.seek(0)
                fp.truncate()
                original_dump(value, fp)

            with mock.patch("pype.json.dump", side_effect=slow_dump):
                writer = threading.Thread(
                    target=_write_json_atomic,
                    args=(destination, {"complete": True}),
                )
                writer.start()
                self.assertTrue(dump_started.wait(timeout=1))
                self.assertFalse(destination.exists())
                finish_dump.set()
                writer.join(timeout=2)

            self.assertFalse(writer.is_alive())
            self.assertEqual(json.loads(destination.read_text()), {"complete": True})


class ClientServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary_directory.name)
        self.server = Server(self.path, poll_interval_s=0.001)
        self.client = Client(
            self.path,
            poll_interval_s=0.001,
            response_timeout_s=0.05,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def write_request(self, request_id, command):
        _write_json_atomic(
            self.server.request_path / f"{request_id}.json",
            {"id": request_id, "command": command},
        )

    def test_existing_request_is_processed_and_files_are_cleaned_up(self):
        request_id = uuid7()
        self.write_request(request_id, "ping")

        self.assertEqual(self.server.process_pending_requests(), 1)
        response = self.client.wait_for_response(request_id)

        self.assertEqual(response["status"], "success")
        self.assertEqual(list(self.server.request_path.glob("*.json")), [])
        self.assertEqual(list(self.server.response_path.glob("*.json")), [])

    def test_commands_are_logged_to_file_and_stdout_without_payloads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            stdout = StringIO()
            with redirect_stdout(stdout):
                server = Server(path)
                request_id = uuid7()
                _write_json_atomic(
                    server.request_path / f"{request_id}.json",
                    {
                        "id": request_id,
                        "command": "echo",
                        "payload": {"secret": "not-logged"},
                    },
                )
                server.process_pending_requests()

            expected = f"request_id='{request_id}' command='echo'"
            file_output = (path / "pype.log").read_text(encoding="utf-8")
            self.assertIn(expected, file_output)
            self.assertIn(expected, stdout.getvalue())
            self.assertNotIn("not-logged", file_output)
            self.assertNotIn("not-logged", stdout.getvalue())

    def test_action_failure_returns_an_error_and_server_continues(self):
        def fail(path, request_id, payload):
            raise RuntimeError("action failed")

        self.server.register_action("fail", fail)
        failed_request_id = uuid7()
        next_request_id = uuid7()
        self.write_request(failed_request_id, "fail")
        self.write_request(next_request_id, "ping")

        self.assertEqual(self.server.process_pending_requests(), 2)

        failed_response = self.client.wait_for_response(failed_request_id)
        next_response = self.client.wait_for_response(next_request_id)
        self.assertEqual(failed_response["status"], "error_action_failed")
        self.assertEqual(failed_response["payload"]["message"], "action failed")
        self.assertEqual(next_response["status"], "success")

    def process_next_request(self):
        deadline = time.monotonic() + 1
        while not self.server.find_new_requests():
            if time.monotonic() >= deadline:
                return
            time.sleep(0.001)
        self.server.process_pending_requests()

    def test_generic_request_passes_and_returns_payloads(self):
        def echo(path, request_id, payload):
            return {"echo": payload}

        self.server.register_action("wrapped_echo", echo)
        processor = threading.Thread(target=self.process_next_request)
        processor.start()

        response_payload, elapsed = self.client.request(
            "wrapped_echo",
            {"value": 42},
        )
        processor.join(timeout=1)

        self.assertFalse(processor.is_alive())
        self.assertEqual(response_payload, {"echo": {"value": 42}})
        self.assertGreaterEqual(elapsed, 0)

    def test_payload_cannot_override_protocol_fields(self):
        def echo(path, request_id, payload):
            return payload

        self.server.register_action("reserved_field_echo", echo)
        processor = threading.Thread(target=self.process_next_request)
        processor.start()

        response_payload, _ = self.client.request(
            "reserved_field_echo",
            {"id": "payload-id", "command": "payload-command"},
        )
        processor.join(timeout=1)

        self.assertFalse(processor.is_alive())
        self.assertEqual(
            response_payload,
            {"id": "payload-id", "command": "payload-command"},
        )

    def test_generic_request_includes_server_error_details(self):
        def fail(path, request_id, payload):
            raise RuntimeError("specific failure")

        self.server.register_action("fail", fail)
        processor = threading.Thread(target=self.process_next_request)
        processor.start()

        with self.assertRaisesRegex(RuntimeError, "specific failure"):
            self.client.request("fail")
        processor.join(timeout=1)
        self.assertFalse(processor.is_alive())

    def test_non_json_response_payload_returns_an_action_error(self):
        def invalid_response(path, request_id, payload):
            return {"not_json": object()}

        self.server.register_action("invalid_response", invalid_response)
        processor = threading.Thread(target=self.process_next_request)
        processor.start()

        with self.assertRaisesRegex(RuntimeError, "not JSON serializable"):
            self.client.request("invalid_response")
        processor.join(timeout=1)

        self.assertFalse(processor.is_alive())
        self.assertEqual(list(self.server.request_path.glob("*.json")), [])

    def test_malformed_and_unsafe_requests_are_quarantined(self):
        self.server.request_decode_grace_s = 0
        malformed_path = self.server.request_path / "malformed.json"
        malformed_path.write_text("{")
        unsafe_path = self.server.request_path / "unsafe.json"
        unsafe_path.write_text(
            json.dumps({"id": "../escape", "command": "ping"})
        )

        self.assertEqual(self.server.process_pending_requests(), 0)

        quarantined = list(self.server.request_path.glob("*.invalid"))
        self.assertEqual(len(quarantined), 2)
        self.assertFalse((self.path / "escape.json").exists())

    def test_temporarily_empty_request_is_retried(self):
        request_id = uuid7()
        request_path = self.server.request_path / f"{request_id}.json"
        request_path.write_text("")

        self.assertEqual(self.server.process_pending_requests(), 0)
        self.assertTrue(request_path.exists())
        self.assertEqual(list(self.server.request_path.glob("*.invalid")), [])

        _write_json_atomic(
            request_path,
            {"id": request_id, "command": "ping", "payload": {}},
        )
        self.assertEqual(self.server.process_pending_requests(), 1)
        response = self.client.wait_for_response(request_id)
        self.assertEqual(response["status"], "success")

    def test_completed_request_that_reappears_empty_is_discarded(self):
        request_id = uuid7()
        request_path = self.server.request_path / f"{request_id}.json"
        _write_json_atomic(
            request_path,
            {"id": request_id, "command": "ping", "payload": {}},
        )

        self.assertEqual(self.server.process_pending_requests(), 1)
        self.client.wait_for_response(request_id)

        request_path.write_text("")
        self.assertEqual(self.server.process_pending_requests(), 0)

        self.assertFalse(request_path.exists())
        self.assertEqual(list(self.server.request_path.glob("*.invalid")), [])

    def test_wait_for_response_times_out(self):
        with self.assertRaises(TimeoutError):
            self.client.wait_for_response(uuid7(), timeout_s=0.005)

    def test_temporarily_empty_response_is_retried(self):
        request_id = uuid7()
        response_path = self.server.response_path / f"{request_id}.json"
        response_path.write_text("")

        def publish_response():
            time.sleep(0.005)
            _write_json_atomic(
                response_path,
                {
                    "id": request_id,
                    "command": "ping",
                    "status": "success",
                    "payload": {},
                },
            )

        publisher = threading.Thread(target=publish_response)
        publisher.start()
        response = self.client.wait_for_response(request_id, timeout_s=0.1)
        publisher.join(timeout=1)

        self.assertEqual(response["status"], "success")
        self.assertFalse(response_path.exists())

    def test_generic_request_accepts_a_per_request_timeout(self):
        with self.assertRaises(TimeoutError):
            self.client.request("ping", timeout_s=0.005)

    def test_invalid_timing_options_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Poll interval"):
            Client(self.path, poll_interval_s=-1)
        with self.assertRaisesRegex(ValueError, "Response timeout"):
            Client(self.path, response_timeout_s=-1)
        with self.assertRaisesRegex(ValueError, "decode grace"):
            Server(self.path, request_decode_grace_s=-1)

    def test_client_requires_response_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "request").mkdir()
            with self.assertRaisesRegex(FileNotFoundError, "Response directory"):
                Client(path)


if __name__ == "__main__":
    unittest.main()
