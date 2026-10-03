from pathlib import Path
from enum import auto, StrEnum
import json
import time
import os

class Server:
    def __init__(self, path):
        self.path = Path(path)

        if self.path.exists():
            raise FileExistsError("Communication directory already exists!")

        self.request_path = self.path / "request"
        self.response_path = self.path / "response"

        self.request_path.mkdir()
        self.response_path.mkdir()
        self.known_requests: set[Path] = set()

    def listen(self):
        while True:
            requests: set[Path] = set(
                self.request_path / f
                for f in os.listdir(self.request_path) if f.endswith(".json")
            )
            new_requests = requests.difference(self.known_requests)
            self.known_requests.update(new_requests)

            for request in new_requests:
                with open(request, "rb") as fp:
                    self.handle_request(json.load(fp))

            time.sleep(0.1)

    def handle_request(self, request: dict):
        id = request["id"]
        cmd = request["command"]
        response = dict(id=id, command=cmd, status="success")

        match cmd:
            case "ping": pass
            case _:
                response["status"] = "error_cmd_not_found"

        with open(self.response_path / f"{id}.json", "w") as fp:
            json.dump(response, fp)

class Client:
    def __init__(self, path):
        if not Path(path).exists():
            raise FileNotFoundError("Communication directory has not been created yet. Server has not been started.")

        self.path = path
        self.request_path = path / "request"
        self.response_path = path / "response"

        if not self.request_path.exists():
            raise FileNotFoundError("Requests directory does not exist. Server may be malfunctioning.")

        if not self.request_path.exists():
            raise FileNotFoundError("Response directory does not exist. Server may be malfunctioning.")

    def wait_for_response(self, id, poll_time_s=1):
        target_path = self.response_path / f"{id}.json"
        while not target_path.exists():
            time.sleep(poll_time_s)

        with open(target_path, "rb") as fp:
            return json.load(fp)

    def ping(self):
        id = 1

        start_time = time.time_ns()

        filename = f"{id}.json"
        with open(self.request_path / filename, "w") as fp:
            json.dump(dict(id=id, command="ping"), fp)

        response = self.wait_for_response(id)

        end_time = time.time_ns()

        assert response["id"] == id
        assert response["command"] == "ping"
        assert response["status"] == "success"

        print(f"Ping time: {(end_time - start_time) / 1e9:.3g} seconds")