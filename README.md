## Pype

Shared-storage IPC in python. Designed for passing messages between processes using shared storage.

Requests and responses are written to temporary files and atomically renamed to
their final `.json` names. This prevents readers from observing partially written
JSON. A request remains on disk until its response has been published, allowing a
restarted server to resume outstanding work. Successfully consumed request and
response files are removed automatically.

Clients wait up to 30 seconds for a response by default. Pass
`response_timeout_s=None` to `Client` to wait indefinitely, or pass `timeout_s` to
`request` or `wait_for_response` to override the default for one request.

Requests and successful responses carry a JSON-object `payload`. Registered server
actions receive the request payload and return a dictionary that becomes the
response payload. Protocol fields such as `id` and `command` are kept separate from
application data.

The server logs every decoded command and request ID to both stdout and `pype.log`
in the communication directory. Payload contents are intentionally excluded from
the log.

The communication directory is intended to have one active `Server` listener.
Requests are otherwise delivered at least once: an action may run again if the
server process exits after the action finishes but before its response is published.
