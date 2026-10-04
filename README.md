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

## SSH-tunnel transport

`NetworkServer` and `NetworkClient` avoid sharing request files between machines.
They use HTTP message framing over a TCP connection, which can be forwarded through
SSH. The directory passed to `NetworkServer` is local server state used for its log
and action output; the client does not mount or access it.

Create a random authentication token and place the same value in `PYPE_TOKEN` on
the Linux server and Mac client. Keep the token out of source control. For example:

```console
$ openssl rand -hex 32
```

On the allocated Great Lakes compute node, run the server bound to loopback:

```python
import os

from pype import NetworkServer


server = NetworkServer(
    "pype-state",
    host="127.0.0.1",
    port=8765,
    token=os.environ["PYPE_TOKEN"],
)

# Register application actions here, then listen until interrupted.
server.listen()
```

For an immediate test using only the built-in `ping` and `echo` actions, the
equivalent command is:

```console
$ PYPE_TOKEN='the-generated-token' uv run python -m pype serve
```

From the Mac, open the tunnel in a separate terminal. This assumes `gl-compute`
is the working `ProxyJump` host from the SSH configuration:

```console
$ ssh -N -T \
    -L 127.0.0.1:8765:127.0.0.1:8765 \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    gl-compute
```

The Mac client connects only to its end of the tunnel:

```python
import os

from pype import NetworkClient


client = NetworkClient(
    "127.0.0.1",
    8765,
    token=os.environ["PYPE_TOKEN"],
)
payload, elapsed = client.request("echo", {"message": "hello"})
print(payload, elapsed)
```

Or exercise the full path directly from the command line on the Mac:

```console
$ PYPE_TOKEN='the-generated-token' uv run python -m pype \
    request echo --payload '{"message":"hello through SSH"}'
```

The token is required even on loopback because Great Lakes compute nodes are
shared systems. It is sent inside the encrypted SSH tunnel and is never included
in Pype's command log. Stop the tunnel with `Ctrl-C`; stop the server with
`Ctrl-C` as well.
