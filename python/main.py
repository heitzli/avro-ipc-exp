import argparse
import os
import socket
import threading
from typing import Any

import avro.ipc
import avro.protocol

PROTOCOL_FILE = os.environ.get("AVRO_PROTOCOL", "../protocol/example.avpr")

UNIX_SOCKET = os.environ.get("AVRO_UNIX_SOCKET", "/tmp/example.sock")

TCP_HOST = os.environ.get("AVRO_TCP_HOST", "127.0.0.1")
TCP_PORT = int(os.environ.get("AVRO_TCP_PORT", "9000"))

RequestDict = dict[str, Any]
ResultDict = dict[str, Any]


def load_protocol(path: str) -> avro.protocol.Protocol:
    with open(path, "r", encoding="utf-8") as f:
        return avro.protocol.parse(f.read())


PROTOCOL = load_protocol(PROTOCOL_FILE)

# TODO: Try to implement as async


def _record_has_field(protocol: avro.protocol.Protocol, record: str, field: str) -> bool:
    for schema in protocol.types:
        if getattr(schema, "fullname", "") in (record, f"example.{record}"):
            return any(f.name == field for f in schema.fields)
    return False


class ExampleResponder(avro.ipc.Responder):
    def __init__(self, protocol: avro.protocol.Protocol | None = None):
        super().__init__(protocol or PROTOCOL)
        # Python's DatumWriter validates strictly: extra keys raise
        # AvroTypeException. Only send `note` when the local (writer)
        # response schema actually declares it (v2).
        self._test_response_has_note = _record_has_field(
            self.local_protocol, "TestResponse", "note"
        )

    def invoke(self, local_message: avro.protocol.Message, request: RequestDict):  # type: ignore
        method = local_message.name

        if method == "echo":
            return self.echo(request)

        if method == "add":
            return self.add(request)

        if method == "test":
            return self.test(request)

        # TODO maybe rather return an error response and ignore request instead of killing the server
        raise RuntimeError(f"Unknown RPC method: {method}")

    def test(self, request: RequestDict) -> ResultDict:
        inner = request["request"]
        a = inner["a"]
        b = inner["b"]
        # `c` only exists in v2; DatumReader fills the default ("new")
        # when an old (v1) client omits it. `.get` keeps v1 servers working
        # when a new client sends `c` that the v1 reader skips.
        c = inner.get("c", "<missing>")

        result = a - b
        print(f"[server] test(a={a}, b={b}, c={c!r}) -> {result}", flush=True)
        response: ResultDict = {"result": result}
        if self._test_response_has_note:
            # v2 writers require every field (DatumWriter does not
            # fill defaults on write); v1 must omit it (extra keys
            # fail strict validation).
            response["note"] = f"c={c}"
        return response

    def echo(self, request: RequestDict) -> ResultDict:
        message = request["request"]["message"]
        print(f"[server] echo({message!r})", flush=True)
        return {"message": message}

    def add(self, request: RequestDict) -> ResultDict:
        a = request["request"]["a"]
        b = request["request"]["b"]
        result = a + b
        print(f"[server] add({a}, {b}) -> {result}", flush=True)
        return {"result": result}


class FramedSocket:
    """
    Adapt socket.socket to the file-like interface expected by Avro's FramedReader / FramedWriter.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            raise ValueError("unbounded read is not supported")

        if size == 0:
            return b""

        data = bytearray()

        while len(data) < size:
            chunk = self.sock.recv(size - len(data))

            if not chunk:
                raise ConnectionError("socket closed while reading")

            data.extend(chunk)

        return bytes(data)

    def write(self, data) -> int:
        self.sock.sendall(data)
        return len(data)

    def flush(self):
        # TODO look into this in more detail
        pass

    def close(self):
        self.sock.close()


_responder_protocol: avro.protocol.Protocol = PROTOCOL


def handle_connection(
    sock: socket.socket, address: str, responder: ExampleResponder | None = None
) -> None:
    print(f"[server] connection from {address}", flush=True)
    framed_socket = FramedSocket(sock)
    reader = avro.ipc.FramedReader(framed_socket)
    writer = avro.ipc.FramedWriter(framed_socket)
    responder = responder or ExampleResponder(_responder_protocol)

    try:
        while True:
            try:
                request = reader.read_framed_message()

            except Exception as exc:
                print(f"[server] connection closed: {exc}", flush=True)
                break

            if not request:
                break

            try:
                response = responder.respond(request)
                writer.write_framed_message(response)

            except Exception as exc:
                print(f"[server] request failed: {exc}", flush=True)
                raise
    finally:
        framed_socket.close()
        print(f"[server] disconnected: {address}", flush=True)


def serve_socket(server_sock: socket.socket, name: str, responder: ExampleResponder):
    print(f"[server] listening on {name}", flush=True)

    while True:
        client_sock, address = server_sock.accept()
        thread = threading.Thread(
            target=handle_connection, args=(client_sock, address, responder), daemon=True
        )
        thread.start()


def start_unix_server(path: str) -> socket.socket:
    if os.path.exists(path):
        os.unlink(path)
    server = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
    )
    server.bind(path)
    server.listen(16)
    return server


def start_tcp_server(host: str, port: int) -> socket.socket:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(16)
    return server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Avro RPC example server")
    parser.add_argument("--protocol", default=PROTOCOL_FILE)
    parser.add_argument("--tcp-host", default=TCP_HOST)
    parser.add_argument("--tcp-port", type=int, default=TCP_PORT)
    parser.add_argument("--unix-socket", default=UNIX_SOCKET)
    parser.add_argument("--no-unix", action="store_true", help="disable unix socket listener (for tests)")
    return parser.parse_args()


def main() -> None:
    global PROTOCOL, _responder_protocol
    args = parse_args()
    PROTOCOL = load_protocol(args.protocol)
    _responder_protocol = PROTOCOL
    responder = ExampleResponder(PROTOCOL)

    threads = []
    servers: list[socket.socket] = []

    if not args.no_unix:
        unix_server = start_unix_server(args.unix_socket)
        servers.append(unix_server)
        unix_thread = threading.Thread(
            target=serve_socket, args=(unix_server, args.unix_socket, responder), daemon=True
        )
        threads.append(unix_thread)

    tcp_server = start_tcp_server(args.tcp_host, args.tcp_port)
    servers.append(tcp_server)
    tcp_thread = threading.Thread(
        target=serve_socket,
        args=(tcp_server, f"{args.tcp_host}:{args.tcp_port}", responder),
        daemon=True,
    )
    threads.append(tcp_thread)

    for thread in threads:
        thread.start()

    print(f"[server] Avro RPC server is running ({args.protocol})", flush=True)
    if not args.no_unix:
        print(f"[server] Unix: {args.unix_socket}", flush=True)
    print(f"[server] TCP:   {args.tcp_host}:{args.tcp_port}", flush=True)

    try:
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("\n[server] shutting down", flush=True)
        for server in servers:
            server.close()
        if not args.no_unix and os.path.exists(args.unix_socket):
            os.unlink(args.unix_socket)


if __name__ == "__main__":
    main()
