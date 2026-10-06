import os
import socket
import threading
from typing import Any

import avro.ipc
import avro.protocol

PROTOCOL_FILE = "../protocol/example.avpr"

UNIX_SOCKET = "/tmp/example.sock"

TCP_HOST = "127.0.0.1"
TCP_PORT = 9000

RequestDict = dict[str, Any]
ResultDict = dict[str, Any]


with open(PROTOCOL_FILE, "r", encoding="utf-8") as f:
    PROTOCOL_TEXT = f.read()


PROTOCOL = avro.protocol.parse(PROTOCOL_TEXT)

# TODO: Try to implement as async


class ExampleResponder(avro.ipc.Responder):
    def __init__(self):
        super().__init__(PROTOCOL)

    def invoke(self, local_message: avro.protocol.Message, request: RequestDict):  # type: ignore
        method = local_message.name

        if method == "echo":
            return self.echo(request)

        if method == "add":
            return self.add(request)

        # TODO maybe rather return an error response and ignore request instead of killing the server
        raise RuntimeError(f"Unknown RPC method: {method}")

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


def handle_connection(sock: socket.socket, address: str) -> None:
    print(f"[server] connection from {address}", flush=True)
    framed_socket = FramedSocket(sock)
    reader = avro.ipc.FramedReader(framed_socket)
    writer = avro.ipc.FramedWriter(framed_socket)
    responder = ExampleResponder()

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


def serve_socket(server_sock: socket.socket, name: str):
    print(f"[server] listening on {name}", flush=True)

    while True:
        client_sock, address = server_sock.accept()
        thread = threading.Thread(target=handle_connection, args=(client_sock, address), daemon=True)
        thread.start()


def start_unix_server() -> socket.socket:
    if os.path.exists(UNIX_SOCKET):
        os.unlink(UNIX_SOCKET)
    server = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
    )
    server.bind(UNIX_SOCKET)
    server.listen(16)
    return server


def start_tcp_server() -> socket.socket:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((TCP_HOST, TCP_PORT))
    server.listen(16)
    return server


def main():
    unix_server = start_unix_server()
    tcp_server = start_tcp_server()
    unix_thread = threading.Thread(target=serve_socket, args=(unix_server, UNIX_SOCKET), daemon=True)

    tcp_thread = threading.Thread(target=serve_socket, args=(tcp_server, f"{TCP_HOST}:{TCP_PORT}"), daemon=True)
    unix_thread.start()
    tcp_thread.start()

    print("[server] Avro RPC server is running", flush=True)
    print(f"[server] Unix: {UNIX_SOCKET}", flush=True)
    print(f"[server] TCP:   {TCP_HOST}:{TCP_PORT}", flush=True)

    try:
        unix_thread.join()
        tcp_thread.join()

    except KeyboardInterrupt:
        print("\n[server] shutting down", flush=True)
        unix_server.close()
        tcp_server.close()
        if os.path.exists(UNIX_SOCKET):
            os.unlink(UNIX_SOCKET)


if __name__ == "__main__":
    main()
