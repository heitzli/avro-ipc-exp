import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericResponder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.List;


/**
 * Java implementation of the ExampleService Avro RPC server.
 *
 * The server speaks the standard Avro RPC protocol defined by:
 *
 *     ../protocol/example.avpr
 *
 * It supports:
 *
 *     TCP      127.0.0.1:9000
 *     Unix     /tmp/example-java.sock
 *
 * The Rust client can communicate with either endpoint.
 */
public class ExampleServer {

    private static final String PROTOCOL_FILE =
            "../protocol/example.avpr";

    private static final String UNIX_SOCKET =
            "/tmp/example-java.sock";

    private static final String TCP_HOST =
            "127.0.0.1";

    private static final int TCP_PORT =
            9000;


    /*
     * ======================================================================
     * Protocol
     * ======================================================================
     */

    private static Protocol loadProtocol()
            throws IOException {

        String protocolText =
                Files.readString(
                        Path.of(PROTOCOL_FILE)
                );

        return Protocol.parse(
                protocolText
        );
    }


    /*
     * ======================================================================
     * Example responder
     * ======================================================================
     *
     * GenericResponder handles:
     *
     *     Avro RPC handshake
     *     method lookup
     *     request decoding
     *     response encoding
     *     RPC errors
     *
     * We only need to implement the actual application methods.
     */

    private static class ExampleResponder
            extends GenericResponder {

        ExampleResponder(
                Protocol protocol) {

            super(protocol);
        }


        @Override
        public Object respond(
                Protocol.Message message,
                Object request)
                throws Exception {

            String method =
                    message.getName();

            System.out.println(
                    "[server] " +
                    method +
                    "(" +
                    request +
                    ")"
            );

            switch (method) {

                case "echo":
                    return echo(
                            request
                    );

                case "add":
                    return add(
                            request
                    );

                default:
                    throw new RuntimeException(
                            "Unknown RPC method: "
                                    + method
                    );
            }
        }


        /*
         * ------------------------------------------------------------------
         * echo
         * ------------------------------------------------------------------
         */

        private Object echo(
                Object request) {

            GenericRecord outer =
                    (GenericRecord) request;

            GenericRecord echoRequest =
                    (GenericRecord)
                            outer.get(
                                    "request"
                            );

            String message =
                    echoRequest
                            .get("message")
                            .toString();

            System.out.println(
                    "[server] echo("
                            + message
                            + ")"
            );

            GenericRecord response =
                    new GenericData.Record(
                            getLocal()
                                    .getMessages()
                                    .get("echo")
                                    .getResponse()
                    );

            response.put(
                    "message",
                    message
            );

            return response;
        }


        /*
         * ------------------------------------------------------------------
         * add
         * ------------------------------------------------------------------
         */

        private Object add(
                Object request) {

            GenericRecord outer =
                    (GenericRecord) request;

            GenericRecord addRequest =
                    (GenericRecord)
                            outer.get(
                                    "request"
                            );

            long a =
                    (Long)
                            addRequest.get(
                                    "a"
                            );

            long b =
                    (Long)
                            addRequest.get(
                                    "b"
                            );

            long result =
                    a + b;

            System.out.println(
                    "[server] add("
                            + a
                            + ", "
                            + b
                            + ") -> "
                            + result
            );

            GenericRecord response =
                    new GenericData.Record(
                            getLocal()
                                    .getMessages()
                                    .get("add")
                                    .getResponse()
                    );

            response.put(
                    "result",
                    result
            );

            return response;
        }
    }


    /*
     * ======================================================================
     * Avro framing
     * ======================================================================
     *
     * Avro RPC messages use:
     *
     *     4-byte big-endian length
     *     payload
     *     4-byte zero terminator
     *
     * The Rust transport implements the same framing.
     */


    private static byte[] readFramedMessage(
            InputStream input)
            throws IOException {

        ByteArrayOutputStream output =
                new ByteArrayOutputStream();

        while (true) {

            int length =
                    readInt(input);

            if (length == 0) {
                break;
            }

            if (length < 0) {
                throw new IOException(
                        "Negative Avro frame length: "
                                + length
                );
            }

            byte[] buffer =
                    input.readNBytes(
                            length
                    );

            if (buffer.length != length) {
                throw new IOException(
                        "Unexpected EOF while reading "
                                + "Avro frame"
                );
            }

            output.write(
                    buffer
            );
        }

        return output.toByteArray();
    }


    private static void writeFramedMessage(
            OutputStream output,
            List<ByteBuffer> buffers)
            throws IOException {

        for (ByteBuffer buffer : buffers) {

            ByteBuffer duplicate =
                    buffer.duplicate();

            int length =
                    duplicate.remaining();

            writeInt(
                    output,
                    length
            );

            byte[] bytes =
                    new byte[length];

            duplicate.get(
                    bytes
            );

            output.write(
                    bytes
            );
        }

        /*
         * Zero-length buffer terminates
         * the Avro framed message.
         */

        writeInt(
                output,
                0
        );

        output.flush();
    }


    private static int readInt(
            InputStream input)
            throws IOException {

        int b1 =
                input.read();

        int b2 =
                input.read();

        int b3 =
                input.read();

        int b4 =
                input.read();

        if (
                b1 < 0 ||
                b2 < 0 ||
                b3 < 0 ||
                b4 < 0
        ) {
            throw new IOException(
                    "Connection closed while "
                            + "reading frame length"
            );
        }

        return
                ((b1 & 0xff) << 24)
                        |
                ((b2 & 0xff) << 16)
                        |
                ((b3 & 0xff) << 8)
                        |
                (b4 & 0xff);
    }


    private static void writeInt(
            OutputStream output,
            int value)
            throws IOException {

        output.write(
                (value >>> 24) & 0xff
        );

        output.write(
                (value >>> 16) & 0xff
        );

        output.write(
                (value >>> 8) & 0xff
        );

        output.write(
                value & 0xff
        );
    }


    /*
     * ======================================================================
     * Handle a single connection
     * ======================================================================
     */

    private static void handleConnection(
            Socket socket,
            ExampleResponder responder) {

        String address =
                socket.getRemoteSocketAddress()
                        .toString();

        System.out.println(
                "[server] connection from "
                        + address
        );

        try (
                socket;
                InputStream input =
                        socket.getInputStream();
                OutputStream output =
                        socket.getOutputStream()
        ) {

            while (true) {

                byte[] request;

                try {

                    request =
                            readFramedMessage(
                                    input
                            );

                } catch (IOException e) {

                    System.out.println(
                            "[server] connection closed: "
                                    + e.getMessage()
                    );

                    break;
                }

                if (request.length == 0) {
                    break;
                }

                try {

                    /*
                     * Responder understands the
                     * complete Avro RPC message:
                     *
                     *     handshake
                     *     call request
                     *
                     * and returns:
                     *
                     *     handshake response
                     *     call response
                     */

                    List<ByteBuffer> response =
                            responder.respond(
                                    List.of(
                                            ByteBuffer.wrap(
                                                    request
                                            )
                                    )
                            );

                    writeFramedMessage(
                            output,
                            response
                    );

                } catch (Exception e) {

                    System.err.println(
                            "[server] request failed: "
                                    + e
                    );

                    e.printStackTrace();
                    break;
                }
            }

        } catch (IOException e) {

            System.err.println(
                    "[server] connection error: "
                            + e
            );

        } finally {

            System.out.println(
                    "[server] disconnected: "
                            + address
            );
        }
    }


    /*
     * ======================================================================
     * TCP server
     * ======================================================================
     */

    private static void startTcpServer(
            ExampleResponder responder)
            throws IOException {

        ServerSocket server =
                new ServerSocket();

        server.bind(
                new InetSocketAddress(
                        TCP_HOST,
                        TCP_PORT
                )
        );

        System.out.println(
                "[server] TCP listening on "
                        + TCP_HOST
                        + ":"
                        + TCP_PORT
        );

        while (true) {

            Socket socket =
                    server.accept();

            Thread thread =
                    new Thread(
                            () -> handleConnection(
                                    socket,
                                    responder
                            )
                    );

            thread.setDaemon(
                    true
            );

            thread.start();
        }
    }


    /*
     * ======================================================================
     * Unix-domain socket server
     * ======================================================================
     */

    private static void startUnixServer(
            ExampleResponder responder)
            throws IOException {

        Path socketPath =
                Path.of(
                        UNIX_SOCKET
                );

        Files.deleteIfExists(
                socketPath
        );

        UnixDomainSocketAddress address =
                UnixDomainSocketAddress.of(
                        socketPath
                );

        ServerSocketChannel server =
                ServerSocketChannel.open(
                        StandardProtocolFamily.UNIX
                );

        server.bind(
                address
        );

        System.out.println(
                "[server] Unix listening on "
                        + UNIX_SOCKET
        );

        while (true) {

            SocketChannel channel =
                    server.accept();

            /*
             * Convert the channel into a
             * regular Socket-like connection
             * for the framing code.
             */

            Thread thread =
                    new Thread(
                            () -> handleUnixConnection(
                                    channel,
                                    responder
                            )
                    );

            thread.setDaemon(
                    true
            );

            thread.start();
        }
    }


    /*
     * ======================================================================
     * Unix connection handler
     * ======================================================================
     */

    private static void handleUnixConnection(
            SocketChannel channel,
            ExampleResponder responder) {

        System.out.println(
                "[server] Unix connection"
        );

        try (
                channel;
                InputStream input =
                        Channels.newInputStream(
                                channel
                        );
                OutputStream output =
                        Channels.newOutputStream(
                                channel
                        )
        ) {

            while (true) {

                byte[] request;

                try {

                    request =
                            readFramedMessage(
                                    input
                            );

                } catch (IOException e) {

                    System.out.println(
                            "[server] Unix connection closed: "
                                    + e.getMessage()
                    );

                    break;
                }

                if (request.length == 0) {
                    break;
                }

                try {

                    List<ByteBuffer> response =
                            responder.respond(
                                    List.of(
                                            ByteBuffer.wrap(
                                                    request
                                            )
                                    )
                            );

                    writeFramedMessage(
                            output,
                            response
                    );

                } catch (Exception e) {

                    System.err.println(
                            "[server] Unix request failed: "
                                    + e
                    );

                    e.printStackTrace();
                    break;
                }
            }

        } catch (IOException e) {

            System.err.println(
                    "[server] Unix connection error: "
                            + e
            );

        } finally {

            System.out.println(
                    "[server] Unix disconnected"
            );
        }
    }


    /*
     * ======================================================================
     * Main
     * ======================================================================
     */

    public static void main(
            String[] args)
            throws Exception {

        System.out.println(
                "=== Avro RPC Java server ==="
        );

        Protocol protocol =
                loadProtocol();

        System.out.println(
                "[server] protocol: "
                        + protocol.getName()
        );

        ExampleResponder responder =
                new ExampleResponder(
                        protocol
                );

        /*
         * Start Unix server.
         */

        Thread unixThread =
                new Thread(
                        () -> {
                            try {
                                startUnixServer(
                                        responder
                                );
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                );

        unixThread.setDaemon(
                true
        );

        unixThread.start();

        /*
         * Start TCP server.
         */

        Thread tcpThread =
                new Thread(
                        () -> {
                            try {
                                startTcpServer(
                                        responder
                                );
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                );

        tcpThread.setDaemon(
                true
        );

        tcpThread.start();

        System.out.println(
                "[server] Avro RPC server is running"
        );

        System.out.println(
                "[server] Unix: "
                        + UNIX_SOCKET
        );

        System.out.println(
                "[server] TCP:   "
                        + TCP_HOST
                        + ":"
                        + TCP_PORT
        );

        /*
         * Keep main alive.
         */

        Thread.currentThread()
                .join();
    }
}
