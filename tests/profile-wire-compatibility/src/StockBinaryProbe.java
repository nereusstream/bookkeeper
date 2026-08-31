/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** Sends the authoritative raw corpus to an actual released stock Bookie process. */
public final class StockBinaryProbe {

    private static final String LICENSE = "Licensed to the Apache Software Foundation (ASF) under one or more "
            + "contributor license agreements. See the NOTICE file distributed with this work for additional "
            + "information regarding copyright ownership. The ASF licenses this file to you under the Apache "
            + "License, Version 2.0.";

    private StockBinaryProbe() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 8) {
            throw new IllegalArgumentException(
                    "usage: StockBinaryProbe <version> <host> <port> <corpus-root> <fixtures.tsv> "
                            + "<jsonl> <summary.json> <read-timeout-ms>");
        }
        String version = arguments[0];
        String host = arguments[1];
        int port = Integer.parseInt(arguments[2]);
        Path corpusRoot = Paths.get(arguments[3]);
        Path fixtures = Paths.get(arguments[4]);
        Path jsonl = Paths.get(arguments[5]);
        Path summary = Paths.get(arguments[6]);
        int readTimeoutMillis = Integer.parseInt(arguments[7]);
        List<String> results = new ArrayList<>();
        results.add("{\"_license\":\"" + LICENSE + "\"}");
        int vectors = 0;
        int connections = 0;
        int connectionFailures = 0;
        int responseBytes = 0;
        int ackOrOk = 0;
        for (String line : Files.readAllLines(fixtures)) {
            if (line.trim().isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("\\t", -1);
            byte[] bytes = Files.readAllBytes(corpusRoot.resolve(fields[1]));
            Response response = send(host, port, bytes, readTimeoutMillis);
            vectors++;
            connections += response.connected ? 1 : 0;
            connectionFailures += response.connected ? 0 : 1;
            responseBytes += response.bytes.length;
            ackOrOk += response.bytes.length == 0 ? 0 : 1;
            results.add("{\"version\":\"" + version + "\",\"vector\":\"" + fields[0]
                    + "\",\"inputBytes\":" + bytes.length
                    + ",\"connected\":" + response.connected
                    + ",\"responseBytes\":" + response.bytes.length
                    + ",\"responsePrefixHex\":\"" + prefix(response.bytes)
                    + "\",\"terminal\":\"" + response.terminal + "\"}");
        }
        Files.write(jsonl, results, StandardOpenOption.CREATE_NEW);
        String status = responseBytes == 0 && ackOrOk == 0 && connectionFailures == 0 ? "PASS" : "FAIL";
        String receipt = "{\n"
                + "  \"version\": \"" + version + "\",\n"
                + "  \"layer\": \"released-stock-bookie-network\",\n"
                + "  \"status\": \"" + status + "\",\n"
                + "  \"vectors\": " + vectors + ",\n"
                + "  \"connections\": " + connections + ",\n"
                + "  \"connectionFailures\": " + connectionFailures + ",\n"
                + "  \"responseBytes\": " + responseBytes + ",\n"
                + "  \"ackOrOk\": " + ackOrOk + "\n"
                + "}\n";
        Files.write(summary, receipt.getBytes(java.nio.charset.StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
        if (!"PASS".equals(status)) {
            System.exit(1);
        }
    }

    private static Response send(String host, int port, byte[] bytes, int readTimeoutMillis) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2_000);
            socket.setSoTimeout(readTimeoutMillis);
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
            socket.shutdownOutput();
            ByteArrayOutputStream response = new ByteArrayOutputStream();
            byte[] buffer = new byte[4_096];
            try {
                int count;
                while ((count = socket.getInputStream().read(buffer)) >= 0) {
                    response.write(buffer, 0, count);
                    if (response.size() > 65_536) {
                        break;
                    }
                }
                return new Response(true, response.toByteArray(), "EOF");
            } catch (SocketTimeoutException timeout) {
                return new Response(true, response.toByteArray(), "TIMEOUT");
            } catch (SocketException reset) {
                return new Response(true, response.toByteArray(), "RESET");
            }
        } catch (IOException failure) {
            return new Response(false, new byte[0], failure.getClass().getSimpleName());
        }
    }

    private static String prefix(byte[] bytes) {
        return HexFormat.of().formatHex(java.util.Arrays.copyOf(bytes, Math.min(bytes.length, 32)));
    }

    private static final class Response {
        final boolean connected;
        final byte[] bytes;
        final String terminal;

        Response(boolean connected, byte[] bytes, String terminal) {
            this.connected = connected;
            this.bytes = bytes;
            this.terminal = terminal;
        }
    }
}
