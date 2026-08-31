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

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.proto.BookieProtoEncoding;

/** Isolated JVM probe using the exact RequestDecoder loaded from a released BookKeeper artifact. */
public final class StockDecoderProbe {

    private static final String LICENSE = "Licensed to the Apache Software Foundation (ASF) under one or more "
            + "contributor license agreements. See the NOTICE file distributed with this work for additional "
            + "information regarding copyright ownership. The ASF licenses this file to you under the Apache "
            + "License, Version 2.0.";

    private StockDecoderProbe() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 5) {
            throw new IllegalArgumentException(
                    "usage: StockDecoderProbe <version> <corpus-root> <fixtures.tsv> <jsonl> <summary.json>");
        }
        String version = arguments[0];
        Path corpusRoot = Paths.get(arguments[1]);
        Path fixtures = Paths.get(arguments[2]);
        Path jsonl = Paths.get(arguments[3]);
        Path summary = Paths.get(arguments[4]);
        List<String> results = new ArrayList<>();
        results.add("{\"_license\":\"" + LICENSE + "\"}");
        int vectors = 0;
        int extractedFrames = 0;
        int incompleteFrames = 0;
        int decoderExceptions = 0;
        int fallbackTransitions = 0;
        int recognizedRequests = 0;
        int fallbackFollowedByEffect = 0;
        for (String line : Files.readAllLines(fixtures)) {
            if (line.trim().isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("\\t", -1);
            ProbeResult result = probe(Files.readAllBytes(corpusRoot.resolve(fields[1])));
            vectors++;
            extractedFrames += result.extractedFrames;
            incompleteFrames += result.incompleteFrames;
            decoderExceptions += result.decoderExceptions;
            fallbackTransitions += result.fallbackTransitions;
            recognizedRequests += result.recognizedRequests;
            fallbackFollowedByEffect += result.fallbackFollowedByEffect;
            results.add("{\"version\":\"" + version + "\",\"vector\":\"" + fields[0]
                    + "\",\"extractedFrames\":" + result.extractedFrames
                    + ",\"incompleteFrames\":" + result.incompleteFrames
                    + ",\"decoderExceptions\":" + result.decoderExceptions
                    + ",\"fallbackTransitions\":" + result.fallbackTransitions
                    + ",\"recognizedRequests\":" + result.recognizedRequests
                    + ",\"fallbackFollowedByEffect\":" + result.fallbackFollowedByEffect
                    + ",\"requestClasses\":\"" + escape(String.join(",", result.requestClasses)) + "\"}");
        }
        Files.write(jsonl, results, StandardOpenOption.CREATE_NEW);
        String status = recognizedRequests == 0 && fallbackFollowedByEffect == 0 ? "PASS" : "FAIL";
        String receipt = "{\n"
                + "  \"version\": \"" + version + "\",\n"
                + "  \"layer\": \"released-hybrid-v3-prev3-decoder\",\n"
                + "  \"status\": \"" + status + "\",\n"
                + "  \"vectors\": " + vectors + ",\n"
                + "  \"extractedFrames\": " + extractedFrames + ",\n"
                + "  \"incompleteFrames\": " + incompleteFrames + ",\n"
                + "  \"decoderExceptions\": " + decoderExceptions + ",\n"
                + "  \"fallbackTransitions\": " + fallbackTransitions + ",\n"
                + "  \"recognizedRequests\": " + recognizedRequests + ",\n"
                + "  \"classicRouteClaims\": " + recognizedRequests + ",\n"
                + "  \"fallbackFollowedByEffect\": " + fallbackFollowedByEffect + "\n"
                + "}\n";
        Files.write(summary, receipt.getBytes(java.nio.charset.StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
        if (!"PASS".equals(status)) {
            System.exit(1);
        }
    }

    private static ProbeResult probe(byte[] bytes) throws Exception {
        BookieProtoEncoding.RequestDecoder decoder = newDecoder();
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        Field usingV3 = BookieProtoEncoding.RequestDecoder.class.getDeclaredField("usingV3Protocol");
        usingV3.setAccessible(true);
        int offset = 0;
        int extractedFrames = 0;
        int incompleteFrames = 0;
        int decoderExceptions = 0;
        int fallbackTransitions = 0;
        int recognizedRequests = 0;
        int fallbackFollowedByEffect = 0;
        boolean previouslyUsingV3 = true;
        List<String> classes = new ArrayList<>();
        try {
            while (bytes.length - offset >= 4) {
                long length = Integer.toUnsignedLong(ByteBuffer.wrap(bytes, offset, 4).getInt());
                if (length > bytes.length - offset - 4L || length > Integer.MAX_VALUE) {
                    incompleteFrames++;
                    break;
                }
                byte[] payload = java.util.Arrays.copyOfRange(bytes, offset + 4, offset + 4 + (int) length);
                offset += 4 + (int) length;
                extractedFrames++;
                try {
                    channel.writeInbound(Unpooled.wrappedBuffer(payload));
                } catch (Throwable failure) {
                    decoderExceptions++;
                }
                boolean nowUsingV3 = usingV3.getBoolean(decoder);
                if (previouslyUsingV3 && !nowUsingV3) {
                    fallbackTransitions++;
                }
                Object request;
                while ((request = channel.readInbound()) != null) {
                    recognizedRequests++;
                    classes.add(request.getClass().getName());
                    if (!nowUsingV3) {
                        fallbackFollowedByEffect++;
                    }
                    ReferenceCountUtil.release(request);
                }
                previouslyUsingV3 = nowUsingV3;
            }
        } finally {
            channel.finishAndReleaseAll();
        }
        return new ProbeResult(
                extractedFrames,
                incompleteFrames,
                decoderExceptions,
                fallbackTransitions,
                recognizedRequests,
                fallbackFollowedByEffect,
                classes);
    }

    private static BookieProtoEncoding.RequestDecoder newDecoder() throws Exception {
        Constructor<?> constructor = BookieProtoEncoding.RequestDecoder.class.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        if (constructor.getParameterCount() == 0) {
            return (BookieProtoEncoding.RequestDecoder) constructor.newInstance();
        }
        Class<?> registryType = constructor.getParameterTypes()[0];
        Object registry = registryType.getMethod("getEmptyRegistry").invoke(null);
        return (BookieProtoEncoding.RequestDecoder) constructor.newInstance(registry);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class ProbeResult {
        final int extractedFrames;
        final int incompleteFrames;
        final int decoderExceptions;
        final int fallbackTransitions;
        final int recognizedRequests;
        final int fallbackFollowedByEffect;
        final List<String> requestClasses;

        ProbeResult(
                int extractedFrames,
                int incompleteFrames,
                int decoderExceptions,
                int fallbackTransitions,
                int recognizedRequests,
                int fallbackFollowedByEffect,
                List<String> requestClasses) {
            this.extractedFrames = extractedFrames;
            this.incompleteFrames = incompleteFrames;
            this.decoderExceptions = decoderExceptions;
            this.fallbackTransitions = fallbackTransitions;
            this.recognizedRequests = recognizedRequests;
            this.fallbackFollowedByEffect = fallbackFollowedByEffect;
            this.requestClasses = requestClasses;
        }
    }
}
