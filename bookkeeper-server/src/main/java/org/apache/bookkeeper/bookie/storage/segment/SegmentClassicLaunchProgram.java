/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Fixed program data, never evidence that these files or arguments are the actual supervised process. */
record SegmentClassicLaunchProgram(String os, String architecture, String runtime, String vendor, Path javaHome,
        String mainClass, int javaFile, int aclJniFile, List<Integer> classPath, List<String> jvmArguments,
        List<File> files) {
    static final int MAX_BYTES = 1024 * 1024;
    static final int MAX_FILES = 4096;
    static final int JDK = 1;
    static final int JAVA = 2;
    static final int ACL_JNI = 4;
    static final int CLASSPATH = 8;
    static final int NATIVE = 16;
    static final String MAIN = "org.apache.bookkeeper.server.ControlledClassicMain";
    private static final long MAGIC = 0x5557434c50303031L; // UWCLP001
    private static final byte[] DOMAIN = "BookKeeper/ControlledClassicProgram/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final Pattern MEMORY = Pattern.compile(
            "(-Xms|-Xmx|-Xss|-XX:MaxDirectMemorySize=|-XX:MaxMetaspaceSize=)([1-9][0-9]{0,18})([kKmMgG]?)");

    record File(Path path, int roles, long length, SegmentClassicHandoff.Digest sha256) {
        File {
            absolute(path);
            Objects.requireNonNull(sha256, "sha256");
            if (!Set.of(JDK, JDK | JAVA, CLASSPATH, JDK | CLASSPATH,
                    NATIVE, JDK | NATIVE, ACL_JNI | NATIVE).contains(roles) || length < 0
                    || length == 0 && roles != JDK) {
                throw new IllegalArgumentException("Invalid fixed program file role or length");
            }
        }
    }

    SegmentClassicLaunchProgram {
        if (!Set.of("linux", "macos").contains(os)
                || !Set.of("x86_64", "aarch64").contains(architecture)) {
            throw new IllegalArgumentException("Unsupported controlled program platform");
        }
        text(runtime, 256);
        text(vendor, 256);
        absolute(javaHome);
        if (!MAIN.equals(mainClass)) {
            throw new IllegalArgumentException("Only the fixed controlled Classic child entry is supported");
        }
        files = List.copyOf(files);
        classPath = List.copyOf(classPath);
        jvmArguments = List.copyOf(jvmArguments);
        if (files.isEmpty() || files.size() > MAX_FILES || classPath.isEmpty() || classPath.size() > 512
                || jvmArguments.size() > 128 || javaFile < 0 || javaFile >= files.size()
                || aclJniFile < 0 || aclJniFile >= files.size()) {
            throw new IllegalArgumentException("Controlled program exceeds fixed capacities");
        }
        if (files.get(javaFile).roles() != (JDK | JAVA)
                || !files.get(javaFile).path().equals(javaHome.resolve("bin/java"))
                || files.get(aclJniFile).roles() != (ACL_JNI | NATIVE)) {
            throw new IllegalArgumentException("Program executable/JNI references do not match their declared roles");
        }
        Set<Integer> jars = new HashSet<>();
        String previous = null;
        long total = 0;
        int executables = 0;
        int aclLibraries = 0;
        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            String path = file.path().toString();
            if (previous != null && previous.compareTo(path) >= 0
                    || ((file.roles() & JDK) != 0) != file.path().startsWith(javaHome)) {
                throw new IllegalArgumentException("Program files must be uniquely ordered and match the JDK root");
            }
            previous = path;
            total = Math.addExact(total, file.length());
            executables += (file.roles() & JAVA) == 0 ? 0 : 1;
            aclLibraries += (file.roles() & ACL_JNI) == 0 ? 0 : 1;
            if ((file.roles() & CLASSPATH) != 0) {
                if (!path.endsWith(".jar") || path.indexOf(':') >= 0) {
                    throw new IllegalArgumentException("Explicit ordinary JAR paths required");
                }
                jars.add(i);
            }
        }
        if (executables != 1 || aclLibraries != 1 || new HashSet<>(classPath).size() != classPath.size()
                || !jars.equals(new HashSet<>(classPath))) {
            throw new IllegalArgumentException("Program must name exactly its complete ordered classpath and JNI");
        }
        Set<Path> members = new HashSet<>(files.stream().map(File::path).toList());
        if (!members.containsAll(Set.of(javaHome.resolve("release"), javaHome.resolve("lib/modules"),
                javaHome.resolve(os.equals("macos") ? "lib/server/libjvm.dylib" : "lib/server/libjvm.so")))) {
            throw new IllegalArgumentException("The declared JDK must include its image, JVM and release identity");
        }
        validateJvmArguments(jvmArguments);
    }

    List<String> command(String recipeId) {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        List<String> result = new ArrayList<>();
        result.add(files.get(javaFile).path().toString());
        result.addAll(jvmArguments);
        result.add("-cp");
        result.add(String.join(":", classPath.stream().map(i -> files.get(i).path().toString()).toList()));
        result.add(mainClass);
        result.add(recipeId);
        return List.copyOf(result);
    }

    /** Version one launches with an empty environment. Parent must perform this before ProcessBuilder.start. */
    static Map<String, String> environment(Map<String, String> source) {
        for (String key : source.keySet()) {
            if (Set.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "CLASSPATH",
                    "BOOKKEEPER_NATIVE_IO_LIBRARY_PATH").contains(key)
                    || key.startsWith("LD_") || key.startsWith("DYLD_")) {
                throw new IllegalArgumentException("Unapproved controlled program environment: " + key);
            }
        }
        return Map.of();
    }

    static SegmentClassicHandoff.Digest identity(byte[] bytes) {
        return SegmentClassicHandoff.digest(DOMAIN, bytes);
    }

    static SegmentClassicLaunchProgram decode(byte[] encoded) throws IOException {
        if (encoded.length < 16 || encoded.length > MAX_BYTES) {
            throw new IOException("Controlled program length exceeds fixed bounds");
        }
        try {
            ByteBuffer b = ByteBuffer.wrap(encoded.clone());
            if (b.getLong() != MAGIC || b.getShort() != 1 || b.getShort() != 0
                    || b.getInt() != encoded.length - 16) {
                throw new IllegalArgumentException("Unknown controlled program framing");
            }
            String os = getText(b, 16);
            String arch = getText(b, 16);
            String runtime = getText(b, 256);
            String vendor = getText(b, 256);
            Path home = Path.of(getText(b, 4096));
            String main = getText(b, 256);
            int java = b.getInt();
            int jni = b.getInt();
            int jars = count(b, 512);
            List<Integer> classPath = new ArrayList<>(jars);
            for (int i = 0; i < jars; i++) {
                classPath.add(b.getInt());
            }
            int arguments = count(b, 128);
            List<String> args = new ArrayList<>(arguments);
            for (int i = 0; i < arguments; i++) {
                args.add(getText(b, 4096));
            }
            int count = count(b, MAX_FILES);
            List<File> files = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Path path = Path.of(getText(b, 4096));
                int roles = b.getInt();
                long length = b.getLong();
                byte[] digest = new byte[32];
                b.get(digest);
                files.add(new File(path, roles, length, new SegmentClassicHandoff.Digest(digest)));
            }
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("Trailing controlled program bytes");
            }
            return new SegmentClassicLaunchProgram(os, arch, runtime, vendor, home, main, java, jni,
                    classPath, args, files);
        } catch (IllegalArgumentException | ArithmeticException | BufferUnderflowException malformed) {
            throw new IOException("Invalid controlled program manifest", malformed);
        }
    }

    static void validateJvmArguments(List<String> args) {
        Set<String> seen = new HashSet<>();
        long minimumHeap = 0;
        long maximumHeap = 0;
        for (String arg : args) {
            text(arg, 4096);
            var memory = MEMORY.matcher(arg);
            String key;
            if (memory.matches()) {
                key = memory.group(1);
                long value = Long.parseLong(memory.group(2));
                String suffix = memory.group(3).toLowerCase(java.util.Locale.ROOT);
                int power = switch (suffix) {
                    case "k" -> 1;
                    case "m" -> 2;
                    case "g" -> 3;
                    default -> 0;
                };
                for (; power > 0; power--) {
                    value = Math.multiplyExact(value, 1024);
                }
                if (key.equals("-Xms")) {
                    minimumHeap = value;
                } else if (key.equals("-Xmx")) {
                    maximumHeap = value;
                }
            } else if (arg.startsWith("-XX:ActiveProcessorCount=")) {
                key = "-XX:ActiveProcessorCount=";
                String value = arg.substring(key.length());
                if (!value.matches("[1-9][0-9]{0,3}") || Integer.parseInt(value) > 4096) {
                    throw new IllegalArgumentException("Finite explicit controlled CPU count required");
                }
            } else if (arg.equals("-ea")) {
                key = arg;
            } else {
                throw new IllegalArgumentException("Unsupported controlled JVM option");
            }
            if (!seen.add(key)) {
                throw new IllegalArgumentException("Duplicate controlled JVM option");
            }
        }
        if (!seen.containsAll(Set.of("-Xms", "-Xmx", "-XX:MaxDirectMemorySize=", "-XX:ActiveProcessorCount="))
                || minimumHeap > maximumHeap) {
            throw new IllegalArgumentException("Explicit heap/direct-memory/CPU limits required");
        }
    }

    private static int count(ByteBuffer b, int maximum) {
        int value = b.getInt();
        if (value < 0 || value > maximum || value > b.remaining()) {
            throw new IllegalArgumentException("Invalid bounded controlled program count");
        }
        return value;
    }

    private static String getText(ByteBuffer b, int maximum) throws IOException {
        int length = Short.toUnsignedInt(b.getShort());
        if (length < 1 || length > maximum || length > b.remaining()) {
            throw new IllegalArgumentException("Invalid bounded controlled program text");
        }
        byte[] bytes = new byte[length];
        b.get(bytes);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }

    private static void absolute(Path path) {
        Objects.requireNonNull(path, "path");
        text(path.toString(), 4096);
        if (!path.isAbsolute() || !path.normalize().equals(path) || path.getParent() == null) {
            throw new IllegalArgumentException("Canonical absolute controlled program path required");
        }
    }

    private static void text(String value, int maximum) {
        if (value == null || value.isBlank() || value.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)
                || value.getBytes(StandardCharsets.UTF_8).length > maximum) {
            throw new IllegalArgumentException("Invalid controlled program text");
        }
    }
}
