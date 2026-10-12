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

import com.sun.jdi.Bootstrap;
import com.sun.jdi.Method;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequest;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** External JDI observer for an isolated released Main, without replacing its classes or methods. */
public final class StockEffectObserver {
    private static final Map<String, Set<String>> METHODS = Map.ofEntries(
            Map.entry("org.apache.bookkeeper.proto.BookieRequestHandler", Set.of("channelActive", "channelInactive")),
            Map.entry("org.apache.bookkeeper.proto.BookieRequestProcessor", Set.of("processRequest")),
            Map.entry("org.apache.bookkeeper.bookie.Bookie", Set.of("addEntry", "recoveryAddEntry")),
            Map.entry("org.apache.bookkeeper.bookie.BookieImpl", Set.of("addEntry", "recoveryAddEntry")),
            Map.entry("org.apache.bookkeeper.bookie.HandleFactoryImpl", Set.of("getHandle", "getReadOnlyHandle")),
            Map.entry("org.apache.bookkeeper.bookie.LedgerDescriptorImpl", Set.of("<init>")),
            Map.entry("org.apache.bookkeeper.bookie.LedgerDescriptorReadOnlyImpl", Set.of("<init>")),
            Map.entry("org.apache.bookkeeper.bookie.InterleavedLedgerStorage", Set.of("setMasterKey", "addEntry")),
            Map.entry("org.apache.bookkeeper.bookie.LedgerCacheImpl", Set.of("setMasterKey")),
            Map.entry("org.apache.bookkeeper.bookie.FileInfo", Set.of("<init>")),
            Map.entry("org.apache.bookkeeper.bookie.Journal", Set.of("logAddEntry")));

    private StockEffectObserver() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: StockEffectObserver <owned-loopback-jdwp-port> <events.jsonl>");
        }
        AttachingConnector connector = Bootstrap.virtualMachineManager().attachingConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketAttach")).findFirst().orElseThrow();
        var options = connector.defaultArguments();
        options.get("hostname").setValue("127.0.0.1");
        options.get("port").setValue(arguments[0]);
        options.get("timeout").setValue("10000");
        VirtualMachine vm = connector.attach(options);
        Set<String> installed = new TreeSet<>();
        Map<String, Map<String, Long>> counts = new TreeMap<>();
        Map<String, Map<String, Long>> channels = new TreeMap<>();
        Set<Long> activeChannels = new TreeSet<>();
        String phase = "startup";
        counts.put(phase, new TreeMap<>());
        channels.put(phase, new TreeMap<>());
        try (BufferedWriter output = Files.newBufferedWriter(Path.of(arguments[1]), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW);
                BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            for (String name : METHODS.keySet()) {
                ClassPrepareRequest request = vm.eventRequestManager().createClassPrepareRequest();
                request.addClassFilter(name);
                request.setSuspendPolicy(EventRequest.SUSPEND_ALL);
                request.enable();
                for (ReferenceType type : vm.classesByName(name)) {
                    install(vm, type, installed);
                }
            }
            vm.resume();
            System.out.println("{\"attached\":true}");
            System.out.flush();
            while (true) {
                if (input.ready()) {
                    String command = input.readLine();
                    if (command == null) {
                        throw new IllegalStateException("Observer command input closed before finish");
                    }
                    if (!command.equals("snapshot") && !command.equals("finish")
                            && !command.matches("phase:[a-z][a-z0-9-]*")) {
                        throw new IllegalArgumentException("Invalid observer command");
                    }
                    vm.suspend();
                    try {
                        EventSet pending;
                        while ((pending = vm.eventQueue().remove(100)) != null) {
                            consume(vm, pending, installed, counts.get(phase), channels.get(phase),
                                    activeChannels, phase, output);
                        }
                        if (command.equals("finish") && !activeChannels.isEmpty()) {
                            throw new IllegalStateException("Cannot finish while stock channels remain active");
                        }
                        if (command.startsWith("phase:")) {
                            if (!activeChannels.isEmpty()) {
                                throw new IllegalStateException(
                                        "Cannot change phase while stock channels remain active");
                            }
                            phase = command.substring(6);
                            if (counts.putIfAbsent(phase, new TreeMap<>()) != null) {
                                throw new IllegalArgumentException("A phase cannot be reused");
                            }
                            channels.put(phase, new TreeMap<>());
                        }
                        output.flush();
                        System.out.println(receipt(phase, installed, counts, channels, activeChannels));
                        System.out.flush();
                    } finally {
                        vm.resume();
                    }
                    if (command.equals("finish")) {
                        break;
                    }
                }
                EventSet events = vm.eventQueue().remove(100);
                if (events != null) {
                    consume(vm, events, installed, counts.get(phase), channels.get(phase),
                            activeChannels, phase, output);
                }
            }
        } finally {
            vm.dispose();
        }
    }

    private static void install(VirtualMachine vm, ReferenceType type, Set<String> installed) {
        for (Method method : type.methods()) {
            if (!METHODS.get(type.name()).contains(method.name()) || method.isAbstract() || method.isNative()) {
                continue;
            }
            String key = type.name() + "#" + method.name() + method.signature();
            if (installed.add(key)) {
                var request = vm.eventRequestManager().createBreakpointRequest(method.location());
                request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
                request.enable();
            }
        }
    }

    private static void consume(VirtualMachine vm, EventSet events, Set<String> installed,
            Map<String, Long> counts, Map<String, Long> channels, Set<Long> activeChannels,
            String phase, BufferedWriter output) throws Exception {
        try {
            for (Event event : events) {
                if (event instanceof ClassPrepareEvent prepared) {
                    install(vm, prepared.referenceType(), installed);
                } else if (event instanceof BreakpointEvent breakpoint) {
                    Method method = breakpoint.location().method();
                    String key = method.declaringType().name() + "#" + method.name() + method.signature();
                    String channel = "";
                    if (method.declaringType().name().equals("org.apache.bookkeeper.proto.BookieRequestHandler")) {
                        ObjectReference context = (ObjectReference) breakpoint.thread().frame(0)
                                .getArgumentValues().get(0);
                        long id = context.uniqueID();
                        boolean changed = method.name().equals("channelActive")
                                ? activeChannels.add(id) : activeChannels.remove(id);
                        if (!changed) {
                            throw new IllegalStateException("Unmatched stock channel lifecycle event");
                        }
                        channels.merge(method.name(), 1L, Long::sum);
                        channel = ",\"channelContextId\":" + id;
                    } else {
                        counts.merge(key, 1L, Long::sum);
                    }
                    output.write("{\"phase\":\"" + phase + "\",\"method\":\"" + key
                            + "\",\"threadId\":" + breakpoint.thread().uniqueID() + channel + "}\n");
                } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                    throw new IllegalStateException("Released Main terminated during observation");
                }
            }
        } finally {
            events.resume();
        }
    }

    private static String receipt(String phase, Set<String> installed, Map<String, Map<String, Long>> counts,
            Map<String, Map<String, Long>> channels, Set<Long> activeChannels) {
        StringBuilder result = new StringBuilder("{\"phase\":\"").append(phase)
                .append("\",\"installed\":[");
        String separator = "";
        for (String method : installed) {
            result.append(separator).append('"').append(method).append('"');
            separator = ",";
        }
        result.append("],\"activeChannels\":").append(activeChannels.toString()).append(',');
        appendCounts(result, "counts", counts);
        result.append(',');
        appendCounts(result, "channels", channels);
        return result.append('}').toString();
    }

    private static void appendCounts(StringBuilder result, String name, Map<String, Map<String, Long>> counts) {
        result.append('"').append(name).append("\":{");
        String separator = "";
        for (var entry : counts.entrySet()) {
            result.append(separator).append('"').append(entry.getKey()).append("\":{");
            String methodSeparator = "";
            for (var count : entry.getValue().entrySet()) {
                result.append(methodSeparator).append('"').append(count.getKey()).append("\":")
                        .append(count.getValue());
                methodSeparator = ",";
            }
            result.append('}');
            separator = ",";
        }
        result.append('}');
    }
}
