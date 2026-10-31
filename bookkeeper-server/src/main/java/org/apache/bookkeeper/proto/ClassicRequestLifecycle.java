/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

/** Concrete ownership for one controlled Classic processor; never a startup or handoff permission. */
final class ClassicRequestLifecycle {
    private final BookieServer server;
    private final Object gate = new Object();
    private final Set<Call> calls = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<Output> outputs = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<Channel> connections = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<Thread, Integer> activities = new HashMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private final ThreadLocal<Call> dispatch = new ThreadLocal<>();
    private volatile boolean closing;
    private volatile Throwable operationFailure;
    private volatile Throwable closeFailure;
    private String acquiring;
    private int acquiredThreadCount;

    ClassicRequestLifecycle(BookieServer server) {
        this.server = server;
    }

    final class Call {
        private Object input;
        private Object packet;
        private int bodies = 1;
        private boolean terminal;
        private boolean requestFinished;
        private boolean uncertain;
        private final Channel channel;
        private final Call previous;

        private Call(Object input, Channel channel) {
            this.previous = dispatch.get();
            this.input = input;
            this.channel = channel;
        }

        void bind(Object candidate) {
            synchronized (gate) {
                if (packet != null && packet != candidate) {
                    failUnknown(new IOException("Controlled request has more than one packet consumer"));
                    uncertain = true;
                } else {
                    packet = candidate;
                }
            }
        }

        boolean discardRejected() {
            Object original;
            synchronized (gate) {
                if (uncertain || bodies != 1 || packet == null || closeFailure != null) {
                    return false;
                }
                original = packet;
            }
            if (original instanceof PacketProcessorBase<?> legacy) {
                legacy.discardForClassicChild();
            } else if (original instanceof PacketProcessorBaseV3 current) {
                current.discardForClassicChild();
            } else {
                return false;
            }
            channel.close();
            return true;
        }

        void complete() {
            synchronized (gate) {
                terminal = true;
                retire();
            }
        }

        boolean finishRequestOnce() {
            synchronized (gate) {
                if (requestFinished) {
                    return false;
                }
                requestFinished = true;
                return true;
            }
        }

        private void retire() {
            if (terminal && bodies == 0 && !uncertain) {
                calls.remove(this);
                packet = null;
                input = null;
            }
            gate.notifyAll();
        }

        void runCallback(Runnable body) {
            Task task = new Task(this, body, false);
            task.run();
        }

        void failUnknown(Throwable problem) {
            synchronized (gate) {
                uncertain = true;
            }
            ClassicRequestLifecycle.this.failUnknown(problem);
        }

        java.util.concurrent.Future<?> submitCallback(java.util.concurrent.ExecutorService executor, Runnable body) {
            Task task = new Task(this, body, false);
            try {
                return executor.submit(task);
            } catch (RejectedExecutionException rejected) {
                task.rejected();
                failUnknown(rejected);
                throw rejected;
            } catch (RuntimeException | Error unknown) {
                failUnknown(unknown);
                throw unknown;
            }
        }

        Executor callbackExecutor(Executor original) {
            return body -> {
                Task task = new Task(this, body, false);
                try {
                    original.execute(task);
                } catch (RejectedExecutionException rejected) {
                    task.rejected();
                    synchronized (gate) {
                        uncertain = true;
                    }
                    failUnknown(rejected);
                    throw rejected;
                } catch (RuntimeException | Error unknown) {
                    failUnknown(unknown);
                    throw unknown;
                }
            };
        }
    }

    final class Task implements Runnable {
        private final Call call;
        private final Runnable body;
        private final boolean initial;
        private boolean started;
        private boolean returned;

        private Task(Call call, Runnable body, boolean initial) {
            this.call = call;
            this.body = body;
            this.initial = initial;
            synchronized (gate) {
                call.bodies++;
            }
        }

        void rejected() {
            synchronized (gate) {
                if (!started && !returned) {
                    returned = true;
                    call.bodies--;
                    call.retire();
                }
            }
        }

        @Override
        public void run() {
            synchronized (gate) {
                if (started || returned) {
                    failUnknown(new IOException("Controlled task entered twice or after definite rejection"));
                    return;
                }
                started = true;
                enter();
            }
            try {
                if (initial && closing) {
                    if (body instanceof PacketProcessorBase<?> packet) {
                        packet.discardForClassicChild();
                    } else if (body instanceof PacketProcessorBaseV3 packet) {
                        packet.discardForClassicChild();
                    } else {
                        throw new IllegalStateException("Controlled task has no owned packet discard path");
                    }
                } else {
                    body.run();
                }
            } catch (RuntimeException | Error problem) {
                synchronized (gate) {
                    call.uncertain = true;
                }
                failUnknown(problem);
                throw problem;
            } finally {
                synchronized (gate) {
                    returned = true;
                    leave();
                    call.bodies--;
                    call.retire();
                }
            }
        }
    }

    Call begin(Object input, Channel channel) {
        synchronized (gate) {
            if (closing) {
                return null;
            }
            Call call = new Call(input, channel);
            calls.add(call);
            enter();
            dispatch.set(call);
            return call;
        }
    }

    Call current() {
        return dispatch.get();
    }

    void end(Call call, Throwable failure) {
        if (call.previous == null) {
            dispatch.remove();
        } else {
            dispatch.set(call.previous);
        }
        synchronized (gate) {
            if (failure != null) {
                call.uncertain = true;
                failUnknown(failure);
            }
            call.bodies--;
            leave();
            call.retire();
        }
    }

    Task task(Runnable body) {
        Call call;
        if (body instanceof PacketProcessorBase<?> packet) {
            call = packet.controlledCall;
        } else if (body instanceof PacketProcessorBaseV3 packet) {
            call = packet.controlledCall;
        } else {
            throw new IllegalArgumentException("Controlled queue requires its original packet consumer");
        }
        if (call == null) {
            throw new IllegalStateException("Controlled packet was constructed outside its dispatch");
        }
        call.bind(body);
        return new Task(call, body, true);
    }

    void requestClose() {
        synchronized (gate) {
            closing = true;
            gate.notifyAll();
        }
    }

    void retireAdmission() {
        List<Call> accepted;
        synchronized (gate) {
            closing = true;
            accepted = List.copyOf(calls);
        }
        for (Call call : accepted) {
            Object packet;
            synchronized (gate) {
                packet = call.packet;
            }
            if (packet instanceof LongPollReadEntryProcessorV3 poll) {
                poll.retireForClassicChild();
            }
        }
    }

    boolean awaitRequests() throws InterruptedException {
        synchronized (gate) {
            if (activities.containsKey(Thread.currentThread())) {
                throw new IllegalStateException("Controlled request activity cannot await itself");
            }
            while (!calls.isEmpty()) {
                if (closeFailure != null) {
                    return false;
                }
                gate.wait();
            }
            return true;
        }
    }

    boolean awaitOutputs() throws InterruptedException {
        synchronized (gate) {
            if (activities.containsKey(Thread.currentThread())) {
                throw new IllegalStateException("Controlled output activity cannot await itself");
            }
            while (!outputs.isEmpty() || !connections.isEmpty()) {
                if (closeFailure != null) {
                    return false;
                }
                gate.wait();
            }
            return true;
        }
    }

    boolean isClosing() {
        return closing;
    }

    ThreadFactory trackThreads(ThreadFactory factory) {
        return body -> {
            Thread thread = factory.newThread(body);
            synchronized (gate) {
                threads.add(thread);
            }
            return thread;
        };
    }

    void acquiring(String stage) {
        synchronized (gate) {
            acquiring = stage;
            acquiredThreadCount = threads.size();
        }
    }

    void acquired() {
        synchronized (gate) {
            acquiring = null;
        }
    }

    boolean joinThreads() throws InterruptedException {
        List<Thread> original;
        boolean known;
        synchronized (gate) {
            known = acquiring == null;
            original = List.copyOf(known ? threads : threads.subList(0, acquiredThreadCount));
        }
        for (Thread thread : original) {
            if (thread == Thread.currentThread()) {
                throw new IllegalStateException("Controlled executor cannot join itself");
            }
            thread.join();
        }
        return known && closeFailure == null;
    }

    boolean isCurrentActivity() {
        synchronized (gate) {
            return activities.containsKey(Thread.currentThread()) || threads.contains(Thread.currentThread());
        }
    }

    private void enter() {
        activities.merge(Thread.currentThread(), 1, Integer::sum);
    }

    private void leave() {
        Thread thread = Thread.currentThread();
        int count = activities.get(thread) - 1;
        if (count == 0) {
            activities.remove(thread);
        } else {
            activities.put(thread, count);
        }
        gate.notifyAll();
    }

    void failUnknown(Throwable problem) {
        synchronized (gate) {
            if (closeFailure == null) {
                closeFailure = problem;
            }
            if (operationFailure == null) {
                operationFailure = problem;
            }
            closing = true;
            gate.notifyAll();
        }
        server.requestCloseForClassicChild();
    }

    Throwable operationFailure() {
        return operationFailure;
    }

    Throwable closeFailure() {
        return closeFailure;
    }

    private final class Output {
        final Object message;
        boolean retained;

        Output(Object message) {
            this.message = message;
        }
    }

    ChannelFuture writeAndFlush(Channel channel, Object message) {
        return writeAndFlush(channel, message, channel.newPromise());
    }

    ChannelFuture writeAndFlush(Channel channel, Object message, ChannelPromise caller) {
        Output queued = new Output(message);
        synchronized (gate) {
            outputs.add(queued);
        }
        try {
            ChannelPromise actual = channel.newPromise();
            actual.setUncancellable();
            actual.addListener(done -> {
                try {
                    // A failed Netty submission can notify its promise before releasing the message.
                    // Retire on the original loop after that write task and its cleanup have returned.
                    channel.eventLoop().execute(() -> {
                        synchronized (gate) {
                            enter();
                        }
                        try {
                            if (done.isSuccess()) {
                                caller.trySuccess();
                            } else {
                                if (!(done.cause() instanceof IOException)) {
                                    failUnknown(done.cause());
                                }
                                caller.tryFailure(done.cause());
                            }
                        } finally {
                            synchronized (gate) {
                                if (done.isSuccess() || done.cause() instanceof IOException) {
                                    outputs.remove(queued);
                                }
                                leave();
                            }
                        }
                    });
                } catch (RuntimeException | Error unknown) {
                    failUnknown(unknown);
                    throw unknown;
                }
            });
            channel.writeAndFlush(message, actual);
            return caller;
        } catch (RuntimeException | Error unknown) {
            // Keep the original queued output; a thrown submission is not a physical completion.
            failUnknown(unknown);
            throw unknown;
        }
    }

    ChannelDuplexHandler outputHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext context) {
                synchronized (gate) {
                    connections.add(context.channel());
                }
            }

            @Override
            public void handlerRemoved(ChannelHandlerContext context) {
                synchronized (gate) {
                    connections.remove(context.channel());
                    gate.notifyAll();
                }
            }

            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise caller) {
                Output output = new Output(message);
                synchronized (gate) {
                    outputs.add(output);
                }
                try {
                    if (!(message instanceof ReferenceCounted reference)) {
                        throw new IllegalStateException("Controlled encoded output has no owned reference");
                    }
                    reference.retain();
                    output.retained = true;
                    ChannelPromise actual = context.newPromise();
                    actual.setUncancellable();
                    actual.addListener(done -> {
                        synchronized (gate) {
                            enter();
                        }
                        try {
                            if (done.isSuccess()) {
                                caller.trySuccess();
                            } else {
                                caller.tryFailure(done.cause());
                            }
                        } finally {
                            try {
                                reference.release();
                                output.retained = false;
                            } catch (RuntimeException | Error releaseFailure) {
                                failUnknown(releaseFailure);
                            } finally {
                                synchronized (gate) {
                                    if (!output.retained) {
                                        outputs.remove(output);
                                    }
                                    leave();
                                }
                            }
                        }
                    });
                    context.write(message, actual);
                } catch (RuntimeException | Error unknown) {
                    // No fabricated promise result: an accepted native write may still complete later.
                    failUnknown(unknown);
                    throw unknown;
                }
            }
        };
    }
}
