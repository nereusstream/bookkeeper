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

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver.BookieIdNotResolvedException;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/** Pure protocol selection using the released resolver; registration I/O and publication are outside this probe. */
public final class StockResolverProbe {
    private StockResolverProbe() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: StockResolverProbe <version> <summary.json>");
        }
        var classic = new BookieServiceInfo.Endpoint("classic", 3181, "127.0.0.1", "bookie-rpc", List.of(), List.of());
        var profile = new BookieServiceInfo.Endpoint("profile", 4181, "127.0.0.1", "bookie-profile",
                List.of(), List.of());
        var info = new BookieServiceInfo();
        RegistrationClient metadata = (RegistrationClient) Proxy.newProxyInstance(
                RegistrationClient.class.getClassLoader(), new Class<?>[] {RegistrationClient.class},
                (proxy, method, parameters) -> {
                    if (!method.getName().equals("getBookieServiceInfo")) {
                        throw new AssertionError("Unexpected registration operation: " + method.getName());
                    }
                    return CompletableFuture.completedFuture(new Versioned<>(info, new LongVersion(0)));
                });
        var resolver = new DefaultBookieAddressResolver(metadata);
        BookieId opaque = BookieId.parse("isolated-stock-resolver");
        for (List<BookieServiceInfo.Endpoint> endpoints :
                List.of(List.of(profile, classic), List.of(classic, profile))) {
            info.setEndpoints(endpoints);
            var selected = resolver.resolve(opaque);
            if (!selected.getHostName().equals("127.0.0.1") || selected.getPort() != 3181) {
                throw new AssertionError("Classic resolver selected a Profile endpoint");
            }
        }
        info.setEndpoints(List.of(profile));
        for (BookieId id : List.of(opaque, BookieId.parse("127.0.0.1:3181"))) {
            try {
                resolver.resolve(id);
                throw new AssertionError("Known Profile-only service info silently resolved as Classic");
            } catch (BookieIdNotResolvedException expected) {
                if (!expected.getBookieId().equals(id)) {
                    throw new AssertionError("Resolver rejection lost Bookie identity");
                }
            }
        }
        Files.writeString(Path.of(arguments[1]), "{\"version\":\"" + arguments[0]
                + "\",\"layer\":\"released-resolver-protocol-selection\",\"status\":\"PASS\",\"cases\":4}"
                + "\n", StandardOpenOption.CREATE_NEW);
    }
}
