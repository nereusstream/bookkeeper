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
package org.apache.bookkeeper.meta.zk;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;

/**
 * ZooKeeper based metadata bookie driver.
 */
public class ZKMetadataBookieDriver
    extends ZKMetadataDriverBase
    implements MetadataBookieDriver {

    // register myself
    static {
        MetadataDrivers.registerBookieDriver(
            SCHEME,
            ZKMetadataBookieDriver.class);
    }

    ServerConfiguration serverConf;
    private boolean controlledClassic;
    private boolean controlledRegistrationAttempted;
    private boolean controlledCloseAttempted;
    private Throwable controlledCloseFailure;
    private Map<String, String> controlledSelectors;
    private LedgerLayout controlledLayout;
    private static final String FACTORY_PREFIX = "org.apache.bookkeeper.meta.";
    private static final Set<String> CONTROLLED_FACTORIES = Set.of(FACTORY_PREFIX + "FlatLedgerManagerFactory",
            FACTORY_PREFIX + "HierarchicalLedgerManagerFactory",
            FACTORY_PREFIX + "LegacyHierarchicalLedgerManagerFactory",
            FACTORY_PREFIX + "LongHierarchicalLedgerManagerFactory");

    /** Borrow the outer child's only raw session. Heap assembly only: no connect, layout write or format. */
    public final synchronized void initializeForClassicChild(ServerConfiguration conf, StatsLogger stats,
            ZooKeeper original) throws IOException {
        if (controlledClassic || this.conf != null || zk != null || lmFactory != null || original == null
                || original.getClass() != ZooKeeper.class) {
            throw new IllegalStateException("Controlled metadata driver requires one original raw session");
        }
        controlledClassic = true;
        controlledSelectors = classicSelectors(conf);
        URI uri = URI.create(controlledSelectors.get("metadataServiceUri"));
        String root = uri.getPath();
        PathUtils.validatePath(root);
        getZKServersFromServiceUri(uri);
        this.conf = conf;
        this.serverConf = conf;
        this.statsLogger = stats;
        this.acls = ZkUtils.getACLs(conf);
        this.ledgersRootPath = root;
        this.zk = original;
        this.ownZKHandle = false;
        this.metadataServiceAvailable = original.getState().isAlive();
        this.disableHealthCheckPath = root + "/" + BookKeeperConstants.DISABLE_HEALTH_CHECK;
        this.layoutManager = new ZkLayoutManager(original, root, acls) {
            @Override
            public LedgerLayout readLedgerLayout() throws IOException {
                LedgerLayout actual = super.readLedgerLayout();
                requireClassicLayout(actual);
                return actual;
            }

            @Override
            public void storeLedgerLayout(LedgerLayout layout) throws IOException {
                throw new IOException("Controlled historical Classic cannot create or replace ledger layout");
            }

            @Override
            public void deleteLedgerLayout() throws IOException {
                throw new IOException("Controlled historical Classic cannot delete ledger layout");
            }
        };
    }

    @Override
    public synchronized MetadataBookieDriver initialize(ServerConfiguration conf,
                                                        StatsLogger statsLogger)
            throws MetadataException {
        if (controlledClassic) {
            throw new IllegalStateException("Controlled metadata driver cannot replace its original raw session");
        }
        super.initialize(
            conf,
            statsLogger.scope(BOOKIE_SCOPE),
            new BoundExponentialBackoffRetryPolicy(conf.getZkRetryBackoffStartMs(),
                        conf.getZkRetryBackoffMaxMs(), conf.getZkRetryBackoffMaxRetries()),
            Optional.empty());
        this.serverConf = conf;
        this.statsLogger = statsLogger;
        return this;
    }

    @Override
    public synchronized RegistrationManager createRegistrationManager() {
        if (controlledClassic) {
            if (controlledRegistrationAttempted || controlledCloseAttempted || this.conf == null) {
                throw new IllegalStateException("Controlled metadata driver creates only one registration manager");
            }
            controlledRegistrationAttempted = true;
        }
        ZKRegistrationManager zkRegistrationManager = newZKRegistrationManager(serverConf, zk);
        return zkRegistrationManager;
    }

    @Override
    public synchronized LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        if (!controlledClassic) {
            return super.getLedgerManagerFactory();
        }
        if (controlledCloseAttempted || controlledSelectors == null || this.conf == null) {
            throw new IllegalStateException("Controlled metadata driver cannot reopen after close");
        }
        try {
            if (!controlledSelectors.equals(classicSelectors(serverConf))) {
                throw new IOException("Original controlled metadata selectors changed");
            }
            if (lmFactory == null) {
                LedgerLayout layout = layoutManager.readLedgerLayout();
                String selected = requireClassicLayout(layout);
                // Assign the exact known heap holder before initialize; no reflection or shaded fallback.
                lmFactory = switch (selected) {
                    case FACTORY_PREFIX + "FlatLedgerManagerFactory" -> new FlatLedgerManagerFactory();
                    case FACTORY_PREFIX + "HierarchicalLedgerManagerFactory" -> new HierarchicalLedgerManagerFactory();
                    case FACTORY_PREFIX + "LegacyHierarchicalLedgerManagerFactory" ->
                            new LegacyHierarchicalLedgerManagerFactory();
                    case FACTORY_PREFIX + "LongHierarchicalLedgerManagerFactory" ->
                            new LongHierarchicalLedgerManagerFactory();
                    default -> throw new IOException("Unsupported controlled metadata factory");
                };
                lmFactory.initialize(conf, layoutManager, layout.getManagerVersion());
            }
            return lmFactory;
        } catch (IOException problem) {
            throw new MetadataException(Code.METADATA_SERVICE_ERROR,
                    "Original controlled ledger layout is unavailable or unsupported", problem);
        }
    }

    private static Map<String, String> classicSelectors(ServerConfiguration conf) throws IOException {
        if (conf.containsKey(AbstractConfiguration.LEDGER_MANAGER_FACTORY_DISABLE_CLASS_CHECK)) {
            throw new IOException("Controlled metadata forbids even a false class-check bypass property");
        }
        Map<String, String> selectors = new HashMap<>();
        for (String key : Set.of("metadataServiceUri", "ledgerManagerFactoryClass", "ledgerManagerType")) {
            if (conf.containsKey(key)) {
                Object raw = conf.getProperty(key);
                if (!(raw instanceof String value) || value.isEmpty()) {
                    throw new IOException("Controlled metadata requires one literal selector: " + key);
                }
                selectors.put(key, value);
            }
        }
        if (!selectors.containsKey("metadataServiceUri")) {
            throw new IOException("Controlled metadata requires its explicit effective URI");
        }
        String configured = selectors.get("ledgerManagerFactoryClass");
        if (configured != null && !CONTROLLED_FACTORIES.contains(configured)) {
            throw new IOException("Unknown/custom controlled factory rejected before class loading");
        }
        String uriFactory = uriFactory(selectors.get("metadataServiceUri"));
        String typeFactory = selectors.containsKey("ledgerManagerType")
                ? aliasFactory(selectors.get("ledgerManagerType")) : null;
        if (configured != null && uriFactory != null && !configured.equals(uriFactory)
                || configured != null && typeFactory != null && !configured.equals(typeFactory)
                || uriFactory != null && typeFactory != null && !uriFactory.equals(typeFactory)) {
            throw new IOException("Explicit controlled metadata selectors disagree");
        }
        return Map.copyOf(selectors);
    }

    private static String uriFactory(String encoded) throws IOException {
        try {
            String scheme = URI.create(encoded).getScheme();
            if (scheme == null) {
                throw new IOException("Missing controlled metadata scheme");
            }
            return switch (scheme.toLowerCase(Locale.ROOT)) {
                case "zk", "zk+null" -> null;
                case "zk+flat" -> FACTORY_PREFIX + "FlatLedgerManagerFactory";
                case "zk+hierarchical" -> FACTORY_PREFIX + "HierarchicalLedgerManagerFactory";
                case "zk+longhierarchical" -> FACTORY_PREFIX + "LongHierarchicalLedgerManagerFactory";
                default -> throw new IOException("Unsupported controlled metadata URI scheme");
            };
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid controlled metadata URI", invalid);
        }
    }

    private static String aliasFactory(String alias) throws IOException {
        return switch (alias) {
            case "flat" -> FACTORY_PREFIX + "FlatLedgerManagerFactory";
            case "hierarchical" -> FACTORY_PREFIX + "HierarchicalLedgerManagerFactory";
            case "longhierarchical" -> FACTORY_PREFIX + "LongHierarchicalLedgerManagerFactory";
            default -> throw new IOException("Unsupported controlled metadata type selector");
        };
    }

    private synchronized String requireClassicLayout(LedgerLayout layout) throws IOException {
        if (!controlledSelectors.equals(classicSelectors(serverConf)) || layout == null
                || layout.getManagerVersion() != 1) {
            throw new IOException("Controlled metadata requires one unchanged existing supported layout");
        }
        String selected;
        if (layout.getLayoutFormatVersion() == 1) {
            if (!Set.of("flat", "hierarchical").contains(layout.getManagerFactoryClass())) {
                throw new IOException("Unknown v1 controlled ledger layout alias");
            }
            selected = aliasFactory(layout.getManagerFactoryClass());
        } else if (layout.getLayoutFormatVersion() == 2
                && CONTROLLED_FACTORIES.contains(layout.getManagerFactoryClass())) {
            selected = layout.getManagerFactoryClass();
        } else {
            throw new IOException("Unknown/custom controlled layout rejected before class loading");
        }
        for (String expected : new String[]{uriFactory(controlledSelectors.get("metadataServiceUri")),
                controlledSelectors.get("ledgerManagerFactoryClass"),
                controlledSelectors.containsKey("ledgerManagerType")
                        ? aliasFactory(controlledSelectors.get("ledgerManagerType")) : null}) {
            if (expected != null && !selected.equals(expected)) {
                throw new IOException("Controlled metadata selector differs from the existing layout");
            }
        }
        if (controlledLayout != null && !controlledLayout.equals(layout)) {
            throw new IOException("Original controlled layout changed between observation and construction");
        }
        controlledLayout = layout;
        return selected;
    }

    /** Strict first factory close; the outer owner separately drains managers and its borrowed raw session. */
    public final synchronized void closeForClassicChild() throws IOException {
        if (!controlledClassic) {
            throw new IllegalStateException("Only the original controlled driver uses this close");
        }
        if (controlledCloseAttempted) {
            if (controlledCloseFailure instanceof IOException original) {
                throw original;
            } else if (controlledCloseFailure instanceof RuntimeException original) {
                throw original;
            } else if (controlledCloseFailure instanceof Error original) {
                throw original;
            }
            return;
        }
        controlledCloseAttempted = true;
        try {
            if (lmFactory != null) {
                lmFactory.close();
                lmFactory = null;
            }
        } catch (IOException | RuntimeException | Error problem) {
            controlledCloseFailure = problem;
            throw problem;
        }
    }

    @VisibleForTesting
    ZKRegistrationManager newZKRegistrationManager(ServerConfiguration serverConf, ZooKeeper zk) {
        return new ZKRegistrationManager(serverConf, zk);
    }

    @Override
    public void close() {
        if (controlledClassic) {
            try {
                closeForClassicChild();
            } catch (IOException problem) {
                throw new UncheckedIOException(problem);
            }
            return;
        }
        super.close();
    }
}
