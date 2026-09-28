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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Test;

public class LedgerDescriptorImplTest {
    @Test
    public void rejectedMasterKeyDoesNotExposeEitherCredentialInTheLog() throws Exception {
        byte[] cached = {11, 22, 33, 44};
        byte[] requested = {55, 66, 77, 88};
        AtomicReference<String> rejectionLog = new AtomicReference<>();
        AbstractAppender capture = new AbstractAppender("master-key-rejection", null, null, true,
                Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getLoggerName().endsWith("LedgerDescriptorImpl")) {
                    LogEvent snapshot = event.toImmutable();
                    rejectionLog.set(snapshot.getMessage().getFormattedMessage() + " "
                            + snapshot.getContextData().toMap() + " " + snapshot);
                }
            }
        };
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        capture.start();
        configuration.addAppender(capture);
        configuration.getRootLogger().addAppender(capture, Level.ERROR, null);
        context.updateLoggers();
        try {
            LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(cached, 7, null);
            assertEquals(BookieException.Code.UnauthorizedAccessException,
                    assertThrows(BookieException.class, () -> descriptor.checkAccess(requested)).getCode());
            assertNotNull(rejectionLog.get());
            assertFalse(rejectionLog.get().contains(Arrays.toString(cached)));
            assertFalse(rejectionLog.get().contains(Arrays.toString(requested)));
        } finally {
            configuration.getRootLogger().removeAppender(capture.getName());
            capture.stop();
            context.updateLoggers();
        }
    }
}
