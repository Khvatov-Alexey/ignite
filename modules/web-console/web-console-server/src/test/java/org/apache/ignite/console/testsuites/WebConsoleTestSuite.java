/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.testsuites;

import org.apache.ignite.console.configuration.WebConsoleConfigurationSelfTest;
import org.apache.ignite.console.listener.NotificationEventListenerTest;
import org.apache.ignite.console.db.TableSelfTest;
import org.apache.ignite.console.services.AccountServiceTest;
import org.apache.ignite.console.services.ActivitiesServiceTest;
import org.apache.ignite.console.services.AdminServiceTest;
import org.apache.ignite.console.web.security.PasswordEncoderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Ignite Web Console test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AccountServiceTest.class,
    PasswordEncoderTest.class,
    TableSelfTest.class,
    ActivitiesServiceTest.class,
    AdminServiceTest.class,
    NotificationEventListenerTest.class,
    WebConsoleConfigurationSelfTest.class
})
public class WebConsoleTestSuite {
}