/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http;

import java.util.function.Supplier;

class TestUtils {
    static void waitForCondition(final Supplier<Boolean> conditionChecker,
                                 final long maxWaitMs,
                                 final String condition) throws InterruptedException {
        final long startTime = System.currentTimeMillis();

        boolean testConditionMet;
        while (!(testConditionMet = conditionChecker.get()) && ((System.currentTimeMillis() - startTime) < maxWaitMs)) {
            Thread.sleep(Math.min(maxWaitMs, 100L));
        }

        if (!testConditionMet) {
            throw new AssertionError("Condition not met within timeout " + maxWaitMs + ": " + condition);
        }
    }
}
