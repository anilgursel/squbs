/*
 * Copyright 2015 PayPal
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

package org.squbs.circuitbreaker;

import akka.util.Unsafe;

class AbstractCircuitBreaker {
    protected final static long stateOffset;
    protected final static long resetTimeoutOffset;

    static {
        try {
            stateOffset = Unsafe.instance.objectFieldOffset(CircuitBreakerLogic.class.getDeclaredField("_currentStateDoNotCallMeDirectly"));
            resetTimeoutOffset = Unsafe.instance.objectFieldOffset(CircuitBreakerLogic.class.getDeclaredField("_currentResetTimeoutDoNotCallMeDirectly"));
        } catch(Throwable t){
            throw new ExceptionInInitializerError(t);
        }
    }
}
