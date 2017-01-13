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

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.BidiFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.junit.Assert;
import org.junit.Test;
import org.squbs.circuitbreaker.impl.AtomicCircuitBreakerState;
import org.squbs.streams.DelayActor;
import org.squbs.streams.FlowTimeoutException;
import org.squbs.streams.TimeoutBidiFlowOrdered;
import org.squbs.streams.TimeoutBidiFlowUnordered;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static akka.pattern.PatternsCS.ask;

public class CircuitBreakerBidiFlowTest {

    // Reading from configuration
    // Custom id retriever
    // failure decider
    // Fallback

    final ActorSystem system = ActorSystem.create("CircuitBreakerBidiFlowTest");
    final Materializer mat = ActorMaterializer.create(system);
    final FiniteDuration timeout = FiniteDuration.apply(60, TimeUnit.MILLISECONDS);
    final Try<String> timeoutFailure = Failure.apply(new FlowTimeoutException("Flow timed out!"));

    @Test
    public void testIncrementFailureCountOnCallTimeout() {
        final ActorRef delayActor = system.actorOf(Props.create(org.squbs.streams.DelayActor.class));
        final Flow<Pair<String, UUID>, Pair<String, UUID>, NotUsed> flow =
                Flow.<Pair<String, UUID>>create()
                        .mapAsyncUnordered(3, elem -> ask(delayActor, elem, 5000))
                        .map(elem -> (Pair<String, UUID>)elem);


        final CircuitBreakerState circuitBreakerState =
                AtomicCircuitBreakerState.create(
                        "JavaIncFailCount",
                        system.scheduler(),
                        2,
                        timeout,
                        FiniteDuration.apply(10, TimeUnit.MILLISECONDS),
                        system.dispatcher());


        final ActorRef ref = Flow.<String>create()
                .map(s -> Pair.create(s, UUID.randomUUID()))
                .via(CircuitBreakerBidiFlow.<String, String, UUID>create(circuitBreakerState).join(flow))
                .to(Sink.ignore())
                .runWith(Source.actorRef(25, OverflowStrategy.fail()), mat);


        new JavaTestKit(system) {{
            circuitBreakerState.subscribe(getRef(), Open.instance());
            ref.tell("a", ActorRef.noSender());
            ref.tell("b", ActorRef.noSender());
            ref.tell("b", ActorRef.noSender());
            expectMsgEquals(Open.instance());
        }};
    }

    @Test
    public void testWithCustomUniqueIdFunction() throws ExecutionException, InterruptedException {

        class MyContext {
            private String s;
            private long id;

            public MyContext(String s, long id) {
                this.s = s;
                this.id = id;
            }

            public long id() {
                return id;
            }

            @Override
            public boolean equals(Object obj) {
                if(obj instanceof MyContext) {
                    MyContext mc = (MyContext)obj;
                    return mc.s.equals(s) && mc.id == id;
                } else return false;
            }
        }

        class IdGen {
            private long counter = 0;
            public long next() {
                return ++ counter;
            }
        }

        final ActorRef delayActor = system.actorOf(Props.create(DelayActor.class));
        final Flow<Pair<String, MyContext>, Pair<String, MyContext>, NotUsed> flow =
                Flow.<Pair<String, MyContext>>create()
                        .mapAsyncUnordered(3, elem -> ask(delayActor, elem, 5000))
                        .map(elem -> (Pair<String, MyContext>)elem);

        final CircuitBreakerState circuitBreakerState =
                AtomicCircuitBreakerState.create(
                        "JavaUniqueId",
                        system.scheduler(),
                        2,
                        timeout,
                        FiniteDuration.apply(10, TimeUnit.MILLISECONDS),
                        system.dispatcher());

        final BidiFlow<Pair<String, MyContext>, Pair<String, MyContext>, Pair<String, MyContext>, Pair<Try<String>, MyContext>, NotUsed> circuitBreakerBidiFlow =
                CircuitBreakerBidiFlow.create(circuitBreakerState, Optional.empty(), Optional.empty(), MyContext::id);

        IdGen idGen = new IdGen();
        final CompletionStage<List<Pair<Try<String>, MyContext>>> result =
                Source.from(Arrays.asList("a", "b", "b", "a"))
                        .map(s -> new Pair<>(s, new MyContext("dummy", idGen.next())))
                        .via(circuitBreakerBidiFlow.join(flow))
                        .runWith(Sink.seq(), mat);
        final List<Pair<Try<String>, MyContext>> expected =
                Arrays.asList(
                        Pair.create(Success.apply("a"), new MyContext("dummy", 1)),
                        Pair.create(Success.apply("a"), new MyContext("dummy", 4)),
                        Pair.create(timeoutFailure, new MyContext("dummy", 2)),
                        Pair.create(timeoutFailure, new MyContext("dummy", 3))
                );
        Assert.assertTrue(result.toCompletableFuture().get().containsAll(expected));
    }
}
