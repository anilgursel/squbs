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

package org.squbs.streams;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.BidiFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import static akka.pattern.PatternsCS.ask;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TimeoutBidiFlowTest {

    final ActorSystem system = ActorSystem.create("TimeoutBidiFlowTest");
    final Materializer mat = ActorMaterializer.create(system);
    final FiniteDuration timeout = FiniteDuration.apply(60, TimeUnit.MILLISECONDS);
    final Try<String> timeoutFailure = Failure.apply(new FlowTimeoutException("Flow timed out!"));

    @Test
    public void testFlowsWithMessageOrderGuarantee() throws ExecutionException, InterruptedException {
        final ActorRef delayActor = system.actorOf(Props.create(DelayActor.class));
        final Flow<String, String, NotUsed> flow =
                Flow.<String>create()
                        .mapAsync(4, elem -> ask(delayActor, elem, 5000))
                        .map(elem -> (String)elem);


        final BidiFlow<String, String, String, Try<String>, NotUsed> timeoutBidiFlow =
                TimeoutBidiFlowOrdered.create(timeout);

        final CompletionStage<List<Try<String>>> result =
                Source.from(Arrays.asList("a", "b", "c"))
                        .via(timeoutBidiFlow.join(flow))
                        .runWith(Sink.seq(), mat);
        // "c" fails because of slowness of "b"
        final List<Try<String>> expected = Arrays.asList(Success.apply("a"), timeoutFailure, timeoutFailure);
        Assert.assertEquals(expected, result.toCompletableFuture().get());
    }

    @Test
    public void testFlowsWithoutMessageOrderGuarantee() throws ExecutionException, InterruptedException {
        final ActorRef delayActor = system.actorOf(Props.create(DelayActor.class));
        final Flow<Tuple2<String, Long>, Tuple2<String, Long>, NotUsed> flow =
                Flow.<Tuple2<String, Long>>create()
                        .mapAsyncUnordered(3, elem -> ask(delayActor, elem, 5000))
                        .map(elem -> (Tuple2<String, Long>)elem);

        final BidiFlow<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<Try<String>, Long>, NotUsed> timeoutBidiFlow =
                TimeoutBidiFlowUnordered.create(timeout);

        class Id {
            long id = 0;
            long next() {
                return id++;
            }
        }

        Id id = new Id();
        final CompletionStage<List<Try<String>>> result =
                Source.from(Arrays.asList("a", "b", "c"))
                        .map(s -> new Tuple2<>(s, id.next()))
                        .via(timeoutBidiFlow.join(flow))
                        .map(t -> t._1())
                        .runWith(Sink.seq(), mat);
        final List<Try<String>> expected = Arrays.asList(Success.apply("a"), Success.apply("c"), timeoutFailure);
        Assert.assertTrue(result.toCompletableFuture().get().containsAll(expected));
    }

    @Test
    public void testWithCustomIdGenerator() throws ExecutionException, InterruptedException {

        class DummyContext {
            public DummyContext(String s, UUID uuid) {
                this.s = s;
                this.uuid = uuid;
            }
            String s;
            UUID uuid;
        }

        final ActorRef delayActor = system.actorOf(Props.create(DelayActor.class));
        final Flow<Tuple2<String, DummyContext>, Tuple2<String, DummyContext>, NotUsed> flow =
                Flow.<Tuple2<String, DummyContext>>create()
                        .mapAsyncUnordered(3, elem -> ask(delayActor, elem, 5000))
                        .map(elem -> (Tuple2<String, DummyContext>)elem);

        final BidiFlow<Tuple2<String, DummyContext>, Tuple2<String, DummyContext>, Tuple2<String, DummyContext>, Tuple2<Try<String>, DummyContext>, NotUsed> timeoutBidiFlow =
                TimeoutBidiFlowUnordered.create(timeout, (DummyContext dummyContext) -> dummyContext.uuid);

        final CompletionStage<List<Try<String>>> result =
                Source.from(Arrays.asList("a", "b", "c"))
                        .map(s -> new Tuple2<>(s, new DummyContext("dummy", UUID.randomUUID())))
                        .via(timeoutBidiFlow.join(flow))
                        .map(t -> t._1())
                        .runWith(Sink.seq(), mat);
        final List<Try<String>> expected = Arrays.asList(Success.apply("a"), Success.apply("c"), timeoutFailure);
        Assert.assertTrue(result.toCompletableFuture().get().containsAll(expected));
    }
}
