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

package org.squbs.circuitbreaker

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}
import org.squbs.circuitbreaker.impl.AtomicCircuitBreakerState
import org.squbs.streams.{FlowTimeoutException, TimeoutBidiFlowUnordered}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class CircuitBreakerBidiFlowSpec extends TestKit(ActorSystem("CircuitBreakerBidiFlowSpec"))
  with FlatSpecLike with Matchers with ImplicitSender with ScalaFutures {

  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val timeout = 60 milliseconds
  val timeoutFailure = Failure(FlowTimeoutException("Flow timed out!"))
  val circuitBreakerOpenFailure = Failure(CircuitBreakerOpenException("Circuit Breaker is open!"))

  def flow(circuitBreakerLogic: CircuitBreakerState) = {
    val delayActor = system.actorOf(Props[DelayActor])
    import akka.pattern.ask
    implicit val askTimeout = Timeout(5.seconds)
    val flow = Flow[(String, UUID)].mapAsyncUnordered(20) { elem =>
      (delayActor ? elem).mapTo[(String, UUID)]
    }

    val timeoutBidiFlow = TimeoutBidiFlowUnordered[String, String, UUID](timeout)


    val circuitBreakerBidiFlow = BidiFlow.fromGraph(new CircuitBreakerBidi[String, String, UUID, UUID]
    (circuitBreakerLogic))

    Flow[String]
      .map(s => (s, UUID.randomUUID()))
      .via(circuitBreakerBidiFlow.atop(timeoutBidiFlow).join(flow))
      .to(Sink.ignore)
      .runWith(Source.actorRef[String](25, OverflowStrategy.fail))
  }

  it should "increment failure count on call timeout" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)
    circuitBreakerLogic.subscribe(self, Open)
    val ref = flow(circuitBreakerLogic)
    ref ! "a"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
  }

  it should "reset failure count after success" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)
    circuitBreakerLogic.subscribe(self, TransitionEvents)
    val ref = flow(circuitBreakerLogic)
    ref ! "a"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
    expectMsg(HalfOpen)
    ref ! "a"
    expectMsg(Closed)
  }

  it should "increment failure count based on the provided function" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)
    circuitBreakerLogic.subscribe(self, TransitionEvents)

    def failureDecider(elem: (Try[String], UUID)): Boolean = elem match {
      case (Success("b"), _) => true
      case _ => false
    }
    val circuitBreakerBidiFlow = BidiFlow.fromGraph {
      new CircuitBreakerBidi[String, String, UUID, UUID](circuitBreakerLogic, hasFailed = failureDecider)
    }

    val flow = circuitBreakerBidiFlow.join(Flow[(String, UUID)].map { case (s, uuid) => (Success(s), uuid) })

    val ref = Flow[String]
      .map(s => (s, UUID.randomUUID())).via(flow)
      .to(Sink.ignore)
      .runWith(Source.actorRef[String](25, OverflowStrategy.fail))

    ref ! "a"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
    expectMsg(HalfOpen)
    ref ! "a"
    expectMsg(Closed)
  }

  it should "respond with fail-fast exception" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)

    val circuitBreakerBidiFlow = BidiFlow.fromGraph {
      new CircuitBreakerBidi[String, String, Long, Long](circuitBreakerLogic)
    }

    val flowFailure = Failure(new RuntimeException("Some dummy exception!"))
    val flow = Flow[(String, Long)].map {
      case ("b", uuid) => (flowFailure, uuid)
      case (elem, uuid) => (Success(elem), uuid)
    }

    var context = 0L
    val result = Source("a" :: "b" :: "b" :: "a" :: Nil)
      .map { s => context += 1; (s, context) }
      .via(circuitBreakerBidiFlow.join(flow))
      .runWith(Sink.seq)

    val failFastFailure = Failure(CircuitBreakerOpenException())
    val expected = (Success("a"), 1) :: (flowFailure, 2) :: (flowFailure, 3) :: (failFastFailure, 4) :: Nil
    whenReady(result) { r =>
      r should contain theSameElementsInOrderAs(expected)
    }
  }

  it should "respond with fallback" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)

    val circuitBreakerBidiFlow = BidiFlow.fromGraph {
      new CircuitBreakerBidi[String, String, Long, Long](circuitBreakerLogic,
                                                        Some((elem: (String, Long)) => (Success("c"), elem._2)))
    }

    val flowFailure = Failure(new RuntimeException("Some dummy exception!"))
    val flow = Flow[(String, Long)].map {
      case ("b", uuid) => (flowFailure, uuid)
      case (elem, uuid) => (Success(elem), uuid)
    }

    var context = 0L
    val result = Source("a" :: "b" :: "b" :: "a" :: Nil)
      .map { s => context += 1; (s, context) }
      .via(circuitBreakerBidiFlow.join(flow))
      .runWith(Sink.seq)

    val expected = (Success("a"), 1) :: (flowFailure, 2) :: (flowFailure, 3) :: (Success("c"), 4) :: Nil
    whenReady(result) { r =>
      r should contain theSameElementsInOrderAs(expected)
    }
  }

  it should "may messages" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerState(system.scheduler, 2, timeout, 10 milliseconds)
    circuitBreakerLogic.subscribe(self, TransitionEvents)
    val ref = flow(circuitBreakerLogic)
    ref ! "a"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
    ref ! "b"
    ref ! "b"
    expectMsg(HalfOpen)
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
    expectMsg(HalfOpen)
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    ref ! "a"
    expectMsg(Closed)
  }
}

class DelayActor extends Actor {

  val delay = Map("a" -> 30.milliseconds, "b" -> 200.milliseconds, "c" -> 30.milliseconds)

  def receive = {
    case element: String =>
      import context.dispatcher
      context.system.scheduler.scheduleOnce(delay(element), sender(), element)
    case element: (String, Long) =>
      import context.dispatcher
      context.system.scheduler.scheduleOnce(delay(element._1), sender(), element)
  }
}
