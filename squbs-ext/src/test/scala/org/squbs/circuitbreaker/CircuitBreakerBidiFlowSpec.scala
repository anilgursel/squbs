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

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.{FlatSpecLike, Matchers}
import org.squbs.circuitbreaker.impl.AtomicCircuitBreakerLogic
import org.squbs.streams.{FlowTimeoutException, TimeoutBidiFlowUnordered}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Failure

class CircuitBreakerBidiFlowSpec extends TestKit(ActorSystem("CircuitBreakerBidiFlowSpec"))
  with FlatSpecLike with Matchers with ImplicitSender {

  implicit val materializer = ActorMaterializer()


  val timeout = 60 milliseconds
  val timeoutFailure = Failure(FlowTimeoutException("Flow timed out!"))
  val circuitBreakerOpenFailure = Failure(CircuitBreakerOpenException("Circuit Breaker is open!"))

  def flow(circuitBreakerLogic: CircuitBreakerLogic) = {
    val delayActor = system.actorOf(Props[DelayActor])
    import akka.pattern.ask
    implicit val askTimeout = Timeout(5.seconds)
    val flow = Flow[(Long, String)].mapAsyncUnordered(20) { elem =>
      (delayActor ? elem).mapTo[(Long, String)]
    }

    val timeoutBidiFlow = TimeoutBidiFlowUnordered[String, String](timeout)


    val circuitBreakerBidiFlow = BidiFlow.fromGraph(new CircuitBreakerBidi[String, String](circuitBreakerLogic))

    Flow[String]
      .via(circuitBreakerBidiFlow.atop(timeoutBidiFlow).join(flow))
      .to(Sink.ignore)
      .runWith(Source.actorRef[String](5, OverflowStrategy.fail))
  }

  it should "increment failure count on call timeout" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerLogic(2, timeout, 10 milliseconds)
    circuitBreakerLogic.subscribe(self, Open)
    val ref = flow(circuitBreakerLogic)
    ref ! "a"
    ref ! "b"
    ref ! "b"
    expectMsg(Open)
  }

  it should "reset failure count after success" in {
    val circuitBreakerLogic = new AtomicCircuitBreakerLogic(2, timeout, 10 milliseconds)
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
}

class DelayActor extends Actor {

  val delay = Map("a" -> 30.milliseconds, "b" -> 200.milliseconds, "c" -> 30.milliseconds)

  def receive = {
    case element: String =>
      import context.dispatcher
      context.system.scheduler.scheduleOnce(delay(element), sender(), element)
    case element: (Long, String) =>
      import context.dispatcher
      context.system.scheduler.scheduleOnce(delay(element._2), sender(), element)
  }
}
