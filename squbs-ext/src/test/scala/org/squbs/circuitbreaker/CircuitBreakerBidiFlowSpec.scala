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
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{AsyncFlatSpecLike, Matchers}
import org.squbs.streams.{FlowTimeoutException, TimeoutBidiFlowUnordered}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class CircuitBreakerBidiFlowSpec extends TestKit(ActorSystem("CircuitBreakerBidiFlowSpec"))
  with AsyncFlatSpecLike with Matchers {

  implicit val materializer = ActorMaterializer()

  val timeout = 60 milliseconds
  val timeoutFailure = Failure(FlowTimeoutException("Flow timed out!"))
  val circuitBreakerOpenFailure = Failure(CircuitBreakerOpenException("Circuit Breaker is open!"))

  it should "work for flows that do not keep the order of messages" in {
    val delayActor = system.actorOf(Props[DelayActor])
    import akka.pattern.ask
    implicit val askTimeout = Timeout(5.seconds)
    val flow = Flow[(Long, String)].mapAsyncUnordered(3) { elem =>
      (delayActor ? elem).mapTo[(Long, String)]
    }

    val timeoutBidiFlow = TimeoutBidiFlowUnordered[String, String](timeout)
    val circuitBreakerBidiFlow = BidiFlow.fromGraph(new CircuitBreakerBidi[String, String](timeout))

    val result = Source("a" :: "b" :: "b" :: "b" :: "c" :: "c" :: "c" :: "c" :: "c" :: "c" :: "c" :: "c" :: "c" :: Nil)
      .via(circuitBreakerBidiFlow.atop(timeoutBidiFlow).join(flow))
      .runWith(Sink.seq)
    // "c" does NOT fail because the original flow lets it go earlier than "b"
    val expected = Success("a") :: timeoutFailure :: Success("c") :: Nil
    result map { _ should contain theSameElementsAs expected }
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
