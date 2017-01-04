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

import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  *    +--------------------+
  * ~> | in       toWrapped | ~>
  *    |                    |
  * <~ | out    fromWrapped | <~
  *    +--------------------+
  *
  * A timer Bidi stage that is used to wrap a flow to add timeout functionality.  An element can be timed out only if
  * there is a downstream demand.
  *
  * Once an element is pushed from the wrapped flow (from fromWrapped), it first checks if the element is already
  * timed out.  If a timeout message has already been sent for that element to downstream, then, the element from
  * the wrapped flow is dropped.
  *
  * Please note, this timeout bidi stage can be used for flows that keep the order of messages as well as for the ones
  * that do not keep the message order.  Please see the corresponding implementations:
  * [[CircuitBreakerBidiOrdered]] and [[CircuitBreakerBidiUnordered]] for more details.
  *
  * To wrap the flows that do not guarantee the message ordering, it requires an id to be carried along with the
  * actual element [[In]] as a tuple to uniquely identify elements.
  *
  * A timer gets scheduled when there is a downstream demand that's not immediately addressed.  This is to make sure
  * that a timeout response is sent to the downstream when upstream cannot address the demand on time.
  *
  * Timer precision is 10ms to avoid unnecessary timer scheduling cycles
  *
  * @param timeout Duration after which a message should be considered timed out.
  */
class CircuitBreakerBidi[In, Out](timeout: FiniteDuration)
  extends GraphStage[BidiShape[In, In, Try[Out], Try[Out]]] {
  val in = Inlet[In]("CircuitBreakerBidi.in")
  val fromWrapped = Inlet[Try[Out]]("CircuitBreakerBidi.fromWrapped")
  val toWrapped = Outlet[In]("CircuitBreakerBidi.toWrapped")
  val out = Outlet[Try[Out]]("CircuitBreakerBidi.out")
  val shape = BidiShape(in, toWrapped, fromWrapped, out)
  private var downstreamDemand = 0
  private var upstreamFinished = false
  val readyToPush = mutable.Queue[Try[Out]]()
  private[this] def timerName = "CircuitBreakerBidi"

  val cb = CircuitBreaker(3, timeout, 30 milliseconds)

  def onPushFromWrapped(elem: Try[Out], isOutAvailable: Boolean): Option[Try[Out]] = {
    readyToPush.enqueue(elem)
    if(isOutAvailable) Some(readyToPush.dequeue())
    else None
  }

  def onPullOut(): Option[Try[Out]] = {
    Try(readyToPush.dequeue()).toOption
  }

  def isBuffersEmpty(): Boolean = readyToPush.isEmpty

  override def initialAttributes = Attributes.name("CircuitBreakerBidi")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        if(cb.shortCircuit()) {
          // TODO Need to have fallback check here before sending an exception.
          if(isAvailable(out)) push(out, Failure(CircuitBreakerOpenException()))
          else readyToPush.enqueue(Failure(CircuitBreakerOpenException()))
          // TODO Add to internal buffer.  This alltogether might be an onShortCircuit.  Moving it to internal buffers
          // and return an Option, like in timeout flow.
        } else push(toWrapped, elem)
      }
      override def onUpstreamFinish(): Unit = complete(toWrapped)
      override def onUpstreamFailure(ex: Throwable): Unit = fail(toWrapped, ex)
    })

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit = {
        if(!hasBeenPulled(in)) pull(in)
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })

    setHandler(fromWrapped, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(fromWrapped)
        elem match {
            // TODO add metrics code here
          case Success(_) => cb.succeed()
          case Failure(_) => cb.fail().foreach(scheduleOnce(timerName, _))
        }
        onPushFromWrapped(elem, isAvailable(out)) foreach { elem =>
          push(out, elem)
        }
      }
      override def onUpstreamFinish(): Unit = {
        if(isBuffersEmpty) completeStage()
        else upstreamFinished = true
      }

      override def onUpstreamFailure(ex: Throwable): Unit = fail(out, ex)
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if(!upstreamFinished || !isBuffersEmpty()) {
          onPullOut() match {
            case Some(elem) => push(out, elem)
            case None =>
              if(!hasBeenPulled(fromWrapped)) pull(fromWrapped)
              else if(!hasBeenPulled(in) && isAvailable(toWrapped)) pull(in)
          }
        } else complete(out)
      }
      override def onDownstreamFinish(): Unit = cancel(fromWrapped)
    })

    override def onTimer(timerKey: Any): Unit = {
      cb.attemptReset()
    }
  }

  override def toString = "CircuitBreakerBidi"

}

case class CircuitBreakerOpenException(msg: String = "Circuit Breaker is open!") extends Exception(msg)
