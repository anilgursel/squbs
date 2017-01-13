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

import java.util.Optional

import akka.NotUsed
import akka.http.org.squbs.util.JavaConverters
import akka.http.org.squbs.util.JavaConverters.toJava
import akka.japi.Pair
import akka.stream._
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import java.util.function.{Function => JFunction}

import org.squbs.streams.TimeoutBidiUnordered

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  *    +--------------------+
  * ~> | in       toWrapped | ~>
  *    |                    |
  * <~ | out    fromWrapped | <~
  *    +--------------------+
  *
  * A Bidi stage that is used to wrap a flow to add Circuit Breaker functionality.  When the wrapped flow is having
  * trouble responding in time or it is responding with failures, then based on the provided settings, it short circuits
  * the stream: Instead of pushing an element to wrapped flow, it directly pushes it down to downstream (out), given
  * that there is downstream demand.
  *
  * The wrapped flow pushes down (fromWrapped) a [[Try]].  By default, any [[Failure]] is considered a problem and
  * causes the circuit breaker failure count to be incremented.  However, [[CircuitBreakerBidi]] also accepts a
  * [[decider]] to decide on what element is actually considered a failure.  For instance, if Circuit Breaker is used to
  * wrap Http calls, a [[Success]] Http Response with status code 500 internal server error should be considered a
  * failure.
  *
  * By default, the circuit breaker functionality is per materialization meaning that it takes into account failures
  * and successes only from a single materialization.  But, it also accepts a [[circuitBreakerState]] that can be shared
  * across multiple streams as well as multiple materializations of the same stream.
  *
  * Please note, if the upstream does not control the throughput, then [[CircuitBreakerBidi]] might cause more elements
  * to be short circuited than expected.  The downstream demand will be addressed with short circuit messages, which
  * most likely takes less time than it takes the wrapped flow to process an element.  To eliminate this problem, a
  * throttle can be applied specifically for circuit breaker related messages.
  *
  * @param circuitBreakerState Circuit Breaker implementation
  */
class CircuitBreakerBidi[In, Out, Context](circuitBreakerState: CircuitBreakerState,
                                           fallback: Option[((In, Context)) => (Try[Out], Context)],
                                           failureDecider: Option[((Try[Out], Context)) => Boolean])
  extends GraphStage[BidiShape[(In, Context), (In, Context), (Try[Out], Context), (Try[Out], Context)]] {

  val in = Inlet[(In, Context)]("CircuitBreakerBidi.in")
  val fromWrapped = Inlet[(Try[Out], Context)]("CircuitBreakerBidi.fromWrapped")
  val toWrapped = Outlet[(In, Context)]("CircuitBreakerBidi.toWrapped")
  val out = Outlet[(Try[Out], Context)]("CircuitBreakerBidi.out")
  val shape = BidiShape(in, toWrapped, fromWrapped, out)
  private var upstreamFinished = false
  val readyToPush = mutable.Queue[(Try[Out], Context)]()
  val isFailure = failureDecider.getOrElse((e: (Try[Out], Context)) => e._1.isFailure)

  def onPushFromWrapped(elem: (Try[Out], Context), isOutAvailable: Boolean): Option[(Try[Out], Context)] = {
    readyToPush.enqueue(elem)
    if(isOutAvailable) Some(readyToPush.dequeue())
    else None
  }

  override def initialAttributes = Attributes.name("CircuitBreakerBidi")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val (elem, context) = grab(in)
        if(circuitBreakerState.shortCircuited) {
          val failFast = fallback.map(_(elem, context)).getOrElse((Failure(CircuitBreakerOpenException()), context))
          if(isAvailable(out) && readyToPush.isEmpty) push(out, failFast)
          else readyToPush.enqueue(failFast)
        } else push(toWrapped, (elem, context))
      }
      override def onUpstreamFinish(): Unit = complete(toWrapped)
      override def onUpstreamFailure(ex: Throwable): Unit = fail(toWrapped, ex)
    })

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
      override def onDownstreamFinish(): Unit = completeStage()
    })

    setHandler(fromWrapped, new InHandler {
      override def onPush(): Unit = {
        val elemWithcontext = grab(fromWrapped)

        if(isFailure(elemWithcontext)) circuitBreakerState.failure()
        else circuitBreakerState.success()

        onPushFromWrapped(elemWithcontext, isAvailable(out)).foreach(tuple => push(out, tuple))
      }
      override def onUpstreamFinish(): Unit = {
        if(readyToPush.isEmpty) completeStage()
        else upstreamFinished = true
      }

      override def onUpstreamFailure(ex: Throwable): Unit = fail(out, ex)
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit =
        if(!upstreamFinished || !readyToPush.isEmpty) readyToPush.dequeueFirst((_: (Try[Out], Context)) => true) match {
          case Some(elemWithContext) => push(out, elemWithContext)
          case None =>
            if(!hasBeenPulled(fromWrapped)) pull(fromWrapped)
            else if(!hasBeenPulled(in) && isAvailable(toWrapped)) pull(in)
        }
        else complete(out)

      override def onDownstreamFinish(): Unit = cancel(fromWrapped)
    })
  }

  override def toString = "CircuitBreakerBidi"

}

object CircuitBreakerBidi {

  def apply[In, Out, Context](circuitBreakerState: CircuitBreakerState,
            fallback: Option[((In, Context)) => (Try[Out], Context)] = None,
            failureDecider: Option[((Try[Out], Context)) => Boolean] = None):
  CircuitBreakerBidi[In, Out, Context] =
    new CircuitBreakerBidi(circuitBreakerState, fallback, failureDecider)
}

object CircuitBreakerBidiFlow {

  def apply[In, Out, Context](circuitBreakerState: CircuitBreakerState,
                              fallback: Option[((In, Context)) => (Try[Out], Context)] = None,
                              failureDecider: Option[((Try[Out], Context)) => Boolean] = None):
  BidiFlow[(In, Context), (In, Context), (Out, Context), (Try[Out], Context), NotUsed] =
    apply(circuitBreakerState, fallback, failureDecider, (context: Context) => context)

  /**
    * Java API
    */
  def create[In, Out, Context](circuitBreakerState: CircuitBreakerState):
  akka.stream.javadsl.BidiFlow[Pair[In, Context], Pair[In, Context], Pair[Out, Context], Pair[Try[Out], Context], NotUsed] = {
    toJava[In, In, Out, Try[Out], Context](apply(circuitBreakerState, None, None))
  }

  /**
    * Java API
    */
  def create[In, Out, Context](circuitBreakerState: CircuitBreakerState,
                               fallback: Optional[JFunction[Pair[In, Context], Pair[Try[Out], Context]]],
                               failureDecider: Optional[JFunction[Pair[Try[Out], Context], Boolean]]):
  akka.stream.javadsl.BidiFlow[Pair[In, Context], Pair[In, Context], Pair[Out, Context], Pair[Try[Out], Context], NotUsed] = {
    JavaConverters.toJava[In, In, Out, Try[Out], Context] {
      apply(circuitBreakerState, fallbackAsScala(fallback), failureDeciderAsScala(failureDecider))
    }
  }

  def apply[In, Out, Context, Id](circuitBreakerState: CircuitBreakerState,
                                  fallback: Option[((In, Context)) => (Try[Out], Context)],
                                  failureDecider: Option[((Try[Out], Context)) => Boolean],
                                  uniqueId: Context => Id):
  BidiFlow[(In, Context), (In, Context), (Out, Context), (Try[Out], Context), NotUsed] =
    BidiFlow
      .fromGraph(CircuitBreakerBidi(circuitBreakerState, fallback, failureDecider))
      .atop(TimeoutBidiUnordered(circuitBreakerState.callTimeout, uniqueId))

  /**
    * Java API
    */
  def create[In, Out, Context, Id](circuitBreakerState: CircuitBreakerState,
                               fallback: Optional[JFunction[Pair[In, Context], Pair[Try[Out], Context]]],
                               failureDecider: Optional[JFunction[Pair[Try[Out], Context], Boolean]],
                               uniqueId: JFunction[Context, Id]):
  akka.stream.javadsl.BidiFlow[Pair[In, Context], Pair[In, Context], Pair[Out, Context], Pair[Try[Out], Context], NotUsed] = {
    import scala.compat.java8.FunctionConverters._
    JavaConverters.toJava[In, In, Out, Try[Out], Context] {
      apply(circuitBreakerState, fallbackAsScala(fallback), failureDeciderAsScala(failureDecider), uniqueId.asScala)
    }
  }

  private def fallbackAsScala[In, Out, Context](fallback: Optional[JFunction[Pair[In, Context], Pair[Try[Out], Context]]]) = {

    import scala.compat.java8.FunctionConverters._
    import scala.compat.java8.OptionConverters._

    def fallbackAsScala(fallback: JFunction[Pair[In, Context], Pair[Try[Out], Context]])
                                                      (tuple: ((In, Context))): (Try[Out], Context) = {
      val response = fallback.asScala.apply(Pair(tuple._1, tuple._2))
      (response.first, response.second)
    }

    fallback.asScala.map(fallbackAsScala _)
  }

  private def failureDeciderAsScala[Out, Context](failureDecider: Optional[JFunction[Pair[Try[Out], Context], Boolean]]) = {

    import scala.compat.java8.FunctionConverters._
    import scala.compat.java8.OptionConverters._

    def failureDeciderAsScala(failureDecider: JFunction[Pair[Try[Out], Context], Boolean])
                       (tuple: ((Try[Out], Context))): Boolean = {
      failureDecider.asScala.apply(Pair(tuple._1, tuple._2))
    }

    failureDecider.asScala.map(failureDeciderAsScala _)
  }
}
