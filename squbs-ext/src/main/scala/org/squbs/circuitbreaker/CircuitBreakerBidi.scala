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

import akka.stream.stage._
import akka.stream._
import org.squbs.circuitbreaker.impl.AtomicCircuitBreakerLogic

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
  * and successes only from a single materialization.  But, it also accepts a [[circuitBreaker]] that can be shared
  * across multiple streams as well as multiple materializations of the same stream.
  *
  * Please note, if the upstream does not control the throughput, then [[CircuitBreakerBidi]] might cause more elements
  * to be short circuited than expected.  The downstream demand will be addressed with short circuit messages, which
  * most likely takes less time than it takes the wrapped flow to process an element.  To eliminate this problem, a
  * throttle can be applied specifically for circuit breaker related messages.
  *
  * @param circuitBreaker Circuit Breaker implementation
  */
class CircuitBreakerBidi[In, Out, Context, Id](circuitBreaker: CircuitBreakerLogic)
  extends GraphStage[BidiShape[(In, Context), (In, Context), (Try[Out], Context), (Try[Out], Context)]] {
  val in = Inlet[(In, Context)]("CircuitBreakerBidi.in")
  val fromWrapped = Inlet[(Try[Out], Context)]("CircuitBreakerBidi.fromWrapped")
  val toWrapped = Outlet[(In, Context)]("CircuitBreakerBidi.toWrapped")
  val out = Outlet[(Try[Out], Context)]("CircuitBreakerBidi.out")
  val shape = BidiShape(in, toWrapped, fromWrapped, out)
  private var downstreamDemand = 0
  private var upstreamFinished = false
  val readyToPush = mutable.Queue[(Try[Out], Context)]()
  private[this] def timerName = "CircuitBreakerBidi"

//  def this(maxFailures: Int,
//           callTimeout: FiniteDuration,
//           resetTimeout: FiniteDuration,
//           maxResetTimeout: FiniteDuration,
//           exponentialBackoffFactor: Double) =
//    this(new AtomicCircuitBreakerLogic(maxFailures, callTimeout, resetTimeout, maxResetTimeout, exponentialBackoffFactor))
//// TODO This should not access to Atomic
//  def this(maxFailures: Int,
//           callTimeout: FiniteDuration,
//           resetTimeout: FiniteDuration) = this(maxFailures, callTimeout, resetTimeout, 36500.days, 1.0)

  def onPushFromWrapped(elem: (Try[Out], Context), isOutAvailable: Boolean): Option[(Try[Out], Context)] = {
    readyToPush.enqueue(elem)
    if(isOutAvailable) Some(readyToPush.dequeue())
    else None
  }

  override def initialAttributes = Attributes.name("CircuitBreakerBidi")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val (elem, context) = grab(in)
        print(s"---> onPushIn elem: ${elem.toString}  ")
        if(circuitBreaker.shortCircuit()) {
          print(s"short circuit: YES ")
          if(isAvailable(out) && readyToPush.isEmpty){
            println("push(out, Failure(CircuitBreakerOpenException()))")
            push(out, (Failure(CircuitBreakerOpenException()), context))
          } else {
            readyToPush.enqueue((Failure(CircuitBreakerOpenException()), context))
            println("readyToPush.enqueue(Failure(CircuitBreakerOpenException()))")
          }
        } else {
          println(s" short circuit: NO push(toWrapped, ${elem.toString})")
          push(toWrapped, (elem, context)) }
      }
      override def onUpstreamFinish(): Unit = complete(toWrapped)
      override def onUpstreamFailure(ex: Throwable): Unit = fail(toWrapped, ex)
    })

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit = {
        print("---> onPullToWrapped  ")
        if(!hasBeenPulled(in)) {
          println("pull(in)")
          pull(in)
        } else println("NOT pull(in)")
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })

    setHandler(fromWrapped, new InHandler {
      override def onPush(): Unit = {
        val (elem, context) = grab(fromWrapped)
        print(s"---> onPushFromWrapped elem: ")
        elem match {
            // TODO add metrics code here
          case Success(_) =>
            circuitBreaker.succeed()
            print(s"${elem.toString}  ")
          case Failure(_) =>
            print("TimeoutException  ")

            circuitBreaker.fail(scheduleResetAttempt)
        }
        onPushFromWrapped((elem, context), isAvailable(out)) foreach { tuple =>
          push(out, tuple)
          print(s"  push(out, ${elem.toString})")
        }
        println("")
      }
      override def onUpstreamFinish(): Unit = {
        if(readyToPush.isEmpty) completeStage()
        else upstreamFinished = true
      }

      override def onUpstreamFailure(ex: Throwable): Unit = fail(out, ex)
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        print(s"---> onPullOut: ")
        if(!upstreamFinished || !readyToPush.isEmpty) {
          print(" NOT finished  ")
          readyToPush.dequeueFirst((_: (Try[Out], Context)) => true) match {
            case Some(elemWithContext) => {
              println(s"push(out, ${elemWithContext.toString})")
              push(out, elemWithContext)
            }
            case None =>
              if(!hasBeenPulled(fromWrapped)) {
                println("pull(fromWrapped)")
                pull(fromWrapped)
              }
              else if(!hasBeenPulled(in) && isAvailable(toWrapped)) {
                println("pull(in)")
                pull(in)
              } else println("do nothing")
          }
        } else {
          println("complete(out)")
          complete(out)
        }
      }
      override def onDownstreamFinish(): Unit = cancel(fromWrapped)
    })

    override def onTimer(timerKey: Any): Unit = {
      circuitBreaker.attemptReset()
//      if(!hasBeenPulled(in) && isAvailable(toWrapped)) pull(in)
    }

    // TODO This could actually be moved to CircuitBreaker; however, then we need to make sure we do not schedule
    // a timer if already one scheduled.  It is a scenario that should not happen though.
    // The other thing to consider is which thread the scheduled task would run.  Doing this way ensure `onTimer` being
    // called in the same thread.
    private def scheduleResetAttempt(d: FiniteDuration): Unit = if(!isTimerActive(timerName)) scheduleOnce(timerName, d)
  }

  override def toString = "CircuitBreakerBidi"

}

case class CircuitBreakerOpenException(msg: String = "Circuit Breaker is open!") extends Exception(msg)
