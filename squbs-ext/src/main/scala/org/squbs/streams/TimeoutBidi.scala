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

package org.squbs.streams

import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

import akka.NotUsed
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
  * [[TimeoutBidiOrdered]] and [[TimeoutBidiUnordered]] for more details.
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
abstract class TimeoutBidi[In, ToWrapped, FromWrapped, Out](timeout: FiniteDuration)
  extends GraphStage[BidiShape[In, ToWrapped, FromWrapped, Try[Out]]] {
  val in = Inlet[In]("TimeoutBidi.in")
  val fromWrapped = Inlet[FromWrapped]("TimeoutBidi.fromWrapped")
  val toWrapped = Outlet[ToWrapped]("TimeoutBidi.toWrapped")
  val out = Outlet[Try[Out]]("TimeoutBidi.out")
  val shape = BidiShape(in, toWrapped, fromWrapped, out)
  val expireOffset = timeout.toNanos
  private[this] def timerName = "TimeoutBidi"
  val precision = 10.milliseconds.toNanos
  val delayMillis = timeout.toMillis
  var downstreamDemand = 0
  var upstreamFinished = false

  def mapFromInToWrapped(elem: In): ToWrapped

  def onPushFromWrapped(elem: FromWrapped, isOutAvailable: Boolean): Option[Try[Out]]

  def onScheduledTimeout(): Option[Try[Out]]

  def onPullOut(): Option[Try[Out]]

  def isBuffersEmpty(): Boolean

  def timeLeftForNextElemToTimeout(): Long = {
    delayMillis - NANOSECONDS.toMillis(System.nanoTime() - firstElemStartTime)
  }

  def expirationTime(): Long = System.nanoTime() - expireOffset - precision

  def firstElemStartTime: Long

  override def initialAttributes = Attributes.name("TimeoutBidi")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = push(toWrapped, mapFromInToWrapped(grab(in)))
      override def onUpstreamFinish(): Unit = complete(toWrapped)
      override def onUpstreamFailure(ex: Throwable): Unit = fail(toWrapped, ex)
    })

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })

    setHandler(fromWrapped, new InHandler {
      override def onPush(): Unit = {
        onPushFromWrapped(grab(fromWrapped), isAvailable(out)) map { elem =>
          push(out, elem)
        }
        if(downstreamDemand > 0) {
          pull(fromWrapped)
          downstreamDemand -= 1
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
            case None => if (!isTimerActive(timerName)) scheduleOnce(timerName, timeLeftForNextElemToTimeout().millis)
          }

          if (!isClosed(fromWrapped) && !hasBeenPulled(fromWrapped)) pull(fromWrapped)
          else downstreamDemand += 1
        } else complete(out)
      }
      override def onDownstreamFinish(): Unit = cancel(fromWrapped)
    })

    final override def onTimer(key: Any): Unit = {
      if(!upstreamFinished || !isBuffersEmpty()) {
        if (isAvailable(out)) {
          onScheduledTimeout() match {
            case Some(elem) => push(out, elem)
            case None => scheduleOnce(timerName, timeLeftForNextElemToTimeout.millis)
          }
        }
      } else complete(out)
    }
  }

  override def toString = "TimeoutBidi"

}

object TimeoutBidiFlowUnordered {

  /**
    * Please see comments of [[apply[In, Out, Id](timeout: FiniteDuration, idGenerator: () => Id)]] for more details.
    *
    * This API provides a simplified version with a default [[Long]] id generator.  It requires the wrapped flow
    * to carry a [[Long]] id to be carried around with input and output as a tuple.
    *
    * @param timeout Duration after which a message should be considered timed out.
    */
  def apply[In, Out](timeout: FiniteDuration): BidiFlow[In, (Long, In), (Long, Out), Try[Out], NotUsed] =
    apply(timeout, LongIdGenerator().nextId())

  /**
    * Java API
    */
  def create[In, Out](timeout: FiniteDuration):
  akka.stream.javadsl.BidiFlow[In, (java.lang.Long, In), (java.lang.Long, Out), Try[Out], NotUsed] =
    apply(timeout, LongIdGenerator().nextIdAsJava()).asJava

  /**
    * Creates a BidiFlow that can be joined with a flow to add timeout functionality.  This API is specifically for
    * the flows that do not guarantee message ordering.  For flows that guarantee message ordering, please use
    * [[TimeoutBidiOrdered]].
    *
    * Timeout functionality requires each message to be uniquely identified, so this API requires the wrapped flow to
    * carry an id of type [[Id]] along with the flow's input and output as a tuple.
    *
    * It takes a custom [[Id]] generator.  Please see [[apply[In, Out](timeout: FiniteDuration)]] for the API with
    * default [[Long]] id generator.
    *
    * Once it pulls in an element (from in),
    *   - generates an [[Id]] to uniquely identify an element.
    *   - marks the system time along with the generated id.
    *
    * @param timeout Duration after which a message should be considered timeout out.
    * @param idGenerator Function that generates a unique id for each message
    */
  def apply[In, Out, Id](timeout: FiniteDuration, idGenerator: () => Id):
  BidiFlow[In, (Id, In), (Id, Out), Try[Out], NotUsed] =
    BidiFlow.fromGraph(TimeoutBidiUnordered(timeout, idGenerator))

  /**
    * Java API
    */
  def create[In, Out, Id](timeout: FiniteDuration, idGenerator: Supplier[Id]):
  akka.stream.javadsl.BidiFlow[In, (Id, In), (Id, Out), Try[Out], NotUsed] =
    apply(timeout, () => idGenerator.get()).asJava

}

object TimeoutBidiUnordered {

  def apply[In, Out](timeout: FiniteDuration): TimeoutBidiUnordered[In, Out, Long] =
    new TimeoutBidiUnordered(timeout, LongIdGenerator().nextId())

  def apply[In, Out, Id](timeout: FiniteDuration, idGenerator: () => Id): TimeoutBidiUnordered[In, Out, Id] =
    new TimeoutBidiUnordered(timeout, idGenerator)
}

final class TimeoutBidiUnordered[In, Out, Id](timeout: FiniteDuration, idGenerator: () => Id) extends
  TimeoutBidi[In, (Id, In), (Id, Out), Out](timeout) {

  val timeouts = new mutable.LinkedHashMap[Id, (In, Long)]
  val readyToPush = mutable.Queue[(Out, Long)]()

  override def mapFromInToWrapped(element: In): (Id, In) = {
    val id = idGenerator()
    timeouts.put(id, (element, System.nanoTime()))
    (id, element)
  }

  override def onPushFromWrapped(fromWrapped: (Id, Out), isOutAvailable: Boolean): Option[Try[Out]] = {
    val (id, element) = fromWrapped
    timeouts.remove(id) map { case(_, startTime) =>
      readyToPush.enqueue((element, startTime))
    }

    if(isOutAvailable) pickNextElemToPush()
    else None
  }

  override def firstElemStartTime = timeouts.headOption map { case (_, (_, startTime)) => startTime } getOrElse(0)

  private def pickNextElemToPush(): Option[Try[Out]] = {
    timeouts.headOption.filter { case(_, (_, firstElemStartTime)) =>
      firstElemStartTime < expirationTime &&
      readyToPush.headOption.filter { case(_, readyToPushStartTime) =>
        readyToPushStartTime <= firstElemStartTime
      }.isEmpty
    } map { case(id, (_, _)) =>
      timeouts.remove(id)
      Failure(FlowTimeoutException())
    } orElse(Try(readyToPush.dequeue()).toOption.map { case(elem, _) => Success(elem)})
  }

  override def onPullOut() = pickNextElemToPush()

  override def onScheduledTimeout() = pickNextElemToPush()

  override def isBuffersEmpty() = timeouts.isEmpty && readyToPush.isEmpty
}

object TimeoutBidiFlowOrdered {
  /**
    * Creates a BidiFlow that can be joined with a flow to add timeout functionality.  This API is specifically for
    * the flows that guarantee message ordering.
    *
    * Since the wrapped flow guarantees message ordering, unlike [[TimeoutBidiFlowUnordered]], it does not require an
    * id to be carried around by the wrapped flow.
    *
    * @param timeout Duration after which a message should be considered timeout out.
    */
  def apply[In, Out](timeout: FiniteDuration): BidiFlow[In, In, Out, Try[Out], NotUsed] =
    BidiFlow.fromGraph(TimeoutBidiOrdered(timeout))

  /**
    * Java API
    */
  def create[In, Out](timeout: FiniteDuration): akka.stream.javadsl.BidiFlow[In, In, Out, Try[Out], NotUsed] =
    apply(timeout).asJava
}

object TimeoutBidiOrdered {
  def apply[In, Out](timeout: FiniteDuration): TimeoutBidiOrdered[In, Out] =
    new TimeoutBidiOrdered(timeout)
}

final class TimeoutBidiOrdered[In, Out](timeout: FiniteDuration) extends
  TimeoutBidi[In, In, Out, Out](timeout) {

  val timeouts = mutable.Queue[TimeoutTracker]()

  override def mapFromInToWrapped(elem: In): In = {
    timeouts.enqueue(TimeoutTracker(System.nanoTime(), false))
    elem
  }

  override def onPushFromWrapped(elem: Out, isOutAvailable: Boolean): Option[Try[Out]] = {
    if(isOutAvailable) {
      if(timeouts.dequeue().isTimedOut) None
      else Some(Success(elem))
    } else None
  }

  override def firstElemStartTime = timeouts.find(_.isTimedOut == false).map(_.startTime).getOrElse(0)

  override def onPullOut() = None

  override def onScheduledTimeout() = {
    timeouts.find(_.isTimedOut == false).filter(_.startTime < expirationTime).map { elem =>
        elem.isTimedOut = true
        Failure(FlowTimeoutException())
    }
  }

  override def isBuffersEmpty() = timeouts.isEmpty || timeouts.forall(_.isTimedOut == true)

  case class TimeoutTracker(startTime: Long, var isTimedOut: Boolean)
}

object LongIdGenerator {
  def apply() = new LongIdGenerator()
}

class LongIdGenerator {
  // This is not concurrent, nor needs it to be
  var id = 0L

  def nextId() = { () =>
    // It may overflow, which should be ok for most scenarios.  If the message with id 0 is still not completed
    // after it overflowed, then deduplication logic would be broken.  For such scenarios, a different
    // id generator should be used.
    id = id + 1
    id
  }

  def nextIdAsJava(): () => java.lang.Long = { () => nextId()()}
}

case class FlowTimeoutException(msg: String = "Flow timed out!") extends TimeoutException(msg)
