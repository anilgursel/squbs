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

import akka.actor.ActorRef
import akka.event.{EventBus, SubchannelClassification}
import akka.util.Subclassification
import com.codahale.metrics.{Gauge, MetricRegistry}

import scala.concurrent.duration._

/**
  * Provides circuit breaker functionality to provide stability when working with "dangerous" operations, e.g. calls to
  * remote systems
  *
  * Transitions through three states:
  * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
  * to open.
  * - In *Open* state, calls fail-fast with a [[scala.util.Failure]] or a fallback response.  After `resetTimeout`,
  * circuit breaker transitions to half-open state.
  * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
  * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
  * execute while the first is running will fail-fast with [[scala.util.Failure]] or a fallback response.
  *
  * An [[ActorRef]] can be subscribed to receive certain events, e.g., [[TransitionEvents]] to receive all transition
  * events or specific transtion events like [[Open]].  Going forward more CircuitBreaker events could be introduced.
  */
trait CircuitBreakerState {

  private val eventBus = new CircuitBreakerEventBusImpl

  val callTimeout: FiniteDuration
  val metricRegistry: MetricRegistry
  val name: String
  private val SuccessCount = s"$name.circuit-breaker.success-count"
  private val FailureCount = s"$name.circuit-breaker.failure-count"
  private val ShortCircuitCount = s"$name.circuit-breaker.short-circuit-count"

  object StateGauge extends Gauge[State] {
    val MetricName = s"$name.circuit-breaker.state"
    override def getValue: State = currentState
  }

  if(!metricRegistry.getGauges.containsKey(StateGauge.MetricName))
    metricRegistry.register(s"$name.circuit-breaker.state", StateGauge)

  /**
    * Subscribe an [[ActorRef]] to receive events that it's interested in.
    *
    * @param subscriber [[ActorRef]] that would receive the events
    * @param to event types that this [[ActorRef]] is interested in.
    * @return
    */
  def subscribe(subscriber: ActorRef, to: EventType): Boolean = {
    eventBus.subscribe(subscriber, to)
  }

  /**
    * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
    * The default exponential backoff factor is 2.
    *
    * @param exponentialBackoffFactor The exponential amount that the wait time will be increased
    * @param maxResetTimeout the upper bound of resetTimeout
    */
  def withExponentialBackoff(exponentialBackoffFactor: Double, maxResetTimeout: FiniteDuration): CircuitBreakerState

  /**
    * The provided [[MetricRegistry]] will be used to register metrics
    *
    * @param metricRegistry The registry to use for codahale metrics
    */
  def withMetricRegistry(metricRegistry: MetricRegistry): CircuitBreakerState

  /**
    * Mark a successful element through CircuitBreaker.
    */
  final def success(): Unit = {
    metricRegistry.meter(SuccessCount).mark()
    succeeds()
  }

  /**
    * Mark a failed element through CircuitBreaker.
    */
  final def failure(): Unit = {
    metricRegistry.meter(FailureCount).mark()
    fails()
  }

  /**
    * Check if circuit should be short circuited.
    */
  final def shortCircuited(): Boolean = {
    val shortCircuited = isShortCircuited
    if(shortCircuited) metricRegistry.meter(ShortCircuitCount).mark()
    shortCircuited
  }

  protected def succeeds(): Unit

  protected def fails(): Unit

  protected def isShortCircuited: Boolean

  protected def currentState: State

  /**
    * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
    *
    * @param fromState State being transitioning from
    * @param toState   State being transitioning from
    */
  protected final def transition(fromState: State, toState: State): Unit =
    if(transitionImpl(fromState, toState)) eventBus.publish(CircuitBreakerEvent(toState, toState))

  protected def transitionImpl(fromState: State, toState: State): Boolean

  /**
    * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
    *
    * @param fromState State we're coming from (Closed or Half-Open)
    */
  protected final def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
    * Resets breaker to a closed state.  This is valid from an Half-Open state only.
    *
    */
  protected final def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
    * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
    *
    */
  protected final def attemptReset(): Unit = transition(Open, HalfOpen)

}

case class CircuitBreakerOpenException(msg: String = "Circuit Breaker is open!") extends Exception(msg)

sealed trait EventType
sealed trait TransitionEvent extends EventType
object TransitionEvents extends TransitionEvent
sealed trait State extends TransitionEvent
object Closed extends State {
  def instance = this
}
object HalfOpen extends State {
  def instance = this
}
object Open extends State {
  def instance = this
}

case class CircuitBreakerEvent(eventType: EventType, payload: Any)

class CircuitBreakerEventClassification extends Subclassification[EventType] {
  override def isEqual(x: EventType, y: EventType): Boolean =
    x == y

  override def isSubclass(x: EventType, y: EventType): Boolean =
    x match {
      case `y` => true
      case _ if x.isInstanceOf[TransitionEvent] && y == TransitionEvents => true
      case _ => false
    }
}

/**
  * Publishes the payload of the [[CircuitBreakerEvent]] when the event type of the
  * [[CircuitBreakerEvent]] matches with the one used during subscription.
  */
class CircuitBreakerEventBusImpl extends EventBus with SubchannelClassification {
  type Event = CircuitBreakerEvent
  type Classifier = EventType
  type Subscriber = ActorRef

  override protected val subclassification: Subclassification[Classifier] =
  new CircuitBreakerEventClassification

  override protected def classify(event: Event): Classifier = event.eventType

  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }
}