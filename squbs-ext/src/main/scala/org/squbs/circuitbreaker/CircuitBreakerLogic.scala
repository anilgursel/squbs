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
import akka.event.EventBus

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
trait CircuitBreakerLogic {

  // TODO Consider making this overridable
  private val eventBus = new CircuitBreakerEventBusImpl

  /**
    * Subscribe an [[ActorRef]] to receive events that it's interested in.
    *
    * @param subscriber [[ActorRef]] that would receive the events
    * @param to event types that this [[ActorRef]] is interested in.
    * @return
    */
  def subscribe(subscriber: ActorRef, to: CircuitBreakerEventType): Boolean = {
    eventBus.subscribe(subscriber, to)
  }

  /**
    * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
    * The default exponential backoff factor is 2.
    *
    * @param maxResetTimeout the upper bound of resetTimeout
    */
  def withExponentialBackoff(maxResetTimeout: FiniteDuration): CircuitBreakerLogic

  /**
    * Mark a successful call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
    * caller Actor. In such a case, it is convenient to mark a successful call instead of using Future
    * via [[withCircuitBreaker]]
    */
  def succeed(): Unit

  /**
    * Mark a failed call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
    * caller Actor. In such a case, it is convenient to mark a failed call instead of using Future
    * via [[withCircuitBreaker]]
    */
  def fail(f: (FiniteDuration => Unit)): Unit

  def shortCircuit(): Boolean

  /**
    * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
    *
    * @param fromState State being transitioning from
    * @param toState   State being transitioning from
    */
  protected final def transition(fromState: CircuitBreakerState, toState: CircuitBreakerState): Unit = {
    if(transitionImpl(fromState, toState))
      eventBus.publish(CircuitBreakerEvent(toState, toState))
  }

  protected def transitionImpl(fromState: CircuitBreakerState, toState: CircuitBreakerState): Boolean

  /**
    * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
    *
    * @param fromState State we're coming from (Closed or Half-Open)
    */
  protected final def tripBreaker(fromState: CircuitBreakerState): Unit = transition(fromState, Open)

  /**
    * Resets breaker to a closed state.  This is valid from an Half-Open state only.
    *
    */
  protected final def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
    * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
    * // TODO Consider making this protected as well
    */
  final def attemptReset(): Unit = transition(Open, HalfOpen)

}


import akka.util.Subclassification

sealed trait CircuitBreakerEventType
sealed trait TransitionEvent extends CircuitBreakerEventType
object TransitionEvents extends TransitionEvent
sealed trait CircuitBreakerState extends TransitionEvent
object Closed extends CircuitBreakerState
object HalfOpen extends CircuitBreakerState
object Open extends CircuitBreakerState

case class CircuitBreakerEvent(eventType: CircuitBreakerEventType, payload: Any)

class CircuitBreakerEventClassification extends Subclassification[CircuitBreakerEventType] {
  override def isEqual(x: CircuitBreakerEventType, y: CircuitBreakerEventType): Boolean =
    x == y

  override def isSubclass(x: CircuitBreakerEventType, y: CircuitBreakerEventType): Boolean =
    x match {
      case `y` => true
      case _ if x.isInstanceOf[TransitionEvent] && y == TransitionEvents => true
      case _ => false
    }
}

import akka.event.SubchannelClassification

/**
  * Publishes the payload of the CircuitBreakerEvent when the event type of the
  * CircuitBreakerEvent matches with the one used during subscription
  */
class CircuitBreakerEventBusImpl extends EventBus with SubchannelClassification {
  type Event = CircuitBreakerEvent
  type Classifier = CircuitBreakerEventType
  type Subscriber = ActorRef

  override protected val subclassification: Subclassification[Classifier] =
  new CircuitBreakerEventClassification

  override protected def classify(event: Event): Classifier = event.eventType

  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }
}