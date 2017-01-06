
package org.squbs.circuitbreaker

// TODO Update the license header of this file!
// Since it is mostly copied from Akka Contrib, removing the PayPal legal header.
import akka.actor.ActorRef
import akka.event.EventBus
import org.squbs.circuitbreaker.impl.AtomicCircuitBreakerLogic

import scala.concurrent.duration._

/**
  * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
  */
object CircuitBreakerLogic {

  /**
    * Create a new CircuitBreaker.
    *
    * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
    * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
    * executor in the constructor.
    *
    * @param scheduler Reference to Akka scheduler
    * @param maxFailures Maximum number of failures before opening the circuit
    * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
    * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
    */
  def apply(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration): CircuitBreakerLogic =
    new AtomicCircuitBreakerLogic(maxFailures, callTimeout, resetTimeout)
  /**
    * Java API: Create a new CircuitBreaker.
    *
    * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
    * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
    * executor in the constructor.
    *
    * @param scheduler Reference to Akka scheduler
    * @param maxFailures Maximum number of failures before opening the circuit
    * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
    * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
    */
  def create(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration): CircuitBreakerLogic =
    apply(maxFailures, callTimeout, resetTimeout)
}

/**
  * Provides circuit breaker functionality to provide stability when working with "dangerous" operations, e.g. calls to
  * remote systems
  *
  * Transitions through three states:
  * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
  * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
  * - In *Open* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
  * half-open state.
  * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
  * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
  * execute while the first is running will fail-fast with an exception.
  *
  * @param scheduler Reference to Akka scheduler
  * @param maxFailures Maximum number of failures before opening the circuit
  * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
  * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
  * @param executor [[scala.concurrent.ExecutionContext]] used for execution of state transition listeners
  */
trait CircuitBreakerLogic {

  // TODO Consider making this overridable
  private val eventBus = new CircuitBreakerEventBusImpl

  /**
    * TODO Add Javadoc
    *
    * @param subscriber
    * @param to
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
  protected def transition(fromState: CircuitBreakerState, toState: CircuitBreakerState): Unit = {
    eventBus.publish(CircuitBreakerEvent(toState, toState))
  }

  /**
    * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
    *
    * @param fromState State we're coming from (Closed or Half-Open)
    */
  protected def tripBreaker(fromState: CircuitBreakerState): Unit = transition(fromState, Open)

  /**
    * Resets breaker to a closed state.  This is valid from an Half-Open state only.
    *
    */
  protected def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
    * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
    * // TODO Consider making this protected as well
    */
  def attemptReset(): Unit = transition(Open, HalfOpen)

}


import akka.util.Subclassification

sealed trait CircuitBreakerEventType
sealed trait TransitionEvent extends CircuitBreakerEventType
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