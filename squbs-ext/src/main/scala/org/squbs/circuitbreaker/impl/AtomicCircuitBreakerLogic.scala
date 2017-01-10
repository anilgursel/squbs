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

package org.squbs.circuitbreaker.impl

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import akka.actor.ActorRef
import akka.util.Unsafe
import org.squbs.circuitbreaker._

import scala.concurrent.duration._


/**
  * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
  */
object AtomicCircuitBreakerLogic {

  /**
    * Create a new CircuitBreaker.
    *
    * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
    * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
    * executor in the constructor.
    *
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
  * - In *AtomicClosed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
  * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
  * - In *AtomicOpen* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
  * half-open state.
  * - In *Half-AtomicOpen* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
  * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
  * execute while the first is running will fail-fast with an exception.
  *
  * @param scheduler Reference to Akka scheduler
  * @param maxFailures Maximum number of failures before opening the circuit
  * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
  * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
  * @param executor [[scala.concurrent.ExecutionContext]] used for execution of state transition listeners
  */
class AtomicCircuitBreakerLogic(maxFailures:              Int,
                                val callTimeout:          FiniteDuration,
                                resetTimeout:             FiniteDuration,
                                maxResetTimeout:          FiniteDuration,
                                exponentialBackoffFactor: Double) extends AbstractAtomicCircuitBreakerLogic
  with CircuitBreakerLogic {

  require(exponentialBackoffFactor >= 1.0, "factor must be >= 1.0")

  def this(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration) = {
    this(maxFailures, callTimeout, resetTimeout, 36500.days, 1.0)
  }

  /**
    * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
    * The default exponential backoff factor is 2.
    *
    * @param maxResetTimeout the upper bound of resetTimeout
    */
  def withExponentialBackoff(maxResetTimeout: FiniteDuration): AtomicCircuitBreakerLogic = {
    new AtomicCircuitBreakerLogic(maxFailures, callTimeout, resetTimeout, maxResetTimeout, 2.0)
  }

  /**
    * Holds reference to current state of CircuitBreaker - *access only via helper methods*
    */
  @volatile
  private[this] var _currentStateDoNotCallMeDirectly: State = AtomicClosed

  /**
    * Holds reference to current resetTimeout of CircuitBreaker - *access only via helper methods*
    */
  @volatile
  private[this] var _currentResetTimeoutDoNotCallMeDirectly: FiniteDuration = resetTimeout

  /**
    * Helper method for access to underlying state via Unsafe
    *
    * @param oldState Previous state on transition
    * @param newState Next state on transition
    * @return Whether the previous state matched correctly
    */
  @inline
  private[this] def swapState(oldState: State, newState: State): Boolean =
  Unsafe.instance.compareAndSwapObject(this, AbstractAtomicCircuitBreakerLogic.stateOffset, oldState, newState)

  /**
    * Helper method for accessing underlying state via Unsafe
    *
    * @return Reference to current state
    */
  @inline
  private[this] def currentState: State =
  Unsafe.instance.getObjectVolatile(this, AbstractAtomicCircuitBreakerLogic.stateOffset).asInstanceOf[State]

  /**
    * Helper method for updating the underlying resetTimeout via Unsafe
    */
  @inline
  private[this] def swapResetTimeout(oldResetTimeout: FiniteDuration, newResetTimeout: FiniteDuration): Boolean =
  Unsafe.instance.compareAndSwapObject(this, AbstractAtomicCircuitBreakerLogic.resetTimeoutOffset, oldResetTimeout, newResetTimeout)

  /**
    * Helper method for accessing to the underlying resetTimeout via Unsafe
    */
  @inline
  private[this] def currentResetTimeout: FiniteDuration =
  Unsafe.instance.getObjectVolatile(this, AbstractAtomicCircuitBreakerLogic.resetTimeoutOffset).asInstanceOf[FiniteDuration]

  /**
    * Mark a successful call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
    * caller Actor. In such a case, it is convenient to mark a successful call instead of using Future
    * via [[withCircuitBreaker]]
    */
  def succeed(): Unit = {
    currentState.callSucceeds()
  }

  /**
    * Mark a failed call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
    * caller Actor. In such a case, it is convenient to mark a failed call instead of using Future
    * via [[withCircuitBreaker]]
    */
  def fail(f: (FiniteDuration => Unit)): Unit = {

    // TODO Should we do a check "if !isTimerActive" ?
    currentState.callFails(f)
  }

  def shortCircuit(): Boolean = {
    currentState.shortCircuit()
  }

  private val mapToInternalState = Map(
    Closed -> AtomicClosed,
    Open -> AtomicOpen,
    HalfOpen -> AtomicHalfOpen
  )

  /**
    * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
    *
    * @param fromState State being transitioning from
    * @param toState State being transitioning from
    */
  override def transitionImpl(fromState: CircuitBreakerState, toState: CircuitBreakerState): Boolean = {
    val internalFromState = mapToInternalState(fromState)
    val internalToState = mapToInternalState(toState)
    val isTransitioned = swapState(internalFromState, internalToState)
    if (isTransitioned) internalToState.enter()
    // else some other thread already swapped state
    isTransitioned
  }

  /**
    * Internal state abstraction
    */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[ActorRef]

    def shortCircuit(): Boolean

    /**
      * Invoked when call succeeds
      *
      */
    def callSucceeds(): Unit

    /**
      * Invoked when call fails
      *
      */
    def callFails(f: (FiniteDuration => Unit)): Unit

    /**
      * Invoked on the transitioned-to state during transition.
      */
    def enter(): Unit
  }

  /**
    * Concrete implementation of AtomicClosed state
    */
  private object AtomicClosed extends AtomicInteger with State {

    /**
      * Implementation of shortCircuit, which simply returns false
      *
      * @return false
      */
    override def shortCircuit: Boolean = false

    /**
      * On successful call, the failure count is reset to 0
      *
      * @return
      */
    override def callSucceeds(): Unit = set(0)

    /**
      * On failed call, the failure count is incremented.  The count is checked against the configured maxFailures, and
      * the breaker is tripped if we have reached maxFailures.
      *
      * @return
      */
    override def callFails(f: (FiniteDuration => Unit)): Unit =
      if (incrementAndGet() == maxFailures) {
        tripBreaker(Closed)
        f(currentResetTimeout)
      }

    /**
      * On entry of this state, failure count and resetTimeout is reset.
      *
      * @return
      */
    override def enter(): Unit = {
      set(0)
      swapResetTimeout(currentResetTimeout, resetTimeout)
    }

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "AtomicClosed with failure count = " + get()
  }

  /**
    * Concrete implementation of half-open state
    */
  private object AtomicHalfOpen extends AtomicBoolean(true) with State {

    /**
      * Allows a single call through, during which all other callers fail-fast.  If the call fails, the breaker reopens.
      * If the call succeeds the breaker closes.
      *
      * @return true if already returned false once.
      */
    override def shortCircuit(): Boolean = !compareAndSet(true, false)

    /**
      * Reset breaker on successful call.
      *
      * @return
      */
    override def callSucceeds(): Unit = resetBreaker()

    /**
      * Reopen breaker on failed call.
      *
      * @return
      */
    override def callFails(f: (FiniteDuration => Unit)): Unit = {
      tripBreaker(HalfOpen)
      val nextResetTimeout = currentResetTimeout * exponentialBackoffFactor match {
        case f: FiniteDuration ⇒ f
        case _                 ⇒ currentResetTimeout
      }

      if (nextResetTimeout < maxResetTimeout)
        swapResetTimeout(currentResetTimeout, nextResetTimeout)

      f(currentResetTimeout)
    }

    /**
      * On entry, guard should be reset for that first call to get in
      *
      * @return
      */
    override def enter(): Unit = set(true)

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Half-AtomicOpen currently testing call for success = " + get()
  }

  /**
    * Concrete implementation of AtomicOpen state
    */
  private object AtomicOpen extends AtomicLong with State {

    /**
      * Fail-fast on any invocation
      *
      * @return true
      */
    override def shortCircuit(): Boolean = true

    /**
      * Calculate remaining duration until reset to inform the caller in case a backoff algorithm is useful
      *
      * @return duration to when the breaker will attempt a reset by transitioning to half-open
      */
    private def remainingDuration(): FiniteDuration = {
      val fromOpened = System.nanoTime() - get
      val diff = currentResetTimeout.toNanos - fromOpened
      if (diff <= 0L) Duration.Zero
      else diff.nanos
    }

    /**
      * No-op for open, calls are never executed so cannot succeed or fail
      *
      * @return
      */
    override def callSucceeds(): Unit = ()

    /**
      * No-op for open, calls are never executed so cannot succeed or fail
      *
      * @return
      */
    override def callFails(f: (FiniteDuration => Unit)): Unit = ()

    /**
      * On entering this state, schedule an attempted reset via [[akka.actor.Scheduler]] and store the entry time to
      * calculate remaining time before attempted reset.
      *
      * @return
      */
    override def enter(): Unit = set(System.nanoTime())

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "AtomicOpen"
  }

}