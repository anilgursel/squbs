
package org.squbs.circuitbreaker

// TODO Update the license header of this file!
// Since it is mostly copied from Akka Contrib, removing the PayPal legal header.
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import akka.actor.{ActorSystem, Scheduler}
import akka.util.Unsafe

import scala.util.control.NoStackTrace
import java.util.concurrent.{Callable, CompletionStage, CopyOnWriteArrayList}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import scala.util.control.NonFatal
import scala.util.Success
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.pattern.{CircuitBreakerOpenException}

import scala.compat.java8.FutureConverters

/**
  * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
  */
object CircuitBreaker {

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
  def apply(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration)
           (implicit system: ActorSystem): CircuitBreaker =
    new CircuitBreaker(maxFailures, callTimeout, resetTimeout)
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
  def create(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration, system: ActorSystem): CircuitBreaker =
    apply(maxFailures, callTimeout, resetTimeout)(system)
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
class CircuitBreaker( maxFailures:              Int,
                      val callTimeout:          FiniteDuration,
                      resetTimeout:             FiniteDuration,
                      maxResetTimeout:          FiniteDuration,
                      exponentialBackoffFactor: Double)(implicit system: ActorSystem) extends AbstractCircuitBreaker {

  require(exponentialBackoffFactor >= 1.0, "factor must be >= 1.0")

  def this(maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration)(implicit system: ActorSystem) = {
    this(maxFailures, callTimeout, resetTimeout, 36500.days, 1.0)
  }

  /**
    * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
    * The default exponential backoff factor is 2.
    *
    * @param maxResetTimeout the upper bound of resetTimeout
    */
  def withExponentialBackoff(maxResetTimeout: FiniteDuration): CircuitBreaker = {
    new CircuitBreaker(maxFailures, callTimeout, resetTimeout, maxResetTimeout, 2.0)
  }

  /**
    * Holds reference to current state of CircuitBreaker - *access only via helper methods*
    */
  @volatile
  private[this] var _currentStateDoNotCallMeDirectly: State = Closed

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
  Unsafe.instance.compareAndSwapObject(this, AbstractCircuitBreaker.stateOffset, oldState, newState)

  /**
    * Helper method for accessing underlying state via Unsafe
    *
    * @return Reference to current state
    */
  @inline
  private[this] def currentState: State =
  Unsafe.instance.getObjectVolatile(this, AbstractCircuitBreaker.stateOffset).asInstanceOf[State]

  /**
    * Helper method for updating the underlying resetTimeout via Unsafe
    */
  @inline
  private[this] def swapResetTimeout(oldResetTimeout: FiniteDuration, newResetTimeout: FiniteDuration): Boolean =
  Unsafe.instance.compareAndSwapObject(this, AbstractCircuitBreaker.resetTimeoutOffset, oldResetTimeout, newResetTimeout)

  /**
    * Helper method for accessing to the underlying resetTimeout via Unsafe
    */
  @inline
  private[this] def currentResetTimeout: FiniteDuration =
  Unsafe.instance.getObjectVolatile(this, AbstractCircuitBreaker.resetTimeoutOffset).asInstanceOf[FiniteDuration]

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
  def fail(): Unit = {
    currentState.callFails()
  }

  def shortCircuit(): Boolean = {
    currentState.shortCircuit()
  }

  /**
    * Return true if the internal state is Closed. WARNING: It is a "power API" call which you should use with care.
    * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
    * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
    * manage the state yourself.
    */
  def isClosed: Boolean = {
    currentState == Closed
  }

  /**
    * Return true if the internal state is Open. WARNING: It is a "power API" call which you should use with care.
    * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
    * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
    * manage the state yourself.
    */
  def isOpen: Boolean = {
    currentState == Open
  }

  /**
    * Return true if the internal state is HalfOpen. WARNING: It is a "power API" call which you should use with care.
    * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
    * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
    * manage the state yourself.
    */
  def isHalfOpen: Boolean = {
    currentState == HalfOpen
  }

  /**
    * Adds a callback to execute when circuit breaker opens
    *
    * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onOpen(callback: ⇒ Unit): CircuitBreaker = onOpen(new Runnable { def run = callback })

  /**
    * Java API for onOpen
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onOpen(callback: Runnable): CircuitBreaker = {
    Open addListener callback
    this
  }

  /**
    * Adds a callback to execute when circuit breaker transitions to half-open
    * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onHalfOpen(callback: ⇒ Unit): CircuitBreaker = onHalfOpen(new Runnable { def run = callback })

  /**
    * JavaAPI for onHalfOpen
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onHalfOpen(callback: Runnable): CircuitBreaker = {
    HalfOpen addListener callback
    this
  }

  /**
    * Adds a callback to execute when circuit breaker state closes
    *
    * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onClose(callback: ⇒ Unit): CircuitBreaker = onClose(new Runnable { def run = callback })

  /**
    * JavaAPI for onClose
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onClose(callback: Runnable): CircuitBreaker = {
    Closed addListener callback
    this
  }

  /**
    * Retrieves current failure count.
    *
    * @return count
    */
  private def currentFailureCount: Int = Closed.get

  /**
    * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
    *
    * @param fromState State being transitioning from
    * @param toState State being transitioning from
    */
  private def transition(fromState: State, toState: State): Unit = {
    if (swapState(fromState, toState))
      toState.enter()
    // else some other thread already swapped state
  }

  /**
    * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
    *
    * @param fromState State we're coming from (Closed or Half-Open)
    */
  private def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
    * Resets breaker to a closed state.  This is valid from an Half-Open state only.
    *
    */
  private def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
    * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
    *
    */
  private def attemptReset(): Unit = transition(Open, HalfOpen)

  private val timeoutFuture = Future.failed(new TimeoutException("Circuit Breaker Timed out.") with NoStackTrace)

  /**
    * Internal state abstraction
    */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[Runnable]

    /**
      * Add a listener function which is invoked on state entry
      *
      * @param listener listener implementation
      */
    def addListener(listener: Runnable): Unit = listeners add listener

    /**
      * Test for whether listeners exist
      *
      * @return whether listeners exist
      */
    private def hasListeners: Boolean = !listeners.isEmpty

    /**
      * Notifies the listeners of the transition event via a Future executed in implicit parameter ExecutionContext
      *
      * @return Promise which executes listener in supplied [[scala.concurrent.ExecutionContext]]
      */
    protected def notifyTransitionListeners() {
      if (hasListeners) {
        val iterator = listeners.iterator
        while (iterator.hasNext) {
          val listener = iterator.next
          //          executor.execute(listener)
        }
      }
    }

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
    def callFails(): Unit

    /**
      * Invoked on the transitioned-to state during transition.  Notifies listeners after invoking subclass template
      * method _enter
      *
      */
    final def enter(): Unit = {
      _enter()
      notifyTransitionListeners()
    }

    /**
      * Template method for concrete traits
      *
      */
    def _enter(): Unit
  }

  /**
    * Concrete implementation of Closed state
    */
  private object Closed extends AtomicInteger with State {

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
    override def callFails(): Unit = if (incrementAndGet() == maxFailures) tripBreaker(Closed)

    /**
      * On entry of this state, failure count and resetTimeout is reset.
      *
      * @return
      */
    override def _enter(): Unit = {
      set(0)
      swapResetTimeout(currentResetTimeout, resetTimeout)
    }

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Closed with failure count = " + get()
  }

  /**
    * Concrete implementation of half-open state
    */
  private object HalfOpen extends AtomicBoolean(true) with State {

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
    override def callFails(): Unit = tripBreaker(HalfOpen)

    /**
      * On entry, guard should be reset for that first call to get in
      *
      * @return
      */
    override def _enter(): Unit = set(true)

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Half-Open currently testing call for success = " + get()
  }

  /**
    * Concrete implementation of Open state
    */
  private object Open extends AtomicLong with State {

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
    override def callFails(): Unit = ()

    /**
      * On entering this state, schedule an attempted reset via [[akka.actor.Scheduler]] and store the entry time to
      * calculate remaining time before attempted reset.
      *
      * @return
      */
    override def _enter(): Unit = {
      set(System.nanoTime())
      import system.dispatcher
      system.scheduler.scheduleOnce(currentResetTimeout) {
        attemptReset()
      }
      val nextResetTimeout = currentResetTimeout * exponentialBackoffFactor match {
        case f: FiniteDuration ⇒ f
        case _                 ⇒ currentResetTimeout
      }

      if (nextResetTimeout < maxResetTimeout)
        swapResetTimeout(currentResetTimeout, nextResetTimeout)
    }

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Open"
  }

}
