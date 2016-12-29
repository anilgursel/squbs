# Timeout Flow

### Overview

Some stream use cases may require each message in a flow to be processed within a bounded time or to send a timeout failure message instead.  squbs introduces `TimeoutBidi` Akka Streams stage to add timeout functionality to streams.

### Dependency

Add the following dependency to your `build.sbt` or scala build file:

```
"org.squbs" %% "squbs-ext" % squbsVersion
```

### Usage

The timeout functionality is provided as a `BidiFlow` that can be connected to a flow via the `join` operator.  The timeout `BidiFlow` sends a `Try` to downstream:

   * If an output `msg` is provided by the wrapped flow within the timeout, then `Success(msg)` is passed down.
   * Otherwise, a `Failure(FlowTimeoutException())` is pushed to downstream.  


If there is no downstream demand, then no timeout message will be pushed down.  Moreover, no timeout check will be done unless there is downstream demand.  Accordingly, based on the demand scenarios, some messages might be send as a `Success` even after the timeout duration.

Timeout precision is 10ms to avoid unnecessary timer scheduling cycles.

The timeout feature is supported for flows that guarantee message ordering as well as for the ones that do not guarantee message ordering.  For the flow that do not guarantee message ordering, an `id` needs to be carried by the wrapped flow to uniquely identify each element.  While the usage of the `BidiFlow` is same for both types of flows, the creation of the `BidiFlow` is done via two different APIs.    

#### Flows with message order guarantee

`TimeoutBidiFlowOrdered` is used to create a timeout `BidiFlow` to wrap flows that do keep the order of messages.

##### Scala

```scala
val timeoutBidiFlow = TimeoutBidiFlowOrdered[String, String](20 milliseconds)
val flow = Flow[String].map(s => findAnEnglishWordThatStartWith(s))
Source("a" :: "b" :: "c" :: Nil)
  .via(timeoutBidiFlow.join(flow))
  .runWith(Sink.seq)
```      

##### Java

```java
final BidiFlow<String, String, String, Try<String>, NotUsed> timeoutBidiFlow =
    TimeoutBidiFlowOrdered.create(timeout);

final Flow<String, String, NotUsed> flow =
    Flow.<String>create().map(s -> findAnEnglishWordThatStartWith(s));
        
Source.from(Arrays.asList("a", "b", "c"))
    .via(timeoutBidiFlow.join(flow))
    .runWith(Sink.seq(), mat);
```   

#### Flows without message order guarantee

`TimeoutBidiFlowUnordered` is used to create a timeout `BidiFlow` to wrap flows that do not guarantee the order of messages.  To uniquely identify each element and its corresponding timing marks, an `id` of type `Long` is generated internally.  This `id` needs to be carried around by the wrapped flow.  Accordingly, the wrapped flow needs to be pulling/pushing a `Tuple2[Long, T]`.  The `id` type and the generator can be customized, please see  [Customizing Id type and Id generators](#customizing-id-type-and-id-generators) section.

##### Scala

```scala   
val timeoutBidiFlow = TimeoutBidiFlowUnordered[String, String](20 milliseconds)
val flow = Flow[(Long, String)].mapAsyncUnordered(10) { elem =>
  (ref ? elem).mapTo[(Long, String)]
}

Source("a" :: "b" :: "c" :: Nil)
  .via(timeoutBidiFlow.join(flow))
  .runWith(Sink.seq)
```

##### Java

```java
final BidiFlow<String, Tuple2<Long, String>, Tuple2<Long, String>, Try<String>, NotUsed> timeoutBidiFlow =
    TimeoutBidiFlowUnordered.create(timeout);
    
final Flow<Tuple2<Long, String>, Tuple2<Long, String>, NotUsed> flow =
    Flow.<Tuple2<Long, String>>create()
        .mapAsyncUnordered(10, elem -> ask(ref, elem, 5000))
        .map(elem -> (Tuple2<Long, String>)elem);

Source.from(Arrays.asList("a", "b", "c"))
    .via(timeoutBidiFlow.join(flow))
    .runWith(Sink.seq(), mat);
```

##### Customizing `Id` type and `Id` generators

Timeout flow feature defines default `Id` type as `Long` and uses a default `Id` generator.  However, it also allows to customize it.  You can use any type as an `Id` and provide a custom `Id` generator.  `Id` generator is just a function with return type of the `Id`.  Below is an example that uses `UUID` instead of `Long`:

###### Scala

```scala
val timeoutBidiFlow = TimeoutBidiFlowUnordered[String, String, UUID](timeout, () => UUID.randomUUID())

val flow = Flow[(UUID, String)].mapAsyncUnordered(10) { elem =>
      (ref ? elem).mapTo[(UUID, String)]
    }

Source("a" :: "b" :: "c" :: Nil)
  .via(timeoutBidiFlow.join(flow))
  .runWith(Sink.seq)
```

###### Java

```java
final BidiFlow<String, Tuple2<UUID, String>, Tuple2<UUID, String>, Try<String>, NotUsed> timeoutBidiFlow =
    TimeoutBidiFlowUnordered.create(timeout, () -> UUID.randomUUID());

final Flow<Tuple2<UUID, String>, Tuple2<UUID, String>, NotUsed> flow =
    Flow.<Tuple2<UUID, String>>create( )
        .mapAsyncUnordered(3, elem -> ask(ref, elem, 5000))
        .map(elem -> (Tuple2<UUID, String>)elem);

Source.from(Arrays.asList("a", "b", "c"))
    .via(timeoutBidiFlow.join(flow))
    .runWith(Sink.seq(), mat);
```