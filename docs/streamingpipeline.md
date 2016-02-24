// TODO Just the basics to show the usage...  Once the design is reviewed, then write the full doc..  
#Streaming Request/Response Pipeline

### Overview


### Streaming pipeline declaration

In `squbs-meta.conf`, you can specify the inbound/outbound flows for your service:

```
squbs-services = [
  {
    class-name = com.ebay.myapp.MyActor
    web-context = mypath
    inbound = [dummyFlow, trackingFlow]
    outbound = [headerFlow]
  }
]
```
* If there are no custom inbound/outbound flows for a squbs-service, just omit.
* Default inbound/outbound flows specified via the below configuration are automatically added to the lists unless `defaultFlowsOn` is set to `false`:

```
squbs.pipeline.streaming {
	default-inbound-flows = [defaultFlow1, defaultFlow2]
	default-outbound-flows = [defaultFlow3]
}
```


### Flow Configuration

A flow can be specified as below:

```
dummyFlow {
  type = squbs.pipelineflow
  factory = com.ebay.myorg.squbsmidverificationserv.svc.DummyFlow
  order = 1
}
```

* type: to idenfity the configuration as a `squbs.pipelineflow`.
* factory: the factor class to create the `Flow` from.
* order: the priority of this flow compared to the others.  If flowA has order 5, flowB has order 1, flowC has order 20, then the pipeline will look as `flowB ~> flowA ~> flowC`.

A sample `FlowFactory` looks like below:

```scala
class DummyFlow extends FlowFactory {

  override def create: Flow[RequestContext, RequestContext, Unit] = {

    Flow.fromGraph(GraphDSL.create() { implicit b =>

      val myDummyFlow = b.add(Flow[RequestContext].map { rc => rc.withAttributes(("myname", "Anil")) })

      FlowShape(myDummyFlow.in, myDummyFlow.out)
    })
  }
}
```