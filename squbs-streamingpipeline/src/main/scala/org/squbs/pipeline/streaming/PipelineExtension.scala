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

package org.squbs.pipeline.streaming

import akka.actor._
import akka.stream.FlowShape
import akka.stream.scaladsl.{GraphDSL, Flow}
import com.typesafe.config.ConfigObject

trait FlowFactory {

  def create: Flow[RequestContext, RequestContext, Unit]
}

class PipelineExtensionImpl(flowMap: Map[String, (PipelineFlow, Int)],
                            defaultInboundFlows: Option[Seq[String]],
                            defaultOutboundFlows: Option[Seq[String]]) extends Extension {

  def getFlows(pipelineSetting: PipelineSetting): (Option[PipelineFlow], Option[PipelineFlow]) = {

    val (inbound, outbound, defaultsOn) = pipelineSetting

    val inboundWithDefaults = if(defaultsOn getOrElse true) {
                                inbound getOrElse Seq.empty[String] ++ (defaultInboundFlows getOrElse Seq.empty[String])
                              } else {
                                inbound getOrElse Seq.empty[String]
                              }

    val outboundWithDefaults = if(defaultsOn getOrElse true) {
                                outbound getOrElse Seq.empty[String] ++ (defaultInboundFlows getOrElse Seq.empty[String])
                              } else {
                                outbound getOrElse Seq.empty[String]
                              }


    (buildPipeline(inboundWithDefaults), buildPipeline(outboundWithDefaults))
  }

  private def buildPipeline(flowNames: Seq[String]) = {

    // Get an ordered (based on order number specified in Config) Seq of Flows mentioned in flowNames
    val flows = flowMap.toSeq collect { case (name, flowWithOrder) if flowNames.contains(name) =>
      flowWithOrder
    } sortBy(_._2) map { case (flow, _) => flow }

    if(flowNames.size != flows.size) {
      throw new IllegalArgumentException(s"Pipeline contains unknown flows: [${flowNames.mkString(",")}]")
    }

    if(flows.size == 0) { None }
    else {
      Some(
        Flow.fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._

          def connectFlows(fs: Seq[FlowShape[RequestContext, RequestContext]], index: Int) {
            if(index + 1 < fs.size) {
              fs(index) ~> fs(index + 1)
              connectFlows(fs, index + 1)
            }
          }

          val flowShapes = flows map(b.add(_))
          connectFlows(flowShapes, 0)

          FlowShape(flowShapes(0).in, flowShapes.last.out)
        })
      )
    }
  }
}

object PipelineExtension extends ExtensionId[PipelineExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): PipelineExtensionImpl = {

    import ConfigHelper._
    import collection.JavaConversions._
    val flows = system.settings.config.root.toSeq collect {
      case (n, v: ConfigObject) if v.toConfig.getOptionalString("type").contains("squbs.pipelineflow") => (n, v.toConfig)
    }

    var flowMap = Map.empty[String, (Flow[RequestContext, RequestContext, Unit], Int)]
    flows foreach { case (name, config) =>
      val order = config.getInt("order")
      val factoryClassName = config.getString("factory")

      val flowFactory = Class.forName(factoryClassName).newInstance().asInstanceOf[FlowFactory]

      flowMap = flowMap + (name -> (flowFactory.create, order))
    }

    val inboundDefaults = system.settings.config.getOptionalStringList("squbs.pipeline.streaming.default-inbound-flows")
    val outboundDefaults = system.settings.config.getOptionalStringList("squbs.pipeline.streaming.default-outbound-flows")
    new PipelineExtensionImpl(flowMap, inboundDefaults, outboundDefaults)
  }

  override def lookup(): ExtensionId[_ <: Extension] = PipelineExtension

  /**
    * Java API: retrieve the Pipeline extension for the given system.
    */
  override def get(system: ActorSystem): PipelineExtensionImpl = super.get(system)
}