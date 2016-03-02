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

import akka.NotUsed
import akka.actor._
import akka.stream.scaladsl._
import com.typesafe.config.ConfigObject

trait PipelineFlowFactory {

  def create: BidiFlow[RequestContext, RequestContext, RequestContext, RequestContext, NotUsed]
}

class PipelineExtensionImpl(flowMap: Map[String, PipelineFlow],
                            defaultPreFlow: Option[String],
                            defaultPostFlow: Option[String]) extends Extension {

  def getFlow(pipelineSetting: PipelineSetting): Option[PipelineFlow] = {

    val (appFlow, defaultsOn) = pipelineSetting

    val pipelineFlowNames = (if(defaultsOn getOrElse true) { defaultPreFlow :: appFlow :: defaultPostFlow :: Nil }
                             else { appFlow :: Nil }) flatten

    if(pipelineFlowNames.isEmpty) { None }
    else { buildPipeline(pipelineFlowNames) }
  }

  private def buildPipeline(flowNames: Seq[String]) = {

    val flows = flowMap.toList collect { case (name, flow) if flowNames.contains(name) => flow }

    // TODO Hmm..  This will be encountered during materialization time, in other words, runtime..  Not startup..
    if(flowNames.size != flows.size) {
      throw new IllegalArgumentException(s"Pipeline contains unknown flows: [${flowNames.mkString(",")}]")
    }

    def connectFlows(flowList: List[PipelineFlow]): PipelineFlow = {

      flowList match {
        case head :: Nil => head
        case head :: tail => head.atop(connectFlows(tail))
      }
    }

    Some(connectFlows(flows))
  }
}

object PipelineExtension extends ExtensionId[PipelineExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): PipelineExtensionImpl = {

    import ConfigHelper._
    import collection.JavaConversions._
    val flows = system.settings.config.root.toSeq collect {
      case (n, v: ConfigObject) if v.toConfig.getOptionalString("type").contains("squbs.pipelineflow") => (n, v.toConfig)
    }

    var flowMap = Map.empty[String, PipelineFlow]
    flows foreach { case (name, config) =>
      val factoryClassName = config.getString("factory")

      val flowFactory = Class.forName(factoryClassName).newInstance().asInstanceOf[PipelineFlowFactory]

      flowMap = flowMap + (name -> flowFactory.create)
    }

    val pre = system.settings.config.getOptionalString("squbs.pipeline.streaming.defaults.pre-flow")
    val post = system.settings.config.getOptionalString("squbs.pipeline.streaming.defaults.post-flow")
    new PipelineExtensionImpl(flowMap, pre, post)
  }

  override def lookup(): ExtensionId[_ <: Extension] = PipelineExtension

  /**
    * Java API: retrieve the Pipeline extension for the given system.
    */
  override def get(system: ActorSystem): PipelineExtensionImpl = super.get(system)
}