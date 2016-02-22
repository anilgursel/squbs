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

package org.squbs.filterchain

import akka.actor._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.typesafe.config.ConfigObject

import scala.concurrent.Future

class FilterC

abstract class Filter {

  // TODO Change these to RequestContext and ResponseContext
  def processRequest(httpRequest: HttpRequest)(implicit context: ActorRefFactory): Future[HttpRequest] = {
    import context.dispatcher
    Future {
      httpRequest
    }
  }

  def processResponse(httpResponse: HttpResponse)(implicit context: ActorRefFactory): Future[HttpResponse] = {
    import context.dispatcher
    Future {
      httpResponse
    }
  }

}

class FilterChainExtensionImpl(filterMap: Map[String, (Filter, Int)]) extends Extension {

  def getFilterChain(filterNames: Seq[String]): Option[Seq[Filter]] = {

    // TODO if defaultsOn, add default to the filterNames, and sort accordingly..

    // Get sorted (based on "order" config key) sequence of Filter instances for the provided filter names
    val filterChain = filterMap.toSeq collect { case (name, t) if filterNames.contains(name) => t } sortWith {
      _._2 < _._2
    } map { case (filter, _) => filter }

    // TODO Right message..
    if(filterChain.size != filterNames.size) throw new IllegalArgumentException("dsfdsfasd")

    // TODO Consider options where it being empty etc..  and detault etc.. Return None, in such situations..
    Some(filterChain)
  }
}

object FilterChainExtension extends ExtensionId[FilterChainExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): FilterChainExtensionImpl = {

    import ConfigHelper._
    import collection.JavaConversions._
    val filters = system.settings.config.root.toSeq collect {
      case (n, v: ConfigObject) if v.toConfig.getOptionalString("type").contains("squbs.filter") => (n, v.toConfig)
    }

    var filterMap = Map.empty[String, (Filter, Int)]
    filters foreach { case (name, config) =>
      val order = config.getInt("order")
      val className = config.getString("class")

      val filter = Class.forName(className).newInstance().asInstanceOf[Filter]

      filterMap = filterMap + (name -> (filter, order))
    }

    new FilterChainExtensionImpl(filterMap)
  }

  override def lookup(): ExtensionId[_ <: Extension] = FilterChainExtension

  /**
    * Java API: retrieve the FilterChain extension for the given system.
    */
  override def get(system: ActorSystem): FilterChainExtensionImpl = super.get(system)
}
