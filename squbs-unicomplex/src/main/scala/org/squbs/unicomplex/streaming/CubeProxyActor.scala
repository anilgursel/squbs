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

package org.squbs.unicomplex.streaming

import akka.actor._
import akka.http.scaladsl.model.HttpRequest
import org.squbs.filterchain.Filter

class CubeProxyActor(filterChain: Seq[Filter], serviceActor: ActorRef) extends Actor with ActorLogging {

  def receive: Receive = {
    case httpRequest: HttpRequest =>
      val responder = sender()
      val filterChainActor = context.actorOf(Props(classOf[FilterChainActor], filterChain, serviceActor, responder))
      filterChainActor ! httpRequest
  }
}


