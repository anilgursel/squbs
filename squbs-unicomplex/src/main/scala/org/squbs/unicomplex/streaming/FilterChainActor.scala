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

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import org.squbs.filterchain.Filter

import scala.concurrent.Future

class FilterChainActor (filterChain: Seq[Filter], serviceActor: ActorRef, responder: ActorRef) extends Actor with ActorLogging {

  import context.dispatcher
  override def receive: Receive = {

    case httpRequest: HttpRequest =>
      // TODO Check for abort status etc..
      filterChain.foldLeft(Future.successful(httpRequest)) {
        (httpRequestFuture, filter) =>
          httpRequestFuture flatMap {
            httpRequest => filter.processRequest(httpRequest)
          }
      }

      serviceActor ! httpRequest

    case httpResponse: HttpResponse =>
      // TODO Run response chain
      responder ! httpResponse

  }
}
