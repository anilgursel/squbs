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

package org.squbs.unicomplex.streaming.dummysvc

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.{RequestContext, RouteResult, Route, Directives}
import akka.pattern.ask
import Directives._
import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import org.squbs.unicomplex._
import org.squbs.unicomplex.streaming.RouteDefinition
import Timeouts._

class DummySvc extends RouteDefinition with WebContext {
  def route: Route = path("msg" / Segment) {param =>
    get {ctx =>
      (context.actorOf(Props(classOf[DummyClient], ctx)) ? EchoMsg(param)).mapTo[RouteResult]
    }
  }
}

class Dummy2VersionedSvc extends RouteDefinition with WebContext {
  def route: Route = path("msg" / Segment) {param =>
    get {ctx =>
      (context.actorOf(Props(classOf[DummyClient], ctx)) ? EchoMsg(param)).mapTo[RouteResult]
    }
  }
}

class Dummy2Svc extends RouteDefinition with WebContext {
  def route: Route = path("msg" / Segment) {param =>
    get {ctx =>
      (context.actorOf(Props(classOf[DummyClient], ctx)) ? EchoMsg(param.reverse)).mapTo[RouteResult]
    }
  }
}

private class DummyClient(ctx: RequestContext) extends Actor with ActorLogging {

  private def receiveMsg(responder: ActorRef): Receive = {

    case AppendedMsg(appendedMsg) => context.actorSelection("/user/DummyCube/Prepender") ! EchoMsg(appendedMsg)

    case PrependedMsg(prependedMsg) => ctx.complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, prependedMsg)))
      context.stop(self)
  }

  def receive: Receive = {
    case msg: EchoMsg => context.actorSelection("/user/DummyCube/Appender") ! msg
      context.become(receiveMsg(sender()))
  }
}