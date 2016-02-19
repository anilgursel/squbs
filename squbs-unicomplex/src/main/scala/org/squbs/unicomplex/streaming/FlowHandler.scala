/*
 *  Copyright 2015 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.squbs.unicomplex.streaming

import akka.actor.ActorSystem
import akka.agent.Agent
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{StatusCodes, HttpHeader, HttpResponse, HttpRequest}
import akka.stream.FlowShape
import akka.stream.scaladsl._
import akka.util.Timeout
import org.squbs.unicomplex.ActorWrapper
import scala.concurrent.Future
import akka.pattern._

object Handler {

  def apply(routes: Agent[Seq[(Path, ActorWrapper)]])(implicit system: ActorSystem): Handler = {
    new Handler(routes)
  }
}

class Handler(routes: Agent[Seq[(Path, ActorWrapper)]])(implicit system: ActorSystem) {

  val akkaHttpConfig = system.settings.config.getConfig("akka.http")

  def flow: Flow[HttpRequest, HttpResponse, Any] = dispatchFlow

  // TODO FIX ME.
  val inbound: Flow[ContextHolder, ContextHolder, Any] = Flow[ContextHolder].map(holder => holder.copy(ctx = holder.ctx.withAttributes("key1" -> "value1").addRequestHeaders(RawHeader("reqHeader", "reqHeaderValue"))))

  val outbound: Flow[RequestContext, RequestContext, Any] = Flow[RequestContext].map {
    ctx =>
      val newResp = ctx.response.map(r => r.copy(headers = r.headers ++ RequestContext.attributes2Headers(ctx.attributes) ++ ctx.request.headers))
      ctx.copy(response = newResp)
  }

  import system.dispatcher

  def normPath(path: Path): Path = if (path.startsWithSlash) path.tail else path

  // TODO FIX ME - Discuss with Akara and Qian.
  // I am not sure what exactly the timeout should be set to.  One option is to use akka.http.server.request-timeout; however,
  // that will be available in the next release: https://github.com/akka/akka/issues/16819.
  // Even then, I am not sure if that would be the right value..
  import scala.concurrent.duration._
  implicit val askTimeOut: Timeout = 5 seconds
  private def asyncHandler(actorWrapper: ActorWrapper) = (req: HttpRequest) => (actorWrapper.actor ? req).mapTo[HttpResponse]

  val dispatchFlow: Flow[HttpRequest, HttpResponse, Any] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip = b.add(Zip[HttpRequest, Int]())
      val broadcast = b.add(Broadcast[ContextHolder](2))
      val merge = b.add(Merge[RequestContext](2))
      val pre = b.add(Flow[(HttpRequest, Int)].map {
        case (request, id) =>
          routes() find { entry =>
            normPath(request.uri.path).startsWith(entry._1)
          } match {
            case Some((_, aw)) => ContextHolder(RequestContext(request, id), Some(asyncHandler(aw)))
            case _ => ContextHolder(RequestContext(request, id), None)
          }
      })

      val goodFilter = Flow[ContextHolder].filter(_.transformer.isDefined)
      val badFilter = Flow[ContextHolder].filter(_.transformer.isEmpty)

      val coreFlow = Flow[ContextHolder].mapAsync(akkaHttpConfig.getInt("server.pipelining-limit")) {
        ch => ch.transformer.get.apply(ch.ctx.request).map(resp => ch.ctx.copy(response = Option(resp)))
      }

      val respFlow = b.add(Flow[RequestContext].map(
        _.response.getOrElse(HttpResponse(StatusCodes.NotFound, entity = StatusCodes.NotFound.defaultMessage))))

      object RequestContextOrdering extends Ordering[RequestContext] {
        def compare(a:RequestContext, b:RequestContext) = b.id compare a.id
      }

      val orderingStage = b.add(new OrderingStage[RequestContext, Int](0, (x: Int) => x + 1, (rc: RequestContext) => rc.id)(RequestContextOrdering))

      Source.fromIterator(() => Iterator.from(0)) ~> zip.in1
      zip.out ~> pre ~> broadcast ~> goodFilter ~> inbound ~> coreFlow ~> outbound  ~> merge ~> orderingStage ~> respFlow
                        broadcast ~> badFilter.map(_.ctx)                           ~> merge

      // expose ports
      FlowShape(zip.in0, respFlow.out)
    })
}

case class ContextHolder(ctx: RequestContext, transformer: Option[HttpRequest => Future[HttpResponse]])


case class ErrorLog(error : Throwable)
// TODO Re-visit this..  Just copying for now..
case class RequestContext(request: HttpRequest,
                          id: Int = 1,
                          response: Option[HttpResponse] = None,
                          attributes: Map[String, Any] = Map.empty,
                          error : Option[ErrorLog] = None) {

  def withAttributes(attributes: (String, Any)*): RequestContext = {
    this.copy(attributes = this.attributes ++ attributes)
  }

  def attribute[T](key: String): Option[T] = {
    attributes.get(key) match {
      case None => None
      case Some(null) => None
      case Some(value) => Some(value.asInstanceOf[T])
    }
  }

  def addRequestHeaders(headers: HttpHeader*): RequestContext = {
    copy(request = request.copy(headers = request.headers ++ headers))
  }

  def addResponseHeaders(headers: HttpHeader*): RequestContext = {
    response.fold(this) {
      resp => copy(response = Option(resp.copy(headers = request.headers ++ headers)))
    }
  }
}

object RequestContext {

  implicit def attributes2Headers(attributes: Map[String, Any]): Seq[HttpHeader] = {
    attributes.toSeq.map {
      attr => RawHeader(attr._1, String.valueOf(attr._2))
    }
  }
}