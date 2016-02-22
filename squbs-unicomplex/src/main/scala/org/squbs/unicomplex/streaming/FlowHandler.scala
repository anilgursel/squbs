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
import akka.pattern._

object Handler {

  def apply(routes: Agent[Seq[(Path, ActorWrapper)]])(implicit system: ActorSystem): Handler = {
    new Handler(routes)
  }
}

class Handler(routes: Agent[Seq[(Path, ActorWrapper)]])(implicit system: ActorSystem) {

  val akkaHttpConfig = system.settings.config.getConfig("akka.http")

  def flow: Flow[HttpRequest, HttpResponse, Any] = dispatchFlow

  import system.dispatcher

  def normPath(path: Path): Path = if (path.startsWithSlash) path.tail else path

  def pathMatch(path: Path, target: Path): Boolean = {
    if(path.length < target.length) false
    else {
      def innerMatch(path: Path, target:Path):Boolean = {
        if (target.isEmpty) true
        else target.head.equals(path.head) match {
          case true => innerMatch(path.tail, target.tail)
          case _ => false
        }
      }
      innerMatch(path, target)
    }
  }

  // TODO FIX ME - Discuss with Akara and Qian.
  // I am not sure what exactly the timeout should be set to.  One option is to use akka.http.server.request-timeout; however,
  // that will be available in the next release: https://github.com/akka/akka/issues/16819.
  // Even then, I am not sure if that would be the right value..
  import scala.concurrent.duration._
  implicit val askTimeOut: Timeout = 5 seconds
  private def asyncHandler(actorWrapper: ActorWrapper) = (req: HttpRequest) => (actorWrapper.actor ? req).mapTo[HttpResponse]

  val notFoundHttpResponse = HttpResponse(StatusCodes.NotFound, entity = StatusCodes.NotFound.defaultMessage)

  lazy val routeFlow =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val (paths, actorWrappers) = routes() unzip
      val pathExtractor = (t: (RequestContext, Int)) => normPath(t._1.request.uri.path)
      val pathMatcher = (p1: Path, p2: Path) => pathMatch(p1, p2)

      val merge = b.add(Merge[(RequestContext, Int)](actorWrappers.size + 1))
      val rss = b.add(RouteSelectorStage(paths, pathExtractor, pathMatcher))
      actorWrappers.zipWithIndex foreach { case (aw, i) =>
        val routeFlow = b.add(Flow[(RequestContext, Int)].mapAsync(akkaHttpConfig.getInt("server.pipelining-limit")) {
          case(rc, id) => asyncHandler(aw)(rc.request) map (httpResponse => (rc.copy(response = Option(httpResponse)), id))
        })
        rss.out(i) ~> routeFlow ~> merge
      }

      val notFound = b.add(Flow[(RequestContext, Int)].map {
        case(rc, id) => (rc.copy(response = Option(notFoundHttpResponse)), id)
      })

      // Last output port is for 404
      rss.out(actorWrappers.size) ~> notFound ~> merge

      FlowShape(rss.in, merge.out)
  })

  lazy val dispatchFlow: Flow[HttpRequest, HttpResponse, Any] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // Wraps HttpRequest in a RequestContext
      val createRequestContext = b.add(Flow[(HttpRequest, Int)].map {
        case (httpRequest, id) => (RequestContext(httpRequest), id)
      })

      object TupleOrdering extends Ordering[(RequestContext, Int)] {
        def compare(t1: (RequestContext, Int), t2: (RequestContext, Int)) = t1._2 compare t2._2
      }

      val orderingStage = b.add(new OrderingStage[(RequestContext, Int), Int](0, (x: Int) => x + 1, (t: (RequestContext, Int)) => t._2)(TupleOrdering))

      val responseFlow = b.add(Flow[(RequestContext, Int)].map { case (rc, _) =>
        rc.response getOrElse notFoundHttpResponse // TODO This actually might be a 500..
      })

      val zip = b.add(Zip[HttpRequest, Int]())

      // Generate id for each request to order requests for  Http Pipelining
      Source.fromIterator(() => Iterator.from(0)) ~> zip.in1
      zip.out ~> createRequestContext ~> routeFlow ~> orderingStage ~> responseFlow

      // expose ports
      FlowShape(zip.in0, responseFlow.out)
    })
}

// TODO The structure of this needs to be re-visited.
case class RequestContext(request: HttpRequest,
                          response: Option[HttpResponse] = None,
                          attributes: Map[String, Any] = Map.empty) {

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