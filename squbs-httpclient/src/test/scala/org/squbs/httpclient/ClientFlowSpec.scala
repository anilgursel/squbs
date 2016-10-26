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

package org.squbs.httpclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.squbs.endpoint.{Endpoint, EndpointResolver, EndpointResolverRegistry}
import org.squbs.env.Environment
import org.squbs.testkit.Timeouts._

import scala.concurrent.{Future, Await}
import scala.util.{Success, Try}

class ClientFlowSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system = ActorSystem("ClientFlowSpec")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher
  import akka.http.scaladsl.server.Directives._

  val route =
    path("hello") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "Hello World!"))
      }
    }

  val serverBinding = Await.result(Http().bindAndHandle(route, "localhost", 0), awaitMax)
  val port = serverBinding.localAddress.getPort

  override def afterAll: Unit = {
    serverBinding.unbind() map {_ => system.terminate()}
  }

  EndpointResolverRegistry(system).register(new EndpointResolver {
    override def name: String = "LocalhostEndpointResolver"

    override def resolve(svcName: String, env: Environment): Option[Endpoint] = svcName match {
      case "hello" => Some(Endpoint(s"http://localhost:$port"))
      case _ => None
    }
  })

  it should "make a call to Hello Service" in {
    val clientFlow = ClientFlow[Int]("hello")

    val responseFuture: Future[(Try[HttpResponse], Int)] =
      Source.single(HttpRequest(uri = "/hello") -> 42)
        .via(clientFlow)
        .runWith(Sink.head)

    val (Success(response), _) = Await.result(responseFuture, awaitMax)
    response.status should be (StatusCodes.OK)
    val entity = response.entity.dataBytes.runFold(ByteString(""))(_ ++ _) map(_.utf8String)
    entity map { e => e shouldEqual ("Hello World!") }

  }

  it should "not resolve an invalid client" in {

  }
}
