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

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, BeforeAndAfterAll, AsyncFlatSpec, Matchers}
import org.squbs.endpoint.{Endpoint, EndpointResolver, EndpointResolverRegistry}
import org.squbs.env.Environment
import org.squbs.testkit.Timeouts._
import org.squbs.unicomplex.JMX

import scala.concurrent.{Future, Await}
import scala.util.{Success, Try}

object ClientFlowSpec {

  implicit val system = ActorSystem("ClientFlowSpec")
  implicit val materializer = ActorMaterializer()

  EndpointResolverRegistry(system).register(new EndpointResolver {
    override def name: String = "LocalhostEndpointResolver"

    override def resolve(svcName: String, env: Environment): Option[Endpoint] = svcName match {
      case "hello" => Some(Endpoint(s"http://localhost:$port"))
      case _ => None
    }
  })

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
}

class ClientFlowSpec  extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  import ClientFlowSpec._

  override def afterAll: Unit = {
    serverBinding.unbind() map {_ => system.terminate()}
  }

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

  it should "throw HttpClientEndpointNotExistException if it cannot resolve the client" in {
    an [HttpClientEndpointNotExistException] should be thrownBy {
      ClientFlow[Int]("cannotResolve")
    }
  }
}

object ClientConfigurationSpec {

  val defaultConfig = ConfigFactory.load()

  val appConfig = ConfigFactory.parseString(
    s"""
      |squbs {
      |  ${JMX.prefixConfig} = true
      |}
      |
      |sampleClient {
      | type = squbs.httpclient
      |
      | akka.http {
      |   host-connection-pool {
      |     max-connections = 987
      |     max-retries = 123
      |
      |     client = {
      |       connecting-timeout = 123 ms
      |     }
      |   }
      | }
      |}
      |
      |sampleClient2 {
      | type = squbs.httpclient
      |
      | akka.http.host-connection-pool {
      |   max-connections = 666
      | }
      |}
      |
      |noOverrides {
      | type = squbs.httpclient
      |}
      |
      |noType {
      |
      | akka.http.host-connection-pool {
      |   max-connections = 987
      |   max-retries = 123
      | }
      |}
      |
      |passedAsParameter {
      | type = squbs.httpclient
      |
      | akka.http.host-connection-pool {
      |   max-connections = 111
      | }
      |}
      |
    """.stripMargin)


  implicit val system = ActorSystem("ClientConfigurationSpec", appConfig.withFallback(defaultConfig))
  implicit val materializer = ActorMaterializer()

  EndpointResolverRegistry(system).register(new EndpointResolver {
    override def name: String = "LocalhostEndpointResolver"

    override def resolve(svcName: String, env: Environment): Option[Endpoint] = Some(Endpoint(s"http://localhost:1234"))
  })

}

class ClientConfigurationSpec extends FlatSpec with Matchers {

  import ClientConfigurationSpec._
  import scala.concurrent.duration._

  it should "give priority to client specific configuration" in {
    ClientFlow("sampleClient")
    assertJmxValue("sampleClient", "MaxConnections",
      appConfig.getInt("sampleClient.akka.http.host-connection-pool.max-connections"))
    assertJmxValue("sampleClient", "MaxRetries",
      appConfig.getInt("sampleClient.akka.http.host-connection-pool.max-retries"))
    assertJmxValue("sampleClient", "ConnectionPoolIdleTimeout",
      Duration(defaultConfig.getString("akka.http.host-connection-pool.idle-timeout")).toString)
    assertJmxValue("sampleClient", "ConnectingTimeout",
      Duration(appConfig.getString("sampleClient.akka.http.host-connection-pool.client.connecting-timeout")).toString)
  }

  it should "fallback to default values if no client specific configuration is provided" in {
    ClientFlow("noSpecificConfiguration")
    assertDefaults("noSpecificConfiguration")
  }

  it should "fallback to default values if client configuration does not override any properties" in {
    ClientFlow("noOverrides")
    assertDefaults("noOverrides")
  }

  it should "ignore client specific configuration if type is not set to squbs.httpclient" in {
    ClientFlow("noType")
    assertDefaults("noType")
  }

  it should "let configuring multiple clients" in {
    ClientFlow("sampleClient2")

    assertJmxValue("sampleClient", "MaxConnections",
      appConfig.getInt("sampleClient.akka.http.host-connection-pool.max-connections"))
    assertJmxValue("sampleClient", "MaxRetries",
      appConfig.getInt("sampleClient.akka.http.host-connection-pool.max-retries"))
    assertJmxValue("sampleClient", "ConnectionPoolIdleTimeout",
      Duration(defaultConfig.getString("akka.http.host-connection-pool.idle-timeout")).toString)
    assertJmxValue("sampleClient", "ConnectingTimeout",
      Duration(appConfig.getString("sampleClient.akka.http.host-connection-pool.client.connecting-timeout")).toString)

    assertJmxValue("sampleClient2", "MaxConnections",
      appConfig.getInt("sampleClient2.akka.http.host-connection-pool.max-connections"))
    assertJmxValue("sampleClient2", "MaxRetries", defaultConfig.getInt("akka.http.host-connection-pool.max-retries"))
    assertJmxValue("sampleClient2", "ConnectionPoolIdleTimeout",
      Duration(defaultConfig.getString("akka.http.host-connection-pool.idle-timeout")).toString)
  }

  it should "configure even if not present in conf file" in {
    ClientFlow("notInConfig")
    assertDefaults("notInConfig")
  }

  it should "give priority to passed in settings" in {
    val MaxConnections = 8778
    val cps = ConnectionPoolSettings(system.settings.config).withMaxConnections(MaxConnections)
    ClientFlow("passedAsParameter", settings = Some(cps))
    assertJmxValue("passedAsParameter", "MaxConnections", MaxConnections)
  }

  def assertJmxValue(clientName: String, key: String, expectedValue: Any) = {
    val oName = ObjectName.getInstance(s"org.squbs.configuration.${system.name}:type=squbs.httpclient,name=$clientName")
    val actualValue = ManagementFactory.getPlatformMBeanServer.getAttribute(oName, key)
    actualValue shouldEqual expectedValue
  }

  private def assertDefaults(clientName: String) = {
    assertJmxValue(clientName, "MaxConnections", defaultConfig.getInt("akka.http.host-connection-pool.max-connections"))
    assertJmxValue(clientName, "MaxRetries", defaultConfig.getInt("akka.http.host-connection-pool.max-retries"))
    assertJmxValue(clientName, "ConnectionPoolIdleTimeout",
      Duration(defaultConfig.getString("akka.http.host-connection-pool.idle-timeout")).toString)
  }
}
