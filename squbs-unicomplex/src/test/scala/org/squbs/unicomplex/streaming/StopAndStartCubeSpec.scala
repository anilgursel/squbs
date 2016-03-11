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

import java.util.concurrent.TimeUnit

import akka.actor.{ActorIdentity, ActorSystem, Identify}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.squbs.lifecycle.GracefulStop
import org.squbs.unicomplex._

import scala.util.Try

object StopAndStartCubeSpec {
  val dummyJarsDir = getClass.getClassLoader.getResource("classpaths/streaming").getPath

  val classPaths = Array(
    "DummyCube",
    "DummyCubeSvc",
    "DummySvc"
  ) map (dummyJarsDir + "/" + _)

  val (_, port) = temporaryServerHostnameAndPort()

  val config = ConfigFactory.parseString(
    s"""
       |squbs {
       |  actorsystem-name = streaming-StopAndStartCubeSpec
       |  ${JMX.prefixConfig} = true
       |  experimental-mode-on = true
       |}
       |default-listener.bind-port = $port
    """.stripMargin
  )

  val boot = UnicomplexBoot(config)
    .createUsing {(name, config) => ActorSystem(name, config)}
    .scanComponents(classPaths)
    .initExtensions.start()
}

class StopAndStartCubeSpec extends TestKit(StopAndStartCubeSpec.boot.actorSystem)
with FlatSpecLike with Matchers with ImplicitSender with BeforeAndAfterAll {

  implicit val timeout: akka.util.Timeout =
    Try(System.getProperty("test.timeout").toLong) map { millis =>
      akka.util.Timeout(millis, TimeUnit.MILLISECONDS)
    } getOrElse Timeouts.askTimeout

  import Timeouts.awaitMax

  val port = system.settings.config getInt "default-listener.bind-port"

  override def afterAll() {
    Unicomplex(system).uniActor ! GracefulStop
  }

  "Unicomplex" should "be able to stop a cube" in {
    Unicomplex(system).uniActor ! StopCube("DummyCube")
    within(awaitMax) {
      expectMsg(Ack)
    }
    system.actorSelection("/user/DummyCube") ! Identify("hello")
    within(awaitMax) {
      val id = expectMsgType[ActorIdentity]
      id.ref should be(None)
    }
  }

  "Unicomplex" should "not be able to stop a stopped cube" in {
    Unicomplex(system).uniActor ! StopCube("DummyCube")
    expectNoMsg()
  }

  "Unicomplex" should "be able to start a cube" in {
    Unicomplex(system).uniActor ! StartCube("DummyCube")
    within(awaitMax) {
      expectMsg(Ack)
    }
    system.actorSelection("/user/DummyCube") ! Identify("hello")
    within(awaitMax) {
      val id = expectMsgType[ActorIdentity]
      id.ref should not be None
    }
  }

  "Unicomplex" should "not be able to start a running cube" in {
    Unicomplex(system).uniActor ! StartCube("DummyCube")
    expectNoMsg()
  }

}
