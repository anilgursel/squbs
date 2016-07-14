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

package org.squbs.unicomplex

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import org.squbs.lifecycle.GracefulStop

object ExternalConfigLoadSpec {

  System.setProperty("squbs.external-config-dir", getClass.getClassLoader.getResource("external-config").getPath)
  val boot = UnicomplexBoot { (name, config) => ActorSystem(name, config) }
    .scanResources()
    .initExtensions
    .start()
}

 class ExternalConfigLoadSpec extends TestKit(ExternalConfigLoadSpec.boot.actorSystem) with FlatSpecLike
with Matchers with BeforeAndAfterAll {

  override def afterAll() {
    System.clearProperty("squbs.external-config-dir")
    Unicomplex(system).uniActor ! GracefulStop
  }

  "UnicomplexBoot" should "first load reference and then application from external config dir" in {
    system.settings.config.getString("test.externalLoadConfigLoadSpec.key1") should be("value1FromApplicationDotConf")
    system.settings.config.getString("test.externalLoadConfigLoadSpec.key2") should be("value2FromReferenceDotConf")
  }
}
