import Versions._

name := "squbs-unicomplex"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalatest" %% "scalatest" % "2.2.1" % "test->*",
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-agent" % akkaV,
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.3",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
  "io.spray" %% "spray-can"     % sprayV,
  "io.spray" %% "spray-http"    % sprayV,
  "io.spray" %% "spray-routing-shapeless2" % sprayV,
  "io.spray" %% "spray-testkit" % sprayV % "test",
  "io.spray" %% "spray-client"  % sprayV % "test",
  "io.spray" %% "spray-json"    % "1.3.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)

org.scalastyle.sbt.ScalastylePlugin.Settings

(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "report/squbs-unicomplex")