scalaVersion in ThisBuild := "2.12.3"

name := "pubsub"

organization in ThisBuild := "org.squbs.samples"

version in ThisBuild := "0.10.0-SNAPSHOT"

publishArtifact := false

checksums in ThisBuild := Nil

fork in ThisBuild := true

lazy val pubsubsvc = project

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)