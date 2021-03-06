import sbtdocker.ImageName
import sbtdocker.Plugin.DockerKeys._
import sbt.Keys._

name := "mongo-metrics-reporter"
description := "Application publishes mongo server stats to Graphite or InfluxDb server."

organization := "de.commercetools"
version := "2.7-SNAPSHOT"

scalaVersion := "2.11.8"
scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "com.netflix.rxjava" % "rxjava-scala" % "0.20.7",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.mongodb" %% "casbah" % "2.8.2",
  "com.typesafe" % "config" % "1.2.1",
  "org.influxdb" % "influxdb-java" % "2.1",
  "net.ceedubs" %% "ficus" % "1.1.2"
)

mainClass in (Compile, packageBin) := Some("de.commercetools.graphite.MongoMetricsReporter")

dockerSettings

docker <<= docker.dependsOn(Keys.`package`.in(Compile, packageBin))

dockerfile in docker <<= (artifactPath.in(Compile, packageBin), fullClasspath in (Compile), mainClass.in(Compile, packageBin)) map {
  case (jarFile, cp, Some(mainClass)) =>
    new sbtdocker.Dockerfile {
      from("develar/java:8u45")
      val files = cp.files.reverse.map { file =>
        val target = "/app/" + file.getName
        add(file, target)
        target
      }
      val classpathString = files.mkString(":")
      expose(80)
      entryPoint("java", "-cp", classpathString, mainClass)
    }
  case (_, _, None) =>
    sys.error("Expected exactly one main class")
}

imageName in docker := ImageName(namespace = Some("tenshi"), repository = "mongo-metrics-reporter")
