import sbtdocker.Plugin.DockerKeys._
import sbt.Keys._

name := "mongodb-graphite-lock-stats"

organization := "io.sphere"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "com.netflix.rxjava" % "rxjava-scala" % "0.18.3",
  "org.reactivemongo" %% "reactivemongo" % "0.10.0",
  "org.scalaz" %% "scalaz-core" % "7.1.0",
  "com.typesafe" % "config" % "1.2.1"
)

mainClass in (Compile, packageBin) := Some("io.sphere.graphite.MongoLockReporter")

dockerSettings

docker <<= docker.dependsOn(Keys.`package`.in(Compile, packageBin))

dockerfile in docker <<= (artifactPath.in(Compile, packageBin), fullClasspath in (Compile), mainClass.in(Compile, packageBin)) map {
  case (jarFile, cp, Some(mainClass)) =>
    new sbtdocker.Dockerfile {
      from("dockerfile/java")
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