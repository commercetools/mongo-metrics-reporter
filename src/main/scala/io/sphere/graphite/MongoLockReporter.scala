package io.sphere.graphite

import com.typesafe.config.{ConfigFactory, Config}

object MongoLockReporter extends App {
  val config = ConfigFactory.load()
  val graphiteHost = config.getString("graphite.host")
  val graphitePort = config.getInt("graphite.port")
  val graphitePrefix = config.getInt("graphite.prefix")


  println(s"Starting graphite reporting to $graphiteHost:$graphitePort...")

}
