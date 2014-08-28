package io.sphere.graphite

import scala.language.reflectiveCalls

import java.util.concurrent.TimeUnit

import com.typesafe.config.{ConfigFactory, Config}

object MongoLockReporter extends App {
  val config = new Conf(ConfigFactory.load())

  println(s"Starting mongo locking reporting to ${config.graphite.host}:${config.graphite.port} with interval ${config.reportIntervalMs} ms...")
}

class Conf(c: Config) {
  val graphite = new {
    val host = c.getString("graphite.host")
    val port = c.getInt("graphite.port")
    val graphitePrefix = c.getString("graphite.prefix")
  }

  val mongo = new {
    val host = c.getString("mongo.host")
    val port = c.getInt("mongo.port")
  }

  val reportIntervalMs = c.getDuration("reportInterval", TimeUnit.MILLISECONDS)
}
