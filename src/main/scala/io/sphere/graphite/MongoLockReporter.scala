package io.sphere.graphite

import java.util.concurrent.TimeUnit
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import reactivemongo.core.actors.Close
import scala.concurrent.ExecutionContext.Implicits.global
import reactivemongo.core.commands.Status
import FutureHelper._

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MongoLockReporter extends App {
  val config = new Conf(ConfigFactory.load())

  init()

  def init() = {
    println(s"Starting mongo locking reporting to ${config.graphite.host}:${config.graphite.port} with interval ${config.reportIntervalMs} ms...")

    val driver = new MongoDriver
    val connection = driver.connection(config.mongo.host :: Nil)

    val adminDb = connection("admin")

    println(adminDb.command(Status().command).awaitForever)
    closeDriver(driver)
  }

  def closeDriver(driver: MongoDriver): Unit = {
    driver.connections.foreach(_.mongosystem ! Close)
    driver.close()
  }
}

object FutureHelper {
  implicit class FutureOps[T](f: Future[T]) {
    def awaitForever = Await.result(f, Duration.Inf)
  }
}

class Conf(c: Config) {
  object graphite {
    val host = c.getString("graphite.host")
    val port = c.getInt("graphite.port")
    val graphitePrefix = c.getString("graphite.prefix")
  }

  object mongo {
    val host = c.getString("mongo.host")
    val port = c.getInt("mongo.port")
    def url = host + ":" + port
  }

  val reportIntervalMs = c.getDuration("reportInterval", TimeUnit.MILLISECONDS)
}
