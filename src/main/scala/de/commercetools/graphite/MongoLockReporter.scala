package de.commercetools.graphite

import java.io.{IOException, OutputStreamWriter}
import java.net.Socket

import language._

import java.util.concurrent.TimeUnit
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson._
import reactivemongo.core.actors.Close
import reactivemongo.utils.LazyLogger
import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global
import reactivemongo.core.commands.Status
import FutureHelper._
import scala.concurrent.duration._

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object MongoLockReporter extends App {
  val config = new Conf(ConfigFactory.load())
  val logger = LazyLogger("de.commercetools.MongoLockReporter")

  init()

  def init() = {
    logger.info(s"Starting mongo locking reporting to ${config.graphite.host}:${config.graphite.port} " +
      s"from mongo ${config.mongo.url} " +
      s"with interval ${config.reportIntervalMs}ms and prefix '${config.graphite.graphitePrefix}'")

    val driver = new MongoDriver
    val connection = driver.connection(config.mongo.url :: Nil)

    Observable.interval(config.reportIntervalMs milliseconds).flatMap(_ => Observable.from(getStats(connection))).subscribe(
      onNext = {
        case Success(stats) => sendToGraphite(stats)
        case Failure(error) => logger.error("Can't get stats from mongo!", error)
      },
      onError = error => {
        logger.error("Error in pipeline!", error)
        closeDriver(driver)
      },
      onCompleted = () =>
        closeDriver(driver)
    )
  }

  def extractStats(value: BSONValue): List[(String, Long)] = value match{
    case BSONInteger(i) => List("" -> i.toLong)
    case BSONDouble(d) => List("" -> d.toLong)
    case BSONLong(l) => List("" -> l)
    case BSONDocument(s) =>
      s.toList.collect {case Success(pair) => pair}
        .flatMap {case (key, v) => extractStats(v) map {case (sk, sv) => createKey(key, sk) -> sv}}
    case _ => Nil
  }

  def sendToGraphite(stats: Map[String, BSONValue]) = {
    val all = stats.toList.flatMap {case (key, value) => extractStats(value) map {case (sk, sv) => createKey(key, sk) -> sv}}

    try {
      write(
        new Socket(config.graphite.host, config.graphite.port),
        all.map {case (key, value) => createKey(config.graphite.graphitePrefix, key) -> value})
    } catch {
      case e: IOException => logger.error("Error connecting to graphite!", e)
    }
  }

  def write(sock: Socket, stats: List[(String, Long)]) = {
    var writer: OutputStreamWriter = null

    try {
      val epoch = System.currentTimeMillis() / 1000

      writer = new OutputStreamWriter(sock.getOutputStream)

      try {
        stats.foreach { case (key, value) =>
          writer.write("%s %.2f %d\n".formatLocal(java.util.Locale.US, key, value.doubleValue, epoch))
        }
      } catch {
        case e: IOException => logger.error("Error writing data to graphite!", e)
      }

      writer.flush()
    } catch {
      case e: Exception =>
        logger.error("Error writing to Graphite!", e)

        if (writer != null) {
          try writer.flush() catch {
            case ioe: IOException => logger.error("Error while flushing writer!", ioe)
          }
        }
    } finally {
      if (sock != null) {
        try
          sock.close()
        catch {
          case ioe: IOException => logger.error("Error while closing socket!", ioe)
        }
      }
    }
  }

  def createKey(parts: String*) = parts
    .filterNot(_.isEmpty)
    .map {
      case "." => "global_lock"
      case name => camelToUnderscores(name.replaceAll(":", "_").replaceAll("/", ".").replaceAll(" ", "_"))
    }
    .mkString(".")

  def camelToUnderscores(name: String) = "[A-Z\\d]".r.replaceAllIn(name, {m =>
    "_" + m.group(0).toLowerCase()
  })

  def getStats(connection: MongoConnection) =
    connection("admin111").command(Status().command)
      .map(s => Success(s.asInstanceOf[Map[String, BSONValue]]))
      .recover {case error => Failure(error)}

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
