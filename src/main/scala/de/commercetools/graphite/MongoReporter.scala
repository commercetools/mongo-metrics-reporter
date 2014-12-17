package de.commercetools.graphite

import java.io.{IOException, OutputStreamWriter}
import java.net.Socket

import org.slf4j.LoggerFactory

import language._

import java.util.concurrent.TimeUnit
import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.mongodb.casbah.Imports._

import com.typesafe.config.{ConfigFactory, Config}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Try, Failure, Success}
import scala.collection.JavaConverters._

object MongoReporter extends App {
  val config = new Conf(ConfigFactory.load())
  val logger = LoggerFactory.getLogger(this.getClass)

  init()

  def init() = {
    logger.info(s"Starting mongo locking reporting to ${config.graphite.host}:${config.graphite.port} " +
      s"from mongo ${config.mongo.url} " +
      s"with interval ${config.reportIntervalMs}ms and prefix '${config.graphite.graphitePrefix}'")

    val mongo = MongoClient(config.mongo.url)

    Observable.interval(config.reportIntervalMs milliseconds).map(_ => getStats(mongo)).subscribe(
      onNext = {
        case Success(stats) =>
          sendToGraphite(stats)
        case Failure(error) =>
          logger.error("Can't get stats from mongo!", error)
      },
      onError = error => {
        logger.error("Error in pipeline!", error)
        closeDriver(mongo)
      },
      onCompleted = () =>
        closeDriver(mongo)
    )

    while (true) {
      Thread.sleep(1000)
    }
  }

  def extractStats(prefix: List[String], value: DBObject): List[(String, Long)] =
    value.keySet().asScala.toList.flatMap { keyName =>
      value.get(keyName) match {
        case i: Integer => List(createKey(prefix :+ keyName: _*) -> i.toLong)
        case d: java.lang.Double => List(createKey(prefix :+ keyName: _*) -> d.toLong)
        case l: java.lang.Long => List(createKey(prefix :+ keyName: _*) -> (l: Long))
        case s: DBObject => extractStats(prefix :+ keyName, s)
        case _ => Nil
      }
    }

  def sendToGraphite(stats: DBObject) = {
    val all = extractStats(Nil, stats)

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

  def getStats(client: MongoClient): Try[DBObject] = {
    try {
      val res = client.getDB("admin").command(DBObject("serverStatus" -> 1))

      if (res.ok())
        Success(res)
      else
        Failure(res.getException)
    }
    catch {
      case e: Exception => Failure(e)
    }
  }

  def closeDriver(client: MongoClient): Unit = {
    client.close()
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
