package de.commercetools.graphite

import java.io.{IOException, OutputStreamWriter}
import java.net.Socket

import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.{InfluxDB, InfluxDBFactory}
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
import scala.util.{Random, Try, Failure, Success}
import scala.collection.JavaConverters._

object MongoInfluxDBReporter extends App {
  val config = new Conf(ConfigFactory.load())
  val logger = LoggerFactory.getLogger(this.getClass)

  def init() = {
    logger.info(s"Starting mongo locking reporting to InfluxDB ${config.influxDb.url} as '${config.influxDb.username}' " +
      s"from mongo ${config.mongo.url} " +
      s"with interval ${config.reportIntervalMs}ms.")

    val mongo = MongoClient(config.mongo.url)

    getStats(mongo) match {
      case Success(stats) =>
        sendToInfluxDB(stats)
      case Failure(error) =>
        logger.error("Can't get stats from mongo!", error)
    }

    Observable.interval(config.reportIntervalMs milliseconds).map(_ => getStats(mongo)).subscribe(
      onNext = {
        case Success(stats) =>
          sendToInfluxDB(stats)
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

  def extractStats(value: DBObject): List[Point] = {
    val tags = Map("databaseId" -> config.mongo.databaseId)
    val locks = value.getObj("locks")

    locks.keys.toList.flatMap {
      case "." => lockPoints(locks.getObj("."), tags + ("project" -> "GLOBAL_LOCK"))
      case name => lockPoints(locks.getObj(name), tags + ("project" -> name))
    }
  }

  def lockPoints(value: DBObject, tags: Map[String, String] = Map.empty) =
    List(
      point("lock_timeLocked_r", Map("value" -> value.getObj("timeLockedMicros").getChain("r", "R")), tags),
      point("lock_timeLocked_w", Map("value" -> value.getObj("timeLockedMicros").getChain("w", "W")), tags),
      point("lock_timeAcquiring_r", Map("value" -> value.getObj("timeAcquiringMicros").getChain("r", "R")), tags),
      point("lock_timeAcquiring_w", Map("value" -> value.getObj("timeAcquiringMicros").getChain("w", "W")), tags)).flatten

  def point(name: String, fields: Map[String, Any], tags: Map[String, String] = Map.empty) = {
    val properFields = fields.toList.flatMap {case (key, value) => getRealValue(value) map (key -> _)}

    if (properFields.nonEmpty) {
      val p = Point.measurement(name)

      tags foreach {case (key, value) => p.tag(key, value)}
      properFields foreach {case (key, value) => p.field(key, value)}

      Some(p.build())
    } else None

  }

  def getRealValue(mongoValue: Any) =
    mongoValue match {
      case i: Integer => Some(i.toLong)
      case d: java.lang.Double => Some(d.toLong)
      case l: java.lang.Long => Some(l)
      case null => None
      case _ => throw new IllegalStateException(s"Unsupported mongo value: $mongoValue")
    }

  implicit class MongoHelpers(value: DBObject) {
    def getObj(key: String) = {
      println("key " + key + " " + value.keySet())
      value.get(key).asInstanceOf[DBObject]
    }
    def getChain(key1: String, key2: String) = value.getOrElse(key1, value.get(key2))
  }

  val influxDb = InfluxDBFactory.connect(config.influxDb.url, config.influxDb.username, config.influxDb.password)

  def sendToInfluxDB(stats: DBObject) = {
    val all = extractStats(stats)

    try {
      send(influxDb, all)
    } catch {
      case e: IOException => logger.error("Error sending stat to InfluxDB!", e)
    }
  }

  def send(db: InfluxDB, points: List[Point]) = {
    logger.debug(s"Sending ${points.size} points to InfluxDB...")

    points foreach (p => println(p.lineProtocol()))

    val epoch = System.currentTimeMillis() / 1000

    val batch = BatchPoints
      .database(config.influxDb.databaseName)
      .retentionPolicy("default")
      .consistency(ConsistencyLevel.ALL)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .build()

    points foreach batch.point

    try {
      db.write(batch)
    } catch {
      case e: Exception => logger.error("Error writing data to InfluxDB!", e)
    }
  }

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

  class Conf(c: Config) {
    object influxDb {
      val url = c.getString("influxDb.url")
      val username = c.getString("influxDb.username")
      val password = c.getString("influxDb.password")
      val databaseName = c.getString("influxDb.databaseName")
    }

    object mongo {
      val host = c.getString("mongo.host")
      val port = c.getInt("mongo.port")
      val databaseId = c.getString("mongo.databaseId")

      def url = host + ":" + port
    }

    val reportIntervalMs = c.getDuration("reportInterval", TimeUnit.MILLISECONDS)
  }

  init()
}

