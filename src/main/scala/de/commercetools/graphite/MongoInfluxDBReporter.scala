package de.commercetools.graphite

import java.io.IOException

import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.LoggerFactory

import language._

import java.util.concurrent.TimeUnit
import rx.lang.scala.Observable
import scala.concurrent.duration._
import com.mongodb.casbah.Imports._

import com.typesafe.config.Config

import scala.util.{Try, Failure, Success}

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

class MongoInfluxDBReporter(cfg: Config) {
  val logger = LoggerFactory.getLogger(this.getClass)

  val reportIntervalMs = cfg.getDuration("reportInterval", TimeUnit.MILLISECONDS)
  val mongoConfig = cfg.as[MongoConfig]("mongo")
  val influxDbConfig = cfg.as[InfluxDbConfig]("influxDb")

  val derivativeValues = scala.collection.mutable.Map[String, Long]()

  def init() = {
    logger.info(s"Starting mongo locking reporting to InfluxDB ${influxDbConfig.url} as '${influxDbConfig.username}' " +
      s"from mongo ${mongoConfig.url} " +
      s"with interval ${reportIntervalMs}ms.")

    val mongo = MongoClient(mongoConfig.url)

    getStats(mongo) match {
      case Success(stats) =>
        sendToInfluxDB(stats)
      case Failure(error) =>
        logger.error("Can't get stats from mongo!", error)
    }

    Observable.interval(reportIntervalMs milliseconds).map(_ => getStats(mongo)).subscribe(
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
    val tags = Map("databaseId" -> mongoConfig.databaseId)
    val locks = value.getObj("locks")

    locks.keys.toList.flatMap {
      case "." => lockPoints(locks.getObj("."), tags + ("project" -> "GLOBAL_LOCK"))
      case name => lockPoints(locks.getObj(name), tags + ("project" -> name))
    }
  }

  def lockPoints(value: DBObject, tags: Map[String, String] = Map.empty) =
    List(
      point("lock_timeLocked_r", Map("value" -> value.getObj("timeLockedMicros").getChain("r", "R")), tags, derivative),
      point("lock_timeLocked_w", Map("value" -> value.getObj("timeLockedMicros").getChain("w", "W")), tags, derivative),
      point("lock_timeAcquiring_r", Map("value" -> value.getObj("timeAcquiringMicros").getChain("r", "R")), tags, derivative),
      point("lock_timeAcquiring_w", Map("value" -> value.getObj("timeAcquiringMicros").getChain("w", "W")), tags, derivative)).flatten

  def derivative(name: String, tags: Map[String, String], mongoValue: Any): Option[Long] = {
    val normalized = getRealValue(mongoValue)

    normalized map { norm =>
      val key = name + "-" + tags.toVector.sortBy(_._1).map (x => x._1 + ":" + x._2).mkString("-")
      val oldValue = derivativeValues.getOrElseUpdate(key, norm)

      derivativeValues.update(key, norm)
      norm - oldValue
    }
  }

  def point(name: String, fields: Map[String, Any], tags: Map[String, String] = Map.empty, normalizeFn: (String, Map[String, String], Any) => Option[Long] = (_, _, v) => getRealValue(v)) = {
    val properFields = fields.toList.flatMap {case (key, value) => normalizeFn(name, tags, value) map (key -> _)}

    if (properFields.nonEmpty) {
      val p = Point.measurement(name)

      tags foreach {case (key, value) => p.tag(key, value)}
      properFields foreach {case (key, value) => p.field(key, value)}

      Some(p.build())
    } else None

  }

  def getRealValue(mongoValue: Any): Option[Long] =
    mongoValue match {
      case i: Integer => Some(i.toLong)
      case d: java.lang.Double => Some(d.toLong)
      case l: java.lang.Long => Some(l)
      case null => None
      case _ => throw new IllegalStateException(s"Unsupported mongo value: $mongoValue")
    }

  implicit class MongoHelpers(value: DBObject) {
    def getObj(key: String) = value.get(key).asInstanceOf[DBObject]
    def getChain(key1: String, key2: String) = value.getOrElse(key1, value.get(key2))
  }

  val influxDb = InfluxDBFactory.connect(influxDbConfig.url, influxDbConfig.username, influxDbConfig.password)

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
      .database(influxDbConfig.databaseName)
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

  init()
}

case class InfluxDbConfig(url: String, username: String, password: String, databaseName: String)
