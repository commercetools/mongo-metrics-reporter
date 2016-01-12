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
      case Success(stats) ⇒
        sendToInfluxDB(stats)
      case Failure(error) ⇒
        logger.error("Can't get stats from mongo!", error)
    }

    Observable.interval(reportIntervalMs milliseconds).map(_ ⇒ getStats(mongo)).subscribe(
      onNext = {
        case Success(stats) ⇒
          sendToInfluxDB(stats)
        case Failure(error) ⇒
          logger.error("Can't get stats from mongo!", error)
      },
      onError = error ⇒ {
        logger.error("Error in pipeline!", error)
        closeDriver(mongo)
      },
      onCompleted = () ⇒
        closeDriver(mongo)
    )

    while (true) {
      Thread.sleep(1000)
    }
  }

  def extractStats(value: DBObject): List[Point] = {
    val tags = Map("databaseId" → mongoConfig.databaseId)
    val locks = value.getObj("locks")

    val locksPoints = locks.keys.toList.flatMap { name ⇒
      val lockType = locks.getObj(name)

      List("acquireCount", "acquireWaitCount", "timeAcquiringMicros", "deadlockCount").filter(lockType.containsField).flatMap { c ⇒
        val metric = lockType.getObj(c)

        List("r", "w", "R", "W").filter(metric.containsField).flatMap(prop ⇒
          point(s"locks_${name}_${c}_$prop", Map("value" → metric.get(prop)), tags, derivative))
      }
    }

    val connPoints = List(
      point("connections_current", Map("value" → value.getObj("connections").get("current")), tags),
      point("connections_available", Map("value" → value.getObj("connections").get("available")), tags),
      point("connections_totalCreated", Map("value" → value.getObj("connections").get("totalCreated")), tags, derivative))

    val opcountersPoints = List("insert", "query", "update", "delete", "getmore", "command") flatMap (c ⇒
      point(s"opcounters_$c", Map("value" → value.getObj("opcounters").get(c)), tags, derivative))

    val assertsPoints = List("regular", "warning", "msg", "user", "rollovers") flatMap (c ⇒
      point(s"asserts_$c", Map("value" → value.getObj("asserts").get(c)), tags, derivative))

    val opcountersReplPoints = List("insert", "query", "update", "delete", "getmore", "command") flatMap (c ⇒
      point(s"opcountersRepl_$c", Map("value" → value.getObj("opcountersRepl").get(c)), tags, derivative))

    val backgroundFlushingPoints = List("flushes", "total_ms") flatMap (c ⇒
      point(s"backgroundFlushing_$c", Map("value" → value.getObj("backgroundFlushing").get(c)), tags, derivative))

    val cursorPoints = List(
      point("cursors_totalOpen", Map("value" → value.getObj("cursors").get("totalOpen")), tags),
      point("cursors_totalNoTimeout", Map("value" → value.getObj("cursors").get("totalNoTimeout")), tags),
      point("cursors_timedOut", Map("value" → value.getObj("cursors").get("timedOut")), tags, derivative))

    val durPoints = List(
      point("dur_commits", Map("value" → value.getObj("dur").get("commits")), tags),
      point("dur_journaledMB", Map("value" → value.getObj("dur").get("journaledMB")), tags),
      point("dur_writeToDataFilesMB", Map("value" → value.getObj("dur").get("writeToDataFilesMB")), tags),
      point("dur_compression", Map("value" → value.getObj("dur").get("compression")), tags),
      point("dur_commitsInWriteLock", Map("value" → value.getObj("dur").get("commitsInWriteLock")), tags),
      point("dur_earlyCommits", Map("value" → value.getObj("dur").get("earlyCommits")), tags),
      point("dur_timeMs_dt", Map("value" → value.getObj("dur").getObj("timeMs").get("dt")), tags),
      point("dur_timeMs_prepLogBuffer", Map("value" → value.getObj("dur").getObj("timeMs").get("prepLogBuffer")), tags),
      point("dur_timeMs_writeToJournal", Map("value" → value.getObj("dur").getObj("timeMs").get("writeToJournal")), tags),
      point("dur_timeMs_writeToDataFiles", Map("value" → value.getObj("dur").getObj("timeMs").get("writeToDataFiles")), tags),
      point("dur_timeMs_remapPrivateView", Map("value" → value.getObj("dur").getObj("timeMs").get("remapPrivateView")), tags))

    val globalLockPoints =
      List("totalTime", "lockTime").flatMap(c ⇒ point(s"globalLock_$c", Map("value" → value.getObj("globalLock").get(c)), tags, derivative)) ++
      List(
        point("globalLock_currentQueue_total", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("total")), tags),
        point("globalLock_currentQueue_readers", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("readers")), tags),
        point("globalLock_currentQueue_writers", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("writers")), tags),
        point("globalLock_activeClients_total", Map("value" → value.getObj("globalLock").getObj("activeClients").get("total")), tags),
        point("globalLock_activeClients_readers", Map("value" → value.getObj("globalLock").getObj("activeClients").get("readers")), tags),
        point("globalLock_activeClients_writers", Map("value" → value.getObj("globalLock").getObj("activeClients").get("writers")), tags)).flatten

    val networkPoints =
      List("bytesIn", "bytesOut", "numRequests").flatMap(c ⇒
        point(s"network_$c", Map("value" → value.getObj("network").get(c)), tags, derivative))

    val memPoints = List(
      point("mem_residentMb", Map("value" → value.getObj("mem").get("resident")), tags),
      point("mem_virtualMb", Map("value" → value.getObj("mem").get("virtual")), tags),
      point("mem_mappedMb", Map("value" → value.getObj("mem").get("mapped")), tags),
      point("mem_mappedWithJournalMb", Map("value" → value.getObj("mem").get("mappedWithJournal")), tags))

    val metrics = value.getObj("metrics")
    val commands = metrics.getObj("commands")

    val commandsPoints = commands.keys.toList.filterNot(_ == "<UNKNOWN>").flatMap(command ⇒
      List("failed", "total").flatMap(c ⇒
        point(s"commands_${command}_$c", Map("value" → commands.getObj(command).get(c)), tags, derivative)))

    val documentPoints =
      List("deleted", "inserted", "returned", "updated").flatMap(c ⇒
        point(s"document_$c", Map("value" → metrics.getObj("document").get(c)), tags, derivative))

    val operationPoints =
      List("fastmod", "idhack", "scanAndOrder").flatMap(c ⇒
        point(s"operation_$c", Map("value" → metrics.getObj("operation").get(c)), tags, derivative))

    val queryExecutorPoints =
      List("scanned").flatMap(c ⇒
        point(s"queryExecutor_$c", Map("value" → metrics.getObj("queryExecutor").get(c)), tags, derivative))

    val recordPoints =
      List("moves").flatMap(c ⇒
        point(s"record_$c", Map("value" → metrics.getObj("record").get(c)), tags, derivative))

    val ttlPoints =
      List("deletedDocuments", "passes").flatMap(c ⇒
        point(s"ttl_$c", Map("value" → metrics.getObj("ttl").get(c)), tags, derivative))

    locksPoints ++
      connPoints.flatten ++
      opcountersPoints ++
      opcountersReplPoints ++
      assertsPoints ++
      backgroundFlushingPoints ++
      cursorPoints.flatten ++
      durPoints.flatten ++
      globalLockPoints ++
      networkPoints ++
      memPoints.flatten ++
      documentPoints ++
      operationPoints ++
      queryExecutorPoints ++
      recordPoints ++
      ttlPoints ++
      commandsPoints
  }

  def derivative(name: String, tags: Map[String, String], mongoValue: Any): Option[Long] = {
    val normalized = getRealValue(mongoValue)

    normalized map { norm ⇒
      val key = name + "-" + tags.toVector.sortBy(_._1).map (x ⇒ x._1 + ":" + x._2).mkString("-")
      val oldValue = derivativeValues.getOrElseUpdate(key, norm)

      derivativeValues.update(key, norm)
      math.max(norm - oldValue, 0)
    }
  }

  def point(name: String, fields: Map[String, Any], tags: Map[String, String] = Map.empty, normalizeFn: (String, Map[String, String], Any) ⇒ Option[Long] = (_, _, v) ⇒ getRealValue(v)) = {
    val properFields = fields.toList.flatMap {case (key, value) ⇒ normalizeFn(name, tags, value) map (key → _)}

    if (properFields.nonEmpty) {
      val p = Point.measurement(name).time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

      tags foreach {case (key, value) ⇒ p.tag(key, value)}
      properFields foreach {case (key, value) ⇒ p.field(key, value + "i")}

      Some(p.build())
    } else None

  }

  def getRealValue(mongoValue: Any): Option[Long] =
    mongoValue match {
      case i: Integer ⇒ Some(i.toLong)
      case d: java.lang.Double ⇒ Some(d.toLong)
      case l: java.lang.Long ⇒ Some(l)
      case null ⇒ None
      case _ ⇒ throw new IllegalStateException(s"Unsupported mongo value: $mongoValue")
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
      case e: IOException ⇒ logger.error("Error sending stat to InfluxDB!", e)
    }
  }

  def send(db: InfluxDB, points: List[Point]) = {
    logger.debug(s"Sending ${points.size} points to InfluxDB...")

//    points foreach { p ⇒
//      println(p.lineProtocol())
//    }

    val batch = BatchPoints
      .database(influxDbConfig.databaseName)
      .build()

    points foreach batch.point

    try {
      db.write(batch)
    } catch {
      case e: Exception ⇒ logger.error("Error writing data to InfluxDB!", e)
    }
  }

  def getStats(client: MongoClient): Try[DBObject] = {
    try {
      val res = client.getDB("admin").command(DBObject("serverStatus" → 1))

      if (res.ok())
        Success(res)
      else
        Failure(res.getException)
    }
    catch {
      case e: Exception ⇒ Failure(e)
    }
  }

  def closeDriver(client: MongoClient): Unit = {
    client.close()
  }

  init()
}

case class InfluxDbConfig(url: String, username: String, password: String, databaseName: String)
