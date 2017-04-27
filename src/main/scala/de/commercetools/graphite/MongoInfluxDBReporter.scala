package de.commercetools.graphite

import java.io.IOException

import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.LoggerFactory

import language._
import java.util.concurrent.TimeUnit

import rx.lang.scala.Observable

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import com.mongodb.casbah.Imports._
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class MongoInfluxDBReporter(cfg: Config) {
  val logger = LoggerFactory.getLogger(this.getClass)

  val reportIntervalMs = cfg.getDuration("reportInterval", TimeUnit.MILLISECONDS)
  val mongoConfig = cfg.as[MongoConfig]("mongo")
  val influxDbConfig = cfg.as[InfluxDbConfig]("influxDb")

  val derivativeValues = TrieMap[String, MutableMap[String, Long]]()

  def withMongoClient[R](url: String)(f: MongoClient ⇒ R): R = {
    val mongo = MongoClient(url)
    try f(mongo)
    finally closeDriver(mongo)
  }

  def init() = {
    logger.info(s"Starting mongo locking reporting to InfluxDB ${influxDbConfig.url} as '${influxDbConfig.username}' " +
      s"from mongo ${mongoConfig.urls mkString ", "} " +
      s"with interval ${reportIntervalMs}ms.")

    def getAllStats() = {
      try {
        discoverMongoHosts().foreach { case (url, primary) ⇒
          withMongoClient(url) { mongo ⇒
            getStats(mongo) match {
              case Success(stats) ⇒
                sendToInfluxDB(stats, url, primary)
              case Failure(error) ⇒
                logger.error(s"Can't get stats from mongo '$url': " + error.getMessage)
            }
          }
        }
      } catch {
        case NonFatal(error) ⇒
          logger.error(s"Unexpected error. Discovery will be retried. ", error)
      }
    }

    getAllStats()

    Observable.interval(reportIntervalMs milliseconds).map(_ ⇒ getAllStats()).subscribe(
      onNext = identity,
      onError = error ⇒ {
        logger.error("Error in pipeline!", error)
      },
      onCompleted = () ⇒ logger.info("Finished!"))

    keepThreadAlive()
  }

  def discoverMongoHosts(): List[(String, Option[Boolean])] =
    if (mongoConfig.discoverMembers) {
      val discovered = doDiscoverMembers(mongoConfig.urls).filterNot(_._2 == NodeType.Other)

      logger.debug("Discovered nodes: " + discovered.map{case (host, tpe) ⇒ host + (if (tpe == NodeType.Primary) " (P)" else "")}.mkString(", "))

      val relevant =
        if (mongoConfig.primaryOnly)
          discovered.filter(_._2 == NodeType.Primary)
        else
          discovered

      relevant.map(i ⇒ i._1 → Some(i._2 == NodeType.Primary))
    } else {
      mongoConfig.urls.map(_ → None)
    }

  def doDiscoverMembers(urls: List[String]): List[(String, NodeType.Value)] = {
    val Success(results) = urls.view
      .map(url ⇒ Try {
        withMongoClient(url) { client ⇒
          val res = client.getDB("admin").command(DBObject("replSetGetStatus" → 1))

          if (res.ok())
            res
          else
            throw res.getException
        }
      }.recover {
        case e: Exception ⇒
          logger.error(s"Can't connect to mongo host '$url' in order to discover other other nodes (will try to use other hosts, if available):" + e.getMessage)
          throw e
      })
      .find(_.isSuccess)
      .getOrElse(throw new IllegalStateException("Can't discover any mongo host because of the errors! Tried all of the provided hosts, but all of them result in error, which you can find in the logs above!"))

    results.getObjArr("members").map { obj ⇒
      val tpe = Option(obj.get("stateStr").asInstanceOf[String]).map {
        case "PRIMARY" ⇒ NodeType.Primary
        case "SECONDARY" ⇒ NodeType.Secondary
        case _ ⇒ NodeType.Other
      }.getOrElse(NodeType.Other)

      obj.get("name").asInstanceOf[String] → tpe
    }.toList
  }

  def extractHost(url: String) = {
    val idx = url.indexOf(":")

    if (idx > 0)
      url.substring(0, idx)
    else
      url
  }

  def keepThreadAlive(): Unit = {
    while (true) {
      Thread.sleep(1000)
    }
  }

  def extractStats(value: DBObject, url: String, primary: Option[Boolean]): List[Point] = {
    val tags = Map("host" → (if (influxDbConfig.extractHost) extractHost(url) else url))
    val locks = value.getObj("locks")

    val locksPoints = locks.keys.toList.flatMap { name ⇒
      val lockType = locks.getObj(name)

      List("acquireCount", "acquireWaitCount", "timeAcquiringMicros", "deadlockCount").filter(lockType.containsField).flatMap { c ⇒
        val metric = lockType.getObj(c)

        List("r", "w", "R", "W").filter(metric.containsField).flatMap(prop ⇒
          point(s"locks_${name}_${c}_$prop", Map("value" → metric.get(prop)), tags, derivative(url)))
      }
    }

    val connPoints = List(
      point("connections_current", Map("value" → value.getObj("connections").get("current")), tags),
      point("connections_available", Map("value" → value.getObj("connections").get("available")), tags),
      point("connections_totalCreated", Map("value" → value.getObj("connections").get("totalCreated")), tags, derivative(url)))

    val extraInfoPoints = List(
      point("extra_info_heap_usage_bytes", Map("value" → value.getObj("extra_info").get("heap_usage_bytes")), tags),
      point("extra_info_page_faults", Map("value" → value.getObj("extra_info").get("page_faults")), tags, derivative(url)),
      primary.flatMap(p ⇒ point("extra_info_primary", Map("value" → (if (p) 1 else 0)), tags)))

    val opcountersPoints = List("insert", "query", "update", "delete", "getmore", "command") flatMap (c ⇒
      point(s"opcounters_$c", Map("value" → value.getObj("opcounters").get(c)), tags, derivative(url)))

    val assertsPoints = List("regular", "warning", "msg", "user", "rollovers") flatMap (c ⇒
      point(s"asserts_$c", Map("value" → value.getObj("asserts").get(c)), tags, derivative(url)))

    val opcountersReplPoints = List("insert", "query", "update", "delete", "getmore", "command") flatMap (c ⇒
      point(s"opcountersRepl_$c", Map("value" → value.getObj("opcountersRepl").get(c)), tags, derivative(url)))

    val backgroundFlushingPoints =
      if (value containsField "backgroundFlushing")
        List("flushes", "total_ms") flatMap (c ⇒
          point(s"backgroundFlushing_$c", Map("value" → value.getObj("backgroundFlushing").get(c)), tags, derivative(url)))
      else
        Nil

    val durPoints =
      if (value containsField "dur")
        List(
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
      else
        Nil

    val globalLockPoints =
      List("totalTime", "lockTime").flatMap(c ⇒ point(s"globalLock_$c", Map("value" → value.getObj("globalLock").get(c)), tags, derivative(url))) ++
      List(
        point("globalLock_currentQueue_total", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("total")), tags),
        point("globalLock_currentQueue_readers", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("readers")), tags),
        point("globalLock_currentQueue_writers", Map("value" → value.getObj("globalLock").getObj("currentQueue").get("writers")), tags),
        point("globalLock_activeClients_total", Map("value" → value.getObj("globalLock").getObj("activeClients").get("total")), tags),
        point("globalLock_activeClients_readers", Map("value" → value.getObj("globalLock").getObj("activeClients").get("readers")), tags),
        point("globalLock_activeClients_writers", Map("value" → value.getObj("globalLock").getObj("activeClients").get("writers")), tags)).flatten

    val networkPoints =
      List("bytesIn", "bytesOut", "numRequests").flatMap(c ⇒
        point(s"network_$c", Map("value" → value.getObj("network").get(c)), tags, derivative(url)))

    val memPoints = List(
      point("mem_residentMb", Map("value" → value.getObj("mem").get("resident")), tags),
      point("mem_virtualMb", Map("value" → value.getObj("mem").get("virtual")), tags),
      point("mem_mappedMb", Map("value" → value.getObj("mem").get("mapped")), tags),
      point("mem_mappedWithJournalMb", Map("value" → value.getObj("mem").get("mappedWithJournal")), tags))

    val metrics = value.getObj("metrics")
    val commands = metrics.getObj("commands")
    val cursor = metrics.getObj("cursor")

    val commandsPoints = commands.keys.toList.filterNot(_ == "<UNKNOWN>").flatMap(command ⇒
      List("failed", "total").flatMap(c ⇒
        point(s"commands_${command}_$c", Map("value" → commands.getObj(command).get(c)), tags, derivative(url))))

    val cursorPoints = List(
      point("cursors_timedOut", Map("value" → cursor.get("timedOut")), tags, derivative(url)),
      point("cursors_open_noTimeout", Map("value" → cursor.getObj("open").get("noTimeout")), tags),
      point("cursors_open_pinned", Map("value" → cursor.getObj("open").get("pinned")), tags),
      point("cursors_open_total", Map("value" → cursor.getObj("open").get("total")), tags),
      point("cursors_open_singleTarget", Map("value" → cursor.getObj("open").get("singleTarget")), tags),
      point("cursors_open_multiTarget", Map("value" → cursor.getObj("open").get("multiTarget")), tags))

    val documentPoints =
      List("deleted", "inserted", "returned", "updated").flatMap(c ⇒
        point(s"document_$c", Map("value" → metrics.getObj("document").get(c)), tags, derivative(url)))

    val operationPoints =
      List("fastmod", "idhack", "scanAndOrder").flatMap(c ⇒
        point(s"operation_$c", Map("value" → metrics.getObj("operation").get(c)), tags, derivative(url)))

    val queryExecutorPoints =
      List("scanned").flatMap(c ⇒
        point(s"queryExecutor_$c", Map("value" → metrics.getObj("queryExecutor").get(c)), tags, derivative(url)))

    val recordPoints =
      List("moves").flatMap(c ⇒
        point(s"record_$c", Map("value" → metrics.getObj("record").get(c)), tags, derivative(url)))

    val ttlPoints =
      List("deletedDocuments", "passes").flatMap(c ⇒
        point(s"ttl_$c", Map("value" → metrics.getObj("ttl").get(c)), tags, derivative(url)))

    val wiredTigerPoints =
      if (value containsField "wiredTiger")
        collectProperties(value, "wiredTiger", "uri" :: Nil, tags)
      else
        Nil

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
      commandsPoints ++
      wiredTigerPoints ++
      extraInfoPoints.flatten
  }

  def collectProperties(value: DBObject, prop: String, excludeProps: List[String], tags: Map[String, String], path: List[String] = Nil): List[Point] = {
    val curr = value.getObj(prop)
    val names = curr.keys.filterNot(excludeProps.contains).toList

    curr.get(names.head) match {
      case _: DBObject ⇒
        names.flatMap(name ⇒ collectProperties(curr, name, excludeProps, tags, path :+ prop))
      case _ ⇒
        val metricName = (path :+ prop) mkString "_"
        val metricValues = names.map(name ⇒ name → curr.get(name)).toMap

        point(metricName, metricValues, tags).toList
    }
  }

  def derivative(url: String)(name: String, tags: Map[String, String], mongoValue: Any): Option[Long] = {
    val normalized = getRealValue(mongoValue)

    normalized map { norm ⇒
      val dv = getDerivativeValues(url)
      val key = name + "-" + tags.toVector.sortBy(_._1).map (x ⇒ x._1 + ":" + x._2).mkString("-")
      val oldValue = dv.getOrElseUpdate(key, norm)

      dv.update(key, norm)
      math.max(norm - oldValue, 0)
    }
  }

  def getDerivativeValues(url: String) = derivativeValues.getOrElseUpdate(url, MutableMap.empty)

  def point(name: String, fields: Map[String, Any], tags: Map[String, String] = Map.empty, normalizeFn: (String, Map[String, String], Any) ⇒ Option[Long] = (_, _, v) ⇒ getRealValue(v)) = {
    val properFields = fields.toList.flatMap {case (key, value) ⇒ normalizeFn(name, tags, value) map (key → _)}

    if (properFields.nonEmpty) {
      val p = Point.measurement(name).time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

      tags foreach {case (key, value) ⇒ p.tag(key, value)}
      properFields foreach {case (key, value) ⇒ p.field(key, value)}

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
    def getObjArr(key: String): Seq[DBObject] = value.get(key).asInstanceOf[BasicDBList].asScala.asInstanceOf[Seq[DBObject]]
    def getChain(key1: String, key2: String) = value.getOrElse(key1, value.get(key2))
  }

  val influxDb = InfluxDBFactory.connect(influxDbConfig.url, influxDbConfig.username, influxDbConfig.password)

  def sendToInfluxDB(stats: DBObject, url: String, primary: Option[Boolean]) = {
    val all = extractStats(stats, url, primary)

    try {
      send(influxDb, all, url)
    } catch {
      case e: IOException ⇒ logger.error("Error sending stat to InfluxDB!", e)
    }
  }

  def send(db: InfluxDB, points: List[Point], url: String) = {
    logger.debug(s"Sending ${points.size} points from '$url' to InfluxDB...")

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
    } catch {
      case e: Exception ⇒ Failure(e)
    }
  }

  def closeDriver(client: MongoClient): Unit = {
    client.close()
  }

  init()
}

object NodeType extends Enumeration {
  val Primary, Secondary, Other = Value
}

case class InfluxDbConfig(url: String, username: String, password: String, databaseName: String, extractHost: Boolean)