package de.commercetools.graphite

import java.util.concurrent.TimeUnit

import language.postfixOps

import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.{Point, BatchPoints}
import rx.lang.scala.Observable

import scala.concurrent.duration._

import scala.util.{Random, Failure, Success}

object InfluxDBTest extends App {
  val influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root")
  val db = "foo"

  val servers = List("app01", "app01", "app03")

  def sendStats() = {
    println("Sending")

    val points = (1 to 30).toList map { i =>
        val p = Point
          .measurement("request33")
          .tag("server", Random.shuffle(servers).head)
          .tag("project", "project" + Random.nextInt(10))
          .field("aa", Random.nextInt(100).toLong)
          .field("value", Random.nextInt(3000).toLong)
          .build()

      println(p.lineProtocol())

      p
    }

    val batch = BatchPoints
      .database(db)
      .retentionPolicy("default")
      .consistency(ConsistencyLevel.ALL)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .build()

    points foreach batch.point

    influxDB.write(batch)
  }

  sendStats()

  Observable.interval(10 seconds).subscribe(
    onNext = _ => sendStats(),
    onError = error => {
      error.printStackTrace()
    }
  )

  while (true) {
    Thread.sleep(1000)
  }
}
