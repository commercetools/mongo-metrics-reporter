package de.commercetools.graphite

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

object MongoMetricsReporter extends App {
  val config = ConfigFactory.load()

  config.as[String]("reporter") match {
    case "influxDb" => new MongoInfluxDBReporter(config)
    case "graphite" => new MongoGraphiteReporter(config)
    case reporter => Console.err.println(s"Unknown reporter: $reporter")
  }
}

case class MongoConfig(host: String, port: Int, databaseId: String) {
  lazy val url = host + ":" + port
}