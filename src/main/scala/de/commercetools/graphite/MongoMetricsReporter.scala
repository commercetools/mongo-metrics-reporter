package de.commercetools.graphite

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._


object MongoMetricsReporter extends App {
  val config = ConfigFactory.load()

  config.as[String]("reporter") match {
    case "influxDb" => new MongoInfluxDBReporter(config)
    case "graphite" => new MongoGraphiteReporter(config)
    case reporter => Console.err.println(s"Unknown reporter: $reporter")
  }
}

case class MongoConfig(hosts: Option[String], host: String, port: Int, discoverMembers: Boolean, primaryOnly: Boolean) {
  lazy val url = host + ":" + port

  lazy val parsedHosts =
    hosts.fold(Nil: List[String])(_.split("\\s*,\\s*").toList)

  lazy val urls = if (parsedHosts.nonEmpty) parsedHosts else List(url)
}