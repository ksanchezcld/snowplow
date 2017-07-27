/* 
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream

import java.io.File
import java.util.concurrent.ScheduledThreadPoolExecutor

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import classy.generic._
import classy.config._
import com.typesafe.config.{ConfigFactory, Config}
import org.slf4j.LoggerFactory

import model._
import sinks._

// Main entry point of the Scala collector.
object ScalaCollector {
  lazy val log = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
      head(generated.Settings.name, generated.Settings.version)
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
    }

    val conf = parser.parse(args, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }

    if (!conf.hasPath("collector")) {
      System.err.println("configuration has no collector path")
      System.exit(-1)
    }

    val decoder = deriveDecoder[Config, CollectorConfig]
    decoder(conf.getConfig("collector")) match {
      case Left(e) =>
        System.err.println(s"configuration error: $e")
        System.exit(-1)
       case Right(c) =>
        main(c, conf)
    }
  }

  def main(collectorConf: CollectorConfig, conf: Config): Unit = {

    implicit val system = ActorSystem.create("scala-stream-collector", conf)
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val sinks = {
      val (good, bad) = collectorConf.sink.sinkType match {
        case Kinesis =>
          val es = new ScheduledThreadPoolExecutor(collectorConf.sink.kinesis.threadPoolSize)
          (KinesisSink.createAndInitialize(collectorConf.sink, InputType.Good, es),
            KinesisSink.createAndInitialize(collectorConf.sink, InputType.Bad, es))
        case Kafka =>
          (new KafkaSink(collectorConf.sink, InputType.Good),
            new KafkaSink(collectorConf.sink, InputType.Bad))
        case Stdout =>
          (new StdoutSink(InputType.Good), new StdoutSink(InputType.Bad))
      }
      CollectorSinks(good, bad) 
    }

    val route = new CollectorRoute {
      override def collectorService = new CollectorService(collectorConf, sinks)
    }

    Http().bindAndHandle(route.collectorRoute, collectorConf.interface, collectorConf.port).map { binding =>
      log.info(s"REST interface bound to ${binding.localAddress}")
    } recover { case ex =>
      log.error("REST interface could not be bound to " +
        s"${collectorConf.interface}:${collectorConf.port}", ex.getMessage)
    }
  }
}
