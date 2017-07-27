/* 
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream

import scala.concurrent.duration.FiniteDuration

import sinks._

package model {

  /** Whether the sink is for good rows or bad rows */
  object InputType extends Enumeration {
    type InputType = Value
    val Good, Bad = Value
  }

  /** Type of sink */
  sealed trait SinkType
  case object Kinesis extends SinkType
  case object Kafka extends SinkType
  case object Stdout extends SinkType

  /**
   * Case class for holding both good and
   * bad sinks for the Stream Collector.
   *
   * @param good
   * @param bad
   */
  case class CollectorSinks(good: Sink, bad: Sink)

  /**
   * Case class for holding the results of
   * splitAndSerializePayload.
   *
   * @param good All good results
   * @param bad All bad results
   */
  case class EventSerializeResult(good: List[Array[Byte]], bad: List[Array[Byte]])

  /**
   * Class for the result of splitting a too-large array of events in the body of a POST request
   *
   * @param goodBatches List of batches of events
   * @param failedBigEvents List of events that were too large
   */
  case class SplitBatchResult(goodBatches: List[List[String]], failedBigEvents: List[String])   

  case class CookieConfig(
    enabled: Boolean,
    name: String,
    expiration: FiniteDuration,
    domain: Option[String]
  )
  case class P3PConfig(policyRef: String, CP: String)
  case class AWSConfig(accessKey: String, secretKey: String)
  case class StreamConfig(region: String, good: String, bad: String) {
    val endpoint = region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  case class BackoffPolicyConfig(minBackoff: Long, maxBackoff: Long)
  case class KinesisConfig(
    threadPoolSize: Int,
    aws: AWSConfig,
    stream: StreamConfig,
    backoffPolicy: BackoffPolicyConfig)
  case class TopicConfig(good: String, bad: String)
  case class KafkaConfig(brokers: String, topic: TopicConfig)
  case class BufferConfig(byteLimit: Int, recordLimit: Int, timeLimit: Long)
  case class SinkConfig(
    enabled: String,
    useIpAddressAsPartitionKey: Boolean,
    kinesis: KinesisConfig,
    kafka: KafkaConfig,
    buffer: BufferConfig
  ) {
    // should be moved to a decoder instance when case classy supports them
    val sinkType = enabled match {
      case "kinesis" => Kinesis
      case "kafka"   => Kafka
      case "stdout"  => Stdout
      case _         => throw new IllegalArgumentException("collector.sink.enabled unknown")
    }
  }
  case class CollectorConfig(
    interface: String,
    port: Int,
    production: Boolean,
    p3p: P3PConfig,
    cookie: CookieConfig,
    sink: SinkConfig
  ) {
    val cookieConfig = if (cookie.enabled) Some(cookie) else None

    def cookieName = cookieConfig.map(_.name)
    def cookieDomain = cookieConfig.flatMap(_.domain)
    def cookieExpiration = cookieConfig.map(_.expiration)
  }
}
