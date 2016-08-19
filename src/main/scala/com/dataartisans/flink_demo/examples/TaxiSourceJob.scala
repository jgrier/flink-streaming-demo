/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.examples

import com.dataartisans.flink_demo.datatypes.{TaxiRide}
import com.dataartisans.flink_demo.sources.TaxiRideSource
import com.dataartisans.flink_demo.utils.{DemoStreamEnvironment}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}

object TaxiSourceJob {

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides =
      env.addSource(new TaxiRideSource(data, maxServingDelay, servingSpeedFactor))
        .map(_.toString)
        .addSink(new FlinkKafkaProducer09[String]("localhost:9092", "taxis", new SimpleSchema))

    env.execute("Taxi Source Job")
  }
}

class SimpleSchema extends SerializationSchema[String] with DeserializationSchema[String] {
  override def serialize(element: String): Array[Byte] = element.getBytes

  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(message: Array[Byte]): String = new String(message)

  override def getProducedType(): TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
}
