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

import com.dataartisans.flink_demo.datatypes.{TaxiRide, TaxiRideSchema}
import com.dataartisans.flink_demo.sources.TaxiRideSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

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
        .addSink(new FlinkKafkaProducer09[TaxiRide]("localhost:9092", "taxis", new TaxiRideSchema))

    env.execute("Taxi Source Job")
  }
}


