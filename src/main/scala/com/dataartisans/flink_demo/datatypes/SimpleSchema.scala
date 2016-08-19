package com.dataartisans.flink_demo.datatypes

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

class TaxiRideSchema extends SerializationSchema[TaxiRide] with DeserializationSchema[TaxiRide] {
  override def serialize(element: TaxiRide): Array[Byte] = element.toString.getBytes

  override def isEndOfStream(nextElement: TaxiRide): Boolean = false

  override def deserialize(message: Array[Byte]): TaxiRide = TaxiRide.fromString(new String(message))

  override def getProducedType(): TypeInformation[TaxiRide] = TypeInformation.of(new TypeHint[TaxiRide](){})
}
