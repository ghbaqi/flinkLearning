package com.ververica.flinktraining.examples.demo01

import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedRide
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._
import org.joda.time.Interval

/**
 * Keyed Streams
 *
 * keyBy()  相当于 GROUP BY
 * 会导致数据重分区
 */
object Demo02 {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new TaxiRideSource(ExerciseBase.pathToRideData))


    val ds03 = ds.filter(!_.isStart)
      .map(ride => {
        val rideInterval = new Interval(ride.startTime, ride.endTime)
        val duration = rideInterval.toDuration.toStandardMinutes
        (ride.driverId, duration.getMinutes)
      })
      .keyBy(0)
      .maxBy(1)


    env.execute("keyed Streams")

  }

}
