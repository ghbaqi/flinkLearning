package com.ververica.flinktraining.exercises

import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedRide
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils}
import org.apache.flink.streaming.api.scala._

/**
 * 过滤掉 ride 事情不是发生在 new york 的记录
 */
object RideCleansingExercise {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new TaxiRideSource(ExerciseBase.pathToRideData))

    ds.filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .map(new EnrichedRide(_)) // 无状态算子  map
      .print()

    env.execute("filter demo")


  }

}
