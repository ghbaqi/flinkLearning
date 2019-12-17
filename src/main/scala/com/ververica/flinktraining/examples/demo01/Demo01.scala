package com.ververica.flinktraining.examples.demo01

import com.ververica.flinktraining.exercises.datastream_java.sources.{TaxiFareSource, TaxiRideSource}
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Demo01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides = env.addSource(new TaxiRideSource(ExerciseBase.pathToRideData))

    rides.print()

    env.execute("my demo 01")

  }

}
