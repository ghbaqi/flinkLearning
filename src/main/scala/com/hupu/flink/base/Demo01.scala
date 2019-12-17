package com.hupu.flink.base


import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo01 {


  def main(args: Array[String]): Unit = {


    val environment = ExecutionEnvironment.getExecutionEnvironment
    val ds = environment.readTextFile("/Users/gaohui/flinkLearning/flinkLearning/f1")


    //    val ds2 = ds.flatMap(new FlatMapFunction[String, String] {
    //      override def flatMap(t: String, collector: Collector[String]): Unit = {
    //        t.split(",").foreach(collector.collect)
    //      }
    //    })
    //
    //    ds2.print()


    ds
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()


    //    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val ds = environment.socketTextStream("127.0.0.1", 7777)
    //
    //    ds.flatMap((s: String) => s.split(" "))
    //      .filter(_.nonEmpty)
    //      .map((_, 1))
    //      .keyBy(0)
    //      .sum(1)
    //
    //    ds.print()
    //    environment.execute("job01")
  }

}
