package com.hupu.flink.base


import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Demo01 {


  def main(args: Array[String]): Unit = {


    val environment = ExecutionEnvironment.getExecutionEnvironment
    val ds = environment.readTextFile("/Users/gaohui/Documents/flinkForwardAsia/flinkDemo01/f1")

    ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1).print()



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
