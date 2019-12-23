package com.hupu.flink.base


import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkStreamWorldCount {


  def main(args: Array[String]): Unit = {

    // 1. 创建一个执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    /**
     * 数据源的种类
     *  env.fromCollection(List)
     *  env.socketTextStream(host, port)
     *  env.readTextFile("file:///path")
     * 自定义数据源
     * kafaka  kinesis 等
     */

    // 2. source
    val ds: DataStream[String] = environment.socketTextStream("127.0.0.1", 7777)

    // 3. 各种 transformations
    // flatMap 空格分隔
    ds.flatMap((s: String) => s.split(" "))
      // 过滤掉空串
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      // keyBy 相当于 mysql 里面的 group by , 会引起数据的重分区。这里按照 单词 来分组
      .keyBy(0)
      .sum(1)
      // 4. 将结果打印到控制台
      .print()

    // 执行
    environment.execute("flink word count demo")
  }

}


