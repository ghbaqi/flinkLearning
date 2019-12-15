package com.hupu.flink.base

// 血泪踩坑 ， 一个导包问题引发的血案 ！ ！ ！
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala._
// import org.apache.flink.streaming.api.environment._
import org.apache.flink.util.Collector

/**
  * 感受一下 flink 有状态的计算
  *
  * 需求 ： 流数据不断输入字符串 ， 以字符串首字符分组， 统计最大长度
  */
object Demo02 {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = environment.socketTextStream("127.0.0.1", 7777)

    val ds02: DataStream[(Char, String, Int)] = ds.filter(s => StringUtils.isNotBlank(s))
      .map(s => (s.charAt(0), s, s.length))

    val ds03: KeyedStream[(Char, String, Int), Char] = ds02.keyBy(_._1)

    val ds04: DataStream[(Char, String, Int)] = ds03.maxBy(2)

    ds04.print()


    environment.execute("max job")

  }

}
