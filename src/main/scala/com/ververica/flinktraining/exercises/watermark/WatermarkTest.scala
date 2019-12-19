package com.ververica.flinktraining.exercises.watermark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * 窗口操作  window  :  滚动窗口 滑动窗口 等
 *
 *
 * 时间语义 : eventTime  ingestionTime   processTime
 * 什么是时间语义 ?
 * 为什么要使用 eventTime ?  eventTime 更真实的反应了事件真实的发生事件
 *
 * waterMark  :
 *
 * 引入 waterMark 的概念是为了更好的利用 eventTime 进行窗口计算。
 * 因为事件到达它所属于的窗口是乱序的 , 没有保障的 。可能时间已经来到窗口的  endTime 此时还有事件尚未到达。这时候也不能无限的等待下去
 * 必须有一个机制去触发窗口计算 。
 * 当 waterMark  >  一个窗口的 endTime 时 , 此时就会触发这个窗口进行计算
 *
 * 如何生成 waterMark ?
 * 通常是 eventTime - 等待时间 , 且随着时间单调递增
 *
 * 很明显 :
 * 等待时间越大 , 可能丢失的数据越少 , 但延迟越高
 * 等待时间越小 , 可能丢失的数据越多 , 但延迟越低  ; (此时就是一个 tradeOff)
 *
 */

// (1 , 12.4 , 2019-12-18 15:00:02)
case class SensorReading(id: Int, temperature: Double, time: String)

object WatermarkTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 指定 flink 按照什么时间来处理 , 默认为 TimeCharacteristic.ProcessingTime
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val sensorDs: DataStream[SensorReading] = env.socketTextStream("127.0.0.1", 7777)
      .filter(s => StringUtils.isNotBlank(s) && s.split(",").length == 3)
      .map(s => {
        val arr = s.split(",")
        SensorReading(arr(0).toInt, arr(1).toDouble, arr(2))
      })

    // 指定 eventTime 和 waterMark 生成方式
    val waterMarkStream: DataStream[SensorReading] = sensorDs.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks)


    // 每十秒的数据作为一个窗口
    val windowStream: WindowedStream[SensorReading, Int, TimeWindow] = waterMarkStream.keyBy(_.id)

      /**
       * 此时的窗口是
       * [ 2019-12-12 10:00:00 , 2019-12-12 10:00:10 )
       * [ 2019-12-12 10:00:10 , 2019-12-12 10:00:20 )
       * [ 2019-12-12 10:00:20 , 2019-12-12 10:00:30 )
       * ...
       * ...
       * [ 2019-12-12 10:00:50 , 2019-12-12 10:01:00 )
       *
       */
      .timeWindow(Time.seconds(10))


    val result: DataStream[String] = windowStream.apply(new WindowFunction[SensorReading, String, Int, TimeWindow] {

      override def apply(key: Int, window: TimeWindow, list: Iterable[SensorReading], collector: Collector[String]): Unit = {

        val res = list.toString()

        val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        println(s"触发窗口计算 id = $key , windowEndTime = ${dataFormat.format(new Date(window.getEnd))} , 窗口内数据 = ${res}")

        collector.collect(res)
      }
    })

    result.print()


    env.execute("water mark test")

  }

}


class MyAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[SensorReading] {

  val waitTime = 5000 // 延迟时间 , 最多等待 5 秒 中 。比窗口结束时间大 5s 的数据都到了 , 不再等待 触发计算

  var currentMaxTimeMills = 0L

  // 生成 eventTime  (1 , 12.5 , 2018-11-11 12:12:20)
  override def extractTimestamp(sensor: SensorReading, previousElementTimestamp: Long): Long = {

    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeMills: Long = dataFormat.parse(sensor.time).getTime
    currentMaxTimeMills = Math.max(timeMills, currentMaxTimeMills)
    println(s"id = ${sensor.id} , eventTime = ${sensor.time} , waterMark = ${dataFormat.format(getCurrentWatermark.getTimestamp)}")
    timeMills

  }

  // 生成 waterMark
  override def getCurrentWatermark: Watermark = {
    val watermark = new Watermark(currentMaxTimeMills - waitTime)
    watermark
  }


}

/**
 * 输入数据						                         所属窗口                waterMark
 * 1 , 11.2 , 2019-12-18 15:00:05               [00 - 10)				        00
 * 2 , 11.2 , 2019-12-18 15:00:01			         [00 - 10)				        00
 * 1 , 11.2 , 2019-12-18 15:00:11			         [10 - 20)                06
 * 2 , 11.2 , 2019-12-18 15:00:16			         [10 - 20)                11  此时 11 > 窗口结束时间 10 , 会触发 [00 - 10) 的窗口进行运算
 *
 */




























