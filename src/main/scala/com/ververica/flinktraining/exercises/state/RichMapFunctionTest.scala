package com.ververica.flinktraining.exercises.state

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._


/**
 * 无状态 : 流式输入的数据 , 后面数据的计算结果不依赖于前面的数据                        eg : map  flatMap keyBy
 * 有状态 : 流式输入的数据 , 后面数据的计算结果依赖于前面的数据 , 随着数据累计结果不停变化  eg : count  max  average
 */

/**
 * 状态编程示例
 *
 * 有状态的  map 算子
 *
 */


case class SensorReading(id: Int, temperature: Double)

object RichMapFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 需求 : 对传感器温度做 map 操作
     *
     * 假设温度 > 100 认为传感器已经损坏 , 后续进来的都  map 标记为为异常传感器
     * 否则认为是正常传感器
     */

    val list = List(
      SensorReading(1, 13.5), // 1,13.5,正常)
      SensorReading(2, 110), // (2,110.0,异常)
      SensorReading(1, 105), // (1,105.0,异常)
      SensorReading(3, 15.5), // (3,15.5,正常)
      SensorReading(2, 16.5), // (2,16.5,异常)
      SensorReading(1, 13.5), // (1,13.5,异常)
      SensorReading(3, 13.5) //  (3,13.5,正常)
    )

    val ds: KeyedStream[SensorReading, Int] = env
      .fromCollection(list)
      // 相当于  mysql 里面的 group by  ,  会导致数据重分区  , 这里我们按照传感器 id 分组
      .keyBy(_.id)

    // SensorReading   =>  ( id , temperature , 正常/异常 )
    val ds02: DataStream[(Int, Double, String)] = ds.map(new MyRichMapFunction01)

    ds02.print()

    env.execute("有状态的  map 算子")
  }

}


class MyRichMapFunction01 extends RichMapFunction[SensorReading, (Int, Double, String)] {

  // 保存传感器状态 true 正常 , false 异常了
  var state: ValueState[Boolean] = _


  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("sensorState", classOf[Boolean]))

    /**
     * 血泪踩坑 , 不能在 open 方法里更新状态
     *
     * Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.
     */
    //    state.update(true)
  }

  override def map(sensor: SensorReading): (Int, Double, String) = {

    // 将状态初始化为 true 正常
    if (state.value() == null)
      state.update(true)

    // 说明前面已经有 > 100 的异常温度了
    if (!state.value())
      return (sensor.id, sensor.temperature, "异常")

    // 正常温度
    if (sensor.temperature < 100) {
      (sensor.id, sensor.temperature, "正常")
    } else {
      // 异常温度 , 更新状态
      state.update(false)
      (sensor.id, sensor.temperature, "异常")
    }


  }
}
