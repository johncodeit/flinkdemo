package run.been.flinkdemo.state

import org.apache.commons.compress.utils.Lists
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.{SensorReading, SensorSource, SensorTimeAssigner}
import java.util
import java.time.Duration

object ListStateFunctionDemo {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(2 * 1000)

    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(5)) //延迟0秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })

    // ingest sensor stream
    val sensorData = env.socketTextStream("localhost", 9999)
      .map { text =>
        val arr: Array[String] = text.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }.assignTimestampsAndWatermarks(strategy)

    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val alerts = keyedSensorData
      .flatMap(new TemperatureListAlertFunction(1.8, 3))


    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }
}

/**
 * The function emits an alert if the temperature measurement of a sensor changed by more than
 * a configured threshold compared to the last reading.
 *
 * @param threshold The threshold to raise an alert.
 */
class TemperatureListAlertFunction(val threshold: Double, val numberOfTimes: Int)
  extends RichFlatMapFunction[SensorReading, util.ArrayList[SensorReading]] {

  // the state handle object
  private var listStateCount: ListState[SensorReading] = _

  override def open(parameters: Configuration): Unit = {
    // create state descriptor,状态的名字和状态的数据类型
    val listStateCountDescriptor = new ListStateDescriptor[SensorReading]("listStateCount", classOf[SensorReading])
    // obtain the state handle
    listStateCount = getRuntimeContext.getListState[SensorReading](listStateCountDescriptor)
  }

  override def flatMap(reading: SensorReading, out: Collector[util.ArrayList[SensorReading]]): Unit = {

    //输入值
    //获取当前温度
    val currentTemp = reading.temperature

    //如果当前值大于给定值
    if (currentTemp > threshold) {
      listStateCount.add(reading)
    }
    val list = Lists.newArrayList(listStateCount.get().iterator())
    //如果不正常的值超过给定次数，就发出报警
    if (list.size() >= numberOfTimes) {
      out.collect(list)
      //清空list状态集合
      listStateCount.clear()
    }
  }
}

/**
 * 输入
 * sensor_1,2000,1.0
 * sensor_1,7000,1.0
 * sensor_1,3000,2.0
 * sensor_1,4000,5.0
 * sensor_1,5000,4.0
 */

/**
 * 输出结果：[SensorReading(sensor_1,3000,2.0), SensorReading(sensor_1,4000,5.0), SensorReading(sensor_1,5000,4.0)]
 */