package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.{SensorReading, SensorSource, SensorTimeAssigner}

import java.time.Duration

object KeyedStateFunction {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(2* 1000)

    // use event time for the application
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
//    env.getConfig.setAutoWatermarkInterval(1000L)


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
//    val sensorData: DataStream[SensorReading] = env
//      // SensorSource generates random temperature readings
//      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
//      .assignTimestampsAndWatermarks(strategy)

    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .flatMap(new TemperatureAlertFunction(1.7))


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
class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // the state handle object
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // create state descriptor,状态的名字和状态的数据类型
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    // obtain the state handle
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    // fetch the last temperature from state
    //获取当前key的状态
    val lastTemp = lastTempState.value()
    println("lastTemp==" + lastTemp)
    // check if we need to emit an alert
    //计算温度的差值
    val tempDiff = (reading.temperature - lastTemp).abs
    //判断温度的差值是否大于给给定值，如果大于就输出，
    if (tempDiff > threshold) {
      // temperature changed by more than the threshold
      out.collect((reading.id, reading.temperature, tempDiff))
    }

    // update lastTemp state，更新状态,温度每次都更新的最新的温度
    this.lastTempState.update(reading.temperature)
  }
}

/**
 * 默认初始温度为0.0，这个是定义状态的数据类型的决定的。
 * lastTemp==0.0
lastTemp==1.0
lastTemp==2.0
(sensor_1,5.0,3.0)
lastTemp==5.0
 */