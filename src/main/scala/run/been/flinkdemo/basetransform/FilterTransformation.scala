package run.been.flinkdemo.basetransform

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.Logger
import run.been.flinkdemo.util.{SensorReading, SensorSource}

/**
 * @description:
 * @projectName:flinkdemo
 * @see:run.been.flinkdemo.basetransform
 * @author:beenrun
 * @createTime:2022/5/9 20:25
 * @version:1.0
 */
object FilterTransformation {



  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val sensorIds = filter1(readings)
    sensorIds.print()
    env.execute("Base transformation map ")

  }

  private def filter1(readings: DataStream[SensorReading]) = {
    //map
    val sensorIds: DataStream[SensorReading] = readings
      .filter(r => r.temperature >25 )
    sensorIds
  }

}
