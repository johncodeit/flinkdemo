package run.been.flinkdemo.basetransform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import run.been.flinkdemo.util.{SensorReading, SensorSource}

/**
 * @description:
 * @projectName:flinkdemo
 * @see:run.been.flinkdemo.basetransform
 * @author:beenrun
 * @createTime:2022/5/9 20:25
 * @version:1.0
 */
object FlatMapTransformation {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val sensorIds = flatMap(readings)
    sensorIds.print()

    env.execute("Base transformation map ")
  }

  private def flatMap(readings: DataStream[SensorReading]) = {
    //flatMap:将id进行切分，id和传感器分开
    val sensorIds: DataStream[String] = readings.map(r =>r.id).flatMap(id => id.split("_"))
    sensorIds
  }
  /**
   * 8> 79
8> sensor
8> 80
3> sensor
   **/

}
