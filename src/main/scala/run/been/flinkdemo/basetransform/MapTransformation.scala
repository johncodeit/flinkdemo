package run.been.flinkdemo.basetransform

import org.apache.flink.api.common.functions.MapFunction
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
object MapTransformation {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

//    val sensorIds = map1(readings)
//    sensorIds.print()

    //自定义MapFunction
    val sensorIds2 = map2(readings)

    sensorIds2.print()
    env.execute("Base transformation map ")

  }

  private def map1(readings: DataStream[SensorReading]) = {
    //map
    val sensorIds: DataStream[String] = readings
      .map(r => r.id)
    sensorIds
  }
  private def map2(readings: DataStream[SensorReading]) = {
    //map
    val sensorIds: DataStream[String] = readings
      .map(new MyMapFunction)
    sensorIds
  }

  class MyMapFunction extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = t.id
  }

}
