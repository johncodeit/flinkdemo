package run.been.flinkdemo.basetransform

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import run.been.flinkdemo.util.{SensorReading}

object UnionTransformationDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\flinksrc\\flinkdemo\\data\\sensor.txt"
    val dataStream1 = env.readTextFile(path).map({data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }})

    val path2 = "D:\\flinksrc\\flinkdemo\\data\\sensor2.txt"
    val dataStream2 = env.readTextFile(path2).map({data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }})
    //union
//    val unionDS = dataStream1.union(dataStream2,dataStream1,dataStream2)
//    unionDS.print()

    //connect
    val connectedStreams = dataStream1.connect(dataStream2)

    //coMap
    val coMap = connectedStreams.map(
      warningData => (warningData.id,warningData.temperature,"warning"),
      lowData => (lowData.id,"healthy"),
    )
    coMap.print()

    env.execute("Base transformation map ")
  }

}
