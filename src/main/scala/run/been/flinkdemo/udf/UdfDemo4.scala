package run.been.flinkdemo.udf

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 匿名函数
 */
object UdfDemo4 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\flinksrc\\flinkdemo\\data\\hello.txt"
    val dataStream1 = env.readTextFile(path).filter(_.contains("pulsar"))
    dataStream1.print()
    env.execute()
  }
}

