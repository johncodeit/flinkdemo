package run.been.flinkdemo.udf

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 将函数实现成匿名函数
 */
object UdfDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\flinksrc\\flinkdemo\\data\\hello.txt"
    val dataStream1 = env.readTextFile(path).filter(new RichFilterFunction[String] {
      override def filter(value: String): Boolean = {
        value.contains("pulsar")
      }
    })
    dataStream1.print()
    env.execute()
  }
}
