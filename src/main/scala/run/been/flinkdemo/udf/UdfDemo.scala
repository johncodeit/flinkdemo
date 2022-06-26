package run.been.flinkdemo.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UdfDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\flinksrc\\flinkdemo\\data\\hello.txt"
    val dataStream1 = env.readTextFile(path).filter(new FilterFilter)
    dataStream1.print()
    env.execute()
  }
}
