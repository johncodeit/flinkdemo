package run.been.flinkdemo.udf

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 自定义key word
 */
object UdfDemo3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\flinksrc\\flinkdemo\\data\\hello.txt"
    val dataStream1 = env.readTextFile(path).filter(new KeyWordFilter("pulsar"))
    dataStream1.print()
    env.execute()
  }
}
class KeyWordFilter(keyWord :String ) extends FilterFunction[String]{
  override def filter(value: String): Boolean = {
    value.contains(keyWord)
  }
}
