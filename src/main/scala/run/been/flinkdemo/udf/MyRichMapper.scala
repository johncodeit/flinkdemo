package run.been.flinkdemo.udf

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import run.been.flinkdemo.util.SensorReading

/**
 * 富函数
 */
class MyRichMapper extends RichMapFunction[SensorReading, String]{

  override def open(parameters: Configuration): Unit = {
    // 做一些初始化操作，比如数据库的连接
    //    getRuntimeContext
  }

  override def map(value: SensorReading): String = value.id + " temperature"

  override def close(): Unit = {
    //  一般做收尾工作，比如关闭连接，或者清空状态
  }
}
