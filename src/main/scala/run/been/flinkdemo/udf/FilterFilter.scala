package run.been.flinkdemo.udf

import org.apache.flink.api.common.functions.FilterFunction

/**
 * 函数类
 */
class FilterFilter extends FilterFunction[String]{
  override def filter(value: String): Boolean = {
    value.contains("pulsar")
  }
}
