package cn.xuyinyin.flink.magic.demo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : XuJiaWei
 * @since : 2023-08-11 22:57
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("127.0.0.1", 9998)

    val counts = text
      .flatMap {
        _.toLowerCase.split("\\W+") filter {
          _.nonEmpty
        }
      }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
    // nc -lk 9999
  }
}
