package com.hxr.networkflowanalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Description: 热门页面浏览量统计
 * @Author: CJ
 * @Data: 2020/12/28 15:27
 */
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, cnt: Long)

object HotPagesNetworkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\不常用的项目\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val dataStream = inputStream.map(data => {
      val arr = data.split(" ")
      val ts = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime
      ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
      })

    val aggStream = dataStream
      .filter(_.method == "GET")
      .filter(data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(10))

    resultStream.print()

    env.execute("hot page")
  }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  def createAccumulator: Long = 0

  def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  def getResult(acc: Long): Long = acc

  def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotPages(topSize:Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))

  override def processElement(input: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageViewCountListState.add(input)
    ctx.timerService().registerProcessingTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allPageListCounts: ListBuffer[PageViewCount] = ListBuffer()
    val iterator = pageViewCountListState.get().iterator()
    while (iterator.hasNext) {
      allPageListCounts += iterator.next()
    }

    pageViewCountListState.clear()

    // 排序
    val sortedPageListCounts = allPageListCounts.sortWith(_.cnt > _.cnt).take(topSize)
    // 格式化为String类型
    val result: StringBuffer = new StringBuffer
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedPageListCounts.indices) {
      val currentPageCounts = sortedPageListCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("页面url = ").append(currentPageCounts.url).append("\t")
        .append("热门度 = ").append(currentPageCounts.cnt).append("\n")
    }

    result.append("====================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString)
  }
}