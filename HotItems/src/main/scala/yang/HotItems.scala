package yang

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 每五分钟统计最近一小时的商品热度 根据pv
  *
  */
object HotItems {
	def main(args: Array[String]): Unit = {

		//environment
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//read data
		val readings: DataStream[String] = env.readTextFile("/Users/maxyang/Documents/IdeaProjects/FlinkProject/HotItems/src/main/resources/UserBehavior.csv")

		readings
				//transform data to object
				.map(str => {
			val strings: Array[String] = str.split(",")
			UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
		})
				//设置读取数据中的时间戳，而且认为数据是有序的设置水印
				.assignAscendingTimestamps(_.timestamp * 1000)
				//只留下pv的数据
				.filter(_.behavior.contains("pv"))
				//根据itemId分组
				.keyBy("itemId")
				//开窗
				.timeWindow(Time.minutes(60), Time.minutes(5))
				//增量聚合和全量聚合联合使用
				.aggregate(new CountAgg(), new WindowResultFunction())
				//根据窗口进行分流，统计窗口内的数据
				.keyBy("windowEnd")
				//对数据进行操作
				.process(new TopItems(3)).print()

		//程序执行
		env.execute()

	}



}
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(in: UserBehavior, acc: Long): Long = acc + 1L

	override def getResult(acc: Long): Long = acc

	override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
	override def apply(key: Tuple,
	                   window: TimeWindow,
	                   input: Iterable[Long],
	                   out: Collector[ItemViewCount]): Unit = {
		val end: Long = window.getEnd
		val count: Long = input.iterator.next()
		val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0

		out.collect(ItemViewCount(itemId, end, count))

	}
}

class TopItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
	private var itemState: ListState[ItemViewCount] = _


	override def open(parameters: Configuration): Unit = {
		super.open(parameters)

		val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])

		itemState = getRuntimeContext.getListState(itemStateDesc)


	}

	override def processElement(i: ItemViewCount,
	                            context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
	                            collector: Collector[String]): Unit = {

		itemState.add(i)
		context.timerService().registerEventTimeTimer(i.windowEnd + 1)


	}


	override def onTimer(timestamp: Long,
	                     ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
	                     out: Collector[String]): Unit = {

		val allitems: ListBuffer[ItemViewCount] = ListBuffer()
		import scala.collection.JavaConversions._
		for (item <- itemState.get()) {

			allitems += item
		}
		itemState.clear()

		val sortedItems: ListBuffer[ItemViewCount] = allitems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
		val result: StringBuilder = new StringBuilder
		result.append("==================================")
		result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

		for (i <- sortedItems.indices) {
			val currentItem: ItemViewCount = sortedItems(i)
			result
					.append("No")
					.append(i + 1)
					.append(currentItem.itemId)
					.append("  浏览量=")
					.append(currentItem.count)
					.append("\n")
		}

		result.append("==================================")
		Thread.sleep(1000)


		out.collect(result.toString())
	}


}

case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)
