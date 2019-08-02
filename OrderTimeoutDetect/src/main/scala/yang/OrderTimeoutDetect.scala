package yang

import java.util

import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map


object OrderTimeoutDetect {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		//数据来源
		val orderEventStream = env.fromCollection(List(
			OrderEvent("1", "create", "1558430842"),
			OrderEvent("2", "create", "1558430843"),
			OrderEvent("2", "pay", "1558430844"),
			OrderEvent("3", "pay", "1558430942"),
			OrderEvent("4", "pay", "1558430943")
		)).assignAscendingTimestamps(_.eventTime.toLong * 1000)

		//CEP模式
		val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
				.where(_.eventType.equals("create"))
				.next("next")
				.where(_.eventType.equals("pay"))
				.within(Time.seconds(5))

		//侧输出流
		val orderTimeoutOutput: OutputTag[OrderEvent] = OutputTag[OrderEvent]("orderTimeout")
		//对输入流做CEP模式匹配
		val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

		//处理模式匹配后超时的数据
		val timeoutFunction = (map: Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[OrderEvent]) => {
			print(timestamp)
			val orderStart = map.get("begin").get.head
			out.collect(orderStart)
		}
		//处理模式匹配后正常的数据
		val selectFunction = (map: Map[String, Iterable[OrderEvent]], out: Collector[OrderEvent]) => {
		}

		val timeoutOrder: DataStream[OrderEvent] = patternStream.flatSelect(orderTimeoutOutput)(timeoutFunction)(selectFunction)

		//val timeoutOrder: DataStream[Nothing] = patternStream.flatSelect(orderTimeoutOutput)(new timeoutFunction)(new selectFunction)
		//获取侧输出的流
		timeoutOrder.getSideOutput(orderTimeoutOutput).print()
		env.execute()

	}

	//样例类
	case class OrderEvent(orderId: String, eventType: String, eventTime: String)

	/*class timeoutFunction extends PatternFlatTimeoutFunction[OrderEvent,OrderEvent] {
		override def timeout(pattern: util.Map[String, util.List[OrderEvent]],
		                     timeoutTimestamp: Long,
		                     out: Collector[OrderEvent]): Unit = {
			print(timeoutTimestamp)
			val orderStart = pattern.get("begin").get(0)
			out.collect(orderStart)
		}

	}

	class selectFunction extends PatternFlatSelectFunction[OrderEvent,OrderEvent] {
		override def flatSelect(map: util.Map[String, util.List[OrderEvent]],
		                        collector: Collector[OrderEvent]): Unit = {

		}

	}*/

}
