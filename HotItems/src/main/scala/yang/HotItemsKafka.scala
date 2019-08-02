package yang

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 每五分钟统计最近一小时的商品热度 根据pv
  *
  */
object HotItemsKafka {
	def main(args: Array[String]): Unit = {

		//environment
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//read data

		val properties = new Properties()

		properties.setProperty("bootstrap.servers", "hadoop102:9092")
		properties.setProperty("group.id", "consumer-group")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")


		val readings=env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))







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
