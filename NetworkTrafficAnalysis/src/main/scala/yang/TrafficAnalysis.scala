package yang

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TrafficAnalysis {
	def main(args: Array[String]): Unit = {
		//environment
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


		val readings: DataStream[String] = env.readTextFile("/Users/maxyang/Documents/IdeaProjects/FlinkProject/NetworkTrafficAnalysis/src/main/resources/apachetest.log")

		readings.map(str => {
			val strings: Array[String] = str.split(" ")
			val formator = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
			val timestamp: Long = formator.parse(strings(3)).getTime
			ApacheLogEvent(strings(0), strings(2), timestamp, strings(5), strings(6))

		})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
					override def extractTimestamp(t: ApacheLogEvent): Long = {
						t.eventTime
					}
				})
        				.keyBy("url")
        				.timeWindow(Time.minutes(10),Time.seconds(5))
        				.


		env.execute()
	}

	case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

	case class UrlViewCount(url: String, windowEnd: Long, count: Long)

}
