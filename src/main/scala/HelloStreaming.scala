import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object HelloStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("/home/nozes/labs/checkpoint")

    val kafkaParams = Map[String, Object](
      //"bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hello-kafka2",
      "auto.offset.reset" -> "earliest"//,
      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val value = stream.map(record => (record.value(), 1L))
    val updated = value.updateStateByKey(updateRunningSum)
    updated.print()



    ssc.start()
    ssc.awaitTermination()
  }

  def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
    Some(state.getOrElse(0L) + values.size)
  }

}