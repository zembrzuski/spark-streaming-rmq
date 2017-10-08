package legacy

import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HelloRbs {

  case class RbsLog(
   isSubscriber: Boolean,
    userName: String,
    url: String,
    timestamp: Long
  )

  val GSON = new Gson

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("/home/rodrigoz/checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hello-kafka2",
      "auto.offset.reset" -> "earliest"
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    contando o numero de acessos de cada usuario.
//
//    val myStream = stream
//        .map[RbsLog](record => {
//          GSON.fromJson(record.value(), classOf[RbsLog])
//        })
//        .map[(String, Long)](x => (x.userName, 1L))
//        .reduceByKey((v1: Long, v2: Long) => v1 + v2)
//        .updateStateByKey(RbsStateUpdater.countUpdateFunc)
//        .print()

    //
    // contando o numero de acessos por secao.
    //
    val myStream = stream
      .map[RbsLog](record => {
        GSON.fromJson(record.value(), classOf[RbsLog])
      })
      .map[(String, Long)](x => (RbsStateUpdater.sessionFromUrlExtractor(x.url), 1L))
      .reduceByKey((v1: Long, v2: Long) => v1 + v2)
      .updateStateByKey(RbsStateUpdater.countUpdateFunc)
      .print()


    ssc.start()
    ssc.awaitTermination()

  }

}
