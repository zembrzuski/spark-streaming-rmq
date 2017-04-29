import com.redis._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingTres {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))

    ssc.checkpoint("/home/nozes/labs/checkpoint")

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

    val myStream = stream
      .map[((String, Int), Long)](record => {
        val splitted = record.value().split(" ")
        // key: (secao, hora)
        // value: 1L --> uso isso pq vou dar um countbykey depois.
        ((splitted(0), splitted(1).toInt), 1L)
      })
      .reduceByKey((v1: Long, v2: Long) => v1+v2)
      .map[(String, (Int, Long))](x => {
        (x._1._1, (x._1._2, x._2))
      })
      .groupByKey()
      .updateStateByKey(StateUpdater.updateFunc)
      //.print()


    myStream
      .flatMap(x => {
        val section = x._1
        x._2.map(xoxo => {
          (x._1 + "---" + xoxo._1, xoxo._2.toString)
        })
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val r = new RedisClient("localhost", 6379)
          partition.foreach(item => {
            r.set(item._1, item._2)
          })
        })
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
