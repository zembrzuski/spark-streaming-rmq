import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

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

    val naosei = stream
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
      .updateStateByKey(updateFunc)

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc(values: Seq[Iterable[(Int, Long)]],
                 state: Option[Iterable[(Int, Long)]]
                ): Option[Iterable[(Int, Long)]] = {

    // TODO prever o caso em que nao tem um value.
    // TODO prever o caso em que os values nao devam ou devam remover o state.
    // example: se nao tem value e o state estah duas horas defasado, removo ele.

    val theValues: Iterable[(Int, Long)] = values.head
    val theState: Iterable[(Int, Long)] = state.head

    None
  }


  def xoxo(v1: Int, v2: Int): Option[Long] = {
    None
  }

}
