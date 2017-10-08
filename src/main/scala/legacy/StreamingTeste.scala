package legacy

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingTeste {

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
      "auto.offset.reset" -> "earliest"//,
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream
      .map[(String, (String, Int))](record => {
        // minha chave eh a tupla (secao, hora) , e depois tem o counter.
        val splitted = record.value().split(" ")
        val secao = splitted(0)
        val hora = splitted(1).toInt

        (secao, (secao, hora))
      })
      .updateStateByKey(updateFunc)
      .updateStateByKey(updateFunc2)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }



  def updateFunc(values: Seq[(String, Int)], state: Option[(String, Int)]): Option[(String, Int)] = {
    val soma = values.headOption.getOrElse(("x", 0))._2 + state.getOrElse(("x", 0))._2
    val secao = values.headOption.getOrElse(state.get)._1
    Some(secao, soma)
  }

  def updateFunc2(values: Seq[(String, Int)], state: Option[(String, Int)]): Option[(String, Int)] = {
    val soma = values.headOption.getOrElse(("x", 0))._2 + state.getOrElse(("x", 0))._2 + 5
    val secao = values.headOption.getOrElse(state.get)._1
    Some(secao, soma)
  }

}
