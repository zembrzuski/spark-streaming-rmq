import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
  * TIVE UM INSIGHT:
  * SE A GENTE VIR UM CARA QUE ESTÁ MANDANDO VARIAS REQUISICOES, ISTO EH, UM
  * NUMERO MUITO MAIOR DO QUE A MEDIA EM GERAL, ESSE CARA ESTÁ ATACANDO A RBS.
  * ENTAO, POSSO BLOQUEA-LO.
  */

object HelloStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))

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

//    val value = stream.map[(String, (String, Long))](record => (record.value(), (record.value(), 1L)))
//    val updated = value.updateStateByKey(updateRunningSum)
//    updated.print()


    stream
      .map[((String, Integer), Long)](record => {
        // minha chave eh a tupla (secao, hora) , e depois tem o counter.
        val splitted = record.value().split(" ")
        val secao = splitted(0)
        val hora = splitted(1).toInt

        ((secao, hora), 1L)
      })
      .reduceByKey((count1, count2) => count1 + count2)
      .map[(String, (Integer, Long))](x => {
        // minha tupla aqui eh (secao, (hora, count))
        (x._1._1, (x._1._2, x._2))
      })
      .groupByKey() // it would be better if I had implemented it using reduceByKey.
      .updateStateByKey(updateFunc)
      .print()


    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc(values: Seq[Iterable[(Integer, Long)]], state: Option[(Int, Long)]): Option[(Int, Long)] = {
    values.head.reduce[Option[(Int, Long)]]((v1, v2) => {
      v1.
      //Some((v1._1, v1._2))// if (v1._1 > v2._1) else (v2._1, v2._2)
      None
    })
  }

//  def updateRunningSum(values: Seq[(String, Long)], state: Option[(String, Long)]) = {
//    val theString = state.getOrElse((values.head._1, 0L))._1
//    val count = state.getOrElse(("a", 0L))._2 + values.size
//    if (count > 4) None else Some((theString, count))
//  }

}