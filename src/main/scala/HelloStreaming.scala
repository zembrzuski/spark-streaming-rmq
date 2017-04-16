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


    val latestSessionInfo = stream
      .map[((String, Integer), (String, Long))](record => {
        // minha chave eh a tupla (secao, hora) , e depois tem o counter.
        val splitted = record.value().split(" ")
        val secao = splitted(0)
        val hora = splitted(1).toInt

        ((secao, hora), (secao, 1L))
      })
      .reduceByKey((t1: (String, Long), t2: (String, Long)) => (t1._1, t1._2 + t2._2))
      .map(x => {
        println(x._1) // imprime a chave
        x
      })
      .updateStateByKey(updateFunc)

    latestSessionInfo
      .print() // I should send it to redis or mongo instead of call print function.

    latestSessionInfo
      .map[(String, (String, Int, Long))](x => {
        //(secao, (secao, hora, counter)
        (x._1._1, (x._2._1, x._1._2, x._2._2))
      })
      .reduceByKey((a, b) => {
        if (a._2 > b._2) a else b
      })
      .map(tup => ((tup._1, tup._2._2), (tup._2._1, tup._2._3)))
      .map(x => {
        println(x._1) // imprime a chave
        x
      })
      .updateStateByKey(updateFuncSecondPhase)
      .print()

//    latestSessionInfo
//      .map[(String, (Int, Long))](x => (x._1._1, (x._1._2, x._2)))
//      .reduceByKey((a: (Int, Long), b: (Int, Long)) => {
//        if (a._1 > b._1) a else b
//      })
//      .map[((String, Int), Long)](tup => ((tup._1, tup._2._1), tup._2._2))
//      .updateStateByKey(updateFuncSecondPhase)
//      .print()


    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc(values: Seq[(String, Long)], state: Option[(String, Long)]): Option[(String, Long)] = {
    val secao = state.getOrElse((values.head._1, 0L))._1
    val soma = state.getOrElse(("x", 0L))._2 + values.headOption.getOrElse(("x", 0L))._2
    Some(secao, soma)
  }


  def updateFuncSecondPhase(values: Seq[(String, Long)], state: Option[(String, Long)]): Option[(String, Long)] = {
    val secao = state.getOrElse((values.head._1, 0L))._1
    val soma = state.getOrElse(("x", 0L))._2 + values.headOption.getOrElse(("x", 0L))._2
    Some(secao, soma)
  }


  //  def updateFuncSecondPhase(values: Seq[Long], state: Option[Long]): Option[Long] = {
//    Some(state.getOrElse(0L) + values.headOption.getOrElse(0L))
//  }


  //  def updateRunningSum(values: Seq[(String, Long)], state: Option[(String, Long)]) = {
//    val theString = state.getOrElse((values.head._1, 0L))._1
//    val count = state.getOrElse(("a", 0L))._2 + values.size
//    if (count > 4) None else Some((theString, count))
//  }

}