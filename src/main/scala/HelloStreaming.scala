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

    val value = stream.map[(String, (String, Long))](record => (record.value(), (record.value(), 1L)))
    val updated = value.updateStateByKey(updateRunningSum)
    updated.print()



    ssc.start()
    ssc.awaitTermination()
  }

  // noob: state evidentemente eh o estado. são as coisas antigas.
  // values: são os dados que chegaram agora.
  // o que devo notar. se está no state, então sempre vai chegar nessa função,
  // mesmo que nao tenha chegado dado nenhum agora.
  // o que devo fazer, no caso de nao quer mais um state, eh retornar um none. entao,
  // minha variavel vai sair do none.
  def updateRunningSum(values: Seq[(String, Long)], state: Option[(String, Long)]) = {
    // pega o string (que eh o nome) do state. se o state for vazio, pega o primeiro nome da seq.
    // espero que isso nunca resulte num npe.
    val theString = state.getOrElse((values.head._1, 0L))._1

    val count = state.getOrElse(("a", 0L))._2 + values.size

    if (count > 4) None else Some((theString, count))
  }

}