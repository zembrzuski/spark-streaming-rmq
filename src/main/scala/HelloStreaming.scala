import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import org.apache.spark.{SparkConf, SparkContext}

object HelloStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(".")

    val queue = "xoxo"
    val exchangeName = Option.apply("")
    val exchangeType = Option.apply("direct")
    val routingKey = Option.apply("xoxo")
    val exchangeAndRouting = ExchangeAndRouting(exchangeName, exchangeType, routingKey)

    val connectionParams = Map("TODO" -> "DO-IT")

    val rmqDistributedKey = RabbitMQDistributedKey(queue, exchangeAndRouting, connectionParams)
    val distributedKeys = Seq(rmqDistributedKey)

    val rabbitMQParams: Map[String, String] = Map(
      "hosts" -> "ERRADO",
      "queueName" -> queue,
      "exchangeName" -> "",
      "exchangeType" -> "direct",
      "routingKeys" -> "xoxo",
      "durable" -> "false"
    )

    val receiverStream = RabbitMQUtils.createDistributedStream[String](ssc, distributedKeys, rabbitMQParams)
    receiverStream.print()
    receiverStream.start()
    receiverStream.stop()



//    receiverStream.start()

    //    basic example.
//   ----------------
//    val lines = ssc.socketTextStream("localhost", 7777)
//    lines.print()
//    ssc.start()
//    ssc.awaitTermination()

  }

}