import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.{SparkConf, SparkContext}

object HelloStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 7777)
    lines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}