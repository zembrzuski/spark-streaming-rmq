import org.apache.spark.{SparkConf, SparkContext}

object HelloStreaming {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("hello-spark").setMaster("local[*]"))
    print("hello scala")

  }

}