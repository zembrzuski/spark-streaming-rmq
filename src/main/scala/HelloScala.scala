import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world para o spark, pela decima vez.
  * Agora, espero que comece a usar sempre e que eu pare de fazer coisas noobs, como essa.
  * Exemplo de como chamar isso.

  spark-submit \
   --class HelloScala \
   --master local[*] \
   --deploy-mode client \
   target/scala-2.10/hellostreaming_2.10-1.0.jar

  */
object HelloScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("hello-world")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sum = sc
      .parallelize(List(1, 2, 3, 4, 5))
      .reduce((x1, x2) => x1 + x2)

    println("=====================")
    println(sum)
    println("=====================")

  }

}
