import domain.{ArticleAsString}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import service.ArticleConverter

object HelloCsv {

  implicit val myCsvReads = Json.reads[ArticleAsString]

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("hello-world")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val first = sc
      .textFile("/home/nozes/Downloads/articles-output-sample.txt")
      .map(x => Json.fromJson[ArticleAsString](Json.parse(x)).get)
      .map((x: ArticleAsString) => ArticleConverter.fromArticleAsStringToArticleTyped(x))
      .first()

    println(first)

  }

}
