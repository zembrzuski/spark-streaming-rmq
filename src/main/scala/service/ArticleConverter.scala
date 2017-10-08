package service

import domain.{ArticleAsString, ArticleTyped}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object ArticleConverter {

  val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def fromArticleAsStringToArticleTyped(inpt: ArticleAsString): ArticleTyped = {
    ArticleTyped(
      inpt.body,
      inpt.id,
      inpt.exposed_id,
      inpt.all_classifications,
      inpt.external_id,
      formatter.parseDateTime(inpt.published_first),
      formatter.parseDateTime(inpt.created_date)
    )
  }

}
