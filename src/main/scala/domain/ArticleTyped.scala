package domain

import org.joda.time.DateTime

case class ArticleTyped(
    body: String,
    id: String,
    exposed_id: String,
    all_classifications: List[String],
    external_id: Option[Long],
    published_first: DateTime,
    created_date: DateTime
)
