package domain

case class ArticleAsString(
    body: String,
    id: String,
    published_first: String,
    exposed_id: String,
    all_classifications: List[String],
    external_id: Option[Long],
    created_date: String
)
