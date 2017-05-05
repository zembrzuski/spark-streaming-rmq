
object RbsStateUpdater {

  val sessionRegex = """.*/rs(.*?)(\d\d\d\d).*""".r

  def countUpdateFunc(values: Seq[Long], state: Option[Long]): Option[Long] = {
    Some(state.getOrElse(0L) + values.headOption.getOrElse(0L))
  }


  def sessionFromUrlExtractor(url: String): String = {
    url match {
      case sessionRegex(session, date) => return session
    }

    throw new IllegalArgumentException("Could not extract a session")
  }

}
