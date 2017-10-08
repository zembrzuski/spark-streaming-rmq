package legacy



object StateUpdater {

  def updateFunc(values: Seq[Iterable[(Int, Long)]], state: Option[Iterable[(Int, Long)]]): Option[Iterable[(Int, Long)]] = {

    if (state.isEmpty) {
      val theValues: Iterable[(Int, Long)] = values.head
      return Some(theValues)
    }

    if (values.isEmpty) {
      return state
    }

    val summed = someCountForEachHour(values, state)
    val ordered = summed.toStream.sortBy(x => x._1)
    val filtered = filterHoursThatAreNotPresentInState(ordered, values)

    Some(filtered)
  }

  private def filterHoursThatAreNotPresentInState(ordered: Stream[(Int, Long)], values: Seq[Iterable[(Int, Long)]]) = {
    val previousHours: Iterable[Int] = values.head.map(x => x._1)

    ordered.filter(x => {
      val got = previousHours.collectFirst({
        case i if (i.equals(x._1)) => 1
      })
      got.isDefined
    }).toList
  }

  private def someCountForEachHour(values: Seq[Iterable[(Int, Long)]], state: Option[Iterable[(Int, Long)]]) = {
    values.head.map(x => {
      val hour = x._1
      val count = x._2

      val collectedFromState = state.get.collectFirst({
        case i if (i._1.equals(hour)) => i
      })

      (hour, count + collectedFromState.getOrElse((0, 0L))._2)
    })
  }

}
