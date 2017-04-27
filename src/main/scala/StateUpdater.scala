import java.util.stream.Collectors

/**
  * Created by nozes on 4/22/17.
  */
object StateUpdater {

  def somaDois(v1: Int, v2: Int): Option[Long] = {
    Some(v1 + v2)
  }

  def updateFunc(values: Seq[Iterable[(Int, Long)]], state: Option[Iterable[(Int, Long)]]): Option[Iterable[(Int, Long)]] = {

    if (state.isEmpty) {
      val theValues: Iterable[(Int, Long)] = values.head
      return Some(List((theValues.head._1, theValues.head._2)))
    }

    if (values.isEmpty) {
      return state
    }

    val summed = values.head.map(x => {
      val hour = x._1
      val count = x._2

      val collectedFromState = state.get.collectFirst({
        case i if (i._1.equals(hour)) => i
      })

      (hour, count+collectedFromState.getOrElse((0, 0L))._2)
    })


    val ordered = summed.toStream.sortBy(x => x._1)


    val previousHours: Iterable[Int] = values.head.map(x => x._1)

    val filtered = ordered.filter(x => {
      val got = previousHours.collectFirst({
        case i if (i.equals(x._1)) => 1
      })
      got.isDefined
    }).toList

    return Some(filtered)
  }

}
