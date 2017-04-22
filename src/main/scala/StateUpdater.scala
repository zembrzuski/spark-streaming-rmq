/**
  * Created by nozes on 4/22/17.
  */
object StateUpdater {

  def somaDois(v1: Int, v2: Int): Option[Long] = {
    Some(v1 + v2)
  }

  def updateFunc(values: Seq[Iterable[(Int, Long)]],
                 state: Option[Iterable[(Int, Long)]]
                ): Option[Iterable[(Int, Long)]] = {

    // TODO prever o caso em que nao tem um value.
    // TODO prever o caso em que os values nao devam ou devam remover o state.
    // example: se nao tem value e o state estah duas horas defasado, removo ele.

    val theValues: Iterable[(Int, Long)] = values.head
    val theState: Iterable[(Int, Long)] = state.head

    None
  }

}
