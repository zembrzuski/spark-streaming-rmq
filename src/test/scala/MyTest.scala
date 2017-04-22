import org.scalatest.FlatSpec

/**
  * Created by nozes on 4/22/17.
  */
class MyTest extends FlatSpec {

  /**
    * TODO after all, devo considerar o caso de remover um cara devido à inatividade.
    */

  // quando existe value e state, e a hora eh a mesma nos dois.
  // eh bem facil. so tenho que somar os dois.
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((14, 2)))
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.head.head._1 === 14)
    assert(newState.head.head._2 === 3)
  }

  // quando existe value e nao tem state, e a hora eh a mesma nos dois.
  // eh bem facil. eh so manter o que ja tem.
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((14, 2)))
    val state: Option[Iterable[(Int, Long)]] = None

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.head.head._1 === 14)
    assert(newState.head.head._2 === 2)
  }

  // quando nao existe value e existe state, e a hora eh a mesma nos dois.
  // eh bem facil. eh so manter o que ja tem.
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List()
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.head.head._1 === 14)
    assert(newState.head.head._2 === 1)
  }

  // quando troca a hora. eh bem facil. eh so manter o ultimo instante
  // caso troca simples
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((15, 2)))
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.head.head._1 === 15)
    assert(newState.head.head._2 === 2)
  }

  // quando troca a hora no meio do stream. eh bem facil, tenho que manter os dois instantes
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((14, 2), (15, 5)))
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    val listToDoTheComparation = newState.get.toList

    assert(listToDoTheComparation(0)._1 === 14)
    assert(listToDoTheComparation(0)._2 === 3)

    assert(listToDoTheComparation(1)._1 === 15)
    assert(listToDoTheComparation(1)._2 === 5)
  }

  // quando no meu state tem dois instantes (devido ao caso anterior) e no meu stream vem
  // dados do ultimo instante. eh bem facil: devo manter somente os dados do ultimo instante.
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((15, 2)))
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1), (15,7)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.get.head._1 === 15)
    assert(newState.get.head._2 === 9)
  }


  // quando tenho um instante maior no meu stream. acho que eh o mesmo caso ja citado em {caso troca simples}
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List(List((16, 2)))
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1), (15,7)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.get.head._1 === 16)
    assert(newState.get.head._2 === 2)
  }

  // quando tenho dois istantes no state e nao tem nada no meu stream. o ideal eh
  // deixar somente o ultimo instante no meu state.
  "state updater" should "sum values in the most simple case " in {
    val values: Seq[Iterable[(Int, Long)]] = List()
    val state: Option[Iterable[(Int, Long)]] = Some(List((14, 1), (15,7)))

    val newState: Option[Iterable[(Int, Long)]] = StateUpdater.updateFunc(values, state)

    assert(newState.get.head._1 === 15)
    assert(newState.get.head._2 === 7)
  }

}
