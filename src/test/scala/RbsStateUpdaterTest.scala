import org.scalatest.FlatSpec

class RbsStateUpdaterTest extends FlatSpec {

  val rbsStateUpdater = RbsStateUpdater

  "test1" should "extract noticia" in {
    val input = "http://zh.clicrbs.com.br/rs/noticia/2015/09/nao-sei-o-que-acontecia-antes-mas-se-trabalhava-pouco-diz-argel-sobre-preparacao-fisica-51755831.html"

    val session = rbsStateUpdater.sessionFromUrlExtractor(input)

    assert(session === "/noticia/")
  }

  "test2" should "extract noticia" in {
    val input = "http://zh.clicrbs.com.br/rs/esportes/gremio/2015/09/nao-sei-o-que-acontecia-antes-mas-se-trabalhava-pouco-diz-argel-sobre-preparacao-fisica-51755831.html"

    val session = rbsStateUpdater.sessionFromUrlExtractor(input)

    assert(session === "/esportes/gremio/")
  }

}