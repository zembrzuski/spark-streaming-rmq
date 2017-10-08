package legacy

import com.google.gson.Gson

object StreamingJson {

  case class Dude(firstName: String, lastName: String, age: Int)

  val gson = new Gson

  def main(args: Array[String]): Unit = {
    val myString = """{"firstName":"Leonard","lastName":"Nimoy","age":81}"""

    val dude = gson.fromJson(myString, classOf[Dude])

    println("FNAME: " + dude.firstName)
    println("LNAME: " + dude.lastName)
  }

}
