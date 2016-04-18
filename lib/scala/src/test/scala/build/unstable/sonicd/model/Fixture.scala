package build.unstable.sonicd.model

import spray.json._

object Fixture {
  val config = """{"class" : "SyntheticSource"}""".parseJson.asJsObject
  val query = Query("10", config)
}
