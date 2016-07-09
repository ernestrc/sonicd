package build.unstable.sonicd.model

import spray.json._

object Fixture {
  val config = """{"class" : "SyntheticSource"}""".parseJson.asJsObject
  val syntheticQuery = Query("10", config, None)
    .copy(query_id = Some(1), trace_id = Some("1234"))
}
