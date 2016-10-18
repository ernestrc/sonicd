package build.unstable.sonicd.model

import build.unstable.sonic.model.Query
import spray.json._

object Fixture {
  val config = """{"class" : "SyntheticSource"}""".parseJson.asJsObject
  val traceId = "1234"
  val syntheticQuery = Query("10", config, None)
    .copy(query_id = Some(1), trace_id = Some(traceId))
}
