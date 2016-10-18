package build.unstable.sonic.model

case class RequestContext(traceId: String, user: Option[ApiUser])
