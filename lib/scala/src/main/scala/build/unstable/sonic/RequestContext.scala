package build.unstable.sonic

import build.unstable.sonicd.auth.ApiUser

case class RequestContext(traceId: String, user: Option[ApiUser])
