package build.unstable.sonicd.model

import build.unstable.sonicd.auth.ApiUser

case class RequestContext(traceId: String, user: Option[ApiUser])
