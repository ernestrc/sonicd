package build.unstable.sonic.server

import build.unstable.tylog.TypedLogging

trait ServerLogging extends TypedLogging {

  sealed trait CallType

  type TraceID = String

  case object MaterializeSource extends CallType

  case object HandleExtractWebSocketUpgrade extends CallType

  case object GenerateToken extends CallType

  case object JWTSignToken extends CallType

  case object JWTVerifyToken extends CallType

}
