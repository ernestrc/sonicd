package build.unstable.sonic.client

import build.unstable.tylog.TypedLogging

trait ClientLogging extends TypedLogging {

  type TraceID = String

  sealed trait CallType

  case object CreateTcpConnection extends CallType

  case object EstablishCommunication extends CallType

}
