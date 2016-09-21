package build.unstable.sonic

import build.unstable.tylog.TypedLogging
import org.slf4j.{Logger, LoggerFactory}

trait ClientLogging extends TypedLogging {

  type TraceID = String

  sealed trait CallType

  /* client */

  case object BuildGraph extends CallType

  case object CreateTcpConnection extends CallType

  case object EstablishCommunication extends CallType

}
