package build.unstable.sonicd.model

import build.unstable.tylog.TypedLogging
import org.slf4j.{Logger, LoggerFactory}

trait SonicdLogging extends TypedLogging {

  type TraceID = String

  sealed trait CallType

  //client
  case object BuildGraph extends CallType
  
  case object CreateTcpConnection extends CallType

  case object EstablishCommunication extends CallType

  //server
  case object HandleVersion extends CallType
  
  case object HandleGetHandlers extends CallType

  case object HandleSubscribe extends CallType

  case object HandleExtractWebSocketUpgrade extends CallType

  case object MaterializeSource extends CallType

  case object ValidateToken extends CallType

  case object CreateToken extends CallType

  final lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

}
