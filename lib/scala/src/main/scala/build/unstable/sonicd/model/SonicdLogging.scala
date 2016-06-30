package build.unstable.sonicd.model

import build.unstable.tylog.TypedLogging
import org.slf4j.{Logger, LoggerFactory}

trait SonicdLogging extends TypedLogging {

  type TraceID = String

  sealed trait CallType


  /* client */

  case object BuildGraph extends CallType
  
  case object CreateTcpConnection extends CallType

  case object EstablishCommunication extends CallType


  /* server */

  case object HandleVersion extends CallType
  
  case object HandleGetHandlers extends CallType

  case object HandleSubscribe extends CallType

  case object HandleExtractWebSocketUpgrade extends CallType

  case object MaterializeSource extends CallType

  case object ValidateToken extends CallType

  case object GenerateToken extends CallType
  
  case object JWTSignToken extends CallType
  
  case object JWTVerifyToken extends CallType


  /* sources */

  case object ExecuteStatement extends CallType

  case object GetJdbcHandle extends CallType

  case object RunInitializationStatements extends CallType

  case class RetryStatement(n: Int) extends CallType

  case class HttpReq(method: String) extends CallType


  case object DownloadHttpBody extends CallType

  case object ParseHttpBody extends CallType

  final lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

}
