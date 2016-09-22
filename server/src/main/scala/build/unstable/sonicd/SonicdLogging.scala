package build.unstable.sonicd

import build.unstable.tylog.TypedLogging
import org.slf4j.{Logger, LoggerFactory}

trait SonicdLogging extends TypedLogging {

  type TraceID = String

  sealed trait CallType


  /* service */

  case object HandleExtractWebSocketUpgrade extends CallType

  case object MaterializeSource extends CallType

  case object AuthenticateUser extends CallType

  case object GenerateToken extends CallType
  
  case object JWTSignToken extends CallType
  
  case object JWTVerifyToken extends CallType


  /* sources */

  case object ExecuteStatement extends CallType

  case object GetJdbcHandle extends CallType

  case object RunInitializationStatements extends CallType

  case class RetryStatement(n: Int) extends CallType

  case class HttpReq(method: String, endpoint: String) extends CallType


  case object DownloadHttpBody extends CallType

  case object ParseHttpBody extends CallType

}
