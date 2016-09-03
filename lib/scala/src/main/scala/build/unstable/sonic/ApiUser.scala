package build.unstable.sonic

import java.net.InetAddress

import JsonProtocol._
import spray.json._

import scala.util.Try

//TODO should be a trait
// and move native auth to sonicd
case class ApiUser(user: String, authorization: Int, mode: ApiKey.Mode, allowedIps: Option[List[InetAddress]])

object ApiUser {
  def fromJWTClaims(verifiedClaims: java.util.Map[String, AnyRef]): Try[ApiUser] = Try {
    ApiKey.Mode(verifiedClaims.get("mode").asInstanceOf[String]).flatMap { mode ⇒
      Try {
        ApiUser(
          verifiedClaims.get("user").asInstanceOf[String],
          verifiedClaims.get("authorization").asInstanceOf[String].toInt,
          mode,
          Option(verifiedClaims.get("from").asInstanceOf[String]).map(_.parseJson.convertTo[List[InetAddress]])
        )
      }
    }
  }.flatten
}
