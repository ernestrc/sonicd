package build.unstable.sonicd.auth

import java.net.InetAddress

import build.unstable.sonicd.model.JsonProtocol._
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/**
 * @param authorization max security level that this api key is authorized for
 * @param from optional list of ip addresses that clients are allowed to connect from
 * @param tokenExpires optional token expiration override from global config
 */
case class ApiKey(key: String,
                  mode: ApiKey.Mode,
                  authorization: Int,
                  from: Option[List[InetAddress]],
                  tokenExpires: Option[FiniteDuration]) {

  override def equals(o: scala.Any): Boolean = o match {
    case that: ApiKey ⇒ that.key == this.key
    case _ ⇒ false
  }

  def toJWTClaims(user: String): java.util.Map[String, AnyRef] = {
    val claims = scala.collection.mutable.Map[String, AnyRef](
      "authorization" → authorization.toString,
      "user" → user,
      "mode" → mode.toString
    )

    //encode allowed ips with token
    from.foreach { ips ⇒
      claims.update("from", ips.toJson.compactPrint)
    }
    claims
  }
}


object ApiKey {

  sealed trait Mode {
    def canWrite: Boolean
  }

  object Mode {

    def apply(str: String): Try[Mode] = str match {
      case "r" ⇒ Success(Read)
      case "rw" ⇒ Success(ReadWrite)
      case e ⇒ Failure(new Exception(s"unknown mode '$e'"))
    }

    case object Read extends Mode {
      override def canWrite: Boolean = false

      override def toString: String = "r"
    }

    case object ReadWrite extends Mode {
      override def canWrite: Boolean = true

      override def toString: String = "rw"
    }

  }

}
