package build.unstable.sonicd.api.auth

/**
 * @param authorization max security level that this api key is authorized for
 */
case class ApiKey(key: String, mode: Mode, authorization: Int)

sealed trait Mode

object Mode {

  case object Read extends Mode

  case object ReadWrite extends Mode

}
