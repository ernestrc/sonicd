package build.unstable.sonicd.api.auth

//TODO this should be the result of decoding a jwt token
case class ApiUser(user: String, autorization: Int, mode: Mode, ip: Option[String]) {
  override def toString: String = s"$user@$ip"
}
