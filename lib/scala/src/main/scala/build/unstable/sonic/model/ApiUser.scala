package build.unstable.sonic.model

import java.net.InetAddress

case class ApiUser(user: String, authorization: Int, mode: AuthConfig.Mode, allowedIps: Option[List[InetAddress]])
