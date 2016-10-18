package build.unstable.sonic.model

import java.net.InetAddress

case class NewQuery(query: Query, clientAddress: Option[InetAddress])
