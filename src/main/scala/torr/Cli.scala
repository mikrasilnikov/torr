package torr

import torr.announce.Peer
import zio.{IO, ZIO}
import zio.cli._

import java.nio.file.Path

object Cli {

  case class TorrOptions(
      port: Int,
      maxConnections: Int,
      maxSimultaneousDownloads: Int,
      proxy: Option[String]
  )

  case class TorrArgs(
      metaInfoPath: Path,
      additionalPeers: List[Peer]
  )

  val portOption: Options[Int] =
    Options.integer("port")
      .alias("p")
      .withDefault(55123, "")
      .asInstanceOf[Options[Int]]

  val maxConnectionsOption: Options[Int] =
    Options.integer("maxConnections")
      .alias("c")
      .withDefault(50, "50 active connections")
      .asInstanceOf[Options[Int]]

  val proxyOption: Options[Option[String]] =
    Options.text("proxy")
      .optional("Optional description")

  val maxSimultaneousDownloadsOption: Options[Int] =
    Options.integer("maxSimultaneousDownloads")
      .alias("d")
      .withDefault(10, "")
      .asInstanceOf[Options[Int]]

  val torrentFileArg: Args[Path]           = Args.file(Exists.Yes)
  val additionalPeersArg: Args[List[Peer]] =
    Args.text("additionalPeer")
      .mapTry { s =>
        val parts = s.split(':')
        Peer(parts(0), parts(1).toInt)
      }
      .repeat

  val default = Command(
    "torr.jar",
    (portOption ++ maxConnectionsOption ++ maxSimultaneousDownloadsOption ++ proxyOption).map {
      case (((port, maxConn), ap), pr) => TorrOptions(port, maxConn, ap, pr)
    },
    (torrentFileArg ++ additionalPeersArg).map(TorrArgs.tupled),
    HelpDoc.empty
  )
}
