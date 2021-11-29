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
      maxSimultaneousUploads: Int,
      proxy: Option[String]
  )

  case class TorrArgs(
      metaInfoPath: Path,
      additionalPeers: List[Peer]
  )

  val portOption: Options[Int] =
    Options.integer("port")
      .map(_.toInt)
      .alias("p")
      .withDefault(55123, "")
      .asInstanceOf[Options[Int]]

  val maxConnectionsOption: Options[Int] =
    Options.integer("maxConn")
      .map(_.toInt)
      .alias("c")
      .withDefault(500, "")
      .asInstanceOf[Options[Int]]

  val proxyOption: Options[Option[String]] =
    Options.text("proxy")
      .optional("Optional description")

  val maxSimultaneousDownloadsOption: Options[Int] =
    Options.integer("maxDown")
      .map(_.toInt)
      .alias("d")
      .withDefault(10, "")
      .asInstanceOf[Options[Int]]

  val maxSimultaneousUploadsOption: Options[Int] =
    Options.integer("maxUp")
      .map(_.toInt)
      .alias("u")
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
    (portOption ++ maxConnectionsOption ++ maxSimultaneousDownloadsOption ++ maxSimultaneousUploadsOption ++ proxyOption).map {
      case ((((port, maxConn), maxDown), maxUp), proxy) => TorrOptions(port, maxConn, maxDown, maxUp, proxy)
    },
    (torrentFileArg ++ additionalPeersArg).map(TorrArgs.tupled),
    HelpDoc.empty
  )
}
