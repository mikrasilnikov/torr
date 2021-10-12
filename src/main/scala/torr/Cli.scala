package torr

import zio.cli._
import java.nio.file.Path

object Cli {

  case class TorrOptions(maxConnections: Int, maxActivePeers: Int, proxy: Option[String])
  case class TorrArgs(metaInfoPath: Path, additionalPeers: List[String])

  val maxConnectionsOption: Options[Int] =
    Options.integer("maxConnections")
      .alias("mc")
      .withDefault(500, "500 active connections")
      .asInstanceOf[Options[Int]]

  val proxyOption: Options[Option[String]] =
    Options.text("proxy")
      .alias("p")
      .optional("Optional description")

  val maxActivePeersOption: Options[Int] =
    Options.integer("maxActivePeers")
      .alias("mp")
      .withDefault(10, "10 active peers")
      .asInstanceOf[Options[Int]]

  val torrentFileArg: Args[Path]             = Args.file(Exists.Yes)
  val additionalPeersArg: Args[List[String]] = Args.text("additionalPeer").repeat

  val default = Command(
    "torr.jar",
    (maxConnectionsOption ++ maxActivePeersOption ++ proxyOption).map {
      case ((mc, ap), pr) => TorrOptions(mc, ap, pr)
    },
    (torrentFileArg ++ additionalPeersArg).map(TorrArgs.tupled),
    HelpDoc.empty
  )
}
