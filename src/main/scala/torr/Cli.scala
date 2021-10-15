package torr

import zio.cli._
import java.nio.file.Path

object Cli {

  case class TorrOptions(
      maxConnections: Int,
      maxActivePeers: Int,
      proxy: Option[String],
      protodump: Boolean
  )

  case class TorrArgs(metaInfoPath: Path, additionalPeers: List[String])

  val maxConnectionsOption: Options[Int] =
    Options.integer("maxConnections")
      .alias("mc")
      .withDefault(50, "50 active connections")
      .asInstanceOf[Options[Int]]

  val proxyOption: Options[Option[String]] =
    Options.text("proxy")
      .alias("px")
      .optional("Optional description")

  val protodumpOption: Options[Boolean] =
    Options.boolean("protodump")
      .alias("pd")

  val maxActivePeersOption: Options[Int] =
    Options.integer("maxActivePeers")
      .alias("mp")
      .withDefault(10, "10 active peers")
      .asInstanceOf[Options[Int]]

  val torrentFileArg: Args[Path]             = Args.file(Exists.Yes)
  val additionalPeersArg: Args[List[String]] = Args.text("additionalPeer").repeat

  val default = Command(
    "torr.jar",
    (maxConnectionsOption ++ maxActivePeersOption ++ proxyOption ++ protodumpOption).map {
      case (((mc, ap), pr), pd) => TorrOptions(mc, ap, pr, pd)
    },
    (torrentFileArg ++ additionalPeersArg).map(TorrArgs.tupled),
    HelpDoc.empty
  )
}
