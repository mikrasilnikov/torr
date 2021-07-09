package torr

import zio._
import torr.bencode._
import zio.duration._
import torr.channels.AsyncFileChannel
import zio.console._
import zio.nio.core.Buffer
import zio.nio.core.file.Path
import zio.nio.file.Files
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption

object Main extends App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val fileName = "c:\\!temp\\LINQPad.torrent"

    val app = AsyncFileChannel.open(Path(fileName), StandardOpenOption.READ)
      .use { chan =>
        for {
          bval <- BEncode.read(chan)
          str  <- ZIO(bval.prettyPrint)
          _    <- Files.writeBytes(
                    Path("c:\\!temp\\LINQPad.torrent.txt"),
                    Chunk.fromArray(str.getBytes(StandardCharsets.UTF_8))
                  )
        } yield ()
      }

    app.exitCode

  }
}
