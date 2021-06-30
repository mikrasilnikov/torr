package tor

import zio._
import tor.bencode._
import zio.duration._
import tor.channels.AsyncFileChannel
import zio.console._
import zio.nio.core.Buffer
import zio.nio.core.file.Path
import zio.nio.file.Files
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption

object Main extends App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val fileName = """c:\!temp\[rutracker.org].t5118555.torrent"""

    val app = AsyncFileChannel.open(Path(fileName), StandardOpenOption.READ)
      .use { chan =>
        for {
          bval <- BEncode.read(chan, 1024 * 100)
          str  <- ZIO(bval.prettyPrint)
          _    <- Files.writeBytes(
                    Path("""c:\!temp\[rutracker.org].t5118555.txt"""),
                    Chunk.fromArray(str.getBytes(StandardCharsets.UTF_8))
                  )
        } yield ()
      }

    app.exitCode

  }
}
