package torr

import torr.bencode.BEncode
import torr.channels._
import torr.metainfo.MetaInfo
import torr.misc.URLEncode
import zio._
import zio.blocking.Blocking
import zio.console._
import zio.nio.core.file.Path
import zio.nio.file.Files

import java.net._
import java.net.http._
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.StandardOpenOption
import scala.util.Random

object Main extends App {

  val fileName = "c:\\!temp\\Ragdoll_Masters v3.1.torrent"

  val getMetaInfo = AsyncFileChannel.open(Path(fileName), StandardOpenOption.READ)
    .use { channel =>
      for {
        data <- BEncode.read(channel)
        meta <- MetaInfo.fromBValue(data)
      } yield meta
    }

  val peerId = Random.nextBytes(20)

  def buildUrl(metainfo: MetaInfo): String = {
    val urlBuilder = new StringBuilder
    urlBuilder.append(metainfo.announce)
    urlBuilder.append("?info_hash=")
    urlBuilder.append(URLEncode.encode(metainfo.infoHash.toArray))
    urlBuilder.append("&peer_id=")
    urlBuilder.append(URLEncode.encode(Random.nextBytes(20)))
    urlBuilder.append("&port=6881")
    urlBuilder.append("&uploaded=0")
    urlBuilder.append("&downloaded=0")
    urlBuilder.append("&left=2818738176")

    urlBuilder.toString
  }

  def buildRequest(url: String): HttpRequest = {
    HttpRequest.newBuilder()
      .uri(URI.create(url))
      .build()
  }

  val client = HttpClient.newBuilder()
    .proxy(ProxySelector.of(InetSocketAddress.createUnresolved("localhost", 8080)))
    .build()

  val effect = for {
    meta     <- getMetaInfo
    url       = buildUrl(meta)
    request   = buildRequest(url)
    _        <- putStrLn(url)
    blocking <- ZIO.service[Blocking.Service]
    response <- blocking.effectBlocking(client.send(request, BodyHandlers.ofByteArray())).map(_.body())
    _        <- Files.writeBytes(
                  Path(s"$fileName.tracker.bin"),
                  Chunk.fromArray(response),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING
                )
    channel  <- InMemoryReadableChannel.make(response)
    decoded  <- BEncode.read(channel)
    _        <- putStrLn(decoded.prettyPrint(Int.MaxValue))
    _        <- Files.writeLines(
                  Path(s"$fileName.tracker.txt"),
                  List(decoded.prettyPrint(Int.MaxValue))
                )
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = { effect.exitCode }
}
