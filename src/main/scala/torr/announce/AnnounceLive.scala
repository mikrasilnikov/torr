package torr.announce

import zio._

import java.net._
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import torr.bencode.BEncode
import torr.misc.URLEncode
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging._

import java.io.IOException
import java.nio.charset.StandardCharsets

case class AnnounceLive(client: HttpClient) extends Announce.Service {

  private val announceRetries = 10

  def update(trackerRequest: TrackerRequest): ZIO[Clock with Logging, Throwable, TrackerResponse] = {
    for {
      url      <- ZIO(buildUrl(trackerRequest))
      request  <- ZIO(HttpRequest.newBuilder(URI.create(url)).build())
      data     <- fetch(request)
      bVal     <- ZIO(BEncode.read(Chunk.fromArray(data)))
                    .orElseFail(new Exception(
                      s"Could not decode tracker response: ${new String(data, StandardCharsets.UTF_8)}"
                    ))
      response <- ZIO.fromOption(TrackerResponse.interpret(bVal))
                    .orElseFail(new Exception("Could not interpret response"))
    } yield response
  }

  def fetch(httpRequest: HttpRequest): ZIO[Clock with Logging, IOException, Array[Byte]] =
    for {
      data <- ZIO.fromCompletableFuture(client.sendAsync(httpRequest, BodyHandlers.ofByteArray()))
                .refineToOrDie[IOException]
    } yield data.body()

  def buildUrl(request: TrackerRequest): String = {
    val urlBuilder = new StringBuilder
    urlBuilder.append(request.announceUrl)
    urlBuilder.append("?info_hash=")
    urlBuilder.append(URLEncode.encode(request.infoHash))
    urlBuilder.append("&peer_id=")
    urlBuilder.append(URLEncode.encode(request.peerId))
    urlBuilder.append("&port=")
    urlBuilder.append(request.port)
    urlBuilder.append("&uploaded=")
    urlBuilder.append(request.uploaded)
    urlBuilder.append("&downloaded=")
    urlBuilder.append(request.downloaded)
    urlBuilder.append("&left=")
    urlBuilder.append(request.left)

    urlBuilder.toString
  }
}

object AnnounceLive {

  def make(proxy: Option[String]): ZLayer[Any, Throwable, Announce] = {

    def makeAddressOption =
      proxy.map { s =>
        val (host :: portStr :: Nil) = s.split(':').toList
        val port                     = portStr.toInt
        InetSocketAddress.createUnresolved(host, port)
      }

    val effect = for {
      addressOption <- ZIO(makeAddressOption)
                         .orElseFail(new IllegalArgumentException("Could not parse proxy server. Specify host:port"))
      client        <- ZIO(buildClient(addressOption))
    } yield AnnounceLive(client)

    effect.toLayer
  }

  private def buildClient(proxy: Option[InetSocketAddress]): HttpClient = {
    val builder = HttpClient.newBuilder()
    proxy.fold(builder.build())(addr =>
      builder.proxy(ProxySelector.of(addr))
        .build()
    )
  }
}
