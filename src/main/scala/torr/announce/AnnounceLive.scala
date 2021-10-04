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

case class AnnounceLive(client: HttpClient) extends Announce.Service {

  private val announceRetries = 10

  def update(trackerRequest: TrackerRequest): ZIO[Clock with Logging, Throwable, TrackerResponse] = {
    for {
      url      <- ZIO(buildUrl(trackerRequest))
      request  <- ZIO(HttpRequest.newBuilder(URI.create(url)).build())
      data     <- fetchWithRetry(request)
      bVal      = BEncode.read(Chunk.fromArray(data))
      response <- ZIO.fromOption(TrackerResponse.interpret(bVal))
                    .orElseFail(new Exception("Could not interpret response"))
    } yield response
  }

  def fetchWithRetry(httpRequest: HttpRequest): ZIO[Clock with Logging, IOException, Array[Byte]] =
    for {
      _       <- ZIO.unit
      schedule = (Schedule.recurs(announceRetries) && Schedule.spaced(10.seconds))
                   .tapOutput { case (n, _) => log.warn(s"Retrying ${httpRequest.uri()}").when(n < announceRetries) }
      data    <- ZIO.fromCompletableFuture(client.sendAsync(httpRequest, BodyHandlers.ofByteArray()))
                   .timeoutFail(new IOException("Timeout"))(30.seconds)
                   .refineToOrDie[IOException]
                   .retry(schedule)
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
  def make(proxy: Option[InetSocketAddress]): ZLayer[Any, Throwable, Announce] = {
    ZIO(buildClient(proxy)).map(client => AnnounceLive(client)).toLayer
  }

  private def buildClient(proxy: Option[InetSocketAddress]): HttpClient = {
    val builder = HttpClient.newBuilder()
    proxy.fold(builder.build())(addr =>
      builder.proxy(ProxySelector.of(addr))
        .build()
    )
  }
}
