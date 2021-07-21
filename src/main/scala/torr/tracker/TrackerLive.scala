package torr.tracker

import zio._
import java.net._
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import torr.bencode.BEncode
import torr.misc.URLEncode
import zio.clock.Clock
import zio.duration.durationInt

import java.io.IOException

case class TrackerLive(client: HttpClient) extends Tracker.Service {
  def update(request: TrackerRequest): ZIO[Clock, Throwable, TrackerResponse] = {
    for {
      url      <- ZIO(buildUrl(request))
      request  <- ZIO(HttpRequest.newBuilder(URI.create(url)).build())
      data     <- ZIO.fromCompletableFuture(client.sendAsync(request, BodyHandlers.ofByteArray()))
                    .timeoutFail(new IOException("Timeout"))(30.seconds)
                    .refineToOrDie[IOException]
                    .retry(Schedule.recurs(10) && Schedule.spaced(10.seconds))
      bVal     <- BEncode.read(data.body())
      response <- ZIO.fromOption(TrackerResponse.interpret(bVal))
                    .orElseFail(new Exception("Could not interpret response"))
    } yield response
  }

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

object TrackerLive {
  def make(proxy: Option[InetSocketAddress]): ZLayer[Any, Throwable, Tracker] = {
    ZIO(buildClient(proxy)).map(client => TrackerLive(client)).toLayer
  }

  private def buildClient(proxy: Option[InetSocketAddress]): HttpClient = {
    val builder = HttpClient.newBuilder()
    proxy.fold(builder.build())(addr =>
      builder.proxy(ProxySelector.of(addr))
        .build()
    )
  }
}
