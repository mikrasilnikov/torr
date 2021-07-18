
import io.netty.handler.codec.http.QueryStringEncoder
import torr.bencode.BEncode
import torr.channels.AsyncFileChannel
import zio._
import zhttp.http._
import zhttp.service._
import zio.console._
import zio.nio.core.file.Path
import java.net.URLEncoder

import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import scala.util.Random



def createUrl(infoHash : Array[Byte]) : String = {



  val qEncoder = new QueryStringEncoder("https://torrent.ubuntu.com/announce")
  qEncoder.addParam("info_hash", URLEncoder.encode(new String(infoHash, StandardCharsets.ISO_8859_1)))
  qEncoder.addParam("peer_id", URLEncoder.encode(new String(infoHash, StandardCharsets.ISO_8859_1)))
  qEncoder.addParam("event", "started")
  qEncoder.addParam("uploaded", "0")
  qEncoder.addParam("downloaded", "0")
  qEncoder.addParam("left", "2818738176")
  qEncoder.addParam("port", "6881")
  qEncoder.addParam("compact", "0")
  qEncoder.toString
}

val getMetaInfo =
  AsyncFileChannel.open(Path("c:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"), StandardOpenOption.READ)
    .use { c => BEncode.read(c) }

val program = for {
  metainfo <- getMetaInfo
  info      = (metainfo / "info").get
  infoHash  <- info.getSHA1
  _         <- putStrLn(infoHash.toArray.map("%02X".format(_)).mkString)
  url        = createUrl(infoHash.toArray)
  _         <- putStrLn(url)
  headers   = List(Header.host("torrent.ubuntu.com"))
  res       <- Client.request(url, headers)
  _         <- console.putStrLn {
    res.content match {
      case HttpData.CompleteData(data) => data.map(_.toChar).mkString
      case HttpData.StreamData(_) => "<Chunked>"
      case HttpData.Empty => ""
    }
  }
} yield ()

val env = ChannelFactory.auto ++ EventLoopGroup.auto()
Runtime.default.unsafeRun(program.provideCustomLayer(env))

//val program = for {
//  res <- Client.request(url, headers)
//  _   <- console.putStrLn {
//    res.content match {
//      case HttpData.CompleteData(data) => data.map(_.toChar).mkString
//      case HttpData.StreamData(_)      => "<Chunked>"
//      case HttpData.Empty              => ""
//    }
//  }
//} yield ()



