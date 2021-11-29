package torr.announce.test

import torr.bencode.{BEncode, BValue}
import torr.channels.InMemoryChannel
import torr.announce._
import zio.test.Assertion._
import zio.test._
import torr.metainfo.test.MetaInfoSuite._
import zio._

import java.nio.charset.StandardCharsets

object TrackerResponseSuite extends DefaultRunnableSpec {
  def spec =
    suite("TrackerResponseSuite")(
      //
      testM("Ubuntu tracker response") {
        val data             = getClass.getResourceAsStream("/ubuntu.tracker-response.bin").readAllBytes()
        val expectedPeers    = List(
          Peer("121.200.9.38", 6947, Some(toBytes("2d6c74304438302d37bf1865b3440a49da1d9735"))),
          Peer("185.242.6.198", 54968, Some(Chunk.fromArray("-TR3000-q7625caq3yls".getBytes(StandardCharsets.UTF_8))))
        )
        val expectedInterval = 1800

        for {
          bVal     <- ZIO(BEncode.read(Chunk.fromArray(data)))
          response <- ZIO.fromOption(TrackerResponse.interpret(bVal))
        } yield assert(())(anything) &&
          assert(response.interval)(equalTo(expectedInterval)) &&
          assert(response.peers)(startsWith(expectedPeers))
      },
      //
      testM("Rutracker response") {
        val data             = getClass.getResourceAsStream("/rutracker.tracker-response.bin").readAllBytes()
        val expectedPeers    = List(
          Peer("37.215.51.166", 59970, None),
          Peer("46.242.67.126", 6881, None)
        )
        val expectedInterval = 3088

        for {
          bVal     <- ZIO(BEncode.read(Chunk.fromArray(data)))
          response <- ZIO.fromOption(TrackerResponse.interpret(bVal))
        } yield assert(())(anything) &&
          assert(response.interval)(equalTo(expectedInterval)) &&
          assert(response.peers)(startsWith(expectedPeers))
      }
    )
}
