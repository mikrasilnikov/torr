package torr.bencode.test

import torr.bencode._
import torr.channels.InMemoryChannel
import torr.metainfo.test._
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, assert}

object BValueSpec extends DefaultRunnableSpec {
  override def spec =
    suite("BValueSuite")(
      //
      testM("Sha1 of torrent1") {
        val expected = MetaInfoSpec.toBytes("634b64cced753ec802c048d3db7784eed77cd2ff")
        val data     = getClass.getResourceAsStream("/torrent1.torrent").readAllBytes()
        for {
          channel <- InMemoryChannel.make(data)
          bVal    <- BEncode.read(channel)
          sha1    <- bVal.getSHA1
        } yield assert(sha1)(equalTo(expected))
      },
      //
      testM("Sha1 of torrent2") {
        val expected = MetaInfoSpec.toBytes("34985ac14fd2850bac5cc824518314f146c2e92a")
        val data     = getClass.getResourceAsStream("/torrent2.torrent").readAllBytes()
        for {
          channel <- InMemoryChannel.make(data)
          bVal    <- BEncode.read(channel)
          sha1    <- bVal.getSHA1
        } yield assert(sha1)(equalTo(expected))
      }
    )
}
