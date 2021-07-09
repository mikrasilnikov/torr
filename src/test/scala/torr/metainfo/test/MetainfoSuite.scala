package torr.metainfo.test

import torr.bencode.BEncode
import torr.channels.test.TestReadableChannel
import torr.metainfo._
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import java.math.BigInteger

object MetaInfoSuite extends DefaultRunnableSpec {
  def spec =
    suite("MetaInfoSuite")(
      //
      testM("Single file torrent") {
        val data     = getClass.getResourceAsStream("/torrent1.torrent").readAllBytes()
        val expected = MetaInfo(
          announce = "udp://tracker.openbittorrent.com:80/announce",
          pieceLength = 262144,
          entries = FileEntry(Path("file1.dat"), 524288) :: Nil,
          pieces =
            PieceHash(toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7")) ::
              PieceHash(toBytes("d93b208338769447004e90bf142769fc004d8b0c")) ::
              Nil
        )
        for {
          channel <- TestReadableChannel.make(data)
          bval    <- BEncode.read(channel)
          info     = MetaInfo.fromBValue(bval).get
        } yield assert(info)(equalTo(expected))
      }
    )

  // https://scalafiddle.io/sf/PZPHBlT/2
  def toBytes(hex: String): Chunk[Byte] = {

    val zeroChar: Byte = '0'.toByte
    val aChar: Byte    = 'a'.toByte

    def toNum(lowerHexChar: Char): Byte =
      (if (lowerHexChar < 'a') lowerHexChar.toByte - zeroChar else 10 + lowerHexChar.toByte - aChar).toByte

    val lowerHex = hex.toLowerCase

    val (result: Array[Byte], startOffset: Int) = if (lowerHex.length % 2 == 1) {
      // Odd
      val r = new Array[Byte]((lowerHex.length >> 1) + 1)
      r(0) = toNum(lowerHex(0))
      (r, 1)
    } else {
      // Even
      (new Array[Byte](lowerHex.length >> 1), 0)
    }

    var inputIndex  = startOffset
    var outputIndex = startOffset
    while (outputIndex < result.length) {

      val byteValue = (toNum(lowerHex(inputIndex)) * 16) + toNum(lowerHex(inputIndex + 1))
      result(outputIndex) = byteValue.toByte

      inputIndex += 2
      outputIndex += 1
    }

    Chunk.fromArray(result)
  }

}
