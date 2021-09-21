package torr.metainfo.test

import torr.bencode.BEncode
import torr.channels.InMemoryChannel
import torr.metainfo._
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._

object MetaInfoSpec extends DefaultRunnableSpec {
  def spec =
    suite("MetaInfoSuite")(
      //
      //
      testM("Single file torrent") {
        val data     = getClass.getResourceAsStream("/torrent1.torrent").readAllBytes()
        val expected = MetaInfo(
          announce = "udp://tracker.openbittorrent.com:80/announce",
          pieceSize = 262144,
          entries = FileEntry(Path("file1.dat"), 524288) :: Nil,
          pieceHashes = Vector(
            toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7"),
            toBytes("d93b208338769447004e90bf142769fc004d8b0c")
          ),
          infoHash = Chunk.fromArray(
            Array[Byte](-81, -93, -38, -63, -123, 80, -128, -23, -44, 115, 25, 102, 115, 73, -8, -128, 1, -36, -23,
              -127)
          )
        )

        for {
          bval   <- ZIO(BEncode.read(Chunk.fromArray(data)))
          actual <- MetaInfo.fromBValue(bval)
        } yield assert(actual)(equalTo(expected))
      },
      //
      //
      testM("Multi file torrent") {
        val data     = getClass.getResourceAsStream("/torrent2.torrent").readAllBytes()
        val expected = MetaInfo(
          announce = "udp://tracker.openbittorrent.com:80/announce",
          pieceSize = 262144,
          entries = FileEntry(Path("file1.dat"), 524288) :: FileEntry(Path("subdir/file2.dat"), 524288) :: Nil,
          pieceHashes = Vector(
            toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7"),
            toBytes("d93b208338769447004e90bf142769fc004d8b0c"),
            toBytes("d93b208338769447004e90bf142769fc004d8b0c"),
            toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7")
          ),
          infoHash = Chunk.fromArray(
            Array[Byte](-20, -4, 67, -91, 14, -110, -79, -102, 42, 35, -80, -78, -86, -100, 45, -95, -101, -117, 109,
              -4)
          )
        )
        for {
          bval   <- ZIO(BEncode.read(Chunk.fromArray(data)))
          actual <- MetaInfo.fromBValue(bval)
        } yield assert(actual)(equalTo(expected))
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
