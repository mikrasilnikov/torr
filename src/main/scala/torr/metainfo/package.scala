package torr

import zio._
import zio.nio.core.file.Path
import torr.bencode.{BEncode, BValue}
import torr.channels.AsyncFileChannel
import torr.misc.Traverse
import zio.blocking.Blocking
import zio.nio.core.Buffer
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.file.Files

import java.io.IOException
import java.nio.file.StandardOpenOption

package object metainfo {

  final case class MetaInfo(
      announce: String,
      pieceSize: Int,
      pieces: List[PieceHash],
      entries: List[FileEntry],
      infoHash: Chunk[Byte]
  )

  final case class PieceHash(value: Chunk[Byte])
  final case class FileEntry(path: Path, size: Long)

  object MetaInfo {

    def fromFile(fileName: String): RIO[Blocking, MetaInfo] = {
      for {
        data <- Files.readAllBytes(Path(fileName))
        root  = BEncode.read(data)
        res  <- fromBValue(root)
      } yield res
    }

    def fromBValue(root: BValue): Task[MetaInfo] = {
      import BValue._
      val createMetaInfoOption: Option[Chunk[Byte] => MetaInfo] = for {
        announce  <- (root / "announce").asString
        pieceSize <- (root / "info" / "piece length").asInt
        pieces    <- (root / "info" / "pieces").asChunk
                       .map(ch => ch.grouped(20).map(PieceHash).toList)
        entries   <- singleFile(root) orElse multiFile(root)
      } yield infoHash => MetaInfo(announce, pieceSize, pieces, entries, infoHash)

      val res = for {
        createMetaInfo <- ZIO.fromOption(createMetaInfoOption)
        info           <- ZIO.fromOption(root / "info")
        infoHash        = info.getSHA1
      } yield createMetaInfo(infoHash)

      res.mapError {
        case e: Throwable => e
        case _            => new Exception(s"Could not create metainfo from ${root.toString}")
      }
    }

    private def singleFile(root: BValue): Option[List[FileEntry]] =
      for {
        name   <- (root / "info" / "name").asString
        length <- (root / "info" / "length").asLong
      } yield FileEntry(Path(name), length) :: Nil

    private def multiFile(root: BValue): Option[List[FileEntry]] = {

      def makeEntry(map: BValue): Option[FileEntry] =
        for {
          length <- (map / "length").asLong
          path   <- (map / "path").asStringList.map(_.foldLeft(Path(""))(_ / _))
        } yield FileEntry(path, length)

      for {
        items  <- (root / "info" / "files").asList
        entries = items.map(makeEntry)
        res    <- Traverse.sequence(entries)
      } yield res
    }

  }
}
