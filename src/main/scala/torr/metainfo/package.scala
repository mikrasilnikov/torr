package torr

import zio._
import zio.nio.core.file.Path
import torr.bencode.BValue
import torr.misc.Traverse

package object metainfo {

  final case class MetaInfo(
      announce: String,
      pieceLength: Long,
      pieces: List[PieceHash],
      entries: List[FileEntry],
      infoHash: Chunk[Byte]
  )

  final case class PieceHash(value: Chunk[Byte])
  final case class FileEntry(path: Path, size: Long)

  object MetaInfo {
    def fromBValue(root: BValue): Task[MetaInfo] = {
      import BValue._
      val createMetaInfoOption: Option[Chunk[Byte] => MetaInfo] = for {
        announce    <- (root / "announce").asString
        pieceLength <- (root / "info" / "piece length").asLong
        pieces      <- (root / "info" / "pieces").asChunk
                         .map(ch => ch.grouped(20).map(PieceHash).toList)
        entries     <- singleFile(root) orElse multiFile(root)
      } yield infoHash => MetaInfo(announce, pieceLength, pieces, entries, infoHash)

      val res = for {
        createMetaInfo <- ZIO.fromOption(createMetaInfoOption)
        info           <- ZIO.fromOption(root / "info")
        infoHash       <- info.getSHA1
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