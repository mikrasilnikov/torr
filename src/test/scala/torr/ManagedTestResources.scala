package torr

import zio._

import java.util.zip.ZipInputStream
import scala.io._

trait ManagedTestResources {

  protected def loadFileManaged

  protected def loadZippedTextFileManaged(name: String): ZManaged[Any, Throwable, String] =
    ZManaged.make(
      ZIO.effect {
        val zipStream = new ZipInputStream(getClass.getResourceAsStream(name))
        val entry     = zipStream.getNextEntry
        val buf       = new Array[Byte](entry.getSize.toInt)

        var off = 0
        while (off < buf.length - 1) {
          val bytesRead = zipStream.read(buf, off, buf.size - off)
          off += bytesRead
        }

        Source.fromBytes(buf)(Codec.UTF8)
      }
    )(source => ZIO.effectTotal(source.close()))
      .map { s => s.getLines().mkString("\n") }
}
