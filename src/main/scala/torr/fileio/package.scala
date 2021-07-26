package torr

import zio._
import zio.macros.accessible
import zio.nio.core.ByteBuffer

package object fileio {
  type FileIO = Has[FileIO.Service]

  @accessible
  object FileIO {
    trait Service {
      def fetch(piece: Int, offset: Int): Task[ByteBuffer]
      def store(piece: Int, offset: Int, data: ByteBuffer): Task[Unit]
      def hash(piece: Int): Task[Array[Byte]]
    }
  }
}
