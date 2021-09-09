package torr

import zio._
import zio.macros.accessible
import zio.nio.core.ByteBuffer
import torr.directbuffers.DirectBufferPool

package object fileio {

  val DefaultBufferSize: Int = 48 * 1024

  type FileIO = Has[FileIO.Service]

  @accessible
  object FileIO {
    trait Service {
      def fetch(piece: Int, offset: Int, amount: Int): ZManaged[DirectBufferPool, Throwable, Chunk[ByteBuffer]]
      def store(piece: Int, offset: Int, data: Chunk[ByteBuffer]): Task[Unit]
      def hash(piece: Int): Task[Array[Byte]]
    }
  }
}
