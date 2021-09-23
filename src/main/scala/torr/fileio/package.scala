package torr

import zio._
import zio.macros.accessible
import zio.nio.core.ByteBuffer
import torr.directbuffers.DirectBufferPool
import torr.metainfo._

package object fileio {

  val DefaultBufferSize: Int = 64 * 1024

  type FileIO = Has[FileIO.Service]

  @accessible
  object FileIO {
    trait Service {
      def fetch(piece: Int, offset: Int, amount: Int): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]]
      def store(piece: Int, offset: Int, data: Chunk[ByteBuffer]): Task[Unit]
      def flush: Task[Unit]
      def metaInfo: Task[MetaInfo]
      def freshFilesWereAllocated: Task[Boolean]
    }
  }
}
