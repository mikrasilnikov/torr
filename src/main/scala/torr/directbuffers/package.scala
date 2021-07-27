package torr

import zio._
import zio.macros.accessible
import zio.nio.core.ByteBuffer

package object directbuffers {

  val DirectBufferSize = 32 * 1024;

  type DirectBufferPool = Has[DirectBufferPool.Service]

  @accessible
  object DirectBufferPool {
    trait Service {
      def allocate: Task[ByteBuffer]
      def free(buf: ByteBuffer): Task[Unit]
      def numAvailable: Task[Int]
    }
  }
}
