package torr

import zio.Task
import zio.nio.core.ByteBuffer

package object channels {

  trait SeekableByteChannel extends ByteChannel with SeekableChannel

  trait ByteChannel {
    def read(buf: ByteBuffer): Task[Int]
    def write(buf: ByteBuffer): Task[Int]
    def isOpen: Task[Boolean]
    def close: Task[Unit]
  }

  trait SeekableChannel {
    def read(buf: ByteBuffer, position: Long): Task[Int]
    def write(buf: ByteBuffer, position: Long): Task[Int]
  }
}
