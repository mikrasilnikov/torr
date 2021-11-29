package torr.channels.test

import zio._
import zio.test._
import zio.test.Assertion._
import torr.channels.ByteChannel
import zio.nio.core.Buffer
import java.nio.charset.StandardCharsets

object TestSocketChannelSuite extends DefaultRunnableSpec {
  override def spec =
    suite("TestSocketChannelSuite")(
      //
      testM("writing") {
        val expected = Chunk.fromArray("Hello привет".getBytes(StandardCharsets.UTF_8))
        for {
          channel <- TestSocketChannel.make
          _       <- writeAll(channel, expected)
          actual  <- readAmount(channel.remote, expected.length)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("reading") {
        val expected = Chunk.fromArray("Hello привет".getBytes(StandardCharsets.UTF_8))
        for {
          channel <- TestSocketChannel.make
          _       <- writeAll(channel.remote, expected)
          actual  <- readAmount(channel, expected.length)
        } yield assert(actual)(equalTo(expected))
      }
    )

  //noinspection SimplifyUnlessInspection
  def writeAll(channel: ByteChannel, data: Chunk[Byte]): Task[Unit] = {
    if (data.isEmpty) ZIO.unit
    else {
      for {
        buf     <- Buffer.byte(data)
        written <- channel.write(buf)
        _       <- writeAll(channel, data.drop(written))
      } yield ()
    }
  }

  def readAmount(channel: ByteChannel, amount: Int, acc: Chunk[Byte] = Chunk.empty): Task[Chunk[Byte]] = {
    if (amount == 0) ZIO.succeed(acc)
    else {
      for {
        buf       <- Buffer.byte(amount)
        bytesRead <- channel.read(buf) <* buf.flip
        chunk     <- buf.getChunk()
        res       <- readAmount(channel, amount - bytesRead, acc ++ chunk)
      } yield res
    }
  }
}
