package torr.fileio.test

import zio._
import zio.nio.core._
import torr.channels.InMemoryChannel
import torr.fileio.Actor.{OpenedFile, State}
import torr.fileio.Cache
import scala.util.Random

object Helpers {

  def randomChunk(rnd: java.util.Random, size: Int): Chunk[Byte] =
    Chunk.fromArray(
      Random.javaRandomToRandom(rnd).nextBytes(size)
    )

  def randomBuf(rnd: java.util.Random, size: Int): Task[(ByteBuffer, Chunk[Byte])] =
    for {
      chunk <- ZIO(randomChunk(rnd, size))
      buf   <- Buffer.byte(chunk)
    } yield (buf, chunk)

  def bufToChunk(buf: ByteBuffer): Task[Chunk[Byte]] =
    for {
      _   <- buf.position(0)
      _   <- buf.limit(buf.capacity)
      res <- buf.getChunk()
    } yield res

  def createState(
      rnd: java.util.Random,
      sizes: List[Int],
      pieceSize: Int,
      cacheEntrySize: Int,
      cacheEntriesNum: Int
  ): Task[State] =
    for {
      files     <- createFiles(rnd, sizes)
      cacheState = Cache(cacheEntrySize, cacheEntriesNum)
    } yield State(pieceSize, sizes.sum, files, cacheState)

  def createState(
      fileContents: List[Chunk[Byte]],
      pieceSize: Int,
      cacheEntrySize: Int,
      cacheEntriesNum: Int
  ): Task[State] =
    for {
      files     <- createFiles(fileContents)
      cacheState = Cache(cacheEntrySize, cacheEntriesNum)
    } yield State(pieceSize, fileContents.map(_.size).sum, files, cacheState)

  def createFiles(
      rnd: java.util.Random,
      sizes: List[Int]
  ): Task[Vector[OpenedFile]] = {
    val contents = sizes.map(s => Chunk.fromArray(Random.javaRandomToRandom(rnd).nextBytes(s)))
    createFiles(contents)
  }

  def createFiles(
      contents: List[Chunk[Byte]],
      offset: Int = 0,
      res: Vector[OpenedFile] = Vector.empty
  ): Task[Vector[OpenedFile]] =
    contents match {
      case Nil     => ZIO.succeed(res)
      case c :: cs => InMemoryChannel.make(c).flatMap { channel =>
          createFiles(cs, offset + c.size, res :+ OpenedFile(offset, c.size, channel))
        }
    }
}
