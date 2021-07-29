package torr.fileio.test

import torr.actorsystem.ActorSystemLive
import torr.channels._
import torr.directbuffers.DirectBufferPoolLive
import torr.fileio.Actor
import torr.fileio.Actor.{File, State}
import zio._
import zio.clock.Clock
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential

import scala.util.Random

object FileIOSpec extends DefaultRunnableSpec {

  val env0 = Clock.live ++ ActorSystemLive.make("Test") ++ Slf4jLogger.make((_, message) => message)

  private val sampleData = Chunk.fromArray(Random.nextBytes(128 * 1024))

  /** Creates a chunk of torr.fileio.Actor.File with given sizes from sample data. */
  private def createSampleFiles(sizes: Seq[Int]): ZIO[Any, Throwable, Chunk[File]] = {
    case class State(remSizes: Seq[Int], remData: Chunk[Byte], dataOffset: Int)

    Chunk.unfoldM(State(sizes, sampleData, dataOffset = 0)) {
      case State(Nil, _, _)                              => ZIO.none
      case State(size :: _, data, _) if size > data.size => ZIO.fail(new Throwable("Not enough sample data"))
      case State(sizeHead :: sizeTail, data, offset)     =>
        val (chunk, chunks) = data.splitAt(sizeHead)
        for {
          channel <- InMemoryChannel.make(chunk)
          file     = torr.fileio.Actor.File(offset, sizeHead, channel)
        } yield Some(file, State(sizeTail, chunks, offset + sizeHead))
      case _                                             => ???
    }
  }

  def spec =
    suite("FileIOSuite")(
      //
      testM("Search by offset - single file 1") {
        for {
          channel <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb")
          file     = File(0, 20, channel)
          state    = State(10, Vector(file))
          (i, o)   = Actor.fileIndexOffset(0, 0, state)
        } yield assert(i, o)(equalTo(0, 0L))
      },
      //
      testM("Search by offset - single file 2") {
        for {
          channel <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb")
          file     = File(0, 20, channel)
          state    = State(10, Vector(file))
          (i, o)   = Actor.fileIndexOffset(1, 5, state)
        } yield assert(i, o)(equalTo(0, 15L))
      },
      //
      testM("Search by offset - multiple files x00") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(0, 5, state)
        } yield assert(i, o)(equalTo(0, 5L))
      },
      //
      testM("Search by offset - multiple files 0x0") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(2, 3, state)
        } yield assert(i, o)(equalTo(1, 3L))
      },
      //
      testM("Search by offset - multiple files 00x") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(5, 1, state)
        } yield assert(i, o)(equalTo(2, 11L))
      },
      //
      testM("Creating sample files") {
        val sizes = List(79, 83, 97)
        for {
          files      <- createSampleFiles(sizes)
          fileData   <- ZIO.foreach(files)(_.channel.asInstanceOf[InMemoryChannel].getData)
          rem0        = sampleData
          (ef0, rem1) = rem0.splitAt(sizes(0))
          (ef1, rem2) = rem1.splitAt(sizes(1))
          (ef2, _)    = rem2.splitAt(sizes(2))
        } yield assert()(anything) &&
          assert(files(0).offset)(equalTo(0L)) &&
          assert(files(0).size)(equalTo(79L)) &&
          assert(fileData(0))(equalTo(ef0)) &&
          assert(files(1).offset)(equalTo(79L)) &&
          assert(files(1).size)(equalTo(83L)) &&
          assert(fileData(1))(equalTo(ef1)) &&
          assert(files(2).offset)(equalTo(79L + 83L)) &&
          assert(files(2).size)(equalTo(97L)) &&
          assert(fileData(2))(equalTo(ef2))
      },
      //
      testM("Reading - single block, no overlap, no offset") {
        val effect =
          for {
            files <- createSampleFiles(List(64))
            resB  <- Actor.read(files, 0, 0, 32)
            resC  <- resB.head.getChunk()
          } yield assert(resB.size)(equalTo(1)) &&
            assert(resC)(equalTo(sampleData.slice(0, 32)))

        val env = env0 >>> DirectBufferPoolLive.make(1, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - single block, no overlap, with offset") {
        val effect =
          for {
            files <- createSampleFiles(List(64))
            resB  <- Actor.read(files, 0, 16, 32)
            resC  <- resB.head.getChunk()
          } yield assert(resB.size)(equalTo(1)) &&
            assert(resC)(equalTo(sampleData.slice(16, 48)))

        val env = env0 >>> DirectBufferPoolLive.make(1, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - single block, overlap, with offset") {
        val effect =
          for {
            files <- createSampleFiles(List(32, 32))
            resB  <- Actor.read(files, 0, 16, 32)
            resC  <- resB.head.getChunk()
          } yield assert(resB.size)(equalTo(1)) &&
            assert(resC)(equalTo(sampleData.slice(16, 48)))

        val env = env0 >>> DirectBufferPoolLive.make(1, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - single block, no overlap, block size = file size") {
        val effect =
          for {
            files <- createSampleFiles(List(32))
            resB  <- Actor.read(files, 0, 0, 32)
            resC  <- resB.head.getChunk()
          } yield assert(resB.size)(equalTo(1)) &&
            assert(resC)(equalTo(sampleData.slice(0, 32)))

        val env = env0 >>> DirectBufferPoolLive.make(1, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - single block, overlap, requested size < block size") {
        val effect =
          for {
            files <- createSampleFiles(List(32, 32, 32))
            resB  <- Actor.read(files, 1, 16, 31)
            resC  <- resB.head.getChunk()
          } yield assert(resB.size)(equalTo(1)) &&
            assert(resC)(equalTo(sampleData.slice(32 + 16, 32 + 16 + 31)))

        val env = env0 >>> DirectBufferPoolLive.make(1, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - multiple blocks, no overlap") {
        val effect =
          for {
            files   <- createSampleFiles(List(64, 64, 64, 64))
            resBufs <- Actor.read(files, 1, 0, 64)
            chunk0  <- resBufs(0).getChunk()
            chunk1  <- resBufs(1).getChunk()
          } yield assert(resBufs.size)(equalTo(2)) &&
            assert(chunk0 ++ chunk1)(equalTo(sampleData.slice(64, 64 + 64)))

        val env = env0 >>> DirectBufferPoolLive.make(2, 32)

        effect.provideCustomLayer(env)
      },
      //
      testM("Reading - multiple blocks, with overlap, size is not divisible by bufSize") {
        val effect =
          for {
            files   <- createSampleFiles(List(64, 64, 64, 64))
            resBufs <- Actor.read(files, 1, 32, 95)
            chunk0  <- resBufs(0).getChunk()
            chunk1  <- resBufs(1).getChunk()
            chunk2  <- resBufs(2).getChunk()
          } yield assert(resBufs.size)(equalTo(3)) &&
            assert(chunk0 ++ chunk1 ++ chunk2)(equalTo(sampleData.slice(96, 96 + 95)))

        val env = env0 >>> DirectBufferPoolLive.make(3, 32)

        effect.provideCustomLayer(env)
      }
    ) @@ sequential
}
