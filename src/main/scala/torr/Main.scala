package torr

import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorSystem, Context}
import zio.clock.Clock
import zio.console.putStrLn
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.nio.core._
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path
import zio.nio.file.Files

import java.nio.file.StandardOpenOption
import java.security.MessageDigest

object Main extends App {

  val path =
    "d:\\Torrents\\Black_Mirror_(s05)_1080p\\Black.Mirror.2019.S05E01.Striking.Vipers.1080p.NF.WEBRip.DD5.1.x264-NTb.mkv"

  val md = java.security.MessageDigest.getInstance("SHA-1")
  //hash    <- ZIO(md.digest(data))

  def scan(channel: AsynchronousFileChannel, free: Queue[ByteBuffer], filled: Queue[ByteBuffer]): Task[Unit] = {
    def loop(position: Long, size: Long, free: Queue[ByteBuffer], filled: Queue[ByteBuffer]): Task[Unit] =
      position match {
        case p if p >= size => ZIO.unit
        case _              =>
          for {
            buf  <- free.take
            read <- buf.clear *> channel.read(buf, position)
            _    <- buf.flip *> filled.offer(buf)
            res  <- loop(position + read, size, free, filled)
          } yield res
      }

    for {
      size <- channel.size
      _    <- loop(0, size, free, filled)
    } yield ()
  }

  def digest(
      free: Queue[ByteBuffer],
      filled: Queue[ByteBuffer],
      completed: Promise[Nothing, Unit],
      md: MessageDigest
  ): ZIO[Clock, Throwable, Unit] = {
    filled.poll.flatMap {
      case None      => completed.poll.flatMap {
          case Some(_) => for {
              remaining <- filled.takeAll
              _         <- ZIO.foreach_(remaining)(buf => buf.withJavaBuffer(b => ZIO(md.update(b))))
            } yield ()
          case None    => clock.sleep(1.millis) *> digest(free, filled, completed, md)
        }
      case Some(buf) => for {
          _ <- buf.withJavaBuffer(b => ZIO(md.update(b)))
          _ <- free.offer(buf)
          _ <- digest(free, filled, completed, md)
        } yield ()
    }
  }

  def effect(channel: AsynchronousFileChannel) =
    for {
      free      <- Queue.unbounded[ByteBuffer]
      filled    <- Queue.unbounded[ByteBuffer]
      f0        <- (free.size <*> filled.size)
                     .flatMap { case (s1, s2) => putStrLn(s"free=$s1, filled=$s2") }
                     .repeat(Schedule.spaced(1.second))
                     .fork
      md         = MessageDigest.getInstance("SHA-1")
      bufs      <- ZIO.foreach(1 to 10)(_ => Buffer.byteDirect(256 * 1024))
      _         <- free.offerAll(bufs)
      completed <- Promise.make[Nothing, Unit]
      f1        <- scan(channel, free, filled).fork
      f2        <- digest(free, filled, completed, md).fork
      _         <- f1.join
      _         <- completed.succeed()
      _         <- f2.join
      _         <- filled.shutdown
      _         <- f0.interrupt
      _         <- putStrLn(md.digest().map("%02x" format _).mkString)
    } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    AsynchronousFileChannel.open(Path(path), StandardOpenOption.READ).use(effect).exitCode
  }
}
