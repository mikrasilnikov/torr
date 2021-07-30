package torr.channels.test

import torr.bencode._
import torr.channels.AsyncSocketChannel
import zio._
import zio.duration.durationInt
import zio.nio.channels._
import zio.nio.core._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object BEncodeOverSocketsSpec extends DefaultRunnableSpec {

  def createServer(proc: AsyncSocketChannel => Task[Unit]): Task[Unit] =
    AsynchronousServerSocketChannel().mapM { socket =>
      for {
        addr <- InetSocketAddress.hostNameResolved("127.0.0.1", 2552)
        _    <- socket.bindTo(addr)
        _    <- socket.accept.preallocate.flatMap(_.use(channel =>
                  proc(AsyncSocketChannel(channel)).whenM(channel.isOpen).fork
                )).forever.fork
      } yield ()
    }.useForever

  def createClient[A](proc: AsyncSocketChannel => Task[A]): Task[A] = {
    val socket = AsynchronousSocketChannel().mapM { client =>
      for {
        address <- InetSocketAddress.hostNameResolved("127.0.0.1", 2552)
        _       <- client.connect(address)
      } yield AsyncSocketChannel(client)
    }

    socket.use(proc)
  }

  override def spec =
    suite("BEncodeOverSocketsSuite")(
      //
      testM("Single message from server to client") {

        def server(channel: AsyncSocketChannel): Task[Unit] = {
          for {
            buf <- Buffer.byte(1024)
            _   <- BEncode.write(BValue.string("hello"), channel, buf)
          } yield ()
        }

        def client(channel: AsyncSocketChannel): Task[String] =
          for {
            buf <- Buffer.byte(1024)
            msg <- BEncode.read(channel, buf)
            res  = msg match {
                     case BStr(v) => new String(v.toArray, "UTF-8")
                     case _       => "not a string"
                   }
          } yield res

        for {
          serverFiber <- createServer(server).fork
          response    <- createClient(client)
          _           <- serverFiber.interrupt
        } yield assert(response)(equalTo("hello"))

      } @@ timeout(5.seconds),
      //
      testM("Echo string") {

        def server(channel: AsyncSocketChannel): Task[Unit] = {
          for {
            buf <- Buffer.byte(1024)
            _   <- BEncode.read(channel, buf)
                     .flatMap(msg => BEncode.write(msg, channel, buf))
                     .repeatWhileM(_ => channel.isOpen)
          } yield ()
        }

        def client(channel: AsyncSocketChannel): Task[(String, String)] =
          for {
            buf   <- Buffer.byte(1024)
            _     <- BEncode.write(BValue.string("hello"), channel, buf)
            resp1 <- BEncode.read(channel, buf)
            _     <- BEncode.write(BValue.string("world"), channel, buf)
            resp2 <- BEncode.read(channel, buf)
          } yield (resp1.asString.get, resp2.asString.get)

        for {
          serverFiber <- createServer(server).fork
          response    <- createClient(client)
          _           <- serverFiber.interrupt
        } yield assert(response)(equalTo(("hello", "world")))

      } @@ timeout(5.seconds),
      //
      testM("Sum of ints") {

        def server(channel: AsyncSocketChannel): Task[Unit] = {

          def performSum(channel: AsyncSocketChannel, buf: ByteBuffer) =
            for {
              list <- BEncode.read(channel, buf).map(_.asList.get)
              res   = list.foldLeft(0L) { case (acc, bVal) => acc + bVal.asInstanceOf[BInt].value }
              _    <- BEncode.write(BInt(res), channel, buf)
            } yield ()

          for {
            buf <- Buffer.byte(1024)
            _   <- performSum(channel, buf).repeatWhileM(_ => channel.isOpen)
          } yield ()
        }

        def client(channel: AsyncSocketChannel): Task[(Long, Long, Long)] = {
          import BValue._
          for {
            buf  <- Buffer.byte(1024)
            _    <- BEncode.write(list(int(1), int(2), int(3)), channel, buf)
            res1 <- BEncode.read(channel, buf).map(_.asLong.get)
            _    <- BEncode.write(list(int(4), int(5), int(6)), channel, buf)
            res2 <- BEncode.read(channel, buf).map(_.asLong.get)
            _    <- BEncode.write(list(int(7), int(8), int(9)), channel, buf)
            res3 <- BEncode.read(channel, buf).map(_.asLong.get)
          } yield (res1, res2, res3)
        }

        for {
          serverFiber <- createServer(server).fork
          response    <- createClient(client)
          _           <- serverFiber.interrupt
        } yield assert(response)(equalTo((6L, 15L, 24L)))

      } @@ timeout(5.seconds)
    ) @@ sequential
}
