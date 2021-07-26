package torr.fileio

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels.AsyncFileChannel

import scala.annotation.tailrec

object Actor {

  sealed trait Command[+_]
  case class Fetch(piece: Int, offset: Int)                  extends Command[ByteBuffer]
  case class Store(piece: Int, offset: Int, buf: ByteBuffer) extends Command[Unit]
  case class Hash(piece: Int)                                extends Command[Array[Byte]]
  case object GetNumAvailable                                extends Command[Int]

  // Multiple files in a torrent are treated like a single continuous file.
  case class File(offset: Long, size: Long, channel: AsyncFileChannel)
  case class State(pieceSize: Int, files: Vector[File])

  val stateful = new Stateful[Any, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] = {
      msg match {
        case Hash(_) => Actor.receive(state, msg)
        case _       => ???
      }
    }

  }

  def receive[A](state: State, msg: Command[A]): RIO[Any, (State, A)] =
    msg match {
      case Hash(piece) => ???
      case _           => ???
    }

  def searchFileByOffset(state: State, piece: Int, pieceOffset: Int): File = {

    // Effective offset in bytes
    val e = piece * state.pieceSize + pieceOffset

    // Binary search
    @tailrec
    def loop(i1: Int, i2: Int): Int = {
      val i = (i1 + i2) / 2
      state.files(i) match {
        case File(o, s, _) if (o >= e && e < o + s) => i
        case File(o, _, _) if e < o                 => loop(i1, i)
        case File(o, s, _) if (o + s <= e)          => loop(i, i2)
        case _                                      =>
          throw new IllegalStateException(
            s"Could not find file by offset. Files: ${state.files.mkString(",")}; piece=$piece; offset=$pieceOffset"
          )
      }
    }

    val res = loop(0, state.files.length)
    state.files(res)
  }
}
