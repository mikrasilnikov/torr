package torr.peerwire

import torr.channels.ByteChannel
import zio._
import zio.actors._
import zio.actors.Actor.Stateful
import zio.nio.core.ByteBuffer

object SendActor {

  sealed trait Command[+_]
  case class Send(message: Message) extends Command[Unit]

  case class State(channel: ByteChannel, sendBuf: ByteBuffer, receiveActor: ActorRef[ReceiveActor.Command])

  val stateful = new Stateful[Any, State, Command] {

    def receive[A](state: State, cmd: Command[A], context: Context): Task[(State, A)] = {
      cmd match {
        case Send(msg) => send(state, msg).as(state, ())
      }
    }
  }

  private[peerwire] def send(
      state: State,
      msg: Message
  ): Task[Unit] = {
    Message.send(msg, state.channel, state.sendBuf)
      .foldM(
        e => (state.receiveActor ! ReceiveActor.StartFailing(e)).orDie,
        _ => ZIO.unit
      )
  }
}
