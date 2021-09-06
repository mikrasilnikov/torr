import scala.collection.mutable
import scala.reflect.runtime.universe.{Type, typeOf, typeTag}

sealed trait Message
case object KeepAlive extends Message
case class WithPayload(value : String) extends Message


val s = new mutable.HashMap[Type, String]()
s += (typeOf[KeepAlive.type] -> "Promise1")


