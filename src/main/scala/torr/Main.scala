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
import scala.collection.mutable

object Main extends App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val treeSet1 = new mutable.TreeSet[Int]()
    val treeSet2 = new mutable.TreeSet[Int]()
    
    val x = treeSet1.intersect(treeSet2)
    
    putStrLn("Hello").repeatN(7).exitCode
  }
}
