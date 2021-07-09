package torr.misc

// To remove dependency on cats
object Traverse {

  def sequence[A](as: List[Option[A]]): Option[List[A]] = {
    val zero: Option[List[A]] = Some(List[A]())
    as.foldLeft(zero) {
      case (acc, oa) => for {
          a   <- oa
          lst <- acc
        } yield a :: lst
    }.map(_.reverse)
  }

}
