
val s1 = "hello3"
val s2 = "hello1"

val ordering = implicitly[Ordering[String]]

ordering.compare(s1, s2)



