package frameless
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop}

case class Source1(l: Long, i: Int, ll1: Boolean)
case class Source2(name: String, o: Option[Long], ll2: Boolean)

case class Target(l: Long, i: Int, ll1: Long, ll2: Long)

class AsGroupByAsTests extends TypedDatasetSuite {

  val genSource = for {
    gl <- Arbitrary.arbShort.arbitrary
    gi <- Arbitrary.arbInt.arbitrary
    gn <- Arbitrary.arbString.arbitrary
    gb <- Arbitrary.arbBool.arbitrary
    go <- Gen.option(Arbitrary.arbLong.arbitrary)
  } yield {
    (
      Source1(gl.toLong, gi, gb),
      Source2(gn, go, !gb)
    )
  }

  implicit val arbSource = Arbitrary(genSource)

  def prop(data: Vector[(Source1, Source2)])(implicit te: TypedEncoder[(Source1, Source2)]): Prop = {

    val ds = TypedDataset.create(data)
    val targets = ds.select(
      ds.colMany('_1, 'l),
      ds.colMany('_1, 'i),
      ds.colMany('_1, 'll1).cast[Long] + 4L,
      ds.colMany('_2, 'll2).cast[Long] + 9L
    ).as[Target]

    val grouped = targets.groupBy[Long, Int](
      (targets('l) / 900L).cast[Long] * 900L,
      targets('i)
    ).agg(
      frameless.functions.aggregate.sum(targets.colMany('ll1)),
      frameless.functions.aggregate.sum(targets.colMany('ll2))
    ).as[Target]

    val result = grouped.collect.run
    println(result.mkString(", "))
    0 ?= 0
  }

  test("as-groupBy-as") {
    check(forAll(prop _))
  }
}
