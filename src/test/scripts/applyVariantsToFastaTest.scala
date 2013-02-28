import java.io.StringWriter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import collection.immutable.SortedSet
import edu.ohsu.sonmezsysbio.cloudbreak.scripts.ApplyVariantsToFasta.{Insertion, SequenceVarier, Deletion, Variation}

package edu.ohsu.sonmezsysbio.cloudbreak.scripts {


@RunWith(classOf[JUnitRunner])
class ApplyVariantsToFastaTest extends FunSuite {

  test("test deletion") {
    val originalString = "AAAATAGCGGGGTTTT"
    var variations =  SortedSet[Variation](new Deletion(5, 8))(Ordering[Int].on[Variation](_ begin))
    val writer: StringWriter = new StringWriter()
    val s = new SequenceVarier(variations, originalString.iterator, writer)
    s.processSequence()

    assert("AAAAGGGGTTTT\n".equals(writer.toString))
  }

  test("test insertion") {
    val originalString = "AAAACCCCGGGGTTTT"
    var variations =  SortedSet[Variation](new Insertion(5, "ACTG"))(Ordering[Int].on[Variation](_ begin))
    val writer: StringWriter = new StringWriter()
    val s = new SequenceVarier(variations, originalString.iterator, writer)
    s.processSequence()

    assert("AAAAACTGCCCCGGGGTTTT\n".equals(writer.toString))
  }


  test("test multiple indels") {
    val originalString = "AAAACCCCGGGGTTTTAAAACCCCGGGGTTTT"
    var variations =  SortedSet[Variation](new Insertion(5, "ACTG"), new Deletion(9,12), new Insertion(17, "G"), new Deletion(21,24))(Ordering[Int].on[Variation](_ begin))
    val writer: StringWriter = new StringWriter()
    val s = new SequenceVarier(variations, originalString.iterator, writer)
    s.processSequence()

    expect("AAAAACTGCCCCTTTTGAAAAGGGGTTTT\n")(writer.toString)
  }

}
}