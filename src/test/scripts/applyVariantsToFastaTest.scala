import edu.ohsu.sonmezsysbio.cloudbreak.ApplyVariantsToFasta.{Deletion, SequenceVarier}
import java.io.StringWriter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

package edu.ohsu.sonmezsysbio.cloudbreak {

import edu.ohsu.sonmezsysbio.cloudbreak.ApplyVariantsToFasta.{Variation, Insertion, SequenceVarier, Deletion}
import collection.immutable.SortedSet

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

}
}