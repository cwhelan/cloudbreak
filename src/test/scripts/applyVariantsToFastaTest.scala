import edu.ohsu.sonmezsysbio.cloudbreak.ApplyVariantsToFasta.{Deletion, SequenceVarier}
import java.io.StringWriter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

package edu.ohsu.sonmezsysbio.cloudbreak {

import edu.ohsu.sonmezsysbio.cloudbreak.ApplyVariantsToFasta.{Insertion, SequenceVarier, Deletion}

@RunWith(classOf[JUnitRunner])
class ApplyVariantsToFastaTest extends FunSuite {

  test("test deletion") {
    val originalString = "AAAACCCCGGGGTTTT"
    var variations =  List(new Deletion(4, 8))
    val writer: StringWriter = new StringWriter()
    val s = new SequenceVarier(variations, originalString.iterator, writer)
    s.processSequence()

    assert("AAAAGGGGTTTT\n".equals(writer.toString))
  }

  test("test insertion") {
    val originalString = "AAAACCCCGGGGTTTT"
    var variations =  List(new Insertion(4, "ACTG"))
    val writer: StringWriter = new StringWriter()
    val s = new SequenceVarier(variations, originalString.iterator, writer)
    s.processSequence()

    assert("AAAAACTGCCCCGGGGTTTT\n".equals(writer.toString))
  }

}
}