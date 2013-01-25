import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class applyVariantsToFastaTest extends FunSuite {

  test("TwoPlusTwo") {
    val twoPlusTwo = 2 + 2
    assert(twoPlusTwo == 4)
  }
}