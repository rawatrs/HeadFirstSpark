import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.flatspec.AnyFlatSpec

class HelloWorldTest extends AnyFunSuite {
    //assert($a == 3)
    test("a new test doesn't test much, really") {
        val a = 3
        assert(a == 3)
    }


    //"A new test" should "be empty and test nothing really" in {
    /*    val a = 3
        assert(a == 1)
    }*/
    //println("Hello World test")
}
