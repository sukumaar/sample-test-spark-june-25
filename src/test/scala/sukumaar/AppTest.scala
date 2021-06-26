package sukumaar

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class AppTest extends FunSuite
{

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("sample-spark")
    .setMaster("local[2]") //local master for testing
  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  sc.setLogLevel("ERROR")

  test("test christmas"){
    val fileUrl=classOf[AppTest].getResource("/fileForTesting.txt").getPath
    App.searchWord(sc,fileUrl,"Christmas")
  }

  test("test google"){
    // negative test case
    val fileUrl=classOf[AppTest].getResource("/fileForTesting.txt").getPath
    App.searchWord(sc,fileUrl,"google")
  }

}
