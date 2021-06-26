package sukumaar

import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("sample-spark") // master hasnt set here, expecting it from spark-submit command
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")

    if(args.length < 2) {
      throw new Exception("Please provide these parameters:\n" +
        "1: File path\n" +
        "2: Word to search\n" +
        "3: Case sensitive [Optional] default true")
    }

    searchWord(sc,args(0),args(1),args.lift(2).getOrElse("true").toBoolean)
  }

  def searchWord(sparkContext: SparkContext,filePath:String,wordToSearch:String,caseSensitive:Boolean=true): Unit ={
    val isWordPresent=sparkContext
      .textFile(filePath)
      .flatMap(line => line.split("\\s+"))
      .map(
        currentWord => {
          val onlyAlphabets= currentWord.replaceAll("[^a-zA-Z]", "")
          if(caseSensitive)
          {
            onlyAlphabets==wordToSearch
        }
          else
          {
            onlyAlphabets.equalsIgnoreCase(wordToSearch)
        }
      }).reduce(_ || _)

    if (isWordPresent){
      println(s"""Keyword "$wordToSearch" exists in the given file""")
    }
    else {
      println(s"""Keyword "$wordToSearch" does not exist in the given file""")
    }

  }
}
