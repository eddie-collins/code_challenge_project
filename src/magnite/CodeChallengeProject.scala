package magnite
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object CodeChallengeProject extends App {
  // Turn off all the Info messages from the log file
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Ed's word count")
  println("===============")
  println("")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("CodeChallengeProject")
      .getOrCreate()

    println("")
    var filename = ""

  // Check if all the necessary parameters are there
    if (args.length == 0) {
      println("Needs at least a file name parameter")
      println("")
      println("Parameters")
      println("File name - required")
      println("Word - optional - defaults to AND if left blank")
      println("Detail - optional - set to Y to get a breakdown of the document processed and counts")
      println("")
      println("Example: CodeChallengeProject.jar testfile.txt the Y")
      println("")
    } else {
      println("File name supplied")
      println("")
      filename = args(0).toString()
      println("File is " + filename)

      var word = ""

      // If there isnt a second variable then default to the AND word to search for.
      if (args.length < 2) {
        println("No word count specified. Defaulting to AND")
        word = "and"
      } else {
        word = args(1).toString()
        println("Search word is " + word)
      }
      println("")

      // Read in the text file into a dataframe with headers turned off and no delimiter set.
      spark.read.option("header", "false").text(filename).createOrReplaceTempView("inputfile")

      // Clean up the file removing special characters before the word can be searched for.
      spark.sql("select TRANSLATE(value, '1234567890\"*\\.,:;-/+=_()?`[]{}<>', ' ') as document from inputfile").createOrReplaceTempView("trimfile")

      // For each line being 1 field, split each word of a row into an array and then explode the
      // resulting array into a field of a new dataframe.
      spark.sql("SELECT explode(SPLIT(document,' ')) as word from trimfile").createOrReplaceTempView("processedfile")

      // Then a simple sql count of all the rows with the AND word in the word field (using upper to avoid missing lower/uppercase versions).
      var count_result = spark.sql("SELECT count(word) word_count FROM processedfile where upper(word) = '" + word.toUpperCase() + "'").first()
      val word_count = count_result.get(0)
      println("Word count is " + word_count)
      println("")

      // Was using these queries for checking the files when developing. Thought it handy to provide it as an option
      // so provided access through an extra parameter
      if (args.length == 3 && args(2).toString() == "Y") {
        println("Original document - first 20 lines")
        spark.sql("select value from inputfile").show(20,false)
        println("Trimmed document - first 20 lines")
        spark.sql("select document from trimfile").show(20,false)
        println("Top 50 count")
        spark.sql("select upper(word) word, count(*) count from processedfile group by upper(word) order by count desc, word").show(50,false)
      }




    }




}
