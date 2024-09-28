import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import org.apache.spark.mllib.rdd.RDDFunctions._


object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val wordRegex = new Regex("""[a-zA-Z]+""")

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    val textFile = ctx.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
                .filter(word => wordRegex.pattern.matcher(word).matches) // wordRegex
                .sliding(2) // pair of words, with distance of 1
                .filter(_.length == 2) // if the sentence is a pair (consists of two words)
                .map(word => (word.mkString(":"), 1)) // mapping the pair
                .reduceByKey(_ + _)
                .filter(_._2 >= 1000) // keep words with a count of more than 1000
                .coalesce(1)       // only one output

    counts.saveAsTextFile(args(1)) 
    ctx.stop()
  }
}
