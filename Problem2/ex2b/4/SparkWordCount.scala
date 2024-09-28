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
    val numRegex = new Regex("""\d+""")


    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    val textFile = ctx.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
                .sliding(2)
                .filter(word => word.length == 2 && numRegex.pattern.matcher(word(0)).matches && wordRegex.pattern.matcher(word(1)).matches)
                .map(word => (word(0) + ":" + word(1), 1))
                .reduceByKey(_ + _)
                .sortBy(_._2, ascending = false)  // sort by the count in descending order
                .coalesce(1)       // only one output

    // taking top 100 of counts
    val top100_counts = counts.take(100)
    ctx.parallelize(top100_counts).saveAsTextFile(args(1)) 
    ctx.stop()
  }
}
