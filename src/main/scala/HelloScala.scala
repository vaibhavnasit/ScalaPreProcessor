import org.apache.spark.{SparkConf, SparkContext}
object HelloScala {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("src/main/resources/shakespeare.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Filter those words which are having more than 5000 occurrence
    val my_count = counts.filter(_._2 >= 4000).sortBy(_._2)

    my_count.foreach(println)
    System.out.println("Total Words having more than 4000 occurrence: " + my_count.count());
    //my_count.saveAsTextFile("/tmp/shakespeareWordCount")
  }
}
