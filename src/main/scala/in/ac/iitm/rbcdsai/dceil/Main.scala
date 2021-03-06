package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j.{Level,Logger}

/** Command line options and their defaults.*/
case class Config(
  input: String = "",
  output: String = "",
  master: String = "local[*]",
  appName: String = "DCEIL",
  jars: String = "",
  sparkHome: String = "",
  parallelism: Int = -1,
  edgeDelimiter: String = ",",
  minProgress: Int = 2000,
  progressCounter: Int = 1,
  ipAddress: Boolean = false,
  properties: Seq[(String, String)] = Seq.empty[(String, String)])

/** Executes the CEIL distributed community detection.
  * Requires an edge file and output directory in HDFS (local files for local mode only)
  */
object Main {

  def main(args: Array[String]) {

    // configure logging
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.INFO)

    // configure CLI parser
    val parser = new scopt.OptionParser[Config]("dceil") {
      head("DCEIL", "1.0.0")

      arg[String]("<input_file>").action( (x, c) => c.copy(input = x) ).
        text("input file")

      arg[String]("<output_file>").action( (x, c) => c.copy(output = x) ).
        text("output path")

      opt[String]('m', "master").action( (x, c) => c.copy(master = x) ).
        text("spark master, local[N] or spark://host:port. default=local")

      opt[String]('h', "sparkhome").action( (x, c) => c.copy(sparkHome = x) ).
        text("$SPARK_HOME required to run on cluster")

      opt[String]('n', "jobname").action( (x, c) => c.copy(appName = x) ).
        text("job name. default=\"DCEIL\"")

      opt[Int]('p', "parallelism").action( (x, c) => c.copy(parallelism = x) ).
        text ("sets spark.default.parallelism and minSplits on the edge file")

      opt[Int]('x', "minprogress").action( (x, c) => c.copy(minProgress = x) ).
        text("""number of vertices that must change communites for the
                algorithm to consider progress. default=2000""")

      opt[Int]('y', "progresscounter").action( (x, c) => c.copy(progressCounter = x) ).
        text("""number of times the algorithm can fail to make progress
                before exiting. default=1""")

      opt[String]('d', "edgedelimiter").action( (x, c) => c.copy(edgeDelimiter = x) ).
        text("input file edge delimiter. default=\",\"")

      opt[String]('j', "jars").valueName("<jar1>,<jar2>...").action( (x, c) =>
        c.copy(jars = x) ).text("comma-separated list of jars")

      opt[Boolean]('z', "ipaddress").action( (x, c) => c.copy(ipAddress = x) ).
        text("set to true to convert IP addresses to Long ids. default=false")

      help("help").text("prints this usage text")

      arg[(String, String)]("<property>=<value>...").unbounded().optional().
        action{ case ((k, v), c) => c.copy(properties = c.properties :+ (k, v)) }.
        text("optional unbounded arguments")
    }

    var edgeFile, outputDir, master, jobName, jars, sparkHome, edgeDelimiter: String = null
    var properties: Seq[(String, String)] = Seq.empty[(String, String)]
    var parallelism, minProgress, progressCounter = -1
    var ipAddress = false

    parser.parse(args, Config()) match {
      case Some(config) =>
        edgeFile = config.input
        outputDir = config.output
        master = config.master
        jobName = config.appName
        jars = config.jars
        sparkHome = config.sparkHome
        properties = config.properties
        parallelism = config.parallelism
        edgeDelimiter = config.edgeDelimiter
        minProgress = config.minProgress
        progressCounter = config.progressCounter
        ipAddress = config.ipAddress

      case None =>
        parser.usage
        sys.exit(1)
    }
    // set system properties
    properties.foreach({
      case (k, v) =>
        System.setProperty(k, v)
    })

    logger.info(s"Input file: $edgeFile")
    logger.info(s"Output path: $outputDir")
    logger.info(s"Master: $master")
    logger.info(s"Job: $jobName")
    logger.info(s"Jars: $jars")
    logger.info(s"Properties: $properties")
    logger.info(s"Spark Home: $sparkHome")
    logger.info(s"IP Address: $ipAddress")

    // Create the spark context
    var sc: SparkContext = null
    if (master.indexOf("local") == 0) {
      sc = new SparkContext(master, jobName)
    } else {
      sc = new SparkContext(master, jobName, sparkHome, jars.split(","))
    }
    
    // read the input into a distributed edge list
    val inputHashFunc = if (ipAddress) (id: String) => IpAddress.toLong(id) else (id: String) => id.toLong
    var edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(edgeDelimiter).map(_.trim())
      tokens.length match {
        case 2 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), 1L) }
        case 3 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), tokens(2).toLong) }
        case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
      }
    })

    // If the parallel option is set, map the input to the correct number of partitions,
    // otherwise parallelism will be based on number of HDFS blocks.
    // For shrinking the RDD, we use coalesce() which is more efficient than repartition() since it
    // avoids a shuffle operation.
    if (parallelism != -1) edgeRDD = edgeRDD.coalesce(parallelism, shuffle = true)
    
    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)
    val startTime: Long = System.currentTimeMillis / 1000L
    // use a helper class to execute the ceil
    // algorithm and save the output.
    // to change the outputs, one can extend CeilRunner
    val runner = new HDFSCeilRunner(minProgress, progressCounter, outputDir)
    runner.run(sc, graph)
    val stopTime: Long = System.currentTimeMillis / 1000L
    val runningTime = stopTime - startTime
    logger.info("Total running time is : " + runningTime)
  }
}
