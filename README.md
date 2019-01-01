<a href="https://rbcdsai.iitm.ac.in/"><img title="RBC-DSAI logo" src="https://github.com/RBC-DSAI-IITM/rbc-dsai-iitm.github.io/blob/master/images/logo.jpg" height="200" width="351"></a>

# DCEIL

DCEIL is the *first* distributed community detection algorithm based on the state-of-the-art CEIL scoring function. Please check the [publication](https://doi.org/10.1109/hpcc-smartcity-dss.2017.19) for more details.

[A. Jain](https://github.com/akash-jain1306), [R. Nasre](http://www.cse.iitm.ac.in/~rupesh/) and [B. Ravindran](http://www.cse.iitm.ac.in/~ravi/), "DCEIL: Distributed Community Detection with the CEIL Score," _2017 IEEE 19th International Conference on High Performance Computing and Communications; IEEE 15th International Conference on Smart City; IEEE 3rd International Conference on Data Science and Systems (HPCC/SmartCity/DSS)_, Bangkok, 2017, pp. 146-153.
[doi: 10.1109/HPCC-SmartCity-DSS.2017.19](https://doi.org/10.1109/hpcc-smartcity-dss.2017.19)

## Getting started

### Building from source

1. Ensure that Java 8 is installed in your system. Run `java -version` to ensure proper installation. If not, please install [Java SE Development Kit 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) before proceeding. You can also use [OpenJDK](https://openjdk.java.net/install/) if you prefer that.
2. Ensure that you have `maven` installed. Run `mvn -v` to ensure proper installation. If not, please install [Maven](https://maven.apache.org/) following the official documentation.
3. Clone this repository to your system and change your working directory to the cloned one. Build DCEIL using `maven`, like so: `mvn clean package`.
4. The above step generates a `dceil-1.0.0-jar-with-dependencies.jar` file in the `target/` directory.
5. Follow the instructions for setting up Apache Spark.

### Using compiled executable file

1. Ensure that Java 8 is installed in your system. Run `java -version` to enure proper installation. If not, please install [Java SE Runtime Environment 8](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) before proceeding. You can also use [OpenJDK](https://openjdk.java.net/install/) if you prefer that.
2. Please download [dceil-1.0.0.jar](https://github.com/RBC-DSAI-IITM/DCEIL/dist/dceil-1.0.0.jar) from the `dist` directory in the repository and follow the Apache Spark setup instructions.

### Setting up Apache Spark

1. Please install [Apache Spark 1.4.0](https://spark.apache.org/releases/spark-release-1-4-0.html). Only Apache Spark 1.4.0 is tested as of now. Using any other version is not recommended. It might lead to errors and unexpected behaviors.
2. Ensure `spark-submit --help` is working.

### Running on cluster

1. To run on cluster, you will need to install [Hadoop 1.2.1](https://archive.apache.org/dist/hadoop/common/hadoop-1.2.1/) and install it as per the instructions. You can use Hadoop 2.x or 3.x, but is not tested by us.
2. You would need to initiate HDFS with proper configuration. Please follow the guide provided by Hadoop. After everything is set up, ensure `hadoop version` is working.
3. Make sure Hadoop and Spark are working fine on the cluster.

## Usage

To run DCEIL, we need to make use of the `spark-submit` script provided by Spark. To get the help, run:

```
$ spark-submit dceil-1.0.0.jar --help

DCEIL 1.0.0
Usage: dceil [options] <input_file> <output_file> [<property>=<value>...]

  <input_file>                   input file
  <output_file>                  output path
  -m, --master <value>           spark master, local[N] or spark://host:port. default=local
  -h, --sparkhome <value>        $SPARK_HOME required to run on cluster
  -n, --jobname <value>          job name. default="DCEIL"
  -p, --parallelism <value>      sets spark.default.parallelism and minSplits on the edge file
  -x, --minprogress <value>      number of vertices that must change communites for the
                                 algorithm to consider progress. default=2000
  -y, --progresscounter <value>  number of times the algorithm can fail to make progress
                                 before exiting. default=1
  -d, --edgedelimiter <value>    input file edge delimiter. default=","
  -j, --jars <jar1>,<jar2>...    comma-separated list of jars
  -z, --ipaddress <value>        set to true to convert IP addresses to Long ids. default=false
  --help                         prints this usage text
  <property>=<value>...          optional unbounded arguments
```

### Input file format

The input file is the only thing apart from the CLI flags (if any), required to be provided by the user. The file is basically an adjacency list containing edges of the graph. Each line denotes an edge, whose nodes are separated by a delimiter. Generally, a single whitespace or comma is used as the delimiter. You can specify the delimiter used, by passing the same to the `-d` flag, like so: `-d ","`. Note that the delimiter must be enclosed within a pair of double quotes. 

A few things to also note about the format:
1. No comments or blank lines.
2. No annotations. 
3. No explicit mention of total nodes or edges.

For clarity, here's a basic example of what `input.txt` can be:

```
1 2
2 3
3 4
4 5
```

### Output format

By default, the outputs are saved for each level of iteration. The format followed is `level_{x}_vertices` and `level_{x}_vertices_Mapped`, where `x` denotes the level. There's also provision of saving only the final level and to do that you would have to add the functionality by overriding `finalSave()` in `HDFSCeilRunner`.

The format followed in `level_{x}_vertices` is `Vertex: VertexState` and that in `level_{x}_vertices_Mapped` is `Community: list of Vertices in the community`. If desired, you can change the format of output to suit your requirement, by changing the behavior in `HDFSCeilRunner.saveLevel()`.

### Running locally

```
$ spark-submit dceil-1.0.0.jar 'file:/path/to/input/edge/file' 'file:/path/to/output/'
```

### Running on cluster

```
$ spark-submit dceil-1.0.0.jar 'hdfs:/path/to/input/edge/file' 'hdfs:/path/to/output' -m spark://host:port
```

## Example

Here's an example of running [email-EuAll](https://snap.stanford.edu/data/email-EuAll.html) dataset from [SNAP](https://snap.stanford.edu/), and input file being stored in HDFS, having input path as `/usr/hadoop/email-Eu-core.txt` and output directory path as `/usr/hadoop/email-Eu-core-output`:
```
$ spark-submit dceil-1.0.0.jar '/usr/hadoop/email-Eu-core.txt' '/usr/hadoop/email-Eu-core-output' -d " "
```

Since the default delimiter is comma, we had to specify the delimiter as a single whitepspace.

## Citation

If you use DCEIL in your work, please cite:

```
@INPROCEEDINGS{8291922, 
    author    = {A. Jain and R. Nasre and B. Ravindran}, 
    booktitle = {2017 IEEE 19th International Conference on High Performance Computing and Communications; IEEE 15th International Conference on Smart City; IEEE 3rd International Conference on Data Science and Systems (HPCC/SmartCity/DSS)}, 
    title     = {DCEIL: Distributed Community Detection with the CEIL Score}, 
    year      = {2017}, 
    pages     = {146-153},
    doi       = {10.1109/HPCC-SmartCity-DSS.2017.19}, 
    month     = {Dec},
}
```
