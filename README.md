<a href="https://rbcdsai.iitm.ac.in/"><img title="RBC-DSAI logo" src="https://github.com/RBC-DSAI-IITM/rbc-dsai-iitm.github.io/blob/master/images/logo.jpg" height="200" width="351"></a>

# DCEIL

DCEIL is the *first* distributed community detection algorithm based on the state-of-the-art CEIL scoring function.

## Getting started

### Building from source

1. Ensure that Java 8 is installed in your system. If not, please head over to [Java 8 Downloads](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) and install Java SE Runtime Environment before proceeding. Run `java -version` to enure proper installation.
1. Clone this repository to your system and change your working directory to the cloned one.
2. Ensure that you have `maven` installed. If not, please check [Maven](https://maven.apache.org) for installation instructions. Run `mvn -version` to ensure proper installation. Build the tool using `maven`, like so: `mvn clean package`.
3. The above step generates a `dceil-1.0.0-jar-with-dependencies.jar` file in the `target/` directory.
4. Follow the instructions for setting up Apache Spark.

### Using compiled executable file

If you prefer a compiled executable file, please download the `dceil-1.0.0.jar` file from the repository and follow the Apache Spark setup instructions.

### Setting up Apache Spark

1. Please [download](https://archive.apache.org/dist/spark/spark-1.4.0/spark-1.4.0-bin-hadoop1-scala2.11.tgz) Apache Spark 1.4.0. [md5](https://archive.apache.org/dist/spark/spark-1.4.0/spark-1.4.0-bin-hadoop1-scala2.11.tgz.md5), [asc](https://archive.apache.org/dist/spark/spark-1.4.0/spark-1.4.0-bin-hadoop1-scala2.11.tgz.asc) and [sha](https://archive.apache.org/dist/spark/spark-1.4.0/spark-1.4.0-bin-hadoop1-scala2.11.tgz.sha) are also provided if you want to check the integrity of the downloaded file.
2. On Linux and macOS (for Windows, check [this](https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows) for help):

```
$ tar xvf <path/to/spark/tgz>
$ su (for macOS) 
$ sudo su (for Linux)
# mv <path/to/spark> /usr/local/spark
# exit
```

For Bash shell:

```
$ export PATH=$PATH:/usr/local/spark/bin
```

For Fish shell:

```
$ set -g fish_user_paths "/usr/local/spark/bin" $fish_user_paths
```

## Usage

```
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

## Examples

To get the help, run:

```
$ spark-submit dceil-1.0.0.jar --help
```

#### Running locally

To run DCEIL, we need to make use of the `spark-submit` script provided by Spark.

```
$ spark-submit dceil-1.0.0.jar 'file:/path/to/input/edge/file' 'file:/path/to/output/'
```

#### Running on cluster

```
$ spark-submit dceil-1.0.0.jar 'hdfs:/path/to/input/edge/file' 'hdfs:/path/to/output' -m 'spark://host:port' 
```

## Citation

If you use DCEIL in your work, please cite [A. Jain, R. Nasre and B. Ravindran, "DCEIL: Distributed Community Detection with the CEIL Score," 2017 IEEE 19th International Conference on High Performance Computing and Communications; IEEE 15th International Conference on Smart City; IEEE 3rd International Conference on Data Science and Systems (HPCC/SmartCity/DSS), Bangkok, 2017, pp. 146-153.
doi: 10.1109/HPCC-SmartCity-DSS.2017.19](https://doi.org/10.1109/hpcc-smartcity-dss.2017.19).
