import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GeneralReports{
	global val TOPIC = "missmalini"
	global val TOPIC_NAME = "MissMalini"
	global val REPORT_DIR = "/home/xiaoluguo/missmalini/"
	global val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
	global val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
	global val ENTITY_FILE = TOPIC + "_entities2.txt"
	global val SLICES = 2000

}



