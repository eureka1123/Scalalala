import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GeneralReports{
    val TOPIC = "missmalini"
    val TOPIC_NAME = "MissMalini"
    val REPORT_DIR = "/home/xiaoluguo/missmalini/"
    val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
    val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
    val ENTITY_FILE = TOPIC + "_entities2.txt"
    val SLICES = 2000

    
    //def safe_date_from_str(input:String) : String = {
    //}
    
    //def safe_float(input:String) : Float ={
    //}

    def main(args: Array[String]) {

        val likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES)
            .map(x => x.split("/t"))
            .filter(x => (x.length > 2))
            .map(x => (x(0),x(1),x(2)))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES)
            .map(x => x.split("/t"))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .cache()

        val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2))))
            .distinct()
    


        val topicLikesB = sc.broadcast(topicLikes.map(x => x(0)).collect().toSet)

        val manyTopicEntities = False

        if (topicLikesB.value.size >= 1000){
            manyTopicEntities = True
        }

    }
}

/**

topic_likes_b = sc.broadcast(set(topic_likes.map(lambda x: x[0]).collect()))

many_topic_entities = False
if len(topic_likes_b.value) >= 1000:
  many_topic_entities = True

**/

