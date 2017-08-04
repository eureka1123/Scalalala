import scala.util.matching.Regex
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Date
import java.util.Calendar
import java.util.GregorianCalendar
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.{FileSystem => FileSystemHadoop}
import org.apache.commons.lang3.text.WordUtils
import org.apache.spark.storage.StorageLevel._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import collection.mutable.HashMap
import scalaj.http.Http
import org.json4s._
import org.json4s.jackson.JsonMethods._
// import play.api.libs.json._
import javax.imageio.ImageIO
import net.iharder.Base64
import java.net.URL

//object GeneralReports {

  //  def main(args: Array[String]) {
        // val conf = SparkConf()
        // conf.setAppName("YOUR APP NAME HERE")
        // conf.setMaster("spark://compute-master:7077")
        // conf.set("spark.cores.max", "52")
        // conf.set("spark.shuffle.consolidateFiles", "true")
        // conf.set("spark.default.parallelism", "100")
        // conf.set("spark.executor.memory", "20g")
            
        // val sc = SparkContext(conf=conf)
        // hicare_cleaning.json
        val TOPIC = "hicare_cleaning"
        val TOPIC_NAME = "HiCare"
        val REPORT_DIR = "/home/xiaoluguo/HiCare/"
        val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
        val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
        val ENTITY_FILE = TOPIC + ".json"
        val SLICES = 2000
        // val TOPIC = "missmalini"
        // val TOPIC_NAME = "MissMalini"
        // val REPORT_DIR = "/home/xiaoluguo/missmalini2/"
        // val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
        // val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
        // val ENTITY_FILE = TOPIC + "_entities2.txt"
        // val SLICES = 2000

        var likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 2))
            .map(x => (x(0),(x(1),x(2))))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .persist(MEMORY_ONLY_SER)

        // val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()

        // THIS IS OLD, BUT CAN HELP FIND ENTITIES
        val evalEntity: (Array[(String,(String, String))]) => String = (fb_entities_file :Array[(String,(String, String))]) => {

            val B = new StringBuilder

            for(entry <- fb_entities_file) {
                B ++= entry._1 + ","
                B ++= entry._2._1 + "," + entry._2._2 + "--"
            }

            B.deleteCharAt(B.length-1)
            val resultString = B.toString

            resultString
        }

        val safe_match: (String,String) => Boolean = (a:String,b:String) =>{
            if (a!=None){
                val sr ="miss".r.findFirstMatchIn(a.toLowerCase())
                val sr2="malini".r.findFirstMatchIn((a+b).toLowerCase())
                if (sr!=None && sr2!=None && sr.get.start< sr2.get.start){
                    true
                } else {
                    false
                }
            }else{
                false
            }
        }

        val fbEntities = likes.filter(x => safe_match(x._2._1,x._2._2)).collect()

        val fbEntitiesString = evalEntity(fbEntities)

        //human readable file
        val fp = new PrintWriter(new File(ENTITY_FILE))
        fp.write(fbEntitiesString)
        fp.close()

        val evalString: (String) => ListBuffer[(String, (String, String))] = (longString : String) => {

            val res : ListBuffer[(String, (String, String))] = ListBuffer()

            val splitString = longString.split("--")

            for (entry <- splitString){
                val internalSplit = entry.split(",")
                res += ((internalSplit(0),(internalSplit(1),internalSplit(2))))
            }

            res
        }

        val topicLikesString = Source.fromFile(ENTITY_FILE).mkString
        implicit val formats = DefaultFormats
	val topicLikesPre = parse(topicLikesString).extract[List[JArray]]
	val topicLikes = sc.parallelize(topicLikesPre.map(x => (x(0).extract[String], ((x(1)(0).extract[String], x(1)(1).extract[String])))))


        // val topicLikesBPre = sc.parallelize(topicLikes.map(x => (x(0).extract[String],1)))

        // val topicLikesB = sc.broadcast(topicLikes.map(x => x(0).extract[String]).toSet)

        val topicLikesBPre = sc.parallelize(topicLikes.map(x => (x._1,1)).collect())

        val topicLikesB = sc.broadcast(topicLikes.map(x => x._1).collect().toSet)
        
        

        // val topicLikesBPre = topicLikes.map(x => x._1).collect().toSet

        // var topicLikesBValue = scala.collection.mutable.Set[Int]()
        
        // topicLikesBPre.foreach{
        //     topicLikesBValue += _.toInt
        // }



        // val topicLikesB = sc.broadcast(topicLikesBPre)
        
        var manyTopicEntities = false

        if (topicLikesB.value.size >= 1000){
            manyTopicEntities = true
        }
        

        val conf = new Configuration()
        val hdfsCorePath = new Path("core-site.xml")
        val hdfsHDFSPath = new Path("hdfs-site.xml")
        conf.addResource(hdfsCorePath)
        conf.addResource(hdfsHDFSPath)
        val fileSystem = FileSystemHadoop.get(conf)
        val path = new Path("/"+TOPIC)
        fileSystem.mkdirs(path) 

        val dir: File = new File(REPORT_DIR);
        val dir2: File = new File(REPORT_DIR+"/predictors");
        dir.mkdir();
        dir2.mkdir();

        val locationFacts = sc.textFile(DATA_ROOT+ "/fact_location", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 2)
            .map(x => (x(2), x(1)))
            .distinct()
            .setName("location_facts")
            .persist(MEMORY_ONLY_SER)

        val indiaLocations = sc.textFile(DATA_ROOT+ "/dim_location", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 7)
            .filter(x => x(6) == "India")
            .filter(x => x(7) != "null")
            .map(x => (x(0),x(7)))
            .distinct()

        val anyIndiaLocations = sc.textFile(DATA_ROOT + "/dim_location", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 6)
            .filter(x => x(6) == "India")
            .map(x => (x(0),x(6)))
            .distinct()
            .setName("any_india_locations")
        
        val indiaPeople = locationFacts.join(anyIndiaLocations)
            .map(x=> (x._2._1, x._2._2))
            .setName("india_people")
            .distinct()
            .persist(MEMORY_ONLY_SER)
        
        val indiaPeopleStates = locationFacts.join(indiaLocations)
            .map(x => (x._2._1, x._2._2))
            .distinct()
            .persist(MEMORY_ONLY_SER)

        likeFacts = likeFacts.map(x => (x._2._1, (x._1, x._2._2)))
            .join(indiaPeople)
            .map(x => (x._2._1._1, (x._1, x._2._1._2)))
            .distinct()
            .coalesce(SLICES)
            .setName("like_facts")
            .persist(MEMORY_ONLY_SER)
            //.persist(MEMORY_ONLY_SER)
        
        val personTotalLikeCounts = likeFacts.map(x => (x._2._1, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personalTotalLikeCounts")
            .persist(MEMORY_ONLY_SER)

        println(personTotalLikeCounts.count())

        var pathBigLikes = new Path("/" + TOPIC + "/big_likes")
        var fileExists = fileSystem.exists(pathBigLikes)

        var data = Array("1","2")
        var bigLikes = sc.parallelize(data)

        if (!fileExists) {
            bigLikes = likeFacts.map(x => (x._2._2, 1))
                .reduceByKey((x,y) => (x+y))
                .filter(x => x._2 > 999)
                .map(x => x._1)
                .coalesce(SLICES)
                .setName("bigLikes")
                .persist(MEMORY_ONLY_SER)
            bigLikes.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
        } else {
            bigLikes = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
                .setName("bigLikes")
                .persist(MEMORY_ONLY_SER)
        }

        // val topicLikesData = Array(("1",2),("3",4)) //placeholder
        // val topicLikes = sc.parallelize(topicLikesData)

        //(like_ID, (name, type))
        val topicAndBigLikes = topicLikes
            .map(x => x._1)
            .union(bigLikes)
            .map(x => (x, 1))
            .join(likes)
            .mapValues(x => x._2)
            .distinct()
            .coalesce(SLICES)
            .setName("topic_and_big_likes")
            .persist(MEMORY_ONLY_SER)

        //(like_ID, user_ID)
        val targetedLikeFacts = likeFacts //#(fact_ID, (person_ID, like_ID))
            .map(x => (x._2._2, x._2._1))
            .setName("targetedLikeFacts")
            .persist(MEMORY_ONLY_SER)
        println(targetedLikeFacts.count())
        likeFacts.unpersist()

        //(person_ID, number_of_topic_and_big_likes) for people who like something big and/or topic
        val personRelevantLikeCounts = topicAndBigLikes
            .join(targetedLikeFacts) // (like_ID, ((name,type), user_ID))
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .coalesce(SLICES)
            .setName("personRelevantLikeCounts")
            .persist(MEMORY_ONLY_SER)

        //(like_ID, (1, user_ID)) for like_IDs in topic
        val topicLikeFacts = topicLikes
            .map(x => (x._1, 1))
            .join(targetedLikeFacts)
            .distinct()
            .setName("topicLikeFacts")
            .persist(MEMORY_ONLY_SER)

        //(person_ID, number_of_topic_likes) for people who like something in topic
        val personTopicLikeCounts = topicLikeFacts
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personTopicLikeCounts")
            .persist(MEMORY_ONLY_SER)

        //(person_ID, fraction of likes that are topic-related)
        val personTopicProportions = personRelevantLikeCounts
            .leftOuterJoin(personTopicLikeCounts)
            .map(x => (x._1, if (x._2._2.getClass.getSimpleName == None.getClass.getSimpleName) { 0.0 } else {x._2._2.get.toFloat/x._2._1}))
            .distinct()
            .setName("personTopicProportions")
            .persist(MEMORY_ONLY_SER)
    
        // This part is dealing with new data person_info
        //Lots of info, for people in India
        val people = sc.textFile(DATA_ROOT + "/person_info", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 8)
            .map(x => (x(0), x.drop(1)))
            .join(indiaPeople)
            .mapValues(x => x._1)
            .setName("people")
            .persist(MEMORY_ONLY_SER)

        val safe_date_from_str: ((String, Array[String])) => (String, Option[Calendar]) = (a:((String, Array[String]))) =>{
            val reg = """\d\d/\d\d/\d\d\d\d""".r
            var allMatch : Array[String] = reg.findAllIn(a._2(4)).toArray
            if (allMatch.length == 1) {
                var dateOB : Date = new Date(allMatch(0))

                var cal : Calendar = new GregorianCalendar()
                cal.setTime(dateOB)

                (a._1, Some(cal))

            } else {
                (a._1, None)
            }
        }
        val getAge: ((String, Option[Calendar])) => (String, Int) = (a:((String, Option[Calendar]))) => {
            var age: Int = 0
            val milliInYear: Double =31557600000.0
            var str = a._1
            var date = a._2
            val today : Calendar = Calendar.getInstance()
            date match {
                case Some(date) => age= ((today.getTime().getTime() - date.getTime().getTime())/milliInYear).toInt
                case None => val age= -1
            }

            (str,age)
        }

        //(person_ID, java calendar structure of birthday) - not actually age yet!
        var peopleBirthdays = people
            .filter(x => x._2(4) != "null")
            .map(safe_date_from_str)
            .filter(x => (x._2.getClass.getSimpleName != None.getClass.getSimpleName))
            .coalesce(SLICES)
            .setName("people_birthdays")
            .persist(MEMORY_ONLY_SER)
    
        //(person_ID, age in years)
        
        val peopleAges = peopleBirthdays
          .map(getAge)
          .coalesce(SLICES)
          .setName("people_ages")
          .persist(MEMORY_ONLY_SER)

        val get_ageband: ((String, Int)) => (String,String) = (a:((String, Int))) => {
            if (a._2 < 18) { (a._1, "Under 18") }
            if (a._2 < 25) { (a._1, "18-24") }
            if (a._2 < 35) { (a._1, "25-34") }
            if (a._2 < 45) { (a._1, "35-44") }
            if (a._2 < 55) { (a._1, "45-54") }
            (a._1, "55+")
        }

        val peopleAgebands = peopleAges
            .map(get_ageband)
            .persist(MEMORY_ONLY_SER)
        
        //(person ID, gender)
        val peopleGenders = people
            .filter(x => x._2(9) != "null")
            .map(x => (x._1, x._2(9)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_genders")
            .persist(MEMORY_ONLY_SER)

        val clean_relationships: ((String, String)) => ((String, String)) = (rel:((String, String))) => {
            (rel._1, WordUtils.capitalize(rel._2).replaceAll("(Pending)","")) //does not include unicode, may need to be fixed
        }

        //(person ID, relationship status)
        //The isdigit filtering is because there are a bunch of relationship values that are dates.
        //Not sure why, or what it indicates, but for now I'm just ignoring them.
        val peopleRelationships = people
            .filter(x => x._2(7) != "null" && !(x._2(7)(0).isDigit))
            .map(x => (x._1, x._2(7)))
            .coalesce(SLICES)
            .map(clean_relationships)
            .distinct()
            .setName("people_relationships")
            .persist(MEMORY_ONLY_SER)
        //persist(MEMORY_ONLY_SER) only used here for debugging, shouldn't be needed?

        //(person ID, locale (i.e. language/country))
        val peopleLocales = people
            .filter(x => x._2(14) != "null")
            .map(x => (x._1, x._2(14)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_locales")
            .persist(MEMORY_ONLY_SER)
        //persist(MEMORY_ONLY_SER) only used here for debugging, shouldn't be needed?
        
        //(person_ID, like_ID) for all big likes and all people who like them
        val bigLikeFacts = bigLikes.map(x => (x,1))
            .join(targetedLikeFacts)
            .map(x => (x._2._2, x._1))
            .persist(MEMORY_ONLY_SER)

        pathBigLikes = new Path("/" + TOPIC + "/like_topic_fractions")
        fileExists = fileSystem.exists(pathBigLikes)
        
        var data1 = Array(("1",2.0),("2",2.0))
        var likeTopicFractions = sc.parallelize(data1)

        val transform: (String) => (String, Double) = (str: String) =>{
            val index = str.indexOf("\'")
            val index2 = str.indexOf("\'", str.indexOf("\'") + 1)
            var a: String =  str.substring(index+1, index2)
            var b: Double = str.substring(str.indexOf(" "), str.length-1).toDouble
            (a,b)
        }

        val transformScala: (String) => (String, Double) = (str: String) =>{
            val index = str.indexOf("(")
            val index2 = str.indexOf(",")
            var a: String =  str.substring(index+1, index2)
            var b: Double = str.substring(index2+1, str.length-1).toDouble
            (a,b)
        }

        if (!fileExists) {
            //(like_ID, topic_like_proportion) for each person who likes like_ID (for big+topic likes)
            likeTopicFractions = bigLikeFacts.join(personTopicProportions)
                .map(x => (x._2._1, x._2._2))
                .persist(MEMORY_ONLY_SER)
            likeTopicFractions.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions")
        } else{
            var pre = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions").persist(MEMORY_ONLY_SER)//.map(eval).persist(MEMORY_ONLY_SER)) //fix eval
            likeTopicFractions = pre.map(transformScala)
        } 

        pathBigLikes = new Path("/" + TOPIC + "/like_topic_sum")
        fileExists = fileSystem.exists(pathBigLikes)

        var likeTopicSum = sc.parallelize(data1)

        if(!fileExists) {
            likeTopicSum = likeTopicFractions.reduceByKey((x,y) => (x+y)).persist(MEMORY_ONLY_SER) //This should give a measure of popularity of things, weighted by how big a topic fan each liker is. Normalizing it by count, as done below, gives a measure of strength of implication.
            likeTopicSum.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum")
        } else{
            var pre = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum")
            likeTopicSum = pre.map(transformScala)
        }

        //(like ID, number of likes for that ID) (for big+topic likes)
        val likeTopicCount = likeTopicFractions.map(x => (x._1, 1)).reduceByKey((x,y) => (x+y)).persist(MEMORY_ONLY_SER)

        //This is actually not just the strength normalized by count; it's also multiplied by an exponentiation (<1.0) of the count, so that more objectively popular things are weighted higher. Without this multiplication, we'd divide by x[1][1], by dividing by something less than that we're multiplying by the corresponding value. E.g., dividing by x[1][1]**0.7, we're weighting by popularity**0.3.
        //(like ID, topic-predictive power for that like ID)
        var likeTopicImplications = likeTopicSum.join(likeTopicCount).map(x => (x._1, x._2._1 / scala.math.pow(x._2._2, 0.7))).persist(MEMORY_ONLY_SER)

        //(like ID, (predictive power, (like name, like type))
        val sortedLikeTopicImplications = likeTopicImplications 
          .join(topicAndBigLikes) 
          .sortBy(x => x._2._1, false, SLICES) 
          .persist(MEMORY_ONLY_SER)

        var sortedOfftopicImplicationsOpp = sortedLikeTopicImplications 
            // .filter(x => topicLikesB.value.contains(x._1.toInt)) 
            .join(topicLikesBPre)
            .map(x => (x._1, x._2._1))

        val sortedOfftopicImplications = sortedLikeTopicImplications
            .subtract(sortedOfftopicImplicationsOpp)
            // .map(x => (x._1,x._2._1))   
            .persist(MEMORY_ONLY_SER)

        //Make a file with the top 5000 putatively off-topic entities, for manual fixing
        val otFile = TOPIC + "_top_offtopics2.txt"
        val otFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(otFile)))
        for (x <- sortedOfftopicImplications.take(5000)) {
            otFileWriter.write(x + "\n")  // however you want to format it
        }
        otFileWriter.close()


        // ############################################################################
        // #
        // # RUN TO HERE, AND THEN EXAMINE THE TOPIC_top_offtopics.txt FILE. COPY THINGS FROM
        // # THAT FILE THAT ACTUALLY BELONG IN THE ENTITIES FILE INTO THE ENTITIES FILE.
        // # IF THERE WERE ANY, MAKE SURE TO DELETE LIKE_TOPIC_FRACTIONS AND LIKE_TOPIC_SUM:
        // # hdfs dfs -rm -r /<TOPIC>/like_topic_fractions
        // # hdfs dfs -rm -r /<TOPIC>/like_topic_sum
        // # THEN RE-RUN EVERYTHING FROM THE BEGINNING; THIS TIME YOU SHOULDN'T HAVE TO STOP HERE,
        // # BUT YOU CAN IF YOU WANT TO DOUBLE-CHECK THAT NO NEW ACTUALLY-ON-TOPIC ENTITIES
        // # CREPT INTO THE TOP PART OF THE OFF-TOPIC LIST.
        // # FOR THE RE-RUN, YOU'LL STILL HAVE TO STOP AROUND LINE 1295 TO GET A FB KEY.
        // # IF THERE WEREN'T ANY PROBLEMS, YOU CAN JUST CONTINUE RUNNING FROM HERE.
        // #
        // ############################################################################

        try{
            sortedLikeTopicImplications.saveAsTextFile("hdfs://compute-master:9000/" + TOPIC + "/like_topic_implications")
        } catch {
            case e: org.apache.hadoop.mapred.FileAlreadyExistsException => println("File exists.")
        }

        val topOffTopicsBPre = sc.parallelize(sortedOfftopicImplications.keys.take(200)).map(x => (x,1))
        //just the like_IDs for the top 200 off-topic predictors
        val topOffTopicsB = sc.broadcast(sortedOfftopicImplications.keys.take(200).toSet)

        val edgeLikeFacts = targetedLikeFacts
            .join(topOffTopicsBPre)
            .map(x => (x._1, x._2._1))

        val edgeLikers = edgeLikeFacts
            .values
            .distinct()

        val fanIDs = personTopicLikeCounts.keys

        //person_IDs who like top off-topic predictors but not any on-topic predictors
        val edgeNonfans = personRelevantLikeCounts
            .keys
            .subtract(fanIDs)
            .intersection(edgeLikers)
            .persist(MEMORY_ONLY_SER)

        //(person_ID, 1) for edge_nonfans
        val edgeKv = edgeNonfans.map(x => (x,1)).persist(MEMORY_ONLY_SER)

        //Takes an RDD of elements like (person_id, factor_value) and returns fandom sums for each factor value
        val fandom_sums_by_factor: (RDD[(String, Int)]) => Map[Int, Int] = (factorRdd: RDD[(String, Int)]) => {
          personTopicProportions 
            .join(factorRdd) 
            .map(x => (x._2._2, x._2._1.toInt)) 
            .reduceByKey((x,y)=>(x+y)) 
            .collect()
            .toMap
        }

        //for each factor, the number of topic fans
        val fan_counts_by_factor: (RDD[(String, Int)]) => Map[Int, Int] = (factorRdd: RDD[(String, Int)]) => {
            personTopicLikeCounts
            .join(factorRdd) 
            .map(x => (x._2._2, 1)) 
            .reduceByKey((x,y)=>(x+y)) 
            .collect()
            .toMap
        }

        //for each factor, the number of edge people
        val edge_counts_by_factor: (RDD[(String, Int)]) => Map[Int, Int] = (factorRdd: RDD[(String, Int)]) => {
          edgeKv
            .join(factorRdd) 
            .map(x => (x._2._2, 1)) 
            .reduceByKey((x,y)=>(x+y)) 
            .collect()
            .toMap
        }

        //for each factor, the number of total people
        val person_counts_by_factor: (RDD[(String, Int)]) => Map[Int, Int] = (factorRdd: RDD[(String, Int)]) => {
          personTotalLikeCounts
            .join(factorRdd) 
            .map(x => (x._2._2, 1)) 
            .reduceByKey((x,y)=>(x+y)) 
            .collect()
            .toMap
        }

        //for each factor, the total number of topic likes
        val fan_like_counts_by_factor: (RDD[(String, Int)]) => Map[Int, Int] = (factorRdd: RDD[(String, Int)]) => {
          personTopicLikeCounts
            .join(factorRdd)
            .map(x => (x._2._2, x._2._1)) 
            .reduceByKey((x,y)=>(x+y)) 
            .collect()
            .toMap
        }

        // #doesn't make sense - I must have thought this was looking at total like count or something
        // def edge_like_counts_by_factor(factor_rdd):
        //   return dict(person_topic_like_counts \
        //     .join(edge_kv) \
        //     .mapValues(lambda x: x[0]) \
        //     .join(factor_rdd) \
        //     .map(lambda x: (x[1][1], x[1][0])) \
        //     .reduceByKey(add) \
        //     .collect())
    
        //for each factor, the mean number of topic likes per person
        val mean_fan_like_counts_by_factor: (RDD[(String, Int)]) => List[(Int,Float)] = (factorRdd: RDD[(String, Int)]) => {
            val flcbf = fan_like_counts_by_factor(factorRdd)
            val fcbf = fan_counts_by_factor(factorRdd)
            for (x <- (fcbf.keys).toList if flcbf.contains(x)) yield (x, (flcbf(x)/fcbf(x)).toFloat)
        }

        // #doesn't make sense - I must have thought this was looking at total like count or something
        // def mean_edge_like_counts_by_factor(factor_rdd):
        //   elcbf = edge_like_counts_by_factor(factor_rdd)
        //   ecbf = edge_counts_by_factor(factor_rdd)
        //   return [(x, float(elcbf[x])/ecbf[x]) for x in ecbf if x in elcbf]

        //for each factor, the fraction of total population that is a fan
        val fan_concentration_by_factor: (RDD[(String, Int)]) => List[(Int,Float)] = (factorRdd: RDD[(String, Int)]) => {
            val fcbf = fan_counts_by_factor(factorRdd)
            val pcbf = person_counts_by_factor(factorRdd)
            for (x <- fcbf.keys.toList if pcbf.contains(x)) yield (x, (100.0*fcbf(x)/pcbf(x)).toFloat)
        }

        //for each factor, the fraction of total population that is an edge case
        val edge_concentration_by_factor: (RDD[(String, Int)]) => List[(Int,Float)] = (factorRdd: RDD[(String, Int)]) => {
            val ecbf = edge_counts_by_factor(factorRdd)
            val pcbf = person_counts_by_factor(factorRdd)
            for (x <- ecbf.keys.toList if pcbf.contains(x)) yield (x, (100.0*ecbf(x)/pcbf(x)).toFloat)
        }

        //for each factor, the mean fandom across fans at that factor level
        val fandom_by_factor: (RDD[(String, Int)]) => List[(Int,Float)] = (factorRdd: RDD[(String, Int)]) => {
            val fsbf = fandom_sums_by_factor(factorRdd)
            val fcbf = fan_counts_by_factor(factorRdd)
            for (x <- fsbf.keys.toList if fcbf.contains(x)) yield (x, (100.0*fsbf(x)/fcbf(x)).toFloat)
        }

        val facebookPeople = Array("Actor/director", "Artist", "Athlete", "Author", "Blogger", "Business person", "Chef", "Coach", "Dancer", "Designer", "Doctor", "Entertainer", "Entrepreneur", "Government official", "Journalist", "Lawyer", "Literary editor", "Monarch", "Musician/band", "News personality", "Personal blog", "Personal website", "Photographer", "Politician", "Producer", "Public figure", "Publisher", "Teacher", "Writer")
        val facebookPeopleB = sc.broadcast(facebookPeople.toSet)


        val facebookMusic = Array("Album", "Arts/entertainment/nightlife", "Concert tour", "Concert venue", "Music", "Music award", "Music chart", "Music video", "Musical genre", "Musical instrument", "Musician/band", "Radio station", "Record label", "Song")
        val facebookMusicB = sc.broadcast(facebookMusic.toSet)


        val facebookNewMedia = Array("App", "App page", "Blogger", "Business/economy website", "Computers/internet website", "Education website", "Entertainment website", "Government website", "Health/wellness website", "Home/garden website", "Internet/software", "News/media website", "Personal blog", "Personal website", "Recreation/sports website", "Reference website", "Regional website", "Science website", "Society/culture website", "Teens/kids website", "Video game", "Website")
        val facebookNewMediaB = sc.broadcast(facebookNewMedia.toSet)


        val facebookOldMedia = Array("Article", "Author", "Book", "Book genre", "Book series", "Book store", "Entertainer", "Journalist", "Magazine", "Media/news/publishing", "Movie", "Movie character", "Movie general", "Movie genre", "Movie theater", "Museum/art gallery", "News personality", "Newspaper", "One-time tv program", "Performance art", "Photographer", "Publisher", "Radio station", "Record label", "Tv", "Tv channel", "Tv genre", "Tv network", "Tv season", "Tv show", "Tv/movie award")
        val facebookOldMediaB = sc.broadcast(facebookOldMedia.toSet)

        val sorted_category_subset_implications: (Broadcast[Set[String]]) => RDD[(String, (Double, (String, String)))] = (categorySubsetB: Broadcast[Set[String]]) => {
            sortedLikeTopicImplications.filter(x => categorySubsetB.value.contains(x._2._2._2))
        }

        val bollywoodNames = Array("Salman Khan", "Amitabh Bachchan", "Shah Rukh Khan", "Mahendra Singh Dhoni", "Akshay Kumar", "Virat Kohli", "Aamir Khan", "Deepika Padukone", "Hrithik Roshan", "Sachin Tendulkar", "Ranbir Kapoor", "Priyanka Chopra", "AR Rahman", "Priety Zinta", "Saif Ali Khan", "Yo Yo Honey Singh", "Sonakshi Sinha", "Virender Sehwag", "Shikhar Dhawan", "Gautam Gambhir", "Katrina Kaif", "Kareena Kapoor Khan", "Karan Johar", "Madhuri Dixit", "Ajay Devgn", "Ravindra Jadeja", "Sonu Nigam", "Shreya Ghoshal", "Suresh Raina", "Mahesh Bab", "Sonam Kapoor", "Shahid Kapoor", "Kapil Sharma", "Arijit Singh", "Yuvraj Singh", "Farhan Akhtar", "Ranveer Singh", "Ajinkya Rahane", "A. R. Murugadoss", "Mika Singh", "Vijay", "Anushka Sharma", "John Abraham", "Sunny Leone", "Rajinikanth", "Bhuvneshwar Kumar", "Harbhajan Singh", "Ishant Sharma", "Saina Nehwal", "Rohit Sharma", "Ajith Kumar", "Abhishek Bachchan", "Alia Bhatt", "Parineeti Chopra", "Sania Mirza", "Sunidhi Chauhan", "MC Mary Kom", "Chetan Bhagat", "Sanjay Leela Bhansali", "Aishwarya Rai Bachchan", "Vidya Balan", "Jacqueline Fernandez", "Arjun Kapoor", "Kangana Ranaut", "Varun Dhawan", "Shraddha Kapoor", "Bipasha Bas", "Riteish Deshmukh", "Anupam Kher", "Rohit Shetty", "Navjot Singh Sidh", "Nargis Fakhri", "Anurag Kashyap", "Pawan Kalyan", "Shilpa Shetty", "Imtiaz Ali", "Anil Kapoor", "Dhanush", "Kirron Kher", "Allu Arjun", "Sukhwinder Singh", "Shankar-Ehsaan-Loy", "Hema Malini", "Viswanathan Anand", "Vishal-Shekhar", "Shaan", "Leander Paes", "Prabhudheva", "Rohan Bopanna", "Sajid Nadiadwala", "Anirban Lahiri", "Mithun Chakraborty", "Vir Das", "Manish Paul", "Malaika Arora Khan", "Amish Tripathi", "Ram Kapoor", "Remo D’Souza", "Remo D'Souza", "Terence Lewis", "Papa CJ")
        
        var buffer = new ListBuffer[String]()

        for (x <- bollywoodNames){ 
            buffer += (x.toLowerCase())
        }

        val bollywoodNamesB = sc.broadcast(buffer.toSet)

        val sorted_names_subset_implications: (Broadcast[Set[String]]) => RDD[(String, (Double, (String, String)))] = (lowercaseNameSubsetB: Broadcast[Set[String]]) => {
            sortedLikeTopicImplications.filter(x => lowercaseNameSubsetB.value.contains(x._2._2._1.toLowerCase()))
        }

        val nLikeTopics = topicLikesB.value.size

        if (manyTopicEntities){
            //top 5% of topic likes by like count, rounded down
            //per discussion on 10/19/15, this should actually be the list of facebook pages that NBA provided us
            //However, we'd need a good way to match those up to our IDs. I'm sure there is one, but in the 
            //interest of time I'm putting that off for today.
            val basicTopicLikesBPre = sc.parallelize(likeTopicCount
                .join(topicLikesBPre)
                .map(x => (x._1, x._2._1))
                .sortBy(x => x._2, false, SLICES)
                .map(x=> (x._1, 1))
                .take((nLikeTopics * 0.05).toInt))

            val basicTopicLikesB = sc.broadcast(likeTopicCount
                .join(topicLikesBPre)
                .map(x => (x._1, x._2._1))
                .sortBy(x => x._2, false, SLICES)
                .map(x=> (x._1, 1))
                .take((nLikeTopics * 0.05).toInt).toSet)

            //top 20% of topic likes by like count, rounded down
            val commonTopicLikesBPre = sc.parallelize(likeTopicCount
                .join(topicLikesBPre)
                .map(x => (x._1, x._2._1))
                .sortBy(x => x._2, false, SLICES)
                .map(x => (x._1, 1))
                .take((nLikeTopics * 0.2).toInt))

            val commonTopicLikesB = sc.broadcast(likeTopicCount
                .join(topicLikesBPre)
                .map(x => (x._1, x._2._1))
                .sortBy(x => x._2, false, SLICES)
                .map(x => (x._1, 1))
                .take((nLikeTopics * 0.2).toInt).toSet)

            // //calculates avidity score for a like, based on ID
            // val score_avidity: RDD[(String, Int)] => Int = (x:RDD[(String, Int)]) => {
            //     if (basicTopicLikesBPre.join(x).count()>0){
            //         1
            //     }
            //     if (topicLikesBPre.join(x).count()>0){
            //         if (commonTopicLikesBPre.join(x).count()>0) 2 else 4
            //     }   
            //     0
            // }

            //(like_ID, avidity score)
            var topicAvidityScorePre = topicAndBigLikes
                .map(x => (x._1, 0))

            var topicAvidityScore1= topicAvidityScorePre
                .join(basicTopicLikesBPre)
                .map(x => (x._1, 0))

            var topicAvidityScore2= topicAvidityScorePre
                .join(topicLikesBPre)
                .map(x => (x._1, 0))

            var topicAvidityScore3= topicAvidityScore2
                .join(commonTopicLikesBPre)
                .map(x => (x._1, 0))

            topicAvidityScore2 = topicAvidityScore2
                .subtract(topicAvidityScore3)

            topicAvidityScorePre = topicAvidityScorePre
                .subtract(topicAvidityScore1)
                .subtract(topicAvidityScore2)
                .subtract(topicAvidityScore3)

            topicAvidityScore1= topicAvidityScore1
                .map(x => (x._1, 1))

            topicAvidityScore2= topicAvidityScore2
                .map(x => (x._1, 4))

            topicAvidityScore3= topicAvidityScore3
                .map(x => (x._1, 2))

            val topicAvidityScore = topicAvidityScorePre
                .union(topicAvidityScore1)
                .union(topicAvidityScore2)
                .union(topicAvidityScore3)


            //returns an avidity ranking
            val avidity_ranking: ((String, Int)) => (String, String) = (x: ((String, Int))) => {
                if (x._2.toInt >= 6){
                    (x._1, "Avid")
                }
                if(x._2.toInt >= 3){
                    (x._1, "Intermediate")
                }
                if(x._2.toInt >= 1){
                    (x._1, "Casual")
                }
                (x._1, "Non-Fan")
            }
            //(person_ID, avidity ranking)
            val personAvidityRanking = targetedLikeFacts
                .join(topicAvidityScore)
                .map(x => (x._2._1, x._2._2))
                .reduceByKey((x,y) => (x+y))
                .coalesce(SLICES)
                .map(avidity_ranking)
                .setName("person_avidity_ranking")
                .cache()

            val highAvidLikeTopicFractions = personTopicProportions 
                .join(personAvidityRanking) 
                .filter(x => x._2._2 == "Avid") 
                .map(x => (x._1, x._2._1)) 
                .join(bigLikeFacts) 
                .map(x => (x._2._2, x._2._1)) 
                .cache()

            val highAvidLikeTopicCount = highAvidLikeTopicFractions
                .map(x => (x._1, 1)).reduceByKey((x,y) => (x+y))

            // (like ID, (predictive power, (like name, like type))
            val highAvidSortedLikeTopicImplications = highAvidLikeTopicFractions 
                .reduceByKey((x,y) => (x+y)) 
                .join(highAvidLikeTopicCount) 
                .map(x => (x._1, x._2._1 / scala.math.pow(x._2._2, 0.7))) 
                .join(topicAndBigLikes) 
                .sortBy(x => x._2._1, false, SLICES) 
                .cache()
            
            println(highAvidSortedLikeTopicImplications.count())

            val medAvidLikeTopicFractions = personTopicProportions 
                .join(personAvidityRanking) 
                .filter(x => x._2._2 == "Intermediate") 
                .map(x => (x._1, x._2._1)) 
                .join(bigLikeFacts) 
                .map(x => (x._2._2, x._2._1)) 
                .cache()

            val medAvidLikeTopicCount = medAvidLikeTopicFractions
                .map(x => (x._1, 1))
                .reduceByKey((x,y) => (x+y))

            // (like ID, (predictive power, (like name, like type))
            val medAvidSortedLikeTopicImplications = medAvidLikeTopicFractions 
                .reduceByKey((x,y) => (x+y)) 
                .join(medAvidLikeTopicCount) 
                .map(x => (x._1, x._2._1 / scala.math.pow(x._2._2, 0.7))) 
                .join(topicAndBigLikes) 
                .sortBy(x => x._2._1, false, SLICES) 
                .cache()
            
            println(medAvidSortedLikeTopicImplications.count())

            val lowAvidLikeTopicFractions = personTopicProportions 
                .join(personAvidityRanking) 
                .filter(x => x._2._2 == "Casual") 
                .map(x => (x._1, x._2._1)) 
                .join(bigLikeFacts) 
                .map(x => (x._2._2, x._2._1)) 
                .cache()

            val lowAvidLikeTopicCount = lowAvidLikeTopicFractions
                .map(x => (x._1, 1))
                .reduceByKey((x,y) => (x+y))

            // (like ID, (predictive power, (like name, like type))
            val lowAvidSortedLikeTopicImplications = lowAvidLikeTopicFractions 
                .reduceByKey((x,y) => (x+y)) 
                .join(lowAvidLikeTopicCount) 
                .map(x => (x._1, x._2._1 / scala.math.pow(x._2._2, 0.7))) 
                .join(topicAndBigLikes) 
                .sortBy(x => x._2._1, false, SLICES) 
                .cache()
          
            println(lowAvidSortedLikeTopicImplications.count())
        }

        val india2011LiterateMalePopByAge = Array(0, 0, 0, 0, 0, 0, 0, 9514681, 12048195, 11016646, 14428312, 12033276, 13550572, 11772852, 12244668, 12563120, 11917057, 10469408, 13488663, 9908961, 13090262, 9525858, 10915987, 8661031, 8924529, 11868108, 8904360, 7493450, 9098579, 6417372, 12211844, 6070718, 7303343, 5224594, 5819425, 11158216, 6434171, 4801621, 6445374, 4741031, 10436450, 4775158, 5302304, 3604811, 3771631, 8413066, 4262767, 3194157, 4174918, 3077690, 7035249, 3138325, 3084680, 2260327, 2523672, 4969516, 2699754, 1866135, 2340320, 1874559, 4732949, 2039301, 1980765, 1498148, 1425861, 3349154, 1449289, 982985, 1127045, 890720, 2514410, 930171, 779215, 520801, 537150, 1152003, 516815, 317492, 346933, 266675, 734820, 267709, 202842, 140563, 141911, 290445, 132870, 80312, 78592, 60899, 156326, 69390, 54049, 34963, 37362, 68576, 38348, 26006, 32901, 15016, 163303)

        val india2011LiterateFemalePopByAge = Array(0, 0, 0, 0, 0, 0, 0, 8563745, 10788764, 9916088, 12669290, 10719197, 11935206, 10697355, 10912716, 10736377, 10162749, 8709604, 10741509, 8371453, 10819313, 7657028, 8535248, 7248941, 7366533, 9289801, 7098521, 5984106, 7511007, 5114221, 8937893, 4576765, 5504337, 4190781, 4425691, 7229887, 4606198, 3599719, 4948853, 3312487, 6167905, 3046994, 3424003, 2583786, 2518198, 4736152, 2675024, 2095081, 2745754, 1865955, 3778264, 1739450, 1770509, 1388417, 1528153, 2721117, 1504141, 1072488, 1402790, 1013154, 2392536, 1042158, 1010783, 805423, 746344, 1606497, 719094, 479478, 590701, 430413, 1079214, 413925, 353231, 250900, 261201, 544247, 253661, 155258, 182147, 136080, 355572, 135802, 106843, 79753, 81548, 154829, 76208, 46960, 49183, 37709, 86180, 44460, 35769, 24456, 25580, 40163, 24842, 17760, 22384, 9550, 115804)

        val india2011LiteratePopByAge = Array(0, 0, 0, 0, 0, 0, 0, 18078426, 22836959, 20932734, 27097602, 22752473, 25485778, 22470207, 23157384, 23299497, 22079806, 19179012, 24230172, 18280414, 23909575, 17182886, 19451235, 15909972, 16291062, 21157909, 16002881, 13477556, 16609586, 11531593, 21149737, 10647483, 12807680, 9415375, 10245116, 18388103, 11040369, 8401340, 11394227, 8053518, 16604355, 7822152, 8726307, 6188597, 6289829, 13149218, 6937791, 5289238, 6920672, 4943645, 10813513, 4877775, 4855189, 3648744, 4051825, 7690633, 4203895, 2938623, 3743110, 2887713, 7125485, 3081459, 2991548, 2303571, 2172205, 4955651, 2168383, 1462463, 1717746, 1321133, 3593624, 1344096, 1132446, 771701, 798351, 1696250, 770476, 472750, 529080, 402755, 1090392, 403511, 309685, 220316, 223459, 445274, 209078, 127272, 127775, 98608, 242506, 113850, 89818, 59419, 62942, 108739, 63190, 43766, 55285, 24566, 279107)

        //Probably better to look at men and women separately. For now, though, combining.
        var fcba = collection.mutable.HashMap(fan_counts_by_factor(peopleAges).toSeq: _*).withDefaultValue(0) 

        try{
            fcba(100) = (for(x <- 100 until 111) yield fcba(x)).sum
            for (x <- 101 until 111){
                fcba.remove(x)
            }
        }catch{ 
            case e: NoSuchElementException => println("no such element")
        }
        
        var pcba = collection.mutable.HashMap(person_counts_by_factor(peopleAges).toSeq: _*).withDefaultValue(0) 

        try{
            pcba(100) = (for(x <- 100 until 111) yield pcba(x)).sum
            for (x <- 101 until 111){
                pcba.remove(x)
            }
        }catch{ 
            case e: NoSuchElementException => println("no such element")
        }

        var estTopicFans = new HashMap[Int,Float]()  { override def default(key:Int) = (0.0).toFloat }
        for (x<-0 until 101){
            try{
                estTopicFans(x) = (fcba(x)).toFloat / pcba(x) * india2011LiteratePopByAge(x)
            }catch{
                case e: NoSuchElementException => println("no such element")
            }
        }

        // val get_fb_image: (String) =>  String =  (ia_id: String) => {
        //     val fbid = "100006182919837" //iaFbMapB.value("123")
        //     val FB_ACCESS_TOKEN = "EAAFz1hY1lnMBALoHipYvdcultXfNB7t2aBjKGY5jK73KHRYDlvsZAep2mcYa2biNBc2wFdQlxzpc7n1YSjZAmC3DY0L9CsvH2ZB0BqasxyQWqZCUR2ibFZA6lZBCoCsh2F9SCVS24mf0hnERbWykZCBIuv5ALzqJ64aWZAd0tTiqZC2rOQBvflkiEJjS711ySIhwZD"
        //     val fbImGetter = "https://graph.facebook.com/" + fbid + "/picture?access_token=" + FB_ACCESS_TOKEN + "&redirect=false&height=200&width=200"
        //     try{
        //         val fbImResponse = Http(fbImGetter).param("verify", "false").asString
        //         val fbImUrlTemp : JsValue = Json.parse(fbImResponse.body)
        //         val fbImUrl = Json.stringify((fbImUrlTemp \ "data" \ "url").get)
        //         val fbImName = """/(\w+\.\w+)\?""".r.findFirstIn(fbImUrl).get
        //         // val fbImName = re.search("/(\w+\.\w+)\?", fbImUrl).groups()(0)
        //         val url = new URL(fbImUrl.substring(1,fbImUrl.length-1))
        //         val im =ImageIO.read(url)

        //         val outputfile: File = new File(REPORT_DIR + "predictors/" + fbImName.substring(0,fbImName.length-1))
                



        //         val imFile = Http(fbImUrl.substring(1,fbImUrl.length-1)).asString
        //         val imageTemp = Base64.decode(imFile.body)
        //         val stream: InputStream = new ByteArrayInputStream(imFile.body.getBytes("UTF-8"))
        //         val temp = Base64.decodeBase64(stream)
        //         val im =ImageIO.read(stream)
        //         ImageIO.write(im, "jpg", outputfile);
        //         fbImName
        //     }catch{
        //         case e: IllegalArgumentException => "PLACEHOLDER.JPG"
        //         case f: NoSuchElementException =>  "PLACEHOLDER.JPG"
        //         "PLACEHOLDER.JPG"
        //     }
        // }
  //  }
//}


