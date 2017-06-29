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

object GeneralReports {

    def main(args: Array[String]) {
        // val conf = SparkConf()
        // conf.setAppName("YOUR APP NAME HERE")
        // conf.setMaster("spark://compute-master:7077")
        // conf.set("spark.cores.max", "52")
        // conf.set("spark.shuffle.consolidateFiles", "true")
        // conf.set("spark.default.parallelism", "100")
        // conf.set("spark.executor.memory", "20g")
            
        // val sc = SparkContext(conf=conf)

        val TOPIC = "missmalini"
        val TOPIC_NAME = "MissMalini"
        val REPORT_DIR = "/home/xiaoluguo/missmalini/"
        val DATA_ROOT = "hdfs://10.142.0.63:9000/data"
        val ARCHIVE_ROOT = "hdfs://10.142.0.63:9000/" + TOPIC
        val ENTITY_FILE = TOPIC + "_entities.txt"
        val SLICES = 2000

        var likeFacts = sc.textFile(DATA_ROOT+"/fact_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 2))
            .map(x => (x(0),(x(1),x(2))))
            .distinct()

        val dimLikes = sc.textFile(DATA_ROOT+"/dim_like", SLICES).map(x => x.split("""\t"""))
            .filter(x => (x.length > 3))
            .setName("dimLikes")
            .cache()

        // val iaFbMapB = sc.broadcast(dimLikes.map(x => (x(0), x(3))).distinct().collect().toMap)

        val likes = dimLikes.map(x => (x(0), (x(1), x(2)))).distinct()

        // THIS IS OLD, BUT CAN HELP FIND ENTITIES
        // val safe_match: (String,String) => Boolean = (a:String,b:String) =>{
        //     if (a!=None){
        //         val sr ="miss".r.findFirstMatchIn(a.toLowerCase())
        //         val sr2="malini".r.findFirstMatchIn((a+b).toLowerCase())
        //         if (sr!=None && sr2!=None && sr.get.start< sr2.get.start){
        //             true
        //         } else {
        //             false
        //         }
        //     }else{
        //         false
        //     }
        // }

        // val fbEntities = likes.filter(x => safe_match(x._2._1,x._2._2)).collect()
        // fbEntities.saveAsTextFile("path/"+ENTITY_FILE)

        // topicLikes.textFile("path/"+ENTITY_FILE)

        // //human readable file
        // val fp = new PrintWriter(new File(ENTITY_FILE))
        // fp.write(fbEntities.mkString(""))
        // fp.close()

        // val topicLikesB = sc.broadcast(topicLikes.map(x => x(0)).collect().toSet)
        // val manyTopicEntities = False

        // if (topicLikesB.value.size >= 1000){
        //     manyTopicEntities = True
        

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
            .cache()

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
            .cache()
        
        val indiaPeopleStates = locationFacts.join(indiaLocations)
            .map(x => (x._2._1, x._2._2))
            .distinct()
            .cache()

        likeFacts = likeFacts.map(x => (x._2._1, (x._1, x._2._2)))
            .join(indiaPeople)
            .map(x => (x._2._1._1, (x._1, x._2._1._2)))
            .distinct()
            .coalesce(SLICES)
            .setName("like_facts")
            .cache()
        
        val personTotalLikeCounts = likeFacts.map(x => (x._2._1, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personalTotalLikeCounts")
            .cache()

        personTotalLikeCounts.count()

        var pathBigLikes = new Path("/" + TOPIC + "big_likes")
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
                .cache()

            bigLikes.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
        } else {
            bigLikes = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/big_likes")
                .setName("bigLikes")
                .cache()
        }

        val topicLikesData = Array(("1",2),("3",4)) //placeholder
        val topicLikes = sc.parallelize(topicLikesData)

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
            .cache()

        //(like_ID, user_ID)
        val targetedLikeFacts = likeFacts //#(fact_ID, (person_ID, like_ID))
            .map(x => (x._2._2, x._2._1))
            .setName("targetedLikeFacts")
            .cache()
        targetedLikeFacts.count()
        likeFacts.unpersist()

        //(person_ID, number_of_topic_and_big_likes) for people who like something big and/or topic
        val personRelevantLikeCounts = topicAndBigLikes
            .join(targetedLikeFacts) // (like_ID, ((name,type), user_ID))
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .coalesce(SLICES)
            .setName("personRelevantLikeCounts")
            .cache()

        //(like_ID, (1, user_ID)) for like_IDs in topic
        val topicLikeFacts = topicLikes
            .map(x => (x._1, 1))
            .join(targetedLikeFacts)
            .distinct()
            .setName("topicLikeFacts")
            .cache()

        //(person_ID, number_of_topic_likes) for people who like something in topic
        val personTopicLikeCounts = topicLikeFacts
            .map(x => (x._2._2, 1))
            .reduceByKey((x,y) => (x+y))
            .distinct()
            .setName("personTopicLikeCounts")
            .cache()

        //(person_ID, fraction of likes that are topic-related)
        val personTopicProportions = personRelevantLikeCounts
            .leftOuterJoin(personTopicLikeCounts)
            .map(x => (x._1, if (x._2._2.getClass.getSimpleName == None.getClass.getSimpleName) { 0.0 } else {x._2._2.get.toFloat/x._2._1}))
            .distinct()
            .setName("personTopicProportions")
            .cache()
    
        // This part is dealing with new data person_info
        //Lots of info, for people in India
        val people = sc.textFile(DATA_ROOT + "/person_info", SLICES).map(x => x.split("""\t"""))
            .filter(x => x.size > 8)
            .map(x => (x(0), x.drop(1)))
            .join(indiaPeople)
            .mapValues(x => x._1)
            .setName("people")
            .cache()

        def safe_date_from_str(input : String) : Option[Calendar] = {
            val reg = """\d\d/\d\d/\d\d\d\d""".r
            var allMatch : Array[String] = reg.findAllIn(input).toArray
            if (allMatch.length == 1) {
                var dateOB : Date = new Date(allMatch(0))

                var cal : Calendar = new GregorianCalendar()
                cal.setTime(dateOB)

                return Some(cal)

            } else {
                return None
            }
        }

        def getAge( date : Option[Calendar]) : Double = {
            var age: Double =0.0
            val milliInYear: Double =31557600000.0
            
            val today : Calendar = Calendar.getInstance()
            date match {
                case Some(date) => age= (today.getTime().getTime() - date.getTime().getTime())/milliInYear
                case None => val age= -1
            }

            return age
        }

        //(person_ID, java calendar structure of birthday) - not actually age yet!
        val peopleBirthdays = people
            .filter(x => x._2(5) != "null")
            .map(x => (x._1, safe_date_from_str(x._2(5))))
            .filter(x => x._2.getClass.getSimpleName != None.getClass.getSimpleName)
            .coalesce(SLICES)
            .setName("people_birthdays")
            .cache()
    
        //(person_ID, age in years)
        
        val peopleAges = peopleBirthdays
          .map(x => (x._1, getAge(x._2)))
          .coalesce(SLICES)
          .setName("people_ages")
          .cache()

        def get_ageband(x: Double) : String = {
            if (x < 18) { return "Under 18" }
            if (x < 25) { return "18-24" }
            if (x < 35) { return "25-34" }
            if (x < 45) { return "35-44" }
            if (x < 55) { return "45-54" }
            return "55+"
        }

        val peopleAgebands = peopleAges
            .map(x => (x._1, get_ageband(x._2)))
            .cache()
        
        //(person ID, gender)
        val peopleGenders = people
            .filter(x => x._2(9) != "null")
            .map(x => (x._1, x._2(9)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_genders")
            .cache()

        def clean_relationships(rel: String) : String = {
            WordUtils.capitalize(rel).replaceAll("""\(Pending)\""","") //does not include unicode, may need to be fixed
        }

        //(person ID, relationship status)
        //The isdigit filtering is because there are a bunch of relationship values that are dates.
        //Not sure why, or what it indicates, but for now I'm just ignoring them.
        val peopleRelationships = people
            .filter(x => x._2(7) != "null" && !(x._2(7)(0).isDigit))
            .map(x => (x._1, x._2(7)))
            .coalesce(SLICES)
            .map(x => (x._1, clean_relationships(x._2)))
            .distinct()
            .setName("people_relationships")
            .cache()
        //cache() only used here for debugging, shouldn't be needed?

        //(person ID, locale (i.e. language/country))
        val peopleLocales = people
            .filter(x => x._2(14) != "null")
            .map(x => (x._1, x._2(14)))
            .coalesce(SLICES)
            .distinct()
            .setName("people_locales")
            .cache()
        //cache() only used here for debugging, shouldn't be needed?
        
        //(person_ID, like_ID) for all big likes and all people who like them
        val bigLikeFacts = bigLikes.map(x => (x,1))
            .join(targetedLikeFacts)
            .map(x => (x._2._2, x._1))
            .cache()

        pathBigLikes = new Path("/" + TOPIC + "like_topic_fractions")
        fileExists = fileSystem.exists(pathBigLikes)

        var likeTopicFractions = sc.parallelize(data)

        if (!fileExists) {
            //(like_ID, topic_like_proportion) for each person who likes like_ID (for big+topic likes)
            likeTopicFractions = bigLikeFacts.join(personTopicProportions)
                .map(x => (x._2._1, x._2._2))
                .cache()
            likeTopicFractions.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions")
        } else{
            like_topic_fractions = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_fractions").map(eval).cache()) //fix eval
        } 

        pathBigLikes = new Path("/" + TOPIC + "like_topic_sum")
        fileExists = fileSystem.exists(pathBigLikes)

        var likeTopicSum = sc.parallelize(data)

        if(!fileExists) {
            likeTopicSum = likeTopicFractions.reduceByKey((x,y) => (x+y)).cache() //This should give a measure of popularity of things, weighted by how big a topic fan each liker is. Normalizing it by count, as done below, gives a measure of strength of implication.
            likeTopicSum.saveAsTextFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum")
        } else{
            likeTopicSum = sc.textFile("hdfs://10.142.0.63:9000/" + TOPIC + "/like_topic_sum").map(eval)
        }

        //(like ID, number of likes for that ID) (for big+topic likes)
        val likeTopicCount = likeTopicFractions.map(x => (x._1, 1)).reduceByKey((x,y) => (x+y)).cache()

        //This is actually not just the strength normalized by count; it's also multiplied by an exponentiation (<1.0) of the count, so that more objectively popular things are weighted higher. Without this multiplication, we'd divide by x[1][1], by dividing by something less than that we're multiplying by the corresponding value. E.g., dividing by x[1][1]**0.7, we're weighting by popularity**0.3.
        //(like ID, topic-predictive power for that like ID)
        val likeTopicImplications = likeTopicSum.join(likeTopicCount).map(x => (x._1, x._2._1 / x._2._2**0.7)).cache()

        //(like ID, (predictive power, (like name, like type))
        val sortedLikeTopicImplications = likeTopicImplications 
          .join(topicAndBigLikes) 
          .sortBy(x => x._2._1, False, SLICES) 
          .cache()

        // val sortedOfftopicImplications = sortedLikeTopicImplications 
        //     .filter(x => !(topicLikesB.value.contains(x._1)))    
        //     .cache()

        //Make a file with the top 5000 putatively off-topic entities, for manual fixing
        // ot_file = codecs.open(TOPIC + "_top_offtopics.txt", "w", "utf-8")
        // ot_file.write(unicode(sorted_offtopic_implications.take(5000)))
        // ot_file.close()
    }
}

// //Idea:
// // build a string from the fb_entities_file
// // Build string with name and type separated by comma and entries by dash "-" and save it to a file, after that, read from that file
// import scala.collection.mutable.ListBuffer
// import scala.collection.mutable.StringBuilder

// object GeneralReports{
//     def main(args: Array[String]) {
//         var sampleEntity: Array[(String,(String,String))] = Array(("13270834", ("@missmalini Yay! Mickey & Minnie Mouse hve just arrived at the Disney Jet Set Go event in Mumbai @RanjitAtWork", "Other")), ("65592", ("MissMalini", "Entertainment website")), ("48143266", ("Missmalini Publishing Pvt. Ltd.", "Local business")), ("4014059", ("Miss Malini", "Movie")))
    
//         val longString = evalEntity(sampleEntity)

//         println(longString)

//         val arrayAgain = evalString(longString)

//         val rdd = sc.parallelize(arrayAgain)

//         println(rdd.collect())

//     }

//     def evalEntity(fb_entities_file :Array[(String,(String, String))]): String = {

//         val B = new StringBuilder

//         for(entry <- fb_entities_file) {
//             B ++= entry._1 + ","
//             B ++= entry._2._1 + "," + entry._2._2 + "--"
//         }
//         val resultString = B.toString

//         return resultString
//     }

//     def evalString(longString :String): ListBuffer[(String, (String, String))] = {

//         val res : ListBuffer[(String, (String, String))] = ListBuffer()

//         val splitString = longString.split("--")

//         for (entry <- splitString){
//             val internalSplit = entry.split(",")
//             res += ((internalSplit(0),(internalSplit(1),internalSplit(2))))
//         }

//         return res
//     }
// }

sortedLikeTopicImplications.saveAsTextFile("hdfs://compute-master:9000/" + TOPIC + "/like_topic_implications")

//just the like_IDs for the top 200 off-topic predictors
val topOffTopicsB = sc.broadcast((sortedOfftopicImplications.keys().take(200)).toSet)

val edgeLikeFacts = targetedLikeFacts.filter(x => topOffTopicsB.contains(x(0)))

val edgeLikers = edgeLikeFacts
    .values()
    .distinct()

val fanIDs = personTopicLikeCounts.keys()

//person_IDs who like top off-topic predictors but not any on-topic predictors
val edgeNonfans = personRelevantLikeCounts
    .keys()
    .substract(fanIDs)
    .intersection(edgeLikers)
    .cache()

//(person_ID, 1) for edge_nonfans
val edgeKv = edgeNonfans.map(x => (x,1)).cache()

























