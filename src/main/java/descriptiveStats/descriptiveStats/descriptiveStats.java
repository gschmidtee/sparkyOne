package descriptiveStats.descriptiveStats;

import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

import scala.collection.Seq;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.explode;


/**
 * Hello world!
 *
 */
public class descriptiveStats 
{
  public static Random generator;
  public static int seed = 1;
  public static SparkSession spark;
  public static SparkContext sc;
  public static JavaSparkContext jsc;
	  
  public static void main( String[] args )
  {
    System.out.println( "Starting Descriptive Stats..." );
        
    // spark setup
    System.out.println( "Setting up spark..." );
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    //generator = new Random( seed );
        
    spark = SparkSession.builder()
        	    .appName("Spark Count2")
        	    .master("local")
        	    .getOrCreate();
    System.out.println("Spark session version = "+spark.version());
        
    sc = spark.sparkContext();
    jsc = new JavaSparkContext(sc);
        		
    	System.out.println("Spark context version = "+sc.version());
        
    	////////////////////
    	// read sample data
    	////////////////////
    	
    // read counts file
    System.out.println( "Reading network file..." );
    Dataset<Row> countsDf = spark.read().
    		//json("/Users/gregoryschmidt/workspace/data/tweetSearch/python_recent.json");
   	    json("/Users/gregoryschmidt/workspace/data/tweetSearch/python_pop20.json");
    
    // extra debug
    // show all data
    countsDf.show(15, 50);
  
    /////////////////////////////////////////////////////////////////
    // follower counts by user using most recent tweet's information
    /////////////////////////////////////////////////////////////////
    System.out.println("Follower counts by user and most recent tweet date");
    // extra debug
    //countsDf.select("user").select("user.id", "user.followers_count", "user.created_at").show(15,500);
    //countsDf.select("user.id", "user.followers_count").distinct().sort("id").show(15, 100);
    //Column timestampColumn = unix_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss '+0000' yyyy").cast("timestamp");
    //Dataset<Row> tt = countsDf.withColumn("ts", timestampColumn);
    //tt.select("ts").show(15,100);
    
    Column epochtimeColumn = unix_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss '+0000' yyyy");
    Dataset<Row> countsDf2 = countsDf.withColumn("epochtime", epochtimeColumn); 
    /*
    Dataset<Row> followerCountsDf = countsDf2
    	  .orderBy(col("epochtime").desc())
      .groupBy("user.id")
      .agg(max("epochtime"), first("user.followers_count"))
      .drop("max(epochtime)")
      .withColumnRenamed("first(user.followers_count, false)", "follower_count")
      .orderBy("id");
    followerCountsDf.show(15, 100);
    */
    // extra debug
    //countsDf2.select("epochtime").show(15,100);
    //countsDf2.select("user.id", "user.followers_count", "epochtime").show(15,100);


    ////////////////////////
    // tweet counts by user
    ////////////////////////
    /*
    System.out.println("Tweet counts by user");
    Dataset<Row> tweetCountsDf = countsDf.select("user.id", "text")
      .groupBy("id")
      .count()
      .withColumnRenamed("count", "tweet_count")
      .orderBy("id");
    tweetCountsDf.show(15, 50);
    */
    // extra debug
    //countsDf.select("user.id", "text").show(15, 50);
    
    
    //////////////////////////
    // retweet counts by user
    //////////////////////////
    // notes:
    // check retweet_count > 0, then retweet_status flag will exist
    // skip "RT @" pattern for now
    /*
    System.out.println("Retweet counts by user");
    Dataset<Row> retweetsDf = countsDf.withColumn("retweet_flag", 
    	  when(col("retweeted_status").isNull(), lit(0)).otherwise(lit(1))); 
    Dataset<Row> retweetCountsDf = retweetsDf.select("user.id", "retweet_flag")
      .groupBy("id")
      .sum("retweet_flag")
      .withColumnRenamed("sum(retweet_flag)", "retweet_count")
      .orderBy("id");
    retweetCountsDf.show(15, 50);
    */
    // extra debug
    //retweetsDf.select("user.id", "retweeted_status", "retweet_flag").show(15, 100);
    
    //////////////////////////////////////////////////
    // how many times a user's retweets are retweeted
    //////////////////////////////////////////////////
    // note: not going to trust the retweet_count directly for non-retweets to be safe.  Will first check 
    //   the retweeted_status flag like above and then assign 0 if a non-retweet.  It could perfectly well 
    //   be done correctly everytime and have a 0 in the retweet_count column, but playing it safe.
    /*
    System.out.println("How many times a user's retweets are retweeted");
    Dataset<Row> retweetedDf = countsDf.withColumn("retweeted_flag", 
    	  when(col("retweeted_status").isNull(), lit(0)).otherwise(col("retweet_count"))); 
    Dataset<Row> retweetedCountsDf = retweetedDf.select("user.id", "retweeted_flag")
    	      .groupBy("id")
    	      .sum("retweeted_flag")
    	      .withColumnRenamed("sum(retweeted_flag)", "retweeted_count")
    	      .orderBy("id");
    retweetedCountsDf.show(15, 50);
    */
    
    /////////////////
    // user mentions
    /////////////////
    
    System.out.println("User mentions");
    Dataset<Row> idDf = countsDf.select("user.id");
    Dataset<Row> mentionsIdDf = countsDf.select("entities.user_mentions.id");
    Dataset<Row> mentionsDf = mentionsIdDf
    	  .withColumn("xid", explode(mentionsIdDf.col("id")))
    	  .drop("id")
    	  .groupBy("xid")
    	  .count();
    
	Dataset<Row> joinedDf = idDf.join(mentionsDf, idDf.col("id").equalTo(mentionsDf.col("xid")), "inner")
	  .drop("id");
	  
	Dataset<Row> usermentionsDf = idDf.join(joinedDf, idDf.col("id").equalTo(joinedDf.col("xid")), "outer")
	  .withColumn("mentions_count", when(col("count").isNull(), lit(0)).otherwise(col("count")))
	  .drop("xid", "count")
	  .groupBy("id")
	  .agg(first("mentions_count"))
	  .withColumnRenamed("first(mentions_count, false)", "mentions_count")
	  .orderBy("id");
	usermentionsDf.show(15, 500);
	
    // extra debug
    //idDf.show(25, 500);
    //mentionsIdDf.show(25, 500);
	//mentionsDf.show(15, 500);
	//joinedDf.show(15, 500);
   
    /////////////////////////////
    // create combined dataframe
    /////////////////////////////
	System.out.println("Combined dataframe...");
	Dataset<Row> metrics = countsDf2
			.select(col("epochtime"), col("user.id"), col("user.name"), col("user.followers_count"), col("retweet_count"), col("favorite_count"))
			.orderBy(col("epochtime").desc())
			.groupBy("id", "name")
			.agg(max("epochtime"), 
				first("followers_count").alias("follower_count"),
				count("*").alias("tweet_count"), 
				sum("retweet_count").alias("retweeted_content_count"),
				sum(when(col("retweet_count").equalTo(lit(0)), lit(0)).otherwise(lit(1))).alias("retweet_count"),
			    sum("favorite_count").alias("favorite_count"))
			.drop("max(epochtime)")
			.orderBy(col("follower_count").desc());
	//metrics.show(15, 500);
	
	Dataset<Row> resultsDf = metrics
	  .join(usermentionsDf, metrics.col("id").equalTo(usermentionsDf.col("id")))
	  .drop(usermentionsDf.col("id"));
	resultsDf.show(15, 500);

	// extra debug
	// Taylors combined dataframe
	// ID | NAME | FOLLOWER COUNT | TWEET COUNT | RETWEETED CONTENT COUNT
	//Dataset<Row> metrics = filteredContentByDate
	/*
	Dataset<Row> metrics = countsDf2
			.select(col("epochtime"), col("user.id"), col("user.name"), col("user.followers_count"), col("retweet_count"))
			.orderBy(col("epochtime").desc())
			.groupBy("id", "name")
			.agg(max("epochtime"), 
				first("followers_count").alias("follower_count"),
				count("*").alias("tweet_count"), 
				sum("retweet_count").alias("rewteeted_content_count"))
			.drop("max(epochtime)")
			.orderBy(col("follower_count").desc());
	metrics.show(15, 500);
	*/
/*
    System.out.println("How many times a user's tweets are faved");
    Dataset<Row> favedDf = countsDf.select("user.id","favorite_count")
    	      .groupBy("id")
    	      .sum("favorite_count")
    	      .withColumnRenamed("sum(favorite_count)", "faves")
    	      .orderBy("id");
    favedDf.show(15, 50);
*/	
  }
}
