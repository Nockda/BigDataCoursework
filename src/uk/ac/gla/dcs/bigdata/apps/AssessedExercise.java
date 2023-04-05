package uk.ac.gla.dcs.bigdata.apps;
import org.apache.spark.sql.functions;



import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.RedundancyRemover;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.ArticleTitleAttacher;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.AvgQueryScoreNewsArticleFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.ContenttoString;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocIDExtractor;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocQueryScoreCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocScoreCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocumentLengthMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocumentMatchtoTFinDocMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.DocumentRankingCreator;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.MakeTokens;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.MapQueryDocIDKey;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.MapQueryKey;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.NewsToContentMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.QueryDocumentFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.TextWithIdMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.TitleExtractor;
import uk.ac.gla.dcs.bigdata.studentfunctions.mapper.TotalScoreCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.reducer.DocumentLengthReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;
import uk.ac.gla.dcs.bigdata.studentstructures.Content;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScoresPerTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentLength;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentMatch;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocID;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoresWithArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TFinDoc;
import uk.ac.gla.dcs.bigdata.studentstructures.TermsInCorpus;
import uk.ac.gla.dcs.bigdata.studentstructures.TextClass;
import uk.ac.gla.dcs.bigdata.studentstructures.Tokens;




/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		//String newsFile = System.getenv("bigdata.news");
		String newsFile = null;
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles	
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";  //Main dataset to use
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	/**
	 * @param spark
	 * @param queryFile
	 * @param newsFile
	 * @return
	 */
	/**
	 * @param spark
	 * @param queryFile
	 * @param newsFile
	 * @return
	 */
	/**
	 * @param spark
	 * @param queryFile
	 * @param newsFile
	 * @return
	 */
	
	//function to produce the list with Document ranking comprising of 10 documents ranked for each query.
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle		
		
		/*
		 * Work here deals with ContentItem to get the subtype, content, id, and title for each record
		 *
		
		NewsToContentMapper function is used to map each NewsArticle object to a list of Content objects, which contain the article's id, title, content, and subtype.
		The function filters out any content that is not a paragraph and concatenates the text of the paragraphs into a list
		
		*/
		NewsToContentMapper Contentmapper = new NewsToContentMapper();
		Dataset<Content> titlesAndContentDataset = news.flatMap(Contentmapper, Encoders.bean(Content.class));						

		/*
		 * This section deals with converting the dataset to a single paragraph that contains title as well as content
		 */
		
		Encoder<Tuple2<String, String>> stringEncoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
		
		//The following function is used to convert the contents of a Content object to a single String. 
		ContenttoString text = new ContenttoString();
		
		Dataset<Tuple2<String, String>> stringsForProcessing = titlesAndContentDataset.map(text, stringEncoder);
				

		//this section deals with mapping the text value from above against the id of each document. 
		TextWithIdMapper mapper = new TextWithIdMapper();
		Dataset<TextClass> textWithIdDataset = stringsForProcessing.map(mapper, Encoders.bean(TextClass.class));

		
		
		/*
		 * This section of the code deals with parsing the lines of strings for each id to generate tokens
		 */

		//MakeTokens: This function is used to convert an input object of type TextClass to an output object of type Tokens
		MakeTokens tokenMaker = new MakeTokens();
		Encoder<Tokens> encoder = Encoders.bean(Tokens.class);
		Dataset<Tokens> tokens = textWithIdDataset.map(tokenMaker, encoder);
		

			
		// This section of the code deals with finding the matches within the dataset for the query terms
			Dataset<String> queryTermDataset = queries.flatMap(
			    (FlatMapFunction<Query, String>) query -> query.getQueryTerms().iterator(),
			    Encoders.STRING()
			);
			
		//Converting the Dataset to the form of a list
			
			List<String> queryTermList = queryTermDataset.collectAsList();
			HashSet<String> queryTermSet = new HashSet<>(queryTermList);
		
			
		//The following function identifies the terms in the document that match the query terms, and creates a DocumentMatch object for each match. 
	
			QueryDocumentFlatMap matchCalculator = new QueryDocumentFlatMap(queryTermSet, queries);
		
		// This section finds a document match against a term in the query.
			
		Dataset<DocumentMatch> matchingTerms = tokens.flatMap(matchCalculator, Encoders.bean(DocumentMatch.class));
				
		//This section deals with finding the term frequency of a term in a document
		DocumentMatchtoTFinDocMapper tfindoc = new DocumentMatchtoTFinDocMapper();
		
		Dataset<TFinDoc> tfDocDataset = matchingTerms.map(tfindoc, Encoders.bean(TFinDoc.class))
				.groupBy("docid", "matchingTerm", "query").count()
				.withColumn("count", functions.col("count").cast("short")).as(Encoders.bean(TFinDoc.class));
		
		
		
		
		Dataset<TermsInCorpus> termCounts = matchingTerms.groupBy("matchingTerm","query").count()
				.withColumn("count", functions.col("count").cast("long")).as(Encoders.bean(TermsInCorpus.class));;
		
		
	
		/*
		 * This section of the code deals with reducing by docid to get within document term frequencies
		 */
		
		
		
		
//     Parameter function storing length of the terms in a document
		Dataset<DocumentLength> docLengthDataset = tokens.map(new DocumentLengthMapper(), Encoders.bean(DocumentLength.class));


		long totalDocNum = (long) tokens.count();
		
		
		//Finding the average document length
		//The following lines of code first computes the sum of all the doc length and the divides by the total no of documents
		//First, creating an Integer dataset to store the integer values from docLengthDataset
		Dataset<Integer> lengthDataset = docLengthDataset.map(
			    (MapFunction<DocumentLength, Integer>) documentLength -> documentLength.getLen(),
			    Encoders.INT());
		
		//Total length of all documents 
		//creating a reducer to find the sum of all the document lengths
		
		Integer TotalLength = lengthDataset.reduce(new DocumentLengthReducer());
		

		//Average document length in the corpus
		
		double AverageLength = (double) (TotalLength/totalDocNum);
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		// Calculating Document Scores using all the necessary parameters
		
		Dataset<DocScoresPerTerm> docScores = tfDocDataset.flatMap(new DocScoreCalculator(totalDocNum, AverageLength, docLengthDataset, termCounts, jsc)
				, Encoders.bean(DocScoresPerTerm.class));
		

		//The following function is used to create a new key for the DocScoresPerTerm objects that will be used to group them by document and query
		MapQueryDocIDKey queryDocID = new MapQueryDocIDKey();
		
		// Group docScores by Query and Document Key
		
		KeyValueGroupedDataset<QueryDocID, DocScoresPerTerm> groupedScores =
			    docScores.groupByKey(queryDocID, Encoders.bean(QueryDocID.class));
		
		// Calculate total score and number of query terms for each group
		TotalScoreCalculator totalScore = new TotalScoreCalculator();
		Dataset<Tuple3<QueryDocID, Double, Integer>> totalScores =
				groupedScores.mapGroups(totalScore, Encoders.tuple(Encoders.bean(QueryDocID.class), Encoders.DOUBLE(), Encoders.INT()));
		
		//The following function is used to calculate the score for each query-document pair
		DocQueryScoreCalculator scoreMap = new DocQueryScoreCalculator(queries);
		//Computing the average score for a Document for a particular query. 
		//takes the individual document term scores and divides by number of terms in the document.
		Dataset<AvgQueryScore> docQueryKeys = totalScores.map(scoreMap, Encoders.bean(AvgQueryScore.class));
		

		/*
		 * This section of the code deals with attaching titles to the docids that have been received through the ranking
		 */
		
		// Extract the Doc IDs
		Dataset<String> docIds = docQueryKeys.map(new DocIDExtractor(), Encoders.STRING())
				.distinct();
		
		// Extract the article titles and create a broadcast variable
		List<Tuple2<String, String>> titleList = news.flatMap(new TitleExtractor(docIds.collectAsList()
				.stream().collect(Collectors.toSet())), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
				.collectAsList();
		
		Map<String, String> titleMap = new HashMap<>();
		for (Tuple2<String, String> tuple : titleList) {
		    titleMap.put(tuple._1(), tuple._2());
		}
		Broadcast<Map<String, String>> titleBroadcast = jsc.broadcast(titleMap);
		
		
		//The following function, ArticleTitleAttacher is used to attach the titles of the articles to the search results. 
		Dataset<AvgQueryScore> titledQueryScores = docQueryKeys.mapPartitions(new ArticleTitleAttacher(titleBroadcast), Encoders.bean(AvgQueryScore.class));
		
		
		/*
		 * This section of the code deals with calculating the textual distance
		 */
		
		
		Dataset<AvgQueryScore> filteredQuery = titledQueryScores.filter(
			    titledQueryScores.col("score").isNotNull().and(titledQueryScores.col("queryText").isNotNull())
			);
			Dataset<AvgQueryScore> orderQuery = filteredQuery.orderBy(functions.asc("queryText"), functions.desc("score"));


		
		
		// Apply the filtering logic to each group
		List<AvgQueryScore> filteredList = RedundancyRemover.removeRedundancy(orderQuery.collectAsList());
		Broadcast<List<AvgQueryScore>> filteredBroadcast = jsc.broadcast(filteredList);
		List<AvgQueryScore> filtered = new ArrayList<AvgQueryScore>();

		for (AvgQueryScore doc : titledQueryScores.collectAsList()) {
		    if (filteredBroadcast.value().contains(doc)) {
		        filtered.add(doc);
		    }
		}
		
		Dataset<AvgQueryScore> filteredDataset = spark.createDataset(filteredList, Encoders.bean(AvgQueryScore.class));
		
		// Create a Map of docid to score
        Map<String, Double> avgQueryScoreMap = filteredDataset
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(AvgQueryScore::getDocid, AvgQueryScore::getScore));
        
     // Broadcast the AvgQueryScore map
        Broadcast<Map<String, Double>> avgQueryScoreMapBroadcast = jsc
                .broadcast(avgQueryScoreMap);
        
        
        //Storing the average Scores for a document using Hashmap

        Map<String, AvgQueryScore> avgQueryScoring = new HashMap<>();
        for (AvgQueryScore entry : filteredDataset.collectAsList()) {
            String docid = entry.getDocid();
            avgQueryScoring.put(docid, entry);
        }
        
        Broadcast<Map<String, AvgQueryScore>> avgQueryScoringBroadcast = jsc.broadcast(avgQueryScoring);
        
        
        //The following function function takes in a NewsArticle object and returns an iterator of ScoresWithArticle objects.
        AvgQueryScoreNewsArticleFlatMap functionNewsArticle = new AvgQueryScoreNewsArticleFlatMap(avgQueryScoringBroadcast);
        
        
        
        Dataset<ScoresWithArticle> articleWithScores = news.flatMap(functionNewsArticle, Encoders.bean(ScoresWithArticle.class));
        
      
        //The following function is used to map the ScoresWithArticle objects to their respective query texts. 
        MapQueryKey queryKeyFunc = new MapQueryKey();
        
        KeyValueGroupedDataset<String, ScoresWithArticle> groupedQueries = articleWithScores.groupByKey(queryKeyFunc, Encoders.STRING());
        
        Map<String, Query> queryMap = queries.collectAsList().stream().collect(Collectors.toMap(Query :: getOriginalQuery, Function.identity()));
        
        Broadcast<Map<String, Query>> queryMapBroadcast = jsc.broadcast(queryMap);       
        
        //The function DocumentRankingCreator creates a DocumentRanking object for each group of ScoresWithArticle records with the same query. 
        Dataset<DocumentRanking> documentRankingDataset = groupedQueries.mapGroups(new DocumentRankingCreator(avgQueryScoreMapBroadcast, queryMapBroadcast), Encoders.bean(DocumentRanking.class));

        List<DocumentRanking> documentRankingList = documentRankingDataset.collectAsList();
        
        //System.out.println(documentRankingList);
        return documentRankingList;  //returns a list comprising of top 10 ranking documents against each query.
        
        
	}
	
	
}
