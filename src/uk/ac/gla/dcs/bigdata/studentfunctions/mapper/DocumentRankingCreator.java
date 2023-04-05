package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.ArrayList;

import java.util.Collections;
import java.util.Comparator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;


import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import uk.ac.gla.dcs.bigdata.studentstructures.ScoresWithArticle;



/*This function creates a DocumentRanking object for each group of ScoresWithArticle records with the same query. 
 * The call() method takes the query text and an iterator of ScoresWithArticle records and returns a DocumentRanking object. 
 * 
 */
public class DocumentRankingCreator implements MapGroupsFunction<String, ScoresWithArticle, DocumentRanking>{
	
	
	private static final long serialVersionUID = 8707498160974133776L;
	private final Broadcast<Map<String, Double>> avgQueryScoreMapBroadcast;
	private final Broadcast<Map<String,Query>> queryMapBroadcast;
	
	
	
	
	public DocumentRankingCreator(Broadcast<Map<String, Double>> avgQueryScoreMapBroadcast, Broadcast<Map<String, Query>> queryMapBroadcast) {
		
		super();
		this.avgQueryScoreMapBroadcast = avgQueryScoreMapBroadcast;
		this.queryMapBroadcast = queryMapBroadcast;
		
	}



	public DocumentRanking call(String queryText, Iterator<ScoresWithArticle> iterator) throws Exception {
		// Create a Query object using the queryText value
		
		Query query = queryMapBroadcast.value().get(queryText);
		// Create a list of RankedResult objects by iterating over the rows in the groups
		List<RankedResult> results = new ArrayList<>();
        while (iterator.hasNext()) {
            ScoresWithArticle row = iterator.next();
            String docid = row.getDocid();
            NewsArticle article = row.getArticle();
            Double score = avgQueryScoreMapBroadcast.getValue().getOrDefault(docid, 0.0);
            RankedResult result = new RankedResult(docid, article, score);
            results.add(result);
        }
		
        // Sort the results list by descending score
        
        Collections.sort(results, Comparator.comparing(RankedResult :: getScore).reversed());
        
        // Create a DocumentRanking object using the Query object and the sorted list of RankedResult objects
        
        DocumentRanking documentRanking = new DocumentRanking(query, results);
        
       
        return documentRanking;
        
        
	}

}
