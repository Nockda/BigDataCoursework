package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.Arrays;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoresWithArticle;


/*
 * This function takes in a NewsArticle object and returns an iterator of ScoresWithArticle objects based on the relevant document ids present in the broadcasted avgQueryScoreMap. 
 * This function is used to enable grouping the DocumentRanking object based on the RankedResult object which contains an object of type NewsArticle as a field.
 */
public class AvgQueryScoreNewsArticleFlatMap implements FlatMapFunction<NewsArticle, ScoresWithArticle> {
	
	
	private static final long serialVersionUID = -3249043291563031325L;
	private Broadcast<Map<String, AvgQueryScore>> avgQueryScoreMap;
	
	public AvgQueryScoreNewsArticleFlatMap(Broadcast<Map<String, AvgQueryScore>> avgQueryScoreMap) {
		this.avgQueryScoreMap = avgQueryScoreMap;
	}
	
	public Iterator<ScoresWithArticle> call(NewsArticle entry) throws Exception {
		//Tuple3<String, Double, String> queryScoreTuple = avgQueryScoreMap.get(entry.getId());
		String docid = entry.getId();
		String queryText = "";
		Double score = 0.0;
		String title = entry.getTitle();
		
        if (avgQueryScoreMap.value().containsKey(docid)) {
            AvgQueryScore avgQueryScore = avgQueryScoreMap.value().get(docid);
            if (avgQueryScore.getScore() != 0) {
                queryText = avgQueryScore.getQueryText();
                score = avgQueryScore.getScore();
                return Arrays.asList(new ScoresWithArticle(docid, entry, queryText, score, title)).iterator();
            }
        }
        return Collections.emptyIterator();
	}

}
