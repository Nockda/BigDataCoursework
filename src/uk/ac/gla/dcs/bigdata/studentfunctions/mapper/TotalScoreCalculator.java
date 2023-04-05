package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScoresPerTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocID;

/*This function calculates the total score of a document for a given query
  The input to this function is an iterator over the DocScoresPerTerm instances for a single query-document pair. 
  It returns a Tuple3 containing the query-document pair, the total score, and the number of matched terms.
*/
public class TotalScoreCalculator implements MapGroupsFunction<QueryDocID, DocScoresPerTerm, Tuple3<QueryDocID, Double, Integer>>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8179242986533936764L;

	public Tuple3<QueryDocID, Double, Integer> call(QueryDocID key, Iterator<DocScoresPerTerm> scores){
		
		double totalScore = 0.0;
		int numTerms = 0;
		while(scores.hasNext()) {
			totalScore += scores.next().getScore();
			numTerms++;
		}
		
		return new Tuple3<>(key, totalScore, numTerms);
	}
	
	

}
