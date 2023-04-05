package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocID;


/*
 * This function calculates the total score of a document for a given query. 
 * The input to this function is an iterator over the DocScoresPerTerm instances for a single query-document pair. 
 */
public class DocQueryScoreCalculator implements MapFunction<Tuple3<QueryDocID, Double, Integer>, AvgQueryScore>{
	
	/**
	 * 
	 */
	
	private final Map<String, Query> queryMap;
	private static final long serialVersionUID = 4552931047724039824L;
	
	
	public DocQueryScoreCalculator(Dataset<Query> queries) {
		this.queryMap = queries.collectAsList().stream().collect(Collectors.toMap(Query::getOriginalQuery, Function.identity()));
	}
	
	public AvgQueryScore call(Tuple3<QueryDocID, Double, Integer> value) throws Exception {
		String docid = value._1().getDocid();
		String queryText = value._1().getQuery();
		Double totalScore = value._2();
		Query query = queryMap.get(queryText);
		Integer numTerms = query.getQueryTerms().size();
		Double score = totalScore / numTerms;
		return new AvgQueryScore(docid, queryText, totalScore, numTerms, score, "");
	}
	

}
