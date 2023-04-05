package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.DocScoresPerTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocID;



/*
 * This function transforms DocScoresPerTerm objects to QueryDocID objects by extracting the docid and query fields from the former and using them to create an instance of the latter. 
 * This function is used to create a new key for the DocScoresPerTerm objects that will be used to group them by document and query, 
 * allowing for the calculation of the total score and average score for each document-query pair.
 * 
 */
public class MapQueryDocIDKey implements MapFunction<DocScoresPerTerm, QueryDocID>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6901220993321865259L;

	public QueryDocID call(DocScoresPerTerm value) throws Exception {
		QueryDocID key = new QueryDocID(value.getDocid(), value.getQuery());
		return key;
	}
	

}
