package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.ArrayList;



import java.util.HashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.sql.Dataset;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import uk.ac.gla.dcs.bigdata.studentstructures.DocumentMatch;
import uk.ac.gla.dcs.bigdata.studentstructures.Tokens;


/*The class is used for using the query terms form each query to find a possible match in all of the documents.
  This function identifies the terms in the document that match the query terms, and creates a DocumentMatch object for each match. 
  For each token in the document, the function checks if it matches any of the query terms. If a match is found, a DocumentMatch object is created with the document ID, the matching token, and the original query. 
The function then returns an iterator of all the DocumentMatch objects that were created.
*/

public class QueryDocumentFlatMap implements FlatMapFunction<Tokens, DocumentMatch>{
	
	private static final long serialVersionUID = -8578947795303802368L;
	public HashSet<String> queryTermSet;

	public HashMap<String, String> queryTermMap;
	
	public QueryDocumentFlatMap(HashSet<String> queryTermSet, Dataset<Query> queryDataset) {
		this.queryTermSet = queryTermSet;
		this.queryTermMap = new HashMap<String, String>();
		List<Query> queryList = queryDataset.collectAsList();
		for (Query query : queryList) {
			List<String> queryTerms = query.getQueryTerms();
			String queryStr = query.getOriginalQuery();
			for (String term : queryTerms) {
				if (queryTermSet.contains(term)) {
				queryTermMap.put(term, queryStr);
			}
		}
		
	}
	}
	
	public Iterator<DocumentMatch> call(Tokens value) throws Exception
	{
		String docID = value.getId();
		List<String> documentTokens = value.getTokens();
		List<DocumentMatch> matchingTokens = new ArrayList<>();
		for (String token : documentTokens) {
			if (queryTermSet.contains(token)) {
				String queryStr = queryTermMap.get(token);
				matchingTokens.add(new DocumentMatch(docID, token, queryStr));
			}
		}
		
		return matchingTokens.iterator();	
		}
}
