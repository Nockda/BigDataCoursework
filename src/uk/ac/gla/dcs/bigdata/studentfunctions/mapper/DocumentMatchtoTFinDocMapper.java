package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.DocumentMatch;
import uk.ac.gla.dcs.bigdata.studentstructures.TFinDoc;


//A simple function that maps DocumentMatch objects to TFinDoc objects. The TFinDoc object is used in the final scoring step to calculate the relevance score for each document.
public class DocumentMatchtoTFinDocMapper implements MapFunction<DocumentMatch, TFinDoc>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7590540593407983958L;

	public TFinDoc call(DocumentMatch value) throws Exception {
		return new TFinDoc(value.getDocid(), value.getMatchingTerm(), value.getQuery(),(short) 1);
	}
	
	

}
