package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;



public class DocIDExtractor implements MapFunction<AvgQueryScore, String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7810038670906798636L;

	//extracts the document id from the AvgQueryScore object
	public String call(AvgQueryScore value) {
		return value.getDocid();
	}

}
