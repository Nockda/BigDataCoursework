package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;


import uk.ac.gla.dcs.bigdata.studentstructures.ScoresWithArticle;


//This function is used to map the ScoresWithArticle objects to their respective query texts. 
//The purpose of this function is to group the ScoresWithArticle objects by their query text in the groupByKey operation performed later in the code
public class MapQueryKey implements MapFunction<ScoresWithArticle, String> {
	
	
	private static final long serialVersionUID = -82822271295985329L;

	public String call(ScoresWithArticle value) throws Exception {
		return value.getQueryText();
	}

}
