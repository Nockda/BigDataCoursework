package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;


import uk.ac.gla.dcs.bigdata.studentstructures.Content;

import scala.Tuple2;


//To transform Content object to Tuple2<String,String>
/*
 This function is used to convert the contents of a Content object to a single String. 
 The function first concatenates the paragraphs of the content into a single string with spaces in between, 
 and then appends the title of the content to the beginning of this string. 
 The resulting string is then returned as the second element of the tuple. 
 The resulting dataset of Tuple2 objects is used to enable the mapping to generate tokens for each text document.
 */
public class ContenttoString implements MapFunction<Content, Tuple2<String, String>> {
	

/**
	 * 
	 */
	private static final long serialVersionUID = 828097244733670820L;

public Tuple2<String, String> call(Content value) throws Exception {
	
	String contentparas = String.join(" ", value.getContent());
	StringBuffer sb = new StringBuffer();
	sb.append(value.getTitle());
	sb.append(" ");
	sb.append(contentparas);
	String result = sb.toString();
	String id = value.getId();
	
	return new Tuple2<String, String> (id, result);
	
	
}

}
