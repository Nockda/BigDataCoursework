package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;
import uk.ac.gla.dcs.bigdata.studentstructures.Tokens;

import uk.ac.gla.dcs.bigdata.studentstructures.DocumentLength;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;

//To transform Tokens to DocumentLength
/*
 * A function that maps a Tokens object to a DocumentLength object. 
 * It takes a Tokens object as input and calculates the number of tokens (i.e., the length) in the document, and then returns a DocumentLength object with the document ID and the length. 
 * This step is necessary to calculate the DPH scores.

 */
public class DocumentLengthMapper implements MapFunction<Tokens, DocumentLength>, Serializable {

	private static final long serialVersionUID = 3515065787167399362L;

	@Override
	public DocumentLength call(Tokens tokens) throws Exception {
		String id = tokens.getId();
	    int length = tokens.getTokens().size();
	    return new DocumentLength(id, length);
	}
	
	

}

