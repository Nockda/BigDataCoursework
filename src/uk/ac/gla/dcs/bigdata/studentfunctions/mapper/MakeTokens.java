package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TextClass;
import uk.ac.gla.dcs.bigdata.studentstructures.Tokens;


//Generates tokens after processing the contents of the Textclass, returns Tokens object with id and the generated tokens.
/*
 * This function is used to convert an input object of type TextClass to an output object of type Tokens. 
 * In the call() method, an instance of the TextPreProcessor class is created, and the process() method is called on it to obtain a List of processed tokens from the input TextClass object. 
 * The ID field of the input object is assigned to the ID field of the output object. 
 * The processed_tokens list is assigned to the tokens field of the output object. 
 * This transformation takes a text string and creates a list of tokens that can be used to match against the query terms.
 * 
 */
public class MakeTokens implements MapFunction<TextClass, Tokens> {
    
	private static final long serialVersionUID = -2000619431985446330L;

	public Tokens call(TextClass value) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        List<String> processed_tokens = processor.process(value.getText());
        String id = value.getId();
        return new Tokens(id, processed_tokens);
    }
}


