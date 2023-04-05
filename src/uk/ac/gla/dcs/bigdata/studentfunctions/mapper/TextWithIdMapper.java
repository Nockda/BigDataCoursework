package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.TextClass;

/*
 * This function is used to convert the tuples produced by the ContenttoString function to TextClass objects, which are used to represent text documents in the dataset. 
 * 
 */
public class TextWithIdMapper implements MapFunction<Tuple2<String, String>, TextClass> {
   
	private static final long serialVersionUID = 3886130704364759128L;


	public TextClass call(Tuple2<String, String> value) throws Exception{
        return new TextClass(value._1(), value._2());
    }

	
}

