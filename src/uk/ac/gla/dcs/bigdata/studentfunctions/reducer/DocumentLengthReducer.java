package uk.ac.gla.dcs.bigdata.studentfunctions.reducer;

import org.apache.spark.api.java.function.ReduceFunction;


/*
 * This function is used to calculate the total length of a document by reducing the lengths of the individual tokens. 
 * It reduces a collection of integers to their sum.
 */
public class DocumentLengthReducer implements ReduceFunction<Integer>{

	private static final long serialVersionUID = 2772898438389119080L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		
		return v1+v2;
	}

}

