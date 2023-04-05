package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class QueryDocID implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7772071936611182757L;
	String docid;
	String query;
	
	public QueryDocID() {
		
	}
	public QueryDocID(String docid, String query) {
		super();
		this.docid = docid;
		this.query = query;
	}
	public String getDocid() {
		return docid;
	}
	public void setDocid(String docid) {
		this.docid = docid;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	
	

}
