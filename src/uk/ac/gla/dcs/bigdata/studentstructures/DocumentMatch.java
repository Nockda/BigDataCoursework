package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class DocumentMatch implements Serializable {
	
	private static final long serialVersionUID = -7192356031088707696L;
	String docid;
	String matchingTerm;
	String query;
//	short count;
	public DocumentMatch(String docid, String matchingTerm, String query) {
		super();
		this.docid = docid;
		this.matchingTerm = matchingTerm;
		this.query = query;
//		this.count = count;

	}
	
//	public short getCount() {
//		return count;
//	}
//	public void setCount(short count) {
//		this.count = count;
//	}
	public String getDocid() {
		return docid;
	}
	public void setDocid(String docid) {
		this.docid = docid;
	}
	public String getMatchingTerm() {
		return matchingTerm;
	}
	public void setMatchingTerm(String matchingTerm) {
		this.matchingTerm = matchingTerm;
	}

	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	

}
