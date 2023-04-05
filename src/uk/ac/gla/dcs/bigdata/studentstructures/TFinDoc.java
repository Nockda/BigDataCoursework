package uk.ac.gla.dcs.bigdata.studentstructures;

//Class to store TermFrequency
//id, the term which is matching in the document from the query, the query which holds the term and the term frequency.

public class TFinDoc {
	private String docid;
	private String matchingTerm;
	String query;
	private short count;
	
	public TFinDoc() {
		
	}
	
	public TFinDoc(String docid, String matchingTerm, String query, short count) {
		this.docid = docid;
		this.matchingTerm = matchingTerm;
		this.query = query;
		this.count = count;
	}

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

	public short getCount() {
		return count;
	}

	public void setCount(short count) {
		this.count = count;
	}

	
}
