package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class DocScoresPerTerm implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1587657278983994999L;
	private String docid;
	private String matchingTerm;
	private String query;
	private double score;
	
	public DocScoresPerTerm() {
		
	}
	
	public DocScoresPerTerm(String docid, String matchingTerm, double score, String query) {
		super();
		this.docid = docid;
		this.matchingTerm = matchingTerm;
		this.query = query;
		this.score = score;
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

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	
	

}
