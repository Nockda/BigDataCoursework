package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;


public class AvgQueryScore implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7051190082080290211L;
	String docid;
	String queryText;
	Double totalScore;
	Integer numTerms;
	Double score;
	String title;
	
	public AvgQueryScore() {
		
	}
	public AvgQueryScore(String docid, String queryText, Double totalScore, Integer numTerms, Double score, String title) {
		super();
		this.docid = docid;
		this.queryText = queryText;
		this.totalScore = totalScore;
		this.numTerms = numTerms;
		this.score = score;
		this.title = (title != null) ? title : "Placeholder";
	}
	public String getDocid() {
		return docid;
	}
	public void setDocid(String docid) {
		this.docid = docid;
	}
	public String getQueryText() {
		return queryText;
	}
	public void setQueryText(String queryText) {
		this.queryText = queryText;
	}
	public Double getTotalScore() {
		return totalScore;
	}
	public void setTotalScore(Double totalScore) {
		this.totalScore = totalScore;
	}
	public Integer getNumTerms() {
		return numTerms;
	}
	public void setNumTerms(Integer numTerms) {
		this.numTerms = numTerms;
	}
	public Double getScore() {
		return score;
	}
	public void setScore(Double score) {
		this.score = score;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
}