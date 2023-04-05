package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ScoresWithArticle {
	
	public String docid;
	public NewsArticle article;
	public String queryText;
	public double score;
	public String title;
	
	public ScoresWithArticle() {
		
	}
	
	public ScoresWithArticle(String docid, NewsArticle article, String queryText, double score, String title) {
		super();
		this.docid = docid;
		this.article = article;
		this.queryText = queryText;
		this.score = score;
		this.title = title;
	}
	public String getDocid() {
		return docid;
	}
	public void setDocid(String docid) {
		this.docid = docid;
	}
	public NewsArticle getArticle() {
		return article;
	}
	public void setArticle(NewsArticle article) {
		this.article = article;
	}
	public String getQueryText() {
		return queryText;
	}
	public void setQueryText(String queryText) {
		this.queryText = queryText;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	
	

}
