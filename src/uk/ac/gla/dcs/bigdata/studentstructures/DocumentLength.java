package uk.ac.gla.dcs.bigdata.studentstructures;

public class DocumentLength {
	
	String id;
	Integer len;
	
	public DocumentLength() {
	
	}
	
	public DocumentLength(String id, Integer len) {
		super();
		this.id = id;
		this.len = len;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Integer getLen() {
		return len;
	}
	public void setLen(Integer len) {
		this.len = len;
	}
	
	

}
