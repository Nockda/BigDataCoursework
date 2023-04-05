package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class TextClass implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6724383632012717854L;
	
	String id;
	String text;
	
	public TextClass() {
    }
	public TextClass(String id, String text) {
		super();
		this.id = id;
		this.text = text;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	
}
