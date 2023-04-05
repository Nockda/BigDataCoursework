package uk.ac.gla.dcs.bigdata.studentstructures;
import java.io.Serializable;

import java.util.List;



public class Content implements Serializable {
	
	private static final long serialVersionUID = 7860293794078412243L;
	
	String subtype;
	List<String> content;
	String id;
	String title;
	
	public Content() {
	}
	
	public Content(String subtype, List<String> content, String id, String title) {
		
		super();
		this.subtype = subtype;
		this.content = content;
		this.id = id;
		this.title = title;
	
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getSubtype() {
		return subtype;
	}

	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}

	public List<String> getContent() {
		return content;
	}

	public void setContent(List<String> content) {
		this.content = content;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}