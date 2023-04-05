package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

public class Tokens implements Serializable {
	  
	private static final long serialVersionUID = -460170426217725794L;
	private String id;
	  private List<String> token;
	  
	  public Tokens() {
		  
	  }

	  public Tokens(String id, List<String> processed_tokens) {
	    this.id = id;
	    this.token = processed_tokens;
	  }


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTokens() {
		return token;
	}

	public void setTokens(List<String> tokens) {
		this.token = tokens;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	  
	  

}

