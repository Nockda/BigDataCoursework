package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.ArrayList;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/*
 * This function that extracts the titles of articles from the NewsArticle dataset. 
 * It takes a set of document IDs as input, and for each NewsArticle in the input dataset, it checks whether the document ID is present in the set. 
 * If the document ID is present, it extracts the title of the article and returns a tuple containing the document ID and title.
 * 
 */

public class TitleExtractor implements FlatMapFunction<NewsArticle, Tuple2<String, String>>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7757803567662897407L;
	private final Set<String> docIds;
	
	public TitleExtractor(Set<String> docIds) {
	this.docIds = docIds;
	}
	
	public Iterator<Tuple2<String, String>> call(NewsArticle article){
		List<Tuple2<String, String>> tuples = new ArrayList<>();
		if(docIds.contains(article.getId())) {
			
			String title = article.getTitle();
			
			if (title != null) {
				tuples.add(new Tuple2<>(article.getId(), title));
			
				}
			}
			return tuples.iterator();
		}
	}


