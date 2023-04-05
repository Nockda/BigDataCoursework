package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.Content;

/*Transforms NewsArticle to type Content. 
NewsToContentMapper function is used to map each NewsArticle object to a list of Content objects, which contain the article's id, title, content, and subtype. 
it extracts the title and up to five paragraphs of content from each article, discarding other content such as images and captions. 
The function filters out any content that is not a paragraph and concatenates the text of the paragraphs into a list
*/

public class NewsToContentMapper implements FlatMapFunction<NewsArticle, Content>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3809402750061500453L;

	public Iterator<Content> call(NewsArticle newsArticle) {
		String newsId = newsArticle.getId();
		String title = newsArticle.getTitle();
		List<String> contentList = new ArrayList<>();
		for(ContentItem contentItem : newsArticle.getContents()) {
			if(contentItem != null && contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")) {
				contentList.add(contentItem.getContent());
				if(contentList.size() >= 5) {
					return Collections.singletonList(new Content("paragraph", contentList, newsId, title)).iterator();
				}
			}
		}
		
		if (!contentList.isEmpty()) {
			return Collections.singletonList(new Content("paragraph", contentList, newsId, title)).iterator();
        }
        return Collections.emptyIterator();
    }
}