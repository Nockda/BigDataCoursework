package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;
public class ArticleTitleAttacher implements MapPartitionsFunction<AvgQueryScore, AvgQueryScore> {
	
	/**
	 * This function is used to attach the titles of the articles to the search results. 
	 * It takes an iterator of AvgQueryScore objects and returns an iterator of AvgQueryScore objects. 
	 * It uses a broadcast variable that contains a map of document IDs to their respective titles. 
	 * 
	 */
	private static final long serialVersionUID = -5594468507699272268L;
	private final Broadcast<Map<String, String>> titleMapBroadcast;
	
	public ArticleTitleAttacher(Broadcast<Map<String, String>> titleBroadcast) {
		this.titleMapBroadcast = titleBroadcast;
	}
	
	public Iterator<AvgQueryScore> call(Iterator<AvgQueryScore> iterator) {
		List<AvgQueryScore> scores = new ArrayList<>();
		Map<String, String> titleMap = titleMapBroadcast.value();
		while (iterator.hasNext()) {
			AvgQueryScore score = iterator.next();
			String title = titleMap.get(score.getDocid());
			if (title != null) {
				score.setTitle(title);
				scores.add(score);
			}
		}
		
		return scores.iterator();
	}

}
