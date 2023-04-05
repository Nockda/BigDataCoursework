package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.AvgQueryScore;


/*To remove the  similar Documents and to keep only top 10 documents based on the average query score of each document.
 * 
 * This function takes in a list of AvgQueryScore objects as input and returns a new list of AvgQueryScore objects with redundancy removed.
 * 
 */
public class RedundancyRemover {

    public static List<AvgQueryScore> removeRedundancy(List<AvgQueryScore> dataset) {
        Map<String, List<AvgQueryScore>> queryToDocs = new HashMap<>();
        for (AvgQueryScore doc : dataset) {
            queryToDocs.computeIfAbsent(doc.getQueryText(), k -> new ArrayList<>()).add(doc);
        }

        List<AvgQueryScore> results = new ArrayList<>();
        Set<String> seenTitles = new HashSet<>();

        for (List<AvgQueryScore> docs : queryToDocs.values()) {
            int count = 0;
            for (AvgQueryScore doc : docs) {
                if (doc.getScore() > 0.0) {
                    double minDistance = Double.MAX_VALUE;
                    for (String title : seenTitles) {
                        double distance = TextDistanceCalculator.similarity(doc.getTitle(), title);
                        if (distance < minDistance) {
                            minDistance = distance;
                        }
                    }
                    if (minDistance >= 0.5 || count < 10) {
                        seenTitles.add(doc.getTitle());
                        results.add(doc);
                        count++;
                    }
                }
                if (count >= 10) {
                    break;
                }
            }
        }

        return results;
    }
    
}