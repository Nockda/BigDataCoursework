package uk.ac.gla.dcs.bigdata.studentfunctions.mapper;


import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScoresPerTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentLength;
import uk.ac.gla.dcs.bigdata.studentstructures.TFinDoc;
import uk.ac.gla.dcs.bigdata.studentstructures.TermsInCorpus;


//To transform TFinDoc object to DocScoresPerTerm
//This function calculates the DPH score for each document and term match. It takes several broadcast variables as input: totalDocsInCorpus, averageDocumentLengthInCorpus, termFrequencyInCorpus, and documentLengths. 
public class DocScoreCalculator implements FlatMapFunction<TFinDoc, DocScoresPerTerm> {
    private static final long serialVersionUID = 7851983303784839417L;
	private final Broadcast<Long> totalDocsInCorpus;
    private final Broadcast<Double> averageDocumentLengthInCorpus;
    private final Broadcast<Map<String, Integer>> termFrequencyInCorpus;
    private final Broadcast<Map<String, Integer>> documentLengths;

    public DocScoreCalculator(long totalDocsInCorpus,
                              double averageDocumentLengthInCorpus,
                              Dataset<DocumentLength> docLengths,
                              Dataset<TermsInCorpus> termCounts,
                              JavaSparkContext jsc) {
        this.totalDocsInCorpus = jsc.broadcast(totalDocsInCorpus);
        this.averageDocumentLengthInCorpus = jsc.broadcast(averageDocumentLengthInCorpus);

        // Filter the termCounts dataset to get the total term frequency of each term in the corpus
        List<TermsInCorpus> termFrequencyList = termCounts.collectAsList();
        Map<String, Integer> termFrequencyInCorpusMap = new HashMap<>();
        for (TermsInCorpus termCount : termFrequencyList) {
            String term = termCount.getMatchingTerm();
            int count = (int) termCount.getCount();
            termFrequencyInCorpusMap.put(term, count);
        }
        this.termFrequencyInCorpus = jsc.broadcast(termFrequencyInCorpusMap);

        // Collect the document lengths as a Map and broadcast it to the executors
        List<DocumentLength> docLengthList = docLengths.collectAsList();
        Map<String, Integer> documentLengthsMap = new HashMap<>();
        for (DocumentLength docLength : docLengthList) {
            documentLengthsMap.put(docLength.getId(), docLength.getLen());
        }
        this.documentLengths = jsc.broadcast(documentLengthsMap);
    }

    public Iterator<DocScoresPerTerm> call(TFinDoc value) {
        // Get the count of the term in the document
        short termFrequencyInCurrentDocument = value.getCount();

        // Get the total term frequency of term in corpus
        int totalTermFrequencyInCorpus = termFrequencyInCorpus.value().getOrDefault(value.getMatchingTerm(), 0);

        // Get the length of the current document using the broadcast variable
        int currentDocumentLength = documentLengths.value().getOrDefault(value.getDocid(), 0);

        // Calculate the DPH score for the current document and term
        double score = DPHScorer.getDPHScore(termFrequencyInCurrentDocument,
                                             totalTermFrequencyInCorpus,
                                             currentDocumentLength,
                                             averageDocumentLengthInCorpus.value(),
                                             totalDocsInCorpus.value());

        return Collections.singleton(new DocScoresPerTerm(value.getDocid(), value.getMatchingTerm(), score, value.getQuery())).iterator();
    }
}