This is the Project of Big Data course at UoG.

Summary
The goal of this exercise is to familiarize yourselves with the design, implementation and performance testing of Big Data analysis tasks using Apache Spark. You will be required to design and implement a single reasonably complex Spark application. You will then test the running of this application locally on a data of various sizes. Finally, you will write a short report describing your design, design decisions, and where appropriate critique your design. You will be evaluated based on code functionality (does it produce the expected outcome), code quality (is it well designed and follows good software engineering practices) and efficiency (how fast is it and does it use resources efficiently).

Task Description
You are to develop a batch-based text search and filtering pipeline in Apache Spark. The core goal of this pipeline is to take in a large set of text documents and a set of user defined queries, then for each query, rank the text documents by relevance for that query, as well as filter out any overly similar documents in the final ranking. The top 10 documents for each query should be returned as output. Each document and query should be processed to remove stopwords (words with little discriminative value, e.g. ‘the’) and apply stemming (which converts each word into its ‘stem’, a shorter version that helps with term mismatch between documents and queries). Documents should be scored using the DPH ranking model. As a final stage, the ranking of documents for each query should be analysed to remove unneeded redundancy (near duplicate documents), if any pairs of documents are found where their titles have a textual distance (using a comparison function provided) less than 0.5 then you should only keep the most relevant of them (based on the DPH score). Note that there should be 10 documents returned for each query, even after redundancy filtering.
You will be provided with a Java template project like the tutorials already provided. Your role is to implement the necessary Spark functions to get from a Dataset<NewsArticle> (the input documents) and a Dataset<Query> (the queries to rank for) to a List<DocumentRanking> (a ranking of 10 documents for each query). Your solution should only include spark transformations and actions, apart from any final processing you choose to do within the driver program. You should not perform any ‘offline’ computation (e.g. pre-constructing a search index), i.e. all processing should happen during the lifecycle of the Spark app. The template project provides implementations of the following code to help you:
• Loading of the query set and converting it to a Dataset<Query>.
• Loading of the news articles and converting it to a Dataset<NewsArticle>
• A static text pre-processor function that converts a piece of text to its tokenised, stopword
removed and stemmed form. This function takes in a String (the input text) and outputs a List<String> (the remaining terms from the input after tokenization, stemming and stopword removal).
• A static DPH scoring function that calculates a score for a <document,term> pair given the following information:
 
o Term Frequency (count) of the term in the document o The length of the document (in terms)
o The average document length in the corpus (in terms) o The total number of documents in the corpus
o The sum of term frequencies for the term across all documents
• A static string distance function that takes two strings and calculates a distance value
between them (within a 0-1 range).
The DPH score for a <document,query> pair is the average of the DPH scores for each <document,term> pair (for each term in the query). When designing your solution, you should primarily be thinking about how you can efficiently calculate the statistics needed to score each document for each query using DPH.
