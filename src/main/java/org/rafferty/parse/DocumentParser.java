package org.rafferty.parse;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.*;
import org.rafferty.invertedindex.*;

import java.util.regex.*;

/*
 * This class parses through a document to extract docID, url, and document text.  It creates Posting objects which
 * are sent to the IntermediatePostingGenerator class for further processing
 */
public class DocumentParser {
    private IntermediatePostingGenerator generator;
//    private String currentDocID = null;
    private String currentURL = null;
    private int currentDocID = 1;

    public DocumentParser(IntermediatePostingGenerator generator) {
        this.generator = generator;
        System.out.println("Document Parser object created.");
    }

    public void parse(String content) throws IOException {
        // Parse the document to extract URL, and text
        int docID = currentDocID;
        currentDocID++;
        String url = extractUrl(content);
        String text = extractText(content);

        // Tokenize the text and create Postings objects with term frequency
        String[] terms = tokenizeString(text);

        // Create a map to store the term frequencies
        Map<String, Integer> termFrequencies = new HashMap<>();

        // Count term frequency
        for (String term : terms) {
            if (term != null && !term.isEmpty() && !term.contains("https")) {
                termFrequencies.put(term, termFrequencies.getOrDefault(term, 0) + 1);
            }
        }

        // Create a list to hold Posting objects
        List<Posting> postings = new ArrayList<>();

        // Create Postings objects for each term
        for (Map.Entry<String, Integer> entry : termFrequencies.entrySet()) {
            String term = entry.getKey();
            int frequency = entry.getValue();

            // Create a Posting for each term and add it to the list
            Posting posting = new Posting(term, docID, frequency);
            postings.add(posting);
        }

        // Send list of postings to the IntermediateIndexGenerator
        generator.processPostings(postings);
    }

    //extract document number
//    private String extractDocNo(String content){
//        try{
//            Pattern docNoPattern = Pattern.compile("<DOCNO>(.*?)</DOCNO>", Pattern.DOTALL);
//            Matcher matcher = docNoPattern.matcher(content);
//            if(matcher.find()){
//                System.out.println("Found doc ID!");
//                return matcher.group(1);
//            }else{
//                return null;
//            }
//        }catch(Exception e){
//            e.printStackTrace();
//            return null;
//        }
//    }

    //use regex to extract url from document to be included in page table
    private String extractUrl(String content) {
        try{
            Pattern urlPattern = Pattern.compile("https?://[^\\s]+");
            Matcher matcher = urlPattern.matcher(content);
            if (matcher.find()) {
                System.out.println("found url!");
                System.out.println(matcher.group());
                return matcher.group();
            } else {
                return null;
            }
        }catch(Exception e){
                e.printStackTrace();
                return null;
        }
    }

    private String extractText(String content) {
        // Use regular expression to extract the text content
        Pattern pattern = Pattern.compile("<TEXT>(.*?)</TEXT>", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);
        if (matcher.find()) {
            System.out.println("found text!");
            return matcher.group(1);
        }
        return "";
    }


    String[] tokenizeString(String s){
        s.replaceAll("\\p{C}", "?"); //remove non-printable characters from unicode string
        StringTokenizer st = new StringTokenizer(s, " \t\n\r\f!\"#$%&()*+,-./:;<=>?@[\\]^_`{|}~");
        String[] tokens = new String[st.countTokens()];
        int i = 0;
        while(st.hasMoreTokens() && i < tokens.length){
            tokens[i] = st.nextToken().trim().toLowerCase(); //normalize tokens
            i++;
        }

        return tokens;
    }


}