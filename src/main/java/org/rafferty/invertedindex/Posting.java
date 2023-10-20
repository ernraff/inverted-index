package org.rafferty.invertedindex;

import javax.xml.crypto.Data;
import java.io.*;

/*
* A Posting consists of term, document ID for a document containing that term, and number of occurrences of the term
* within the document.
*/
public class Posting implements Serializable,Comparable<Posting>{
    private String term;
    private int docID;
    private int frequency;

    public Posting(String term, int docID, int frequency){
        this.term = term;
        this.docID = docID;
        this.frequency = frequency;
    }

    public int getFrequency() {
        return frequency;
    }

    public int getDocID(){
        return docID;
    }

    public String getTerm() {
        return term;
    }

    //comparator for posting objects
    @Override
    public int compareTo(Posting otherPosting){
        if(this.getTerm().compareTo(otherPosting.getTerm()) == 0){
            Integer.compare(docID, otherPosting.getDocID());
        }
        return this.getTerm().compareTo(otherPosting.getTerm());
    }

    public String toString(){
        return "Term: " + term + " DOCID: " + docID + " Frequency: " + frequency;
    }

}
