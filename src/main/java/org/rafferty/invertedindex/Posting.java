package org.rafferty.invertedindex;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/*
* A Posting consists of term, document ID for a document containing that term, and number of occurrences of the term
* within the document.
*/
public class Posting implements Serializable,Comparable<Posting>, KryoSerializable{
    private String term;
    private int docID;
    private int frequency;

    public Posting(String term, int docID, int frequency){
        this.term = term;
        this.docID = docID;
        this.frequency = frequency;
    }

    public Posting(){
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

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(this.term);
        output.writeInt(this.docID);
        output.writeInt(this.frequency);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        term = input.readString();
        docID = input.readInt();
        frequency = input.readInt();
    }
}
