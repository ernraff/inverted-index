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

    @Override
    public int compareTo(Posting otherPosting){
        if(this.getTerm().compareTo(otherPosting.getTerm()) == 0){
            if(docID < otherPosting.getDocID()){
                return -1;
            }else if(docID > otherPosting.getDocID()){
                return 1;
            }else{
                return 0;
            }
        }
        return this.getTerm().compareTo(otherPosting.getTerm());
    }

    public String toString(){
        return "Term: " + term + " DOCID: " + docID + " Frequency: " + frequency;
    }


    //convert posting to byte array to be written to file
//    public byte[] serialize(){
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        ObjectOutputStream out = null;
//        try {
//            out = new ObjectOutputStream(bos);
//            out.writeObject(this);
//            out.flush();
//            byte[] serializedPosting = bos.toByteArray();
//            return bos.toByteArray();
//        }catch(IOException e){
//            e.printStackTrace();
//            return new byte[0];
//        }finally{
//            try{
//                bos.close();
//            }catch (IOException e){
//                //ignore
//            }
//        }
//    }

}
