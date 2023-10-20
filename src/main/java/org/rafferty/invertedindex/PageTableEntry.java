package org.rafferty.invertedindex;

public class PageTableEntry {
    String docID;
    String url;

    public PageTableEntry(String docID, String url){
        this.docID = docID;
        this.url = url;
    }
    public String getDocID(){
        return docID;
    }
    public String getUrl(){
        return url;
    }
}
