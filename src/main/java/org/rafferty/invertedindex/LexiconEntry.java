package org.rafferty.invertedindex;

public class LexiconEntry {
    private long startOffset; //start position of inverted list
    private long endOffset; //end position of inverted list
    private int documentCount; //number of documents containing the word

    public LexiconEntry() {
        this.startOffset = 0;
        this.endOffset = 0;
        this.documentCount = 0;
    }

    public long getStartOffset(){
        return startOffset;
    }

    public void setStartOffset(long startOffset){
        this.startOffset = startOffset;
    }

    public long getEndOffset(){
        return endOffset;
    }

    public void setEndOffset(long endOffset){
        this.endOffset = endOffset;
    }

    public int getDocumentCount(){
        return documentCount;
    }

    public void incrementDocumentCount(){
        documentCount++;
    }

}


