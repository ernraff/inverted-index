package org.rafferty.invertedindex;

public class LexiconEntry {
    private long startOffset; //start position of inverted list in our final encoded file
    private long endOffset; //end position of inverted list in our final encoded file
    private int documentCount; //number of documents containing the word (need to know number of blocks and size of last block.)
    private int numOfBlocks; //number of blocks in inverted list for this term

    public LexiconEntry() {
        this.startOffset = 0;
        this.endOffset = 0;
        this.documentCount = 0;
        this.numOfBlocks = 0;
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

    public void setDocumentCount(int documentCount){
        this.documentCount = documentCount;
    }

    public int getNumOfBlocks(){
        return numOfBlocks;
    }

    public void setNumOfBlocks(int numOfBlocks){
        this.numOfBlocks = numOfBlocks;
    }

}


