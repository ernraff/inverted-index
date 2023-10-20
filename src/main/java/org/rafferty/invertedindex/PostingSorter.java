package org.rafferty.invertedindex;

public class PostingSorter {
    Posting posting;
    int index;

    public PostingSorter(Posting posting, int index){
        this.posting = posting;
        this.index = index;
    }

    public Posting getPosting(){
        return posting;
    }

    public int getIndex(){
        return index;
    }

}
