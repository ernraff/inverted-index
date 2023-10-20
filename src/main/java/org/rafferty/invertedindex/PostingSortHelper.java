package org.rafferty.invertedindex;

/*allows us to keep track of which file postings are coming from to aid in sorting and merging*/
public class PostingSortHelper {
    Posting posting;
    int index;

    public PostingSortHelper(Posting posting, int index){
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
