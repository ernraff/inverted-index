package org.rafferty.invertedindex;

import java.util.HashMap;

public class Lexicon {
    private HashMap<String, LexiconEntry> lexicon = new HashMap<>();
    public Lexicon() {
        lexicon = new HashMap<>();
    }

    public void put(String term, LexiconEntry entry){
        lexicon.put(term, entry);
    }

}


