package org.rafferty.invertedindex;

import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.util.HashMap;
import com.esotericsoftware.kryo.*;

public class Lexicon {
    private HashMap<String, LexiconEntry> lexicon = new HashMap<>();
    public Lexicon() {
        lexicon = new HashMap<>();
    }

    public void put(String term, LexiconEntry entry){
        lexicon.put(term, entry);
    }

    //write finished lexicon to file
    public void write() throws IOException {
        ObjectOutputStream out = null;
        try{
            out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("../lexicon.bin")));
            out.writeObject(lexicon);
            out.reset();
        }finally{
            if(out != null){
                out.close();
            }
        }

    }

}


