package org.rafferty.invertedindex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class Lexicon {
    private HashMap<String, LexiconEntry> lexicon = new HashMap<>();
    public Lexicon() {
        lexicon = new HashMap<>();
    }

    public void put(String term, LexiconEntry entry){
        lexicon.put(term, entry);
    }

    //write finished lexicon to file
    public void write(){
        try{
            File file = new File("../lexicon.bin");
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream out = new ObjectOutputStream(fos);
            out.writeObject(lexicon);
            out.flush();
            out.close();
            fos.close();
        }catch(IOException e){
            e.printStackTrace();
        }

    }

}


