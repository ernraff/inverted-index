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
    public void write(){
        try{
            File file = new File("../lexicon.bin");
            FileOutputStream fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            Kryo kryo = new Kryo();
            Output output = new Output(bos);
            kryo.writeObject(output, lexicon);
            fos.close();
        }catch(IOException e){
            e.printStackTrace();
        }

    }

}


