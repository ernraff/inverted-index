package org.rafferty.invertedindex;

import java.io.*;
import java.nio.Buffer;
import java.util.HashMap;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Output;

//stores URLs w/corresponding document IDs
public class PageTable {
    HashMap<Integer,String> table;

    public PageTable(){
        table = new HashMap<>();
    }

    //add entry to page table
    public void put(int docID, String url){
        table.put(docID, url);
    }

    //write finished URL table to file
    public void write(){
        try{
            File file = new File("../page-table.bin");
            FileOutputStream fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            Kryo kryo = new Kryo();
            Output output = new Output(bos);
            kryo.writeObject(output, table);
            fos.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public HashMap<Integer, String> getTable(){
        return table;
    }
}
