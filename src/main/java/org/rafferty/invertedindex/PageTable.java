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
    public void write() throws IOException {
        ObjectOutputStream out = null;
        try{
            out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("../page-table.bin")));
            out.writeObject(table);
            out.reset();
        }finally{
            if (out != null){
                out.close();
            }
        }
    }

    public HashMap<Integer, String> getTable(){
        return table;
    }
}
