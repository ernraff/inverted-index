package org.rafferty.invertedindex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

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
            ObjectOutputStream out = new ObjectOutputStream(fos);
            out.writeObject(table);
            out.flush();
            out.close();
            fos.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public HashMap<Integer, String> getTable(){
        return table;
    }
}
