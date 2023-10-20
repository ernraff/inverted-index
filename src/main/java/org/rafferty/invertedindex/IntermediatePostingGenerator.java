package org.rafferty.invertedindex;

import java.io.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
* This class takes a list of Posting objects, sorts them, and writes them to intermediate files.
* Intermediate files are not in their final form.
*/
public class IntermediatePostingGenerator {
    private List<Posting> postingBuffer;
    private int bufferSize;
    private static final String TEMP_DIRECTORY = "../temp-files.bin";

    public IntermediatePostingGenerator(int bufferSize) {
        postingBuffer = new ArrayList<>();
        this.bufferSize = bufferSize;
        File directory = new File(TEMP_DIRECTORY);
        //create directory for temp files if it does not already exist.
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    //add postings to "buffer" list. when list reaches a certain number of postings, the postings will be written to file and the list will be cleared.
    public void processPostings(List<Posting> postings) {
        Collections.sort(postings, (a,b)-> a.compareTo(b));
        for (Posting posting : postings) {
            if (postingBuffer.size() < bufferSize) {
                postingBuffer.add(posting);
            } else {
                writePostingsToFile();
                postingBuffer.clear();
                postingBuffer.add(posting);
            }
        }
    }

    public void writePostingsToFile() {
        System.out.println("writing postings to file");
        //create filePath for temporary file
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
        String filePath = TEMP_DIRECTORY + "/" + timeStamp;
        try {
            FileOutputStream fos = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(fos);
            for (Posting posting : postingBuffer) {
                out.writeObject(posting);
                System.out.println("posting written to " + filePath);
            }
            out.close();
            fos.close();
        } catch (IOException e) {

            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        IntermediatePostingGenerator generator = new IntermediatePostingGenerator(100);
//        try{
//            FileMerger merger = new FileMerger(generator);
//            merger.merge("../temp-files.bin");
//        }catch(IOException e){
//            e.printStackTrace();
//        }
//        try {
//            FileInputStream fis = new FileInputStream("../merged-index-2.bin");
//            ObjectInputStream ois = new ObjectInputStream(fis);
//            while (true) {
//                try {
//                    Posting posting = (Posting) ois.readObject();
//                    System.out.println(posting.toString());
//                    System.out.println("------------------------------------------------------------------------------------------------------");
//                } catch (EOFException e) {
//                    break;
//                } catch (ClassNotFoundException e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
//}
