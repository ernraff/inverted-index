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
        for (Posting posting : postings) {
            if (postingBuffer.size() < bufferSize) {
                //if posting buffer is not full, add posting to buffer
                postingBuffer.add(posting);
            } else {
                //if buffer is full, sort postings and write to file
                Collections.sort(postingBuffer, (a,b)-> a.compareTo(b));
                writePostingsToFile();
                //clear list and add current posting
                postingBuffer.clear();
                postingBuffer.add(posting);
            }
        }
    }

    public void writePostingsToFile() {
        System.out.println("writing postings to file");
        // Create filePath for temporary file
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
        String filePath = TEMP_DIRECTORY + "/" + timeStamp;
        FileOutputStream fos = null;
        ObjectOutputStream out = null;

        try {
            fos = new FileOutputStream(filePath);
            out = new ObjectOutputStream(fos);
            for (Posting posting : postingBuffer) {
                out.writeObject(posting);
                System.out.println("posting written to " + filePath);
            }
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close the streams
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
