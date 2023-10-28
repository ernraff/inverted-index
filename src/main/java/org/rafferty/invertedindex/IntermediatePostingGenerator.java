package org.rafferty.invertedindex;


import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.time.StopWatch;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.esotericsoftware.kryo.*;
import org.apache.commons.lang3.time.StopWatch;

/*
* This class takes a list of Posting objects, sorts them, and writes them to intermediate files.
* Intermediate files are not in their final form.
*/
public class IntermediatePostingGenerator {
    private List<Posting> postingBuffer;
    private long bufferSize;
    private static final String TEMP_DIRECTORY = "../temp-files";
    private StopWatch stopWatch;
//    private Object sortWriteLock;

    public IntermediatePostingGenerator(long bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        File directory = new File(TEMP_DIRECTORY);
        //create directory for temp files if it does not already exist.
        if (!directory.exists()) {
            directory.mkdirs();
        }
        postingBuffer = new ArrayList<Posting>();
        stopWatch = StopWatch.createStarted();
    }

    //add postings to "buffer" list. when list reaches a certain number of postings, the postings will be written to file and the list will be cleared.
    public void processPostings(List<Posting> postings) {
        for (Posting posting : postings) {
            postingBuffer.add(posting);

            if (postingBuffer.size() >= bufferSize) {
                // If buffer is full, sort postings and write to file
                Collections.sort(postingBuffer, (a, b) -> a.compareTo(b));
                stopWatch.stop();
                System.out.println("It took " + stopWatch.getTime(TimeUnit.SECONDS) + " seconds to collect " + bufferSize + " postings.");
                stopWatch = StopWatch.createStarted();
                writePostingsToFile();
                postingBuffer.clear();
            }
        }
    }

    public void writePostingsToFile() {
//        System.out.println("writing postings to file");
        // Create filePath for temporary file
        String timeStamp = Long.toString(System.currentTimeMillis());
        String filePath = TEMP_DIRECTORY + "/" + timeStamp;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        ObjectOutputStream out = null;

        try {
            StopWatch writeStopWatch = StopWatch.createStarted();
            //use buffered output stream to reduce I/O overhead
            fos = new FileOutputStream(filePath);
            bos = new BufferedOutputStream(fos);
            Kryo kryo = new Kryo();
            Output output = new Output(bos);
            int i = 0;

            for (Posting posting : postingBuffer) {
                posting.write(kryo,output);
//                System.out.println("posting written to " + filePath);
//                if (i == 1000000){
//                    stopWatch.stop();
//                }
            }
//            System.out.println("It takes " + stopWatch.getTime(TimeUnit.SECONDS) + " seconds to serialize 1,000,000 postings.");
            writeStopWatch.stop();
            System.out.println("It takes " + writeStopWatch.getTime(TimeUnit.SECONDS) + " seconds to serialize " + bufferSize + " postings.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close the streams
            try {
                bos.close();
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public List<Posting>getBuffer(){
        return postingBuffer;
    }

}
