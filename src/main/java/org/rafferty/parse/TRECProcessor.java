package org.rafferty.parse;


import org.rafferty.invertedindex.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.time.StopWatch;

/*
* This class processes our TREC file.  It loads pieces of the compressed file, decompresses, and passes individual
 * documents to DocumentParser object to be tokenized.
 */
public class TRECProcessor {
    private final String fileName;
    private DocumentParser parser;
    private IntermediatePostingGenerator generator;
    private FileMerger merger;

    public TRECProcessor(String fileName, long bufferSize) throws IOException {
        this.fileName = fileName;
        this.generator = new IntermediatePostingGenerator(bufferSize);
        parser = new DocumentParser(generator);
        try{
            merger = new FileMerger(this.generator);
        }catch(IOException e){
            e.printStackTrace();
        }
//        System.out.println("trec processor object created.");
//        fileChannel = fileChannel = AsynchronousFileChannel.open(
//                Paths.get(fileName), READ, WRITE, CREATE, DELETE_ON_CLOSE);
    }

    public void processFile(){
        FileInputStream fileStream = null;
        GZIPInputStream gzipStream = null;
        InputStreamReader reader = null;
        BufferedReader bufferedReader = null;

        try {
            fileStream = new FileInputStream(fileName);
            gzipStream = new GZIPInputStream(fileStream);
            reader = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
            bufferedReader = new BufferedReader(reader);

            boolean insideDocument = false;
            StringBuilder document = new StringBuilder();
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("<DOC>")) {
                    insideDocument = true;
                }

                if (insideDocument) {
                    document.append(line).append("\n");

                    if (line.startsWith("</DOC>")) {
                        insideDocument = false;
                        String decompressedDocument = document.toString();

                        List<Posting> postings = parser.parse(decompressedDocument);
                        document.setLength(0);

                        generator.processPostings(postings);

//                        if (parser.getTotalDocsParsed() == 100000) {
//                            return;
//                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (gzipStream != null) {
                    gzipStream.close();
                }
                if (fileStream != null) {
                    fileStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // If there are any postings remaining in the list, write to file.
        if (this.generator.getBuffer().size() > 0) {
            generator.writePostingsToFile();
        }
    }

    public static void main(String[] args) throws Exception {

        final long bufferSize = 10000000; //number of postings per file

        TRECProcessor processor = new TRECProcessor("../msmarco-docs.trec.gz", bufferSize);

        StopWatch stopWatch = StopWatch.createStarted();
//        System.out.println("Processing file.");
        processor.processFile();
        stopWatch.stop();
        //check how long it takes to process file
        System.out.println("It took " + stopWatch.getTime(TimeUnit.MINUTES) + " to process file and create temporary files.");

        //merge files
//        StopWatch mergerStopWatch = StopWatch.createStarted();
//        System.out.println("Entering merge.");
//        processor.merger.merge("../temp-files");
//        mergerStopWatch.stop();
//        System.out.println("It took " + mergerStopWatch.getTime(TimeUnit.SECONDS) + " to merge the temporary files into one sorted file.");

        //write lexicon and page table to file
        PageTable table = processor.parser.getPageTable();
        System.out.println("Writing page table to file.");
        table.write();
        Lexicon lexicon = processor.merger.getLexicon();
        System.out.println("Writing lexicon to file.");
        lexicon.write();

        //print final file size in mb
        long byteLength = processor.merger.getFileSize();
        long MEGABYTE = 1024L * 1024L;
        long megabyteLength = byteLength / MEGABYTE;
        System.out.println("Final inverted index size: " + megabyteLength);
    }
}


