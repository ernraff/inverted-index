package org.rafferty.parse;

import org.rafferty.invertedindex.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/*
* This class processes our TREC file.  It loads pieces of the compressed file, decompresses, and passes individual
 * documents to DocumentParser object to be tokenized.
 */
public class TRECProcessor {
    private final String fileName;
    private DocumentParser parser;
    private IntermediatePostingGenerator generator;
    private FileMerger merger;

    public TRECProcessor(String fileName, int bufferSize){
        this.fileName = fileName;
        this.generator = new IntermediatePostingGenerator(bufferSize);
        parser = new DocumentParser(generator);
        try{
            merger = new FileMerger(this.generator);
        }catch(IOException e){
            e.printStackTrace();
        }
//        System.out.println("trec processor object created.");
    }

    public void processFile() throws IOException {
        try (FileInputStream fileStream = new FileInputStream(fileName);
             GZIPInputStream gzipStream = new GZIPInputStream(fileStream);
             InputStreamReader reader = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

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
                        parser.parse(decompressedDocument);
                        document.setLength(0);
                        System.out.println("Document parsed and sent to DocumentParser.");
                    }
                }
            }
            System.out.println("File parsed successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //get start time
        long startTime = System.currentTimeMillis();
        TRECProcessor processor = new TRECProcessor("../msmarco-docs.trec.gz", 100);
        processor.processFile();

        //write lexicon and page table to file
        PageTable table = processor.parser.getPageTable();
        table.write();
        Lexicon lexicon = processor.merger.getLexicon();
        lexicon.write();

        //print total running time
        long endTime = System.currentTimeMillis();
        long milliseconds = endTime - startTime;
        long seconds = milliseconds / 1000;
        long minutes = (seconds / 60) % 60;
        long hours = (seconds / (60 * 60)) % 24;
        System.out.println("Total running time: " + String.format("%d:%02d:%02d",hours,minutes,seconds));
        //print final file size in mb
        long byteLength = processor.merger.getFileSize();
        long MEGABYTE = 1024L * 1024L;
        long megabyteLength = byteLength / MEGABYTE;
        System.out.println("Final inverted index size: " + megabyteLength);
    }
}


