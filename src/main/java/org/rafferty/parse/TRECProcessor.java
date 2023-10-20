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

    private static void decompress(InputStream compressedStream, File outputFile) throws IOException {
        try (GZIPInputStream gzipStream = new GZIPInputStream(compressedStream);
             FileOutputStream fileStream = new FileOutputStream(outputFile)) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipStream.read(buffer)) != -1) {
                fileStream.write(buffer, 0, len);
            }
        }
    }

    public String readDataFromFile(File file) throws IOException{
        try{
            //read text data from file
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            StringBuilder sb = new StringBuilder();
            //create byte array of same length as file
//            byte[] arr = new byte[(int)file.length()];
            while((line = reader.readLine()) != null){
                sb.append(line);
            }
            //close FileInputStream object to avoid memory leakage
            reader.close();
            return sb.toString();
        }catch(IOException e){
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        TRECProcessor processor = new TRECProcessor("../msmarco-docs.trec.gz", 100);
        processor.processFile();
    }
}


