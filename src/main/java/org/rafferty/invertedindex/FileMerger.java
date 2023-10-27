package org.rafferty.invertedindex;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/*
* Merge k sorted files.  Track necessary metadata.  Var-byte encode document IDs and frequencies.  Write to file, concurrently building lexicon.
*/
public class FileMerger {
    private static String mergedFilename = "../final-index.bin";
    private IntermediatePostingGenerator generator;
    private Lexicon lexicon;
    private int blockSize = 64; // number of postings per block
    private List<Integer> docIds;
    private List<Integer> frequencies;
    private List<byte[]> encodedIds;
    private List<byte[]> encodedFrequencies;
    private List<Integer> lastDocIDs;
    private List<Integer> docIdBlockSizes;
    private List<Integer> frequencyBlockSizes;
    private int numPostingsLastBlock = 0;
    private int numOfBlocks = 0;
    private String currentTerm;
    private File file;
    private FileOutputStream fos;

    private BufferedOutputStream bos;
//    private ObjectOutputStream oos;
    private FileChannel channel;

    private long fileSize = 0L;

    private static int outPutBufferSize = 20000000;
    private static int inPutBufferSize = 5000000;
    private Kryo kryo;
    private Output output;

    public FileMerger(IntermediatePostingGenerator generator) throws IOException {
        this.generator = generator;
        //initialize all lists
        docIds = new ArrayList<>();
        frequencies = new ArrayList<>();
        encodedIds = new ArrayList<>();
        encodedFrequencies = new ArrayList<>();
        lastDocIDs = new ArrayList<>();
        docIdBlockSizes = new ArrayList<>();
        frequencyBlockSizes = new ArrayList<>();
        lexicon = new Lexicon();
        file = new File(mergedFilename);
        fos = new FileOutputStream(file);
        bos = new BufferedOutputStream(fos, outPutBufferSize);
//        oos = new ObjectOutputStream(bos);
        channel = fos.getChannel();
        kryo = new Kryo();
        output = new Output(bos);

    }

    public void merge(String inputDirectory){
        //get list of files in directory
        List<String> temporaryFiles = Stream.of(new File(inputDirectory).listFiles())
                                                .filter(file -> !file.isDirectory())
                                                .map(File::getPath)
                                                .collect(Collectors.toList());

        List<FileInputStream> fstreams = new ArrayList<>();
        List<BufferedInputStream> bufferedStreams = new ArrayList<>();
//        List<ObjectInputStream> ostreams = new ArrayList<>();
//        List<Kryo> kryos = new ArrayList<>();
        List<Input> inputs = new ArrayList<>();
        Kryo kryo = new Kryo();

        //initialize minHeap using overridden posting comparator method
        PriorityQueue<PostingSortHelper> heap = new PriorityQueue<>((a, b) -> a.getPosting().compareTo(b.getPosting()));
        boolean allFilesEmpty = false;

        for(String file: temporaryFiles){
            try{
                //create input streams for each file
                FileInputStream fis = new FileInputStream(file);
                fstreams.add(fis);
                BufferedInputStream bufferedStream = new BufferedInputStream(fis, inPutBufferSize);
                bufferedStreams.add(bufferedStream);
//                ostreams.add(new ObjectInputStream(bufferedStream));
//                kryos.add(new Kryo());
                inputs.add(new Input(bufferedStream));
            }catch(IOException e){
                e.printStackTrace();
            }
        }

        try {
            //iterate through sorted files, get first object from each
            for (Input input : inputs) {
                try {
//                            System.out.println("entering for loop!");
                    //deserialize the posting and add to heap
                    int index = inputs.indexOf(input);

                    Posting p = new Posting();
                    p.read(kryo, input);

//                    Posting p = (Posting) ostream.readObject();
//                            System.out.println("got posting!");
                    heap.offer(new PostingSortHelper(p, index));
//                            System.out.println("added to heap!");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            while (!heap.isEmpty()) {
                PostingSortHelper ps = heap.poll();
                if (!ps.getPosting().getTerm().equals(currentTerm)) {

                    //initialize LexiconEntry
                    LexiconEntry entry = new LexiconEntry();
                    //get the number of postings in our last block
                    numPostingsLastBlock = docIds.size();
                    //set document count  and number of blocks in lexicon entry
                    int documentCount = blockSize * (numOfBlocks - 1) + numPostingsLastBlock;
                    entry.setDocumentCount(documentCount);
                    entry.setNumOfBlocks(numOfBlocks);
                    //write our chunk to file
                    writeChunkToFile(entry);
                    //clear all of our lists for the next chunk (next inverted list)
                    resetChunk();
                    //set currentTerm to next term
                    this.currentTerm = ps.getPosting().getTerm();
                    //add entry to lexicon
                    lexicon.put(currentTerm, entry);
                }
                if (docIds.size() < blockSize) {
                    docIds.add(ps.getPosting().getDocID());
                    frequencies.add(ps.getPosting().getFrequency());
                } else {
                    //we are at block size
                    //store the last block id in each block
                    numOfBlocks++;
                    lastDocIDs.add(docIds.get(docIds.size() - 1));
                    for (int docId : docIds) {
                        byte[] encodedId = varByteEncode(docId);
                        encodedIds.add(encodedId);
                        docIdBlockSizes.add(encodedId.length);
                    }
                    for (int frequency : frequencies) {
                        byte[] encodedFrequency = varByteEncode(frequency);
                        encodedFrequencies.add(encodedFrequency);
                        frequencyBlockSizes.add(encodedFrequency.length);
                    }
                }

                int index = ps.getIndex();
//                ObjectInputStream ostream = ostreams.get(index);
                Input input = inputs.get(index);
                try {
//                    Posting p = (Posting) ostream.readObject();
//                            System.out.println("got posting!");
                    Posting p = new Posting();
                    p.read(kryo, input);
                    heap.offer(new PostingSortHelper(p, index));
//                            System.out.println("added to heap!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        fileSize = file.length();

        //close streams;
//        for(ObjectInputStream ostream: ostreams){
//            try{
//                ostream.close();
//            }catch (IOException e){
//                e.printStackTrace();
//            }
//        }

        for(FileInputStream fstream: fstreams){
            try{
                fstream.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        try{
            closeOutputStreams();
        }catch(IOException e){
            e.printStackTrace();
        }

        try{
            deleteTemporaryFiles(inputDirectory);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    //use variable length byte encoding to compress document IDs and frequencies
    public byte[] varByteEncode(int data){
        //find out how many bytes we need to represent the integer
        int numBytes = ((32 - Integer.numberOfLeadingZeros(data)) + 6) / 7;
        numBytes = numBytes > 0 ? numBytes : 1;
        byte[] output = new byte[numBytes];
        //for each byte of output
        for(int i = 0; i < numBytes; i++){
            //take least significant 7 bits of input and set MSB to 1
            output[i] = (byte) ((data & 0b1111111) | 0b10000000);
            //shift the input right by 7 places, discarding the 7 bits we just used.
            data >>= 7;
        }
        //reset the MSB on teh last byte
        output[0] &= 0b01111111;
        return output;
    }

    //write chunk to file, update lexicon entry with start and end offsets for inverted list
    public void writeChunkToFile(LexiconEntry entry){
        try{
            //add start position of list to lexicon entry
            entry.setStartOffset(channel.position());
            //write metadata to file
//            oos.writeObject(lastDocIDs);
//            oos.writeObject(docIdBlockSizes);
//            oos.writeObject(frequencyBlockSizes);
            kryo.writeObject(output, lastDocIDs);
            kryo.writeObject(output, docIdBlockSizes);
            kryo.writeObject(output, frequencyBlockSizes);
            //write compressed document IDs and frequencies to file.
            int i = 0;
            int j = 0;
            while(!encodedIds.isEmpty() && !encodedIds.isEmpty()) {
                //write
                for (int n = 0; n < blockSize; n++) {
                    if (i < encodedIds.size()) {
//                        oos.write(encodedIds.get(i));
                        kryo.writeObject(output, encodedIds.get(i));
                        i++;
                    }
                }
                for (int n = 0; n < blockSize; n++) {
                    if (j < encodedFrequencies.size()) {
//                        oos.write(encodedFrequencies.get(j));
                        kryo.writeObject(output, encodedFrequencies.get(j));
                        j++;
                    }
                }
                System.out.println("Chunk written to file.");
            }
            //add end position of list to lexicon entry
            entry.setEndOffset(channel.position());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    //clears all chunk data
    public void resetChunk(){
        docIds.clear();
        encodedIds.clear();
        frequencies.clear();
        encodedFrequencies.clear();
        lastDocIDs.clear();
        docIdBlockSizes.clear();
        frequencyBlockSizes.clear();
        numPostingsLastBlock = 0;
        numOfBlocks = 0;
    }

    //closes all output streams
    public void closeOutputStreams() throws IOException{
//        oos.close();
        bos.close();
        channel.close();
        fos.close();
    }

    //deletes temporary files
    public void deleteTemporaryFiles(String directory) throws IOException {
        Path dir = Paths.get(directory); //path to the directory
        Files
                .walk(dir) // Traverse the file tree in depth-first order
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
//                        System.out.println("Deleting: " + path);
                        Files.delete(path);  //delete each file or directory
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    //get final size of file
    public long getFileSize(){
        return fileSize;
    }

    public Lexicon getLexicon(){
        return lexicon;
    }
}
