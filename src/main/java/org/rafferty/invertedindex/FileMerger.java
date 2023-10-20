package org.rafferty.invertedindex;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
* Merges k sorted files.  Begin by adding first element of each file to a min heap, writing heap contents to output file, so on
* until we have a complete merged file.  Delete temp files.
*/
public class FileMerger {

    private IntermediatePostingGenerator generator;
    int postingsPerFile;


    public FileMerger(IntermediatePostingGenerator generator) {
        this.generator = generator;
        postingsPerFile = this.generator.getBufferSize(); //gets number of posting objects per file.
    }

    public void merge(String inputDirectory){
        //get list of files in directory
        List<String> temporaryFiles = Stream.of(new File(inputDirectory).listFiles())
                                                .filter(file -> !file.isDirectory())
                                                .map(File::getPath)
                                                .collect(Collectors.toList());

        List<FileInputStream> fstreams = new ArrayList<>();
        List<ObjectInputStream> ostreams = new ArrayList<>();


        //initialize minHeap using overridden posting comparator method
        PriorityQueue<PostingSorter> heap = new PriorityQueue<>((a,b) -> a.getPosting().compareTo(b.getPosting()));
        boolean allFilesEmpty = false;

        //iterate through files and create input stream
        for(String file: temporaryFiles){
            try{
                //create input streams for each file
                FileInputStream fis = new FileInputStream(file);
                fstreams.add(fis);
                ostreams.add(new ObjectInputStream(fis));
            }catch(IOException e){
                e.printStackTrace();
            }
        }

                try {
                    FileOutputStream fos = new FileOutputStream("../merged-index-4.bin");
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    //iterate through sorted files, get first object from each
                    for (ObjectInputStream ostream : ostreams) {
                        try {
//                            System.out.println("entering for loop!");
                            //deserialize the posting and add to heap
                            int index = ostreams.indexOf(ostream);

                            Posting p = (Posting) ostream.readObject();
//                            System.out.println("got posting!");
                            heap.offer(new PostingSorter(p, index));
//                            System.out.println("added to heap!");

                        } catch (EOFException e) {
                            e.printStackTrace();
                        }
                    }

                    while (!heap.isEmpty()) {
                        PostingSorter ps = heap.poll();
//                        CompressedPosting compressedPosting = new CompressedPosting(ps.getPosting());
                        oos.writeObject(ps.getPosting());
                        int index = ps.getIndex();
                        ObjectInputStream ostream = ostreams.get(index);
                        try {
                            Posting p = (Posting) ostream.readObject();
//                            System.out.println("got posting!");
                            heap.offer(new PostingSorter(p, index));
//                            System.out.println("added to heap!");
                        }catch (EOFException e){
                            e.printStackTrace();
                        }

                    }

//                    for(String file: temporaryFiles){
//                        int i = temporaryFiles.indexOf(file);
//                        try{
//                            ostreams.get(i).readObject();
//                            System.out.println("still some objects here!");
//                        }catch (EOFException e){
//                        }
//                    }

                    oos.close();
                }catch(IOException | ClassNotFoundException e){
                    e.printStackTrace();
                }


        //close streams
        for(ObjectInputStream ostream: ostreams){
            try{
                ostream.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        for(FileInputStream fstream: fstreams){
            try{
                fstream.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        //delete temporary files
//        for(String filePath: temporaryFiles){
//            File file = new File(filePath);
//            if(file.delete()){
//                System.out.println("Temporary file successfully deleted.");
//            }else{
//                System.out.println("Failed to delete temporary file.");
//            }
//        }

    }

}
