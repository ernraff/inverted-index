package org.rafferty.invertedindex;

import java.io.*;
import java.util.HashMap;

public class Lexicon {
    private HashMap<String, LexiconEntry> lexicon = new HashMap<>();
    private String compressedFilePath;
    private LexiconEntry previousEntry;

    public Lexicon(String compressedFilePath) {
        lexicon = new HashMap<>();
        this.compressedFilePath = compressedFilePath;
    }

    public void buildLexicon() {
        try {
            DataInputStream dataInput = new DataInputStream(new FileInputStream(compressedFilePath));
            long currentOffset = 0; // Track the byte position within the file
            LexiconEntry previousEntry = null;

            while (true) {
                try {
                    //get term
                    String term = readObject(dataInput).getTerm();

                    // Get the startOffset, which is the current byte position
                    long startOffset = currentOffset;

                    // decompress and deserialize the Posting object
                    Posting posting = (Posting) readObject(dataInput);

                    // Calculate the size of the serialized varbyte compressed posting
                    int compressedPostingSize = dataInput.readInt();

                    // Update the lexicon
                    if (lexicon.containsKey(term)) {
                        LexiconEntry entry = lexicon.get(term);
                        entry.incrementDocumentCount();
                        entry.setEndOffset(currentOffset);
                    } else {
                        LexiconEntry entry = new LexiconEntry();
                        entry.incrementDocumentCount();
                        entry.setStartOffset(startOffset); // Set startOffset
                        entry.setEndOffset(currentOffset);
                        lexicon.put(term, entry);
                    }

                    // Update the endOffset for the previous entry
                    if (previousEntry != null) {
                        previousEntry.setEndOffset(currentOffset);
                    }

                    // Update the current offset for the next posting
                    currentOffset += compressedPostingSize;
                    previousEntry = lexicon.get(term); // Update the previous entry
                } catch (EOFException e) {
                    break;
                }
            }

            // Update endOffset for the last entry if necessary
            if (previousEntry != null) {
                previousEntry.setEndOffset(currentOffset);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Posting readObject(DataInputStream dataInput) throws IOException, ClassNotFoundException {
        int objectLength = dataInput.readInt();
        byte[] objectBytes = new byte[objectLength];
        dataInput.readFully(objectBytes);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(objectBytes))) {
            return (Posting) ois.readObject();
        }
    }

    public static void main(String[] args) {
        String filePath = "your_varbyte_compressed_file.bin"; // Replace with your file path
        Lexicon lexicon = new Lexicon(filePath);
        lexicon.buildLexicon();

        // Access the built lexicon
        HashMap<String, LexiconEntry> lexiconMap = lexicon.lexicon;
        for (String term : lexiconMap.keySet()) {
            LexiconEntry entry = lexiconMap.get(term);
            System.out.println("Term: " + term + ", DocCount: " + entry.getDocumentCount() +
                    ", StartOffset: " + entry.getStartOffset() + ", EndOffset: " + entry.getEndOffset());
        }
    }
}


