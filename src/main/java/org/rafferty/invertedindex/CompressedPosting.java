package org.rafferty.invertedindex;

import java.io.IOException;
import java.io.Serializable;

/*
* posting object with var-byte encoded docID and frequency
*/
public class CompressedPosting implements Serializable {
    String term;
    byte[] docID;
    byte[] frequency;

    public CompressedPosting(Posting posting)throws IOException {
        docID = varbyteEncode(posting.getDocID());
        frequency = varbyteEncode(posting.getFrequency());
        this.term = posting.getTerm();
    }

    //returns decompressed posting object
//    public Posting decompress(){
//        int decodedDocID = varByteDecode(docID);
//        int decodedFrequency = varByteDecode(frequency);
//        Posting posting = new Posting(term, decodedDocID, decodedFrequency);
//        return posting;
//    }

    public long varByteDecode(byte[]data){
        long result = 0;
        long shift = 1;

        for(byte x: data){
            result += (x & 0x7F) * shift;
            if((x & 0x80) != 0){
                break;
            }
            shift<<=7;
            result += shift;
        }
        return result;
    }

    public byte[] varbyteEncode(int input){
        // first find out how many bytes we need to represent the integer
        int numBytes = ((32 - Integer.numberOfLeadingZeros(input)) + 6) / 7;
        // if the integer is 0, we still need 1 byte
        numBytes = numBytes > 0 ? numBytes : 1;
        byte[] output = new byte[numBytes];
        // for each byte of output ...
        for(int i = 0; i < numBytes; i++) {
            // ... take the least significant 7 bits of input and set the MSB to 1 ...
            output[i] = (byte) ((input & 0b1111111) | 0b10000000);
            // ... shift the input right by 7 places, discarding the 7 bits we just used
            input >>= 7;
        }
        // finally reset the MSB on the last byte
        output[0] &= 0b01111111;
        return output;
    }


}
