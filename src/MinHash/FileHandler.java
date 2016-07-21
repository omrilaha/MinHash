package MinHash;

import MinHash.Utils.GeneralUtils;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileHandler {

    private String filename;
    private final int DEFAULT_BUFFER_SIZE = 1024 * 15;
    private int fileOffset;
    private int windowOffset;
    private int length;
    private int windowSize;
    private int stepSize;
    private int printSize;
    private boolean init;

    private InputStream fileInputStream;
    private byte[] currentChunk;
    private byte[] nextChunk;
    private long fileRead;

    private long start = 0;
    private long end = 0;

    public FileHandler(String filename, int windowSize, int stepSize) throws IOException {
        this.filename = filename;
        this.fileOffset = 0;
        this.windowOffset = 0;
        this.windowSize = windowSize;
        this.stepSize = stepSize;
        this.printSize = 0;
        this.fileRead = 0;
        this.start = 0;

        this.currentChunk = null;
        this.nextChunk = null;
        this.init = false;
        this.fileInputStream = new BufferedInputStream(new FileInputStream(this.filename));
    }

    public byte[] ReadNextWindow() {
        try {
            if (!this.init) {
                this.currentChunk = read(this.fileOffset, this.windowSize);
                this.fileOffset += this.windowSize;
                this.nextChunk = read(this.fileOffset, this.windowSize);
            }

            if (this.currentChunk != null && this.nextChunk != null) {
                if (this.windowOffset == 0) {
                    this.windowOffset = this.stepSize;
                    return this.currentChunk;
                } else if (this.windowOffset > 0 && this.windowOffset < this.windowSize) {
                    byte[] firstChunk = Arrays.copyOfRange(this.currentChunk, this.windowOffset, this.windowSize);
                    byte[] lastChunk = Arrays.copyOfRange(this.nextChunk, 0, this.windowOffset);
                    this.windowOffset += this.stepSize;
                    return GeneralUtils.combine(firstChunk, lastChunk);
                } else if (this.windowOffset == this.windowSize) {
                    this.fileOffset += this.windowSize;
                    this.currentChunk = this.nextChunk;
                    this.nextChunk = read(this.fileOffset, this.windowSize);
                    this.windowOffset = this.stepSize;
                    //this.printSize += this.windowSize;

//                    double mod = this.fileOffset % (64^2);
//                    if (mod == 0)
                        //System.out.println("File Offset: " + this.fileOffset);

//                    if (this.printSize > 10000) {
//                        System.out.println("File Offset: " + this.fileOffset);
//                        this.printSize = 0;
//                    }
                    return this.currentChunk;
                }
            }

            return null;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

//    public byte[] readNextChunk() {
//        byte[] chunk = read(this.offset, this.length);
//        this.offset += this.length;
//        return chunk;
//    }

    private byte[] read(int from, int length) {
        byte[] buffer = new byte[length];
        try {

            int read = this.fileInputStream.read(buffer, 0, length);
            //int read = this.fileInputStream.read(chunk);
            //int read = new FileInputStream(this.filename).read(chunk, 0, length);

            this.printSize += read;
            this.fileRead += read;
            if (this.fileRead > 1000000) {
//                this.end = System.currentTimeMillis();
//                System.out.println("time:" + (this.end - this.start));
//                this.start = System.currentTimeMillis();
                System.out.println(this.filename + " position: " + (this.printSize/1000000) + "MB");
                this.fileRead = 0;
            }
            if (read > 0) {
                return buffer;
            } else {
                return null;
            }
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
            return null;
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Index out of bounds");
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        try {
            if (this.fileInputStream != null) {
                this.fileInputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
