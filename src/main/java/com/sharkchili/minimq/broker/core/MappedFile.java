package com.sharkchili.minimq.broker.core;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReflectUtil;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MappedFile {

    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    public MappedFile() {

    }

    public MappedFile(String fileName, int offset, int size) throws IOException {
        if (!FileUtil.exist(fileName)) {
            throw new FileNotFoundException(fileName);
        }

        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size);
    }


    public byte[] read(int offset, int size) throws IOException {
        byte[] bytes = new byte[size];
        this.mappedByteBuffer.position(offset);
        this.mappedByteBuffer.get(bytes, 0, size);
        return bytes;
    }


    public void write(byte[] bytes) throws IOException {
        write(bytes, false);
    }


    public void write(byte[] bytes, boolean flush) throws IOException {
        this.mappedByteBuffer.put(bytes);
        if (flush) {
            mappedByteBuffer.force();
        }
    }


    public void cleaner() {
        Cleaner cleaner = ReflectUtil.invoke(this.mappedByteBuffer, "cleaner");
        cleaner.clean();

    }


}
