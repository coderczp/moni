package org.virtualdb.mpp;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.virtualdb.mpp.BytesArray.CallBack;

/**
 * 
 * 基于文件的byteArray,作为memmapArray的后备
 * 
 * @author Czp
 */
public class FileBytesArray {

	private File tmpFile;
	private boolean hasWrite;
	private FileChannel channel;
	private ByteBuffer bufWrite;
	private RandomAccessFile fos;
	private static final int BUFF_SZIE = 1024 * 1024 * 10;

	public FileBytesArray(String swapFile) {
		try {
			File directory = new File(swapFile);
			if (!directory.exists())
				directory.mkdirs();
			tmpFile = File.createTempFile("file-", ".dat", directory);
			fos = new RandomAccessFile(tmpFile, "rw");
			bufWrite = ByteBuffer.allocate(BUFF_SZIE);
			channel = fos.getChannel();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized boolean add(byte[] data) {
		int len = data.length;
		int pos = bufWrite.position();
		int capacity = bufWrite.capacity();
		if (capacity > pos + len + 2) {
			bufWrite.putChar((char) len);
			bufWrite.put(data);
			return true;
		}
		try {
			bufWrite.flip();
			channel.write(bufWrite);
			bufWrite.clear();
			bufWrite.putChar((char) len);
			bufWrite.put(data);
			return true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void foreach(CallBack cb) {
		try {
			long size = channel.size();
			ByteBuffer data = null;
			ByteBuffer len = ByteBuffer.allocate(2);
			while (channel.position() < size) {
				channel.read(len);
				len.flip();
				char char1 = len.getChar();
				len.clear();
				data = ByteBuffer.allocate(char1);
				channel.read(data);
				data.flip();
				cb.onData(data.array());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void writeFinished() {
		try {
			if (hasWrite)
				return;
			bufWrite.flip();
			channel.write(bufWrite);
			channel.force(true);
			bufWrite.clear();
			channel.position(0);
			hasWrite = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		MemMapUtil.close(fos, channel);
		tmpFile.delete();
	}

}
