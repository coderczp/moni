package org.virtualdb.mpp;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

/**
 * 基于虚拟内存的固定长度数据项Array<br/>
 * 非HotSpotVM需要重写release函<br/>
 * 数释放文件句柄 <br/>
 * 
 * @author Czp
 *
 */

public class MemMapLongArray {

	private int size;
	private boolean isClose;
	private MappedByteBuffer curBuf;
	private LinkedList<File> mapFiles;
	private ArrayList<MappedByteBuffer> bufs;
	private LinkedList<RandomAccessFile> foss;

	/**
	 * 每次扩容的大小,必须是long字节数的倍数以及2的x次方
	 */
	private static final int INIT_SIZE = 8 * 16 * 1024 * 1024;

	// private static final int MOD_NUM = 3 + 4 + 10 + 10;

	public MemMapLongArray() {
		mapFiles = new LinkedList<File>();
		bufs = new ArrayList<MappedByteBuffer>();
		foss = new LinkedList<RandomAccessFile>();
		createBuffer();
	}

	public synchronized boolean add(long data) {
		int dataLen = 8;
		if (curBuf.capacity() - curBuf.position() == dataLen) {
			curBuf.putLong(data);
			createBuffer();
		} else {
			curBuf.putLong(data);
		}
		size++;
		return true;
	}

	public synchronized long get(int index) {
		MemMapUtil.checkRangeState(index, size, isClose);
		int pos = index * 8;// index << 3;
		int bufIndex = pos / INIT_SIZE;// pos >> MOD_NUM;
		MappedByteBuffer buf = bufs.get(bufIndex);
		int oldPos = buf.position();
		buf.position(pos % INIT_SIZE);// pos & ((1 << MOD_NUM) - 1)
		long data = buf.getLong();
		buf.position(oldPos);
		return data;
	}

	public synchronized long set(int index, long data) {
		MemMapUtil.checkRangeState(index, size, isClose);
		int pos = index * 8;
		int bufIndex = pos / INIT_SIZE;
		MappedByteBuffer buf = bufs.get(bufIndex);
		int oldPos = buf.position();
		buf.position(pos % INIT_SIZE);
		buf.putLong(data);
		buf.position(oldPos);
		return data;
	}

	public synchronized int size() {
		return size;
	}

	public synchronized void release() {
		for (RandomAccessFile fos : foss) {
			MemMapUtil.close(fos);
		}
		for (MappedByteBuffer mbuf : bufs) {
			MemMapUtil.releaseMemory(mbuf);
		}
		for (File f : mapFiles) {
			if (!f.delete())
				System.out.println("release mapbuf fail");
		}
		isClose = true;
		foss.clear();
		bufs.clear();
		mapFiles.clear();
	}

	private void createBuffer() {
		File tempFile = null;
		RandomAccessFile mapFos = null;
		try {
			tempFile = File.createTempFile("map-long-index-", ".dat");
			mapFos = new RandomAccessFile(tempFile, "rw");
			curBuf = mapFos.getChannel().map(MapMode.READ_WRITE, 0, INIT_SIZE);
			foss.add(mapFos);
			bufs.add(curBuf);
			mapFiles.add(tempFile);
		} catch (Exception e) {
			MemMapUtil.close(mapFos);
			if (tempFile != null)
				tempFile.delete();
			throw new RuntimeException(e);
		}
	}

}