package org.virtualdb.mpp;

import java.io.File;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.virtualdb.mpp.MemMapUtil;

/**
 * 基于内存映射的自动增长byteArray<br/>
 * 只支持顺序添加和遍历,比MemMapBytesArray<br/>
 * 高效,也更节省空间。非HotSpotVM需<br/>
 * 要重写release函数释放文件句柄
 * 
 * @author Czp
 *
 */

public class MemMapBytesItor implements Iterable<byte[]> {

	private int size;
	private int curIndex;
	private int bufsIndex;
	private ThisItor itor;
	private boolean isClose;
	private File swapFilePath;
	private LinkedList<File> files;
	private MappedByteBuffer curBuf;
	private ArrayList<MappedByteBuffer> bufs;
	private LinkedList<RandomAccessFile> foss;
	private static final int INIT_SIZE = 8 * 50 * 1024 * 1024;

	public MemMapBytesItor(String swapPath) {
		itor = new ThisItor();
		files = new LinkedList<File>();
		bufs = new ArrayList<MappedByteBuffer>();
		foss = new LinkedList<RandomAccessFile>();
		createSwapPath(swapPath);
		createBuffer();
	}

	/**
	 * 添加数据,数据必须小于65535
	 * 
	 * @param data
	 * @return
	 */
	public synchronized boolean add(byte[] data) {
		MemMapUtil.checkStateAndLen(isClose, data.length);
		byte[] tmp = data;
		int len = tmp.length;
		int pos = curBuf.position();
		if (curBuf.capacity() >= len + pos) {
			curBuf.putChar((char) len);
			for (int i = 0; i < len; i++) {
				curBuf.put(tmp[i]);
			}
		} else {
			createBuffer();
			curBuf.putChar((char) len);
			for (int i = 0; i < len; i++) {
				curBuf.put(tmp[i]);
			}
		}
		size++;
		return true;
	}

	/**
	 * 获取数据的总数
	 * 
	 * @return
	 */
	public int size() {
		return size;
	}

	/**
	 * 使用完后必须调用此方法释放句柄
	 */
	public synchronized void release() {
		isClose = true;
		for (RandomAccessFile fos : foss) {
			MemMapUtil.close(fos);
		}
		for (MappedByteBuffer mbuf : bufs) {
			MemMapUtil.releaseMemory(mbuf);
		}
		for (File f : files) {
			if (!f.delete())
				System.out.println("release mapbuf fail");
		}
		foss.clear();
		bufs.clear();
		files.clear();
	}

	// @Override
	public synchronized Iterator<byte[]> iterator() {
		MemMapUtil.checkState(isClose);
		curIndex = 0;
		curBuf = bufs.get(bufsIndex++);
		curBuf.flip();
		return itor;
	}

	private void createSwapPath(String swapPath) {
		swapFilePath = new File(swapPath);
		if (!swapFilePath.exists())
			swapFilePath.mkdirs();
	}

	private void createBuffer() {
		File file = null;
		RandomAccessFile mapFos = null;
		try {
			file = File.createTempFile("mapxb-", ".dat", swapFilePath);
			mapFos = new RandomAccessFile(file, "rw");
			curBuf = mapFos.getChannel().map(MapMode.READ_WRITE, 0, INIT_SIZE);
			foss.add(mapFos);
			bufs.add(curBuf);
			files.add(file);
		} catch (Exception e) {
			MemMapUtil.close(mapFos);
			if (file != null)
				file.delete();
			throw new RuntimeException(e);
		}
	}

	private class ThisItor implements Iterator<byte[]> {

		// @Override
		public synchronized boolean hasNext() {
			boolean b = curIndex < size;
			if (b && !curBuf.hasRemaining())
				curBuf = bufs.get(bufsIndex++);
			return b;
		}

		// @Override
		public synchronized byte[] next() {
			MemMapUtil.checkState(isClose);
			int size = curBuf.getChar();
			byte[] data = new byte[size];
			curBuf.get(data);
			curIndex++;
			return data;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}