package org.virtualdb.mpp;

import java.io.File;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.virtualdb.mpp.MemMapUtil;
import org.virtualdb.mpp.MemMapLongArray;

/**
 * 基于内存映射的自动增长byteArray<br/>
 * 非HotSpotVM需要重写release函<br/>
 * 数释放文件句柄 <br/>
 * 
 * @author Czp
 *
 */

public class MemMapBytesArray implements Iterable<byte[]> {

	private int size;
	private int curIndex;
	private int bufsSize;
	private int bufsIndex;
	private ThisItor itor;
	private boolean isClose;
	private File swapFilePath;
	private LinkedList<File> files;
	private HeapLongArray offsets;
	private MappedByteBuffer curBuf;
	private ArrayList<MappedByteBuffer> bufs;
	private LinkedList<RandomAccessFile> foss;
	private static final int INIT_SIZE = Integer.MAX_VALUE;

	public MemMapBytesArray(String swapPath) {
		itor = new ThisItor();
		files = new LinkedList<File>();
		bufs = new ArrayList<MappedByteBuffer>();
		foss = new LinkedList<RandomAccessFile>();
		createOffsetAndSwapPath(swapPath);
		createBuffer();
	}

	public synchronized byte[] get(int index) {
		MemMapUtil.checkRangeState(index, size, isClose);
		long curDataOffset = offsets.get(index);
		int[] pageAndOffset = pageAndLenDecode(curDataOffset);
		int pageIndex = pageAndOffset[0];
		int dataPosOffset = pageAndOffset[1];
		MappedByteBuffer buf = bufs.get(pageIndex);
		int curPos = buf.position();
		buf.position(dataPosOffset);
		short dataLen = buf.getShort();
		int len = (dataLen << 16) >>> 16;
		byte[] data = new byte[len];
		buf.get(data);
		buf.position(curPos);
		return data;

	}

	/**
	 * 交换数据,主要用于排序,实际上只是改变数据的指针
	 * 
	 * @param k
	 * @param j
	 */
	public synchronized void swap(int k, int j) {
		if (k != j) {
			MemMapUtil.checkRangeState(k, j, size, isClose);
			long kPos = offsets.get(k);
			long jPos = offsets.get(j);
			offsets.set(k, jPos);
			offsets.set(j, kPos);
		}

	}

	/**
	 * 添加数据,如果数据长度大于65535,数据将被截断
	 * 
	 * @param data
	 * @return
	 */
	public synchronized boolean add(byte[] data) {
		int len = data.length;
		if (len > 65535)
			throw new RuntimeException("length must be less than 65535");
		int pos = curBuf.position();
		int curBufRemainSize = curBuf.capacity() - pos;
		if (curBufRemainSize >= len + 2) {
			offsets.add(pageAndLenEncode(bufsSize - 1, pos));
			curBuf.putShort((short) len);
			curBuf.put(data);
		} else {
			createBuffer();
			offsets.add(pageAndLenEncode(bufsSize - 1, 0));
			curBuf.putShort((short) len);
			curBuf.put(data);
		}
		size++;
		return true;
	}

	/**
	 * 编码长度和位置
	 * 
	 * @param page
	 * @param len
	 * @return
	 */
	private final static long pageAndLenEncode(int page, int len) {
		long codeLen = len;
		codeLen <<= 32;
		codeLen |= page;
		return codeLen;
	}

	/**
	 * 解码长度和位置
	 * 
	 * @param len
	 * @return
	 */
	private final static int[] pageAndLenDecode(long len) {
		int page = (int) len;
		int realLen = (int) (len >> 32);
		return new int[] { page, realLen };
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
		offsets.release();
		isClose = true;
		foss.clear();
		bufs.clear();
		files.clear();
	}

	// @Override
	public synchronized Iterator<byte[]> iterator() {
		curIndex = 0;
		curBuf = bufs.get(bufsIndex++);
		curBuf.flip();
		return itor;
	}

	private void createOffsetAndSwapPath(String swapPath) {
		swapFilePath = new File(swapPath);
		if (!swapFilePath.exists())
			swapFilePath.mkdirs();
		offsets = new HeapLongArray(swapPath);
	}

	private void createBuffer() {
		File file = null;
		RandomAccessFile mapFos = null;
		try {
			file = File.createTempFile("mapb-", ".dat", swapFilePath);
			mapFos = new RandomAccessFile(file, "rw");
			curBuf = mapFos.getChannel().map(MapMode.READ_WRITE, 0, INIT_SIZE);
			foss.add(mapFos);
			bufs.add(curBuf);
			files.add(file);
			bufsSize++;
		} catch (Exception e) {
			MemMapUtil.close(mapFos);
			if (file != null)
				file.delete();
			if (offsets != null)
				offsets.release();
			throw new RuntimeException(e);
		}
	}

	private class ThisItor implements Iterator<byte[]> {

		// @Override
		public synchronized boolean hasNext() {
			return curIndex < size;
		}

		// @Override
		public synchronized byte[] next() {
			return get(curIndex++);
		}

		public void remove() {
			throw new RuntimeException("not supported");
		}
	}

}