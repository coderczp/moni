package org.virtualdb.mpp;

import java.io.File;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

/**
 * 基于内存映射的自动增长byteArray<br/>
 * 非HotSpotVM需要重写release函<br/>
 * 数释放文件句柄 <br/>
 * 
 * @author Czp
 *
 */

public class MemMapByteArray implements Iterable<byte[]> {

	private int size;
	private int curIndex;
	private int bufsIndex;
	private int bufsSize;
	private ThisItor itor;
	private boolean isClose;
	private LinkedList<File> files;
	private MemMapLongArray offsets;
	private MappedByteBuffer curBuf;
	private ArrayList<MappedByteBuffer> bufs;
	private LinkedList<RandomAccessFile> foss;
	private static final int INIT_SIZE = 150 * 1024 * 1024;

	public MemMapByteArray() {
		itor = new ThisItor();
		files = new LinkedList<File>();
		offsets = new MemMapLongArray();
		bufs = new ArrayList<MappedByteBuffer>();
		foss = new LinkedList<RandomAccessFile>();
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
		int dataLen = buf.getInt();
		byte[] data = new byte[dataLen];
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
			MemMapUtil.checkRangeState(k, size, isClose);
			MemMapUtil.checkRangeState(j, size, isClose);
			long kPos = offsets.get(k);
			long jPos = offsets.get(j);
			offsets.set(k, jPos);
			offsets.set(j, kPos);
		}

	}

	/**
	 * 添加数据
	 * 
	 * @param data
	 * @return
	 */
	public synchronized boolean add(byte[] data) {
		int curBufRemainSize = curBuf.capacity() - curBuf.position();
		if (curBufRemainSize >= data.length + 4) {
			offsets.add(pageAndLenEncode(bufsSize - 1, curBuf.position()));
			curBuf.putInt(data.length);
			curBuf.put(data);
		} else {
			// 当前buffer空间不足以存放data,扩容后放到下一个buffer
			createBuffer();
			offsets.add(pageAndLenEncode(bufsSize - 1, curBuf.position()));
			curBuf.putInt(data.length);
			curBuf.put(data);
		}
		size++;
		return true;
	}

	/**
	 * 
	 * @param page
	 * @param len
	 * @return
	 */
	private static long pageAndLenEncode(int page, int len) {
		ByteBuffer buf = ByteBuffer.allocate(8);
		buf.putInt(page);
		buf.putInt(len);
		buf.flip();
		return buf.getLong();
	}

	/**
	 * 解码长度和位置
	 * 
	 * @param len
	 * @return
	 */
	private static int[] pageAndLenDecode(long len) {
		ByteBuffer buf = ByteBuffer.allocate(8).putLong(len);
		buf.flip();
		return new int[] { buf.getInt(), buf.getInt() };
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

	@Override
	public synchronized Iterator<byte[]> iterator() {
		curIndex = 0;
		curBuf = bufs.get(bufsIndex++);
		curBuf.flip();
		return itor;
	}

	private void createBuffer() {
		File tempFile = null;
		RandomAccessFile mapFos = null;
		try {
			tempFile = File.createTempFile("map-arr-", ".dat");
			mapFos = new RandomAccessFile(tempFile, "rw");
			curBuf = mapFos.getChannel().map(MapMode.READ_WRITE, 0, INIT_SIZE);
			foss.add(mapFos);
			bufs.add(curBuf);
			files.add(tempFile);
			bufsSize++;
		} catch (Exception e) {
			MemMapUtil.close(mapFos);
			if (tempFile != null)
				tempFile.delete();
			throw new RuntimeException(e);
		}
	}

	private class ThisItor implements Iterator<byte[]> {

		@Override
		public synchronized boolean hasNext() {
			return curIndex < size;
		}

		@Override
		public synchronized byte[] next() {
			return get(curIndex++);
		}
	}

}