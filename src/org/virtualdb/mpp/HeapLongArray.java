package org.virtualdb.mpp;

import java.util.ArrayList;

/**
 * 基于堆内存的固定长度数据项Array<br/>
 * 
 * @author Czp
 *
 */

public class HeapLongArray {

	private ArrayList<Long> bufs;

	public HeapLongArray(String swapPath) {
		bufs = new ArrayList<Long>();
	}

	public synchronized boolean add(long data) {
		bufs.add(data);
		return true;
	}

	public synchronized long get(int index) {
		return bufs.get(index);
	}

	public synchronized long set(int index, long data) {
		bufs.set(index, data);
		return data;
	}

	public synchronized int size() {
		return bufs.size();
	}

	public synchronized void release() {
		bufs.clear();
	}

}