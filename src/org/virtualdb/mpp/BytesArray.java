package org.virtualdb.mpp;

public class BytesArray {

	private long size;
	private long dataBytes;
	private MemArray memArray;
	private MemMapBytesItor mapBytesItor;
	private FileBytesArray fileBytesArray;

	private static final long MAX_BYTES = 1024 * 1024 * 1024 * 20l;

	public BytesArray(int memorySize, String swapDir) {
		memArray = new MemArray(memorySize);
		mapBytesItor = new MemMapBytesItor(swapDir);
		fileBytesArray = new FileBytesArray(swapDir);
	}

	public synchronized boolean add(byte[] data) {
		if (memArray.add(data))
			return true;
		if (dataBytes < MAX_BYTES) {
			mapBytesItor.add(data);
			dataBytes += data.length;
		} else {
			fileBytesArray.add(data);
		}
		size++;
		return true;
	}

	public synchronized long size() {
		return size;
	}

	public synchronized void foreach(CallBack cb) {
		memArray.foreach(cb);
		for (byte[] bs : mapBytesItor) {
			cb.onData(bs);
		}
		fileBytesArray.writeFinished();
		fileBytesArray.foreach(cb);
	}

	public static interface CallBack {
		void onData(byte[] data);
	}

	public void release() {
		mapBytesItor.release();
		fileBytesArray.close();
	}
}
