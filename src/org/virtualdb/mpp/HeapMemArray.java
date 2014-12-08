package org.virtualdb.mpp;

import java.nio.ByteBuffer;

import org.virtualdb.mpp.BytesArray.CallBack;

/***
 * 针对大内存设备优化的类,根据用户设置内存大小存储数据 <br>
 * 为了不影响同进程的java程序,申请直接内存
 * 
 * @author czp
 *
 */
public class HeapMemArray {

	private ByteBuffer buf;

	public HeapMemArray(int memorySize) {
		this.buf = ByteBuffer.allocateDirect(memorySize);
	}

	public synchronized boolean add(byte[] data) {
		char len = (char) data.length;
		int remain = buf.capacity() - buf.position();
		if (remain > len + 2) {
			buf.putChar(len);
			buf.put(data);
			return true;
		}
		return false;
	}

	public synchronized void foreach(CallBack cb) {
		buf.flip();
		char len = 0;
		byte[] data = null;
		while (buf.remaining() > 2) {
			len = buf.getChar();
			if (buf.remaining() < len)
				break;
			data = new byte[len];
			buf.get(data);
			cb.onData(data);
		}
	}
}
