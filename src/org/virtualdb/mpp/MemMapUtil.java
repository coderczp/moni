package org.virtualdb.mpp;

import java.io.Closeable;
import java.nio.MappedByteBuffer;

public class MemMapUtil {

	/**
	 * 检测是否数组越界<br>
	 * 映射是否已经关闭<br>
	 * 
	 */
	public static void checkRangeState(int index, int size, boolean isClose) {
		if (isClose) {
			throw new RuntimeException("memory map is destory");
		}
		if (index >= size) {
			String format = "index over flow size:%s,index:%s";
			String msg = String.format(format, size, index);
			throw new RuntimeException(msg);
		}

	}

	/**
	 * 关闭资源
	 * 
	 * @param ios
	 */
	public static void close(Closeable... ios) {
		for (Closeable closeable : ios) {
			try {
				if (closeable != null)
					closeable.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void releaseMemory(MappedByteBuffer mbuf) {
		((sun.nio.ch.DirectBuffer) mbuf).cleaner().clean();
	}

}
