package org.virtualdb.mpp.test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.virtualdb.mpp.BytesArray;
import org.virtualdb.mpp.BytesArray.CallBack;
import org.virtualdb.mpp.MemMapSorter;
import org.virtualdb.mpp.MemMapBytesItor;
import org.virtualdb.mpp.MemMapLongArray;
import org.virtualdb.mpp.MemMapBytesArray;
import org.virtualdb.mpp.test.DataGenertor.ReadCallBack;

public class MemMapTest extends TestCase {

	private static String swapPath = "g:/swap";

	public void testAdd1billion() {
		int dataCount = 1000000000;
		long st = System.currentTimeMillis();
		MemMapLongArray arr = new MemMapLongArray(swapPath);
		try {
			while (dataCount-- > 0) {
				arr.add(dataCount);
			}
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time:" + time);
		} finally {
			arr.release();
		}
	}

	public void testBytesAdd10000wBArr() throws InterruptedException {
		final int threadCount = 5;
		final int dataCount = 150000000;
		long st = System.currentTimeMillis();
		final BytesArray arr = new BytesArray(1024 * 1024 * 100, swapPath);
		final CountDownLatch cd = new CountDownLatch(threadCount);
		try {
			for (int i = 0; i < threadCount; i++) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						int i = dataCount / threadCount;
						byte[] data = new byte[80];
						while (i-- > 0)
							arr.add(data);
						cd.countDown();
					}
				}).start();
			}
			cd.await();
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time--:" + time);
			arr.foreach(new CallBack() {

				@Override
				public void onData(byte[] data) {
					assertEquals(80, data.length);
				}
			});
		} finally {
			arr.release();
		}
	}

	public void testMemMapBytesItor() {
		final int dataCount = 10000;
		final MemMapBytesItor arr = new MemMapBytesItor(swapPath);
		try {
			for (int i = 0; i < dataCount; i++) {
				arr.add(String.valueOf(i).getBytes());
			}
			int i = 0;
			for (byte[] bs : arr) {
				String string = new String(bs);
				assertEquals(String.valueOf(i++), string);
			}
		} finally {
			arr.release();
		}
	}

	public void testBytesAdd10000wItor() throws InterruptedException {
		final int threadCount = 5;
		final int dataCount = 100000000;
		long st = System.currentTimeMillis();
		final MemMapBytesItor arr = new MemMapBytesItor(swapPath);
		final CountDownLatch cd = new CountDownLatch(threadCount);
		try {
			for (int i = 0; i < threadCount; i++) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						int i = dataCount / threadCount;
						byte[] data = new byte[100];
						while (i-- > 0)
							arr.add(data);
						cd.countDown();
					}
				}).start();
			}
			cd.await();
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time:" + time);
			for (byte[] bs : arr) {
				assertEquals(100, bs.length);
			}
		} finally {
			arr.release();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		final int dataCount = 100000000;
		long st = System.currentTimeMillis();
		final MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		final CountDownLatch cd = new CountDownLatch(5);
		try {
			for (int i = 0; i < 5; i++) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						byte[] data = new byte[50];
						int i = dataCount / 5;
						while (i-- > 0)
							arr.add(data);
						cd.countDown();
					}
				}).start();
			}
			cd.await();
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time:" + time);
		} finally {
			arr.release();
		}
	}

	public static void testBytesAdd5000W() throws InterruptedException {
		final int dataCount = 50000000;
		long st = System.currentTimeMillis();
		final MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		final CountDownLatch cd = new CountDownLatch(5);
		try {
			for (int i = 0; i < 5; i++) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						byte[] data = new byte[100];
						int i = dataCount / 5;
						while (i-- > 0)
							arr.add(data);
						cd.countDown();
					}
				}).start();
			}
			cd.await();
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time:" + time);
		} finally {
			arr.release();
		}
	}

	public void testPutAndGet() {
		MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		try {
			arr.add("test".getBytes());
			assertEquals("test", new String(arr.get(0)));
		} finally {
			arr.release();
		}
	}

	public void testMutilThread() throws InterruptedException {
		final MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		final CountDownLatch cd = new CountDownLatch(3);
		try {
			for (int i = 0; i < 3; i++) {
				new Thread(new Runnable() {

					public void run() {
						arr.add(Thread.currentThread().toString().getBytes());
						cd.countDown();
					}
				}).start();
			}
			cd.await();
			assertEquals(3, arr.size());
		} finally {
			arr.release();
		}
	}

	public void testSwap() {
		MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		try {
			arr.add("test1".getBytes());
			arr.add("test2".getBytes());
			arr.swap(0, 1);
			assertEquals("test2", new String(arr.get(0)));
			assertEquals("test1", new String(arr.get(1)));
		} finally {
			arr.release();
		}
	}

	public void testSortByteArr() {
		int dataCount = DataGenertor.generate(10000000);
		long st = System.currentTimeMillis();
		final MemMapBytesArray arr = new MemMapBytesArray(swapPath);
		try {
			DataGenertor.read(new ReadCallBack() {

				// @Override
				public void onRead(DBData dat) {
					arr.add(dat.toByte());
				}
			}, dataCount);
			double time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("add-time:" + time);
			st = System.currentTimeMillis();
			MemMapSorter.MERGE_SORTER.sort(arr, new DBDataCmp());
			time = (System.currentTimeMillis() - st) / 1000.0;
			System.out.println("sort-time:" + time);
			assertEquals(dataCount, arr.size());
			writeToFile(arr);
		} finally {
			arr.release();
		}
	}

	private void writeToFile(MemMapBytesArray arr) {
		try {
			int dataCount = arr.size();
			String file = "d:/" + dataCount + "-sortdata.txt";
			FileWriter out = new FileWriter(file);
			BufferedWriter bos = new BufferedWriter(out);
			for (byte[] bs : arr) {
				bos.write(new String(bs));
				bos.write('\n');
			}
			bos.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
