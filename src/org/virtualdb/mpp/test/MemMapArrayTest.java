package org.virtualdb.mpp.test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.io.BufferedWriter;

import org.virtualdb.mpp.MemMapSorter;
import org.virtualdb.mpp.MemMapByteArray;
import org.virtualdb.mpp.test.DataGenertor.CallBack;

public class MemMapArrayTest {

	static Comparator<byte[]> cmp = new Comparator<byte[]>() {

		@Override
		public int compare(byte[] o1, byte[] o2) {
			return Data.toObj(o1).compareTo(Data.toObj(o2));
		}
	};

	public static void main(String[] args) throws IOException {
		int dataCount = DataGenertor.max;
		long st = System.currentTimeMillis();
		final MemMapByteArray arr = new MemMapByteArray();
		DataGenertor.read(new CallBack() {

			@Override
			public void onRead(Data dat) {
				arr.add(dat.toByte());
			}
		}, dataCount);
		arr.swap(14, 2);//408.176
		double time = (System.currentTimeMillis() - st) / 1000.0;
		System.out.println("add-time:" + time);
		st = System.currentTimeMillis();
		MemMapSorter.MERGE_SORTER.sort(arr, cmp);
		time = (System.currentTimeMillis() - st) / 1000.0;
		System.out.println("sort-time:" + time);
		writeToFile(dataCount, arr);
		arr.release();
		System.out.println("over");
	}

	private static void writeToFile(int dataCount, MemMapByteArray arr)
			throws IOException {
		String file = "d:/" + dataCount + "-sortdata.txt";
		FileWriter out = new FileWriter(file);
		BufferedWriter bos = new BufferedWriter(out);
		for (int i = 0; i < dataCount; i++) {
			bos.write(new String(arr.get(i)));
			bos.write('\n');
		}
		bos.close();
	}
}
