package org.virtualdb.mpp.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class DataGenertor {

	public static int max = 10000000;

	public static void main(String[] args) throws Exception {
		Set<Integer> set = new HashSet<Integer>();
		Random rd = new Random();
		int bound = max * 2;
		while (set.size() < max) {
			set.add(rd.nextInt(bound));
		}
		String file = "d:/" + max + "-unsortdata.txt";
		FileWriter out = new FileWriter(file);
		BufferedWriter bos = new BufferedWriter(out);
		for (Integer integer : set) {
			String id = String.valueOf(integer);
			bos.write(id);
			bos.write('#');
			bos.write("name".concat(id));
			bos.write('\n');
		}
		bos.close();
		System.out.println("generate:" + max + " file:" + file);
	}

	public static void read(CallBack cb, int dataCount) {
		try {
			String file = "d:/" + dataCount + "-unsortdata.txt";
			FileReader in = new FileReader(file);
			BufferedReader bis = new BufferedReader(in);
			String line = null;
			while ((line = bis.readLine()) != null) {
				String[] ds = line.split("#");
				cb.onRead(new Data(ds[0], ds[1]));
			}
			bis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static interface CallBack {
		void onRead(Data dt);
	}
}
