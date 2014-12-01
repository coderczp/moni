package org.virtualdb.mpp.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class DataGenertor {

    public static void read(CallBack cb, int dataCount) {
        try {
            FileReader in = new FileReader("d:/" + dataCount + "-unsortdata.txt");
            BufferedReader bis = new BufferedReader(in);
            String line = null;
            while ((line = bis.readLine()) != null) {
                String[] ds = line.split("#");
                cb.onRead(new DBData(ds[0], ds[1]));
            }
            bis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static interface CallBack {
        void onRead(DBData dt);
    }

    public static int generate(int dataCount) {
        try {
            String file = "d:/" + dataCount + "-unsortdata.txt";
            File f = new File(file);
            if (f.exists())
                return dataCount;
            Set<Integer> set = new HashSet<Integer>();
            Random rd = new Random();
            int bound = dataCount * 2;
            while (set.size() < dataCount) {
                set.add(rd.nextInt(bound));
            }
            FileWriter out = new FileWriter(f);
            BufferedWriter bos = new BufferedWriter(out);
            for (Integer integer : set) {
                String id = String.valueOf(integer);
                bos.write(id);
                bos.write('#');
                bos.write("name".concat(id));
                bos.write('\n');
            }
            bos.close();
            System.out.println("generate:" + dataCount + " file:" + file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dataCount;
    }
}
