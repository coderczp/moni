package org.virtualdb.mpp.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.virtualdb.mpp.MemMapBytesArray;
import org.virtualdb.mpp.MemMapLongArray;
import org.virtualdb.mpp.MemMapSorter;
import org.virtualdb.mpp.test.DataGenertor.CallBack;

import junit.framework.TestCase;

public class MemMapTest extends TestCase {

    private static String swapPath = "g:/swap";

    public void testAdd10billion() {
        int dataCount = 1000000000;
        long st = System.currentTimeMillis();
        MemMapLongArray arr = new MemMapLongArray(swapPath);
        try {
            for (int i = 0; i < dataCount; i++) {
                arr.add(i);
            }
            double time = (System.currentTimeMillis() - st) / 1000.0;
            System.out.println("add-time:" + time);
        } finally {
            arr.release();
        }
    }
    public void testBytesAdd10billion() {
        int dataCount = 100000000;
        long st = System.currentTimeMillis();
        MemMapBytesArray arr = new MemMapBytesArray(swapPath);
        try {
            byte[] data = new byte[128/2];
            for (int i = 0; i < dataCount; i++) {
                arr.add(data);
            }
            double time = (System.currentTimeMillis() - st) / 1000.0;
            System.out.println("add-time:" + time);
        } finally {
            arr.release();
        }
    }
    public void testAndAndGet() {
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
            for (byte[] bs : arr) {
                System.out.println(new String(bs));
            }
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
            DataGenertor.read(new CallBack() {

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
