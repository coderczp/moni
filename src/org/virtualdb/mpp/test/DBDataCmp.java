package org.virtualdb.mpp.test;

import java.util.Comparator;

final class DBDataCmp implements Comparator<byte[]> {
    // @Override
    public int compare(byte[] o1, byte[] o2) {
        return DBData.toObj(o1).compareTo(DBData.toObj(o2));
    }
}