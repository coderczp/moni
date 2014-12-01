package org.virtualdb.mpp.test;

class DBData implements Comparable<DBData> {
    String id;
    String name;

    public DBData(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte[] toByte() {
        return id.concat("#").concat(name).getBytes();
    }

    public static DBData toObj(byte[] data) {
        try {
            int k = 0;
            while (k < data.length)
                if (data[k++] == '#')
                    break;
            String idTmp = new String(data, 0, k - 1);
            String nameTmp = new String(data, k, data.length - k);
            return new DBData(idTmp, nameTmp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // @Override
    public int compareTo(DBData paramT) {
        Integer id2 = Integer.valueOf(paramT.id);
        Integer id1 = Integer.valueOf(id);
        return id1.compareTo(id2);
    }
}