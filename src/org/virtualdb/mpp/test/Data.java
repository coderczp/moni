package org.virtualdb.mpp.test;

class Data implements Comparable<Data> {
	String id;
	String name;

	public Data(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public byte[] toByte() {
		return id.concat("#").concat(name).getBytes();
	}

	public static Data toObj(byte[] data) {
		try {
			String string = new String(data);
			String[] line = string.split("#");
			return new Data(line[0], line[1]);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int compareTo(Data paramT) {
		Integer id2 = Integer.valueOf(paramT.id);
		Integer id1 = Integer.valueOf(id);
		return id1.compareTo(id2);
	}
}