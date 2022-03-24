/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.ali.alirockscluster;

import com.taobao.tair.etc.TairUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * data entry object, includes key, value and meta infomations
 */
@SuppressWarnings("unused")
public class DataEntry extends DataEntryAbstract<Object> implements Serializable {

	private static final long serialVersionUID = -3492001385938512871L;
	private static byte[] DEFAULT_DATA = new byte[29];
	static {
		Arrays.fill(DEFAULT_DATA, (byte)0);
	}
	private Object key;

	// meta data

	private int magic;
	private int checkSum;
	private int keySize;
	private int version;
	private int padSize;
	private int valueSize;
	private int flag;
	private int cdate;
	private int mdate;
	private int edate;

	// meta flag
	public static final int TAIR_ITEM_FLAG_ADDCOUNT = 1;
	public static final int TAIR_ITEM_LOCKED = 8;

	public boolean isLocked() {
		return (flag & TAIR_ITEM_LOCKED) != 0;
	}

	public int getExpriedDate() {
		return edate;
	}
	
	public int getCreateDate() {
		return cdate;
	}
	
	public int getModifyDate() {
		return mdate;
	}

	public DataEntry() {
	}

	public DataEntry(Object value) {
		this.value = value;
	}

	public DataEntry(Object key, Object value) {
		this.key = key;
		this.value = value;
	}

	public DataEntry(Object key, Object value, int version) {
		this.key = key;
		this.value = value;
		this.version = version;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public void decodeMeta(ByteBuffer bytes) {
		magic = bytes.getShort();
		checkSum = bytes.getShort();
		keySize = bytes.getShort();
		version = bytes.getShort();
		padSize = bytes.getInt();		
		valueSize = bytes.getInt();
		flag = bytes.get();
		cdate = bytes.getInt();
		mdate = bytes.getInt();
		edate = bytes.getInt();
	}

	public static void encodeMeta(ByteBuffer bytes) {
		bytes.put(DEFAULT_DATA);
	}

	public static void encodeMeta(ByteBuffer bytes, int flag) {
		bytes.put(DEFAULT_DATA);
		if (flag != 0) {
			// put flag implicitly
			bytes.putInt(bytes.position() - 16, flag);
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("key: ").append(key);
		sb.append(", value: ").append(value);
		sb.append(", version: ").append(version).append("\n\t");
		sb.append("cdate: ").append(TairUtil.formatDate(cdate)).append("\n\t");
		sb.append("mdate: ").append(TairUtil.formatDate(mdate)).append("\n\t");
		sb.append("edate: ").append(edate > 0 ? TairUtil.formatDate(edate) : "NEVER").append("\n");
		return sb.toString();
	}

}
