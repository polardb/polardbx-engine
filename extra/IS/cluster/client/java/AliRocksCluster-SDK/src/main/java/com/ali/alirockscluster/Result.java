/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.ali.alirockscluster;

import java.util.Collection;

/**
 * Result object return by tair server
 */
public class Result<V> {
	private ResultCode rc;
	private V value;

	public Result(ResultCode rc) {
		this.rc = rc;
	}

	public Result(ResultCode rc, V value) {
		this.rc = rc;
		this.value = value;
	}

	/**
	 * whether the request is success.
	 * <p>
	 * if the target is not exist, this method return true. 
	 */
	public boolean isSuccess() {
		return rc.isSuccess();
	}

	public V getValue() {
		return this.value;
	}

	/**
	 * @return the result code of this request
	 */
	public ResultCode getRc() {
		return rc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Result: [").append(rc.toString()).append("]\n");
		if(value != null) {
			if(value instanceof DataEntry) {
				sb.append("\t").append(value.toString()).append("\n");
			} else if (value instanceof Collection) {
				Collection<DataEntry> des = (Collection<DataEntry>) value;
				sb.append("\tentry size: ").append(des.size()).append("\n");
				for (DataEntry de : des) {
					sb.append("\t").append(de.toString()).append("\n");
				}
			} else {
				sb.append("\tvalue: ").append(value);
			}
		}
		return sb.toString();
	}
}
