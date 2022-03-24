/**
 * (C) 2007-2010 Taobao Inc.
 * <p/>
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.ali.alirockscluster.impl;

import com.ali.alirockscluster.AliRocksClusterSDK;
import com.ali.alirockscluster.DataEntry;
import com.ali.alirockscluster.Result;
import com.ali.alirockscluster.ResultCode;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Vector;

public class ClusterSDKImpl implements AliRocksClusterSDK {
    private static final Log log = LogFactory.getLog(AliRocksClusterSDK.class);
    private String hostList;
    private Vector<MemcachedClient> clientList = new Vector<MemcachedClient>();
    private Vector<String> hostNameList = new Vector<String>();
    private int leaderIndex;

    public ClusterSDKImpl(String hostList) {
        this.hostList = hostList;
        leaderIndex = -1;
    }

    public void init() throws Exception {
        String[] hosts = hostList.split(",");

        for (String host : hosts) {
            MemcachedClientBuilder builder =
                    new XMemcachedClientBuilder(AddrUtil.getAddresses(host));
            hostNameList.add(host);
            builder.setConnectionPoolSize(5);
            MemcachedClient memcachedClient = builder.build();
            clientList.add(memcachedClient);
        }
    }

    public Result<DataEntry> get(int namespace, Serializable key) {
        DataEntry resultObject;
        ResultCode rc = ResultCode.SUCCESS;
        if (leaderIndex != -1)
        {
            try {
                MemcachedClient client = clientList.get(leaderIndex);
                String value = client.get((String) key);
                resultObject = new DataEntry(value);
                return new Result<DataEntry>(rc, resultObject);
            } catch (Exception e) {
                log.warn("get " + key + " failed! Leader is offline");
            }
        }

        for (int i = 0; i < hostList.length(); i++) {
            try {
                MemcachedClient client = clientList.get(i);
                String value = client.get((String) key);
                resultObject = new DataEntry(value);
                leaderIndex = i;
                return new Result<DataEntry>(rc, resultObject);
            } catch (Exception e) {
                log.warn("get " + key + " failed! host "
                        + hostNameList.get(i) + " is offline");
            }
        }

        return new Result<DataEntry>(ResultCode.CONNERROR);
    }

    public ResultCode put(int namespace, Serializable key, Serializable value) {
        return put(namespace, key, value, 0, 0);
    }

    public ResultCode put(int namespace, Serializable key, Serializable value, int version) {
        return put(namespace, key, value, version, 0);
    }

    public ResultCode put(int namespace, Serializable key, Serializable value,
                          int version, int expireTime) {
        if (expireTime < 0){
            return ResultCode.INVALIDARG;
        }
        ResultCode rc = ResultCode.CONNERROR;

        if (leaderIndex != -1)
        {
            try {
                MemcachedClient client = clientList.get(leaderIndex);
                boolean success = client.set((String) key, expireTime, value);
                if (success) {
                    return ResultCode.SUCCESS;
                }
            } catch (Exception e) {
                log.warn("get " + key + " failed! Leader is offline");
            }
        }

        for (int i = 0; i < hostList.length(); i++) {
            try {
                MemcachedClient client = clientList.get(i);
                boolean success = client.set((String) key, expireTime, value);
                if (success) {
                    leaderIndex = i;
                    return ResultCode.SUCCESS;
                }
            } catch (Exception e) {
                log.warn("put " + key + " failed! host "
                        + hostNameList.get(i) + " is offline");
            }
        }

        return rc;
    }

    public ResultCode delete(int namespace, Serializable key) {
        if (key == null) {
            return ResultCode.SERIALIZEERROR;
        }
        ResultCode rc = ResultCode.CONNERROR;

        if (leaderIndex != -1)
        {
            try {
                MemcachedClient client = clientList.get(leaderIndex);
                boolean success = client.delete((String) key);
                if (success) {
                    return ResultCode.SUCCESS;
                }
            } catch (Exception e) {
                log.warn("get " + key + " failed! Leader is offline");
            }
        }

        for (int i = 0; i < hostList.length(); i++) {
            try {
                MemcachedClient client = clientList.get(i);
                boolean success = client.delete((String) key);
                if (success) {
                    leaderIndex = i;
                    return ResultCode.SUCCESS;
                }
            } catch (Exception e) {
                log.warn("delete " + key + " failed! host "
                        + hostNameList.get(i) + " is offline");
            }
        }

        return rc;
    }

    /**
     * 将key对应的数据加上value，如果key对应的数据不存在，则新增，并将值设置为defaultValue
     * 如果key对应的数据不是int型，则返回失败
     *
     * @param namespace    数据所在的namspace
     * @param key          数据的key
     * @param value        要加的值
     * @param defaultValue 不存在时的默认值
     * @return 更新后的值
     */
    public Result<Integer> incr(int namespace, Serializable key, int value,
                                int defaultValue, int expireTime) {
        return null;
    }
}
