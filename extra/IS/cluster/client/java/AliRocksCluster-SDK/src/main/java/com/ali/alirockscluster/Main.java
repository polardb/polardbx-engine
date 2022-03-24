package com.ali.alirockscluster;


import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws Exception {

        MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("10.218.145.142:10002"));
        MemcachedClient memcachedClient = builder.build();

        try {
            memcachedClient.set("key", 0, "Hello World!");
            String value = memcachedClient.get("key");
            System.out.println("key值：" + value);
            memcachedClient.delete("key");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            memcachedClient.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
