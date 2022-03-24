package com.ali.alirockscluster;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.impl.DefaultTairManager;
import com.taobao.tair.impl.mc.MultiClusterExtendTairManager;
import com.taobao.tair.impl.mc.MultiClusterTairManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TairTest {

    private static int NAMESPACE = 0;
    static String netdevice;

    //private MultiClusterTairManager tairManager;
    private TairManager tairManager;
    enum storageType {
        mdb,
        ldb,
        rdb
    }
    private storageType type;
    private int testCount;
    boolean looping = false;

    public TairTest(String engine, String configKey, String strTestCount, String isUsername) {
//        if (engine.equals("rdb")) {
//            tairManager = new MultiClusterExtendTairManager();
//        } else {
//            tairManager = new MultiClusterTairManager();
//        }
        tairManager = new DefaultTairManager();
//        if (isUsername.equals("1")) {
//            tairManager.setUserName(configKey);
//        } else {
//            tairManager.setConfigID(configKey);
//        }
//        tairManager.setDynamicConfig(true);
        tairManager.setTimeout(1000);

        if(netdevice != null) {
            //tairManager.setNetDeviceName(netdevice);
        }

        try {
            tairManager.init();
        } catch (Exception ex) {
            System.out.println("Tair client init error");
            ex.printStackTrace();
        }

        if (engine.equals("ldb")) {
            type = storageType.ldb;
        } else if (engine.equals("mdb")) {
            type = storageType.mdb;
        } else if (engine.equals("rdb")) {
            type = storageType.rdb;
        }
        testCount = Integer.valueOf(strTestCount);
        if (testCount <= 0) {
            looping = true;
            testCount = 1;
        }
    }

    public void runTest() {
        testPut();
        testGet();
        testDelete();
        if (type == storageType.mdb) {
            testInvalid();
        }
        if (type != storageType.rdb) {
            testMget();
            testMdelete();
            testPrefixPutGetDelete();
            testPrefixPuts();
            if (type == storageType.ldb) {
                testGetRange();
            }
            testPrefixDeletes();
        }
    }

    private void testPut() {

        String key_prefix = "key_";
        String value_prefix = "value_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i);
            String value = value_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.put(NAMESPACE, key, value, 0, 60);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Put Test done, aver time: " + averTime + " ms.");
    }

    private void testGet() {

        String key_prefix = "key_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i);
            Result<DataEntry> dataEntryResult = tairManager.get(NAMESPACE, key);
            if (!dataEntryResult.isSuccess() || dataEntryResult.getRc().getCode() != 0) {
                System.out.println(dataEntryResult.getRc().toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Get Test done, aver time: " + averTime + " ms.");
    }

    private void testDelete() {
        String key_prefix = "key_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.delete(NAMESPACE, key);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Delete Test done, aver time: " + averTime + " ms.");
    }

    private void testInvalid() {
        String key_prefix = "key_";
        String value_prefix = "value_";

        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i);
            String value = value_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.put(NAMESPACE, key, value, 0, 60);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.invalid(NAMESPACE, key);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Invalid Test done, aver time: " + averTime + " ms.");
    }

    private void testMget() {
        String key_prefix = "key_";
        String value_prefix = "value_";

        List<Object> keys = new ArrayList<Object>(100);
        for (int i = 0; i < 100; ++i) {
            String key = key_prefix + Integer.toString(i);
            String value = value_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.put(NAMESPACE, key, value, 0, 60);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
            keys.add(key);
        }

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            Result<List<DataEntry>> result = tairManager.mget(NAMESPACE, keys);
            if (!result.isSuccess() || ResultCode.SUCCESS != result.getRc()
                    || result.getValue().size() != keys.size()) {
                System.out.println(result.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Mget Test done, keys.length = 100, aver time: " + averTime + " ms.");
    }

    private void testMdelete() {
        String key_prefix = "key_";

        List<Object> keys = new ArrayList<Object>(100);
        for (int i = 0; i < 100; ++i) {
            String key = key_prefix + Integer.toString(i);
            keys.add(key);
        }

        double startTime = System.currentTimeMillis();
        ResultCode resultCode = tairManager.mdelete(NAMESPACE, keys);
        if (!resultCode.isSuccess()) {
            System.out.println(resultCode.toString());
        }
        double endTime = System.currentTimeMillis();
        double averTime = endTime - startTime;
        System.out.println("MDeleteTest done, keys.length = 100, aver time: " + averTime + " ms.");
    }

    private void testPrefixPutGetDelete() {
        String pkey_prefix = "pkey_";
        String skey_prefix = "skey_";
        String value_prefix = "value_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            String skey = skey_prefix + Integer.toString(i);
            String value = value_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.prefixPut(NAMESPACE, pkey, skey, value, 0, 60);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            String skey = skey_prefix + Integer.toString(i);
            Result<DataEntry> dataEntryResult = tairManager.prefixGet(NAMESPACE, pkey, skey);
            if (!dataEntryResult.isSuccess() || dataEntryResult.getRc().getCode() != 0) {
                System.out.println(dataEntryResult.getRc().toString());
                return;
            }
        }
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            String skey = skey_prefix + Integer.toString(i);
            ResultCode resultCode = tairManager.prefixDelete(NAMESPACE, pkey, skey);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / (testCount * 3);
        System.out.println("Prefix Test done, aver time: " + averTime + " ms.");
    }

    private void testPrefixPuts() {
        String pkey_prefix = "pkey_";
        String skey_prefix = "skey_";
        String value_prefix = "value_";
        List<KeyValuePack> keyValuePackList = new ArrayList<KeyValuePack>(100);
        for (int i = 0; i < 100; ++i) {
            String skey = skey_prefix + Integer.toString(i);
            String value = value_prefix + Integer.toString(i);
            KeyValuePack keyValuePack = new KeyValuePack(skey, value, (short)0, 60);
            keyValuePackList.add(keyValuePack);
        }

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            Result<Map<Object, ResultCode>> mapResult = tairManager.prefixPuts(NAMESPACE, pkey, keyValuePackList);
            if (mapResult.getRc().getCode() != 0) {
                System.out.println(mapResult.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("PrefixPuts Test done, value.length = 100, aver time: " + averTime + " ms.");
    }

    private void testGetRange() {
        String pkey_prefix = "pkey_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            Result<List<DataEntry>> listResult = tairManager.getRange(NAMESPACE, pkey, null, null, 0, 0);
            if (!listResult.isSuccess() || ResultCode.SUCCESS != listResult.getRc()
                    || listResult.getValue().size() != 100) {
                System.out.println(listResult.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("GetRange Test done, aver time: " + averTime + " ms.");
    }

    private void testPrefixDeletes() {
        String pkey_prefix = "pkey_";
        String skey_prefix = "skey_";
        List<String> skeys = new ArrayList<String>(100);
        for (int i = 0; i < 100; ++i) {
            String skey = skey_prefix + Integer.toString(i);
            skeys.add(skey);
        }

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String pkey = pkey_prefix + Integer.toString(i);
            Result<Map<Object, ResultCode>> mapResult = tairManager.prefixDeletes(NAMESPACE, pkey, skeys);
            if (mapResult.getRc().getCode() != 0) {
                System.out.println(mapResult.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("PrefixDeletes Test done, value.length = 100, aver time: " + averTime + " ms.");
    }

    public static void main(String[] args) {
        if (args.length < 5 || (!args[0].equals("mdb") && !args[0].equals("ldb") && !args[0].equals("rdb"))
                || (!args[3].equals("1") && !args[3].equals("0"))) {
            System.out.println("Usage: \n" +
                    "\t java -jar TairTest.jar engine(mdb or ldb or rdb) DiamondDataID/username testCount isUsername(1 or 0) namespace netdevice\n");
            return;
        }
        NAMESPACE = Integer.valueOf(args[4]);
        if (args.length > 5) {
            netdevice = args[5];
        }

        TairTest tairTest = new TairTest(args[0], args[1], args[2], args[3]);
        System.out.println("Simple Test Start.");
        do {
            tairTest.runTest();
        } while (tairTest.looping);
        System.out.println("Simple Test Done.");

        System.exit(0);
    }
}
