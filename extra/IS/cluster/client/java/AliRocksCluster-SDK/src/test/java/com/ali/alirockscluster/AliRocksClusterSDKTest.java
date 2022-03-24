package com.ali.alirockscluster;

import com.ali.alirockscluster.impl.ClusterSDKImpl;

public class AliRocksClusterSDKTest {
    private int testCount;
    boolean looping = false;
    private AliRocksClusterSDK sdk;

    public AliRocksClusterSDKTest(String hostList, String strTestCount) {
        sdk = new ClusterSDKImpl(hostList);
        try {
            sdk.init();
        } catch (Exception ex) {
            System.out.println("AliRocksClusterSDK init error");
            ex.printStackTrace();
        }

        testCount = Integer.valueOf(strTestCount);
        if (testCount <= 0) {
            looping = true;
            testCount = 1;
        }
    }


    public void runTest() {
        //testPut();
        testGet();
        testDelete();
    }

//    private void testPut() {
//
//        String key_prefix = "key_";
//        String value_prefix = "value_";
//
//        double startTime = System.currentTimeMillis();
//        for (int i = 0; i < testCount; ++i) {
//            String key = key_prefix + Integer.toString(i);
//            String value = value_prefix + Integer.toString(i);
//            com.taobao.tair.ResultCode resultCode = sdk.put(NAMESPACE, key, value, 0, 60);
//            if (!resultCode.isSuccess()) {
//                System.out.println(resultCode.toString());
//                return;
//            }
//        }
//        double endTime = System.currentTimeMillis();
//        double averTime = (endTime - startTime) / testCount;
//        System.out.println("Put Test done, aver time: " + averTime + " ms.");
//    }

    private void testGet() {

        String key_prefix = "key_";

        double startTime = System.currentTimeMillis();
        for (int i = 0; i < testCount; ++i) {
            String key = key_prefix + Integer.toString(i + 1);
            Result<DataEntry> dataEntryResult = sdk.get(0, key);
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
            String key = key_prefix + Integer.toString(i + 1);
            ResultCode resultCode = sdk.delete(0, key);
            if (!resultCode.isSuccess()) {
                System.out.println(resultCode.toString());
                return;
            }
        }
        double endTime = System.currentTimeMillis();
        double averTime = (endTime - startTime) / testCount;
        System.out.println("Delete Test done, aver time: " + averTime + " ms.");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: \n" +
                    "\t java -jar ClusterSDKTest.jar hostNameList testCount\n");
            return;
        }

        AliRocksClusterSDKTest clusterTest = new AliRocksClusterSDKTest(args[0], args[1]);
        System.out.println("Simple Test Start.");
        do {
            clusterTest.runTest();
        } while (clusterTest.looping);
        System.out.println("Simple Test Done.");

        System.exit(0);
    }
}
