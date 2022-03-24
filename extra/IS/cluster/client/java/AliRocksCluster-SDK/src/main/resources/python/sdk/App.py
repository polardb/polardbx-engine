
from CloudDBA_SDK_Core import RequestInfoDO
from CloudDBA_SDK_Core import ResponseInfoDO
from CloudDBA_SDK_Core import SDKConfigDO
from CloudDBA_SDK_Core import DriverManager
from CloudDBA_SDK_Core import DataSourceManager
import threading
import thread
import time

class CallBackSDK(threading.Thread):
    def __init__(self,client,threadID):
        threading.Thread.__init__(self)
        self.client = client;
        self.threadID = threadID;
        self.resTimes = 0;

    def run(self):
        count = 0;
        while count < 1:
            count = count + 1;
            print ("app thread doCommand time is : %s" %str(count))
            requestInfo = RequestInfoDO("local","yuyue51",171419, "42.121.38.217","3306","42.121.38.217","3306",100, "mysql");
            requestInfo.setCmd("TIMEDTUNING");
            try:
                responseInfo = self.client.doCommand(requestInfo);
            except Exception,error:
                pass
            if not responseInfo is None:
                self.resTimes = self.resTimes + 1; 
                print responseInfo.toString();
            else:
                print "get responseInfo is None";
            time.sleep(10);
        print 'threadID:' + str(self.threadID) + ',callTimes:' + str(self.resTimes) + '\n';
        thread.exit();
      
        

def main():
    sdkConfig = SDKConfigDO("127.0.0.1",8108,"DMS");
    #print sdkConfig.toString();
    client = DriverManager.getInstance(sdkConfig).getCloudDBAClient();
 
    count = 0;
    threads = [];
 
    while count < 2:
        count = count + 1;
        t = CallBackSDK(client,count);
        threads.append(t);
        t.start();
 
    resTimes = 0;
    for t in threads:
        t.join();
        resTimes = resTimes + t.resTimes
        print "trehadID:" + str(t.threadID) + " restimes:" + str(resTimes) ;
 
     
    print DataSourceManager.toString();
    print "-------------------------";
    DriverManager.release();


if __name__ == '__main__':
    main()
