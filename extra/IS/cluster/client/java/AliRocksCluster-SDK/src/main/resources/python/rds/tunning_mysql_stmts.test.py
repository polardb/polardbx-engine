#!env python
#coding: utf-8

import time
from rds.domain.lib.master_dbutils import do_select_fetchall
from rds.base.lib import log
from rds.stat.schedule.jobworker import Worker
from rds.base.const import TIMED_TUNING
from rds.base.lib.thread_pool import TIMEINTERNAL_FETCH_TASK
from CloudDBA_SDK_Core import RequestInfoDO
from CloudDBA_SDK_Core import ResponseInfoDO
from CloudDBA_SDK_Core import SDKConfigDO
from CloudDBA_SDK_Core import DriverManager
import Queue
import thread
import threading
import datetime



CREATOR = 901
LOCAL_IP = "127.0.0.1"
LOCAL_PORT = 8108
APP_KEY = "RDS_MONITOR_DS"
TUNNING_CMD = "TIMEDTUNING";
TRHEAD_NUMBER = 5;
LOOP_NUMBER = 500;


class CallSDKThread(threading.Thread):
    def __init__(self, tunningSqlStmts, threadID):
        threading.Thread.__init__(self)
        self.tunningIns = 0;
        self.threadID = APP_KEY + " thread " + str(threadID) + " ";
        self.tunningSqlStmts = tunningSqlStmts;

    def run(self):
        while self.tunningSqlStmts.custInstances.qsize() > 0 :
            self.tunning(self.tunningSqlStmts.custInstances);
            
        while self.tunningSqlStmts.readonlyInstances.qsize() > 0 :
            self.tunning(self.tunningSqlStmts.readonlyInstances);
        
        log.info ('%s tunnings %d instances successfully !' % (self.threadID, self.tunningIns));
        thread.exit();
                   
                
        
    def tunning(self, instanceQueue):
        try:
            request = instanceQueue.get();
            if request == None:
                return;
            responseInfo = self.tunningSqlStmts.client.doCommand(request);
            if not responseInfo is None:
                self.tunningIns = self.tunningIns + 1; 
                log.info ('%s tunnings %d instances' % (self.threadID, self.tunningIns));
            else:
                log.error("%s get responseInfo is None , requestInstName is %s" % (self.threadID, request.getCustInsName()));
        except Queue.Empty, e:
                pass;
        except Exception, e :
            log.error('Exception: %s' % e, exc_info=True)
        

class TunningSqlStmts(Worker):
    lock = threading.Lock()
    
    def __init__(self):
        self.custInstances = None;
        self.readonlyInstances = None;
        self.appThreads = None;
        
    def process(self, job_target):
        """
            Get all instances according to the cluster, Put them into worker
            thread pool. These daemon threads will work until the queue is empty.
        """
        try:
            self.custInstances = Queue.Queue();
            self.readonlyInstances = Queue.Queue(); 
            self.appThreads = [];
        
            cluster = job_target
            log.info("Begin to check ins of cluster:%s..." % cluster)
            self.get_instances(cluster)
            #self.get_readonly_instances(cluster)
            instanceNum = self.custInstances.qsize();
            threadNum = TRHEAD_NUMBER
                
            """
            init sdk  and init threadNum appThread call doCommand
            """
            sdkConfig = SDKConfigDO(LOCAL_IP,LOCAL_PORT,APP_KEY);
            sdkConfig.concurrentConnections = threadNum;
            self.client = DriverManager.getInstance(sdkConfig).getCloudDBAClient();
            for threadID in range(0, threadNum):
                appThread = CallSDKThread(self, threadID);
                self.appThreads.append(appThread);
                appThread.start();
            
            for appThread in self.appThreads:
                appThread.join();
           
            log.info("Finish to check ins of cluster:%s..." % cluster)
        except Exception, e:
            log.error(e, exc_info=True)
        
        finally:
            DriverManager.release();
            
        
    def get_instances(self, cluster):
        for index in range(0,LOOP_NUMBER):
            request = RequestInfoDO('TBC_MYSQL03', 'rmtestmysql03001', 123971,'10.118.136.210','3001','10.118.136.217','3001',100,'mysql');
            request.setCmd(TUNNING_CMD);
            self.custInstances.put_nowait(request);
            
            request = RequestInfoDO('TBC_MYSQL03', 'rmtestmysql03002', 123973,'10.118.136.210','3005','10.118.136.217','3005',100,'mysql');
            request.setCmd(TUNNING_CMD);
            self.custInstances.put_nowait(request);
            
            request = RequestInfoDO('TBC_MYSQL03', 'rmtestmysql03003', 123975,'10.118.136.210','3006','10.118.136.217','3006',100,'mysql');
            request.setCmd(TUNNING_CMD);
            self.custInstances.put_nowait(request);
            
            request = RequestInfoDO('TBC_MYSQL03', 'rmtestmysql03004', 123977,'10.118.136.210','3007','10.118.136.217','3007',100,'mysql');
            request.setCmd(TUNNING_CMD);
            self.custInstances.put_nowait(request);
            
            request = RequestInfoDO('TBC_MYSQL03', 'rmtestmysql03005', 123979,'10.118.136.210','3008','10.118.136.217','3008',100,'mysql');
            request.setCmd(TUNNING_CMD);
            self.custInstances.put_nowait(request);
            
        log.info('Ins number:%s' % self.custInstances.qsize())
        return self.custInstances


    def get_readonly_instances(self, cluster):
        """
            Get readonly instance info from metadb according to cluster.
        """
        sql = r'''select ci.ins_name, ci.id, h.ip, i.port, ci.disk_size from cust_instance ci, instance i, 
            custins_hostins_rel chr, hostinfo h, instance_stat stat 
            where  ci.type = 'x' and 
            ci.is_tmp = 0 and  
            ci.cluster_name = '%s' and 
            ci.id = chr.custins_id and 
            chr.hostins_id = i.id and 
            ci.db_type = 'mysql' and 
            h.is_deleted = 0 and 
            h.host_name != '' and 
            stat.ins_id=i.id and 
            h.id = i.host_id and 
            (ci.is_deleted = 0 and ci.status in (1, 6, 7) and i.is_deleted = 0 and ci.ins_type = 3 and stat.role = 0) ;''' % cluster    
        db_type = "mysql"                       
        result_set = do_select_fetchall(sql)
                
        for ins_name, id, ip, port, disk_size in result_set:
            master_ip = ip
            master_port = port
            sql_for_readonly_ins = ''' select h.ip, i.port from 
                cust_instance ci, instance i, custins_hostins_rel chr, hostinfo h, instance_stat stat 
                where ci.primary_custins_id = chr.custins_id and 
                chr.hostins_id = i.id and 
                stat.ins_id=i.id and 
                h.id = i.host_id and 
                stat.role=1 and ci.ins_name = '%s' limit 1;''' % ins_name
            result_for_readonly_ins = do_select_fetchall(sql_for_readonly_ins)
            if not result_for_readonly_ins:
                log.error("Miss slave ip and port for readonly instance:%s" % ins_name)
                continue
            slave_ip = result_for_readonly_ins[0][0]
            slave_port = result_for_readonly_ins[0][1]
            log.info("ReadOnly Instance slave_ip:%s, slave_port:%s" % (slave_ip, slave_port))
            
            request = RequestInfoDO(cluster, ins_name, id, master_ip, master_port, slave_ip, slave_port, disk_size, db_type);
            request.setCmd(TUNNING_CMD);
            self.readonlyInstances.put_nowait(request);
        
        log.info('ReadOnly Instance number:%s' % self.readonlyInstances.qsize())
        return self.readonlyInstances

    def split_str(self, string, delimiter, begin_end = " "):  
        string = string.strip(begin_end)  
        a = string.find(delimiter)  
        first_word = string[:a]  
        result = []  
        result.append(first_word)  
        b = a  
        while b <= len(string) and b != -1:  
            while string[a] == delimiter:  
                a = a + 1  
            b = string.find(delimiter,a)  
            if b != -1:  
                res = string[a:b]  
            else:  
                res = string[a:]  
            a = b  
            result.append(res)  
        return result
    

def main():
    starttime = datetime.datetime.now()
    o = TunningSqlStmts()
    o.process('TBC_DNS01')
    endtime = datetime.datetime.now()   
    print "time cost " , (endtime - starttime).seconds

if __name__ == "__main__":
    main()
