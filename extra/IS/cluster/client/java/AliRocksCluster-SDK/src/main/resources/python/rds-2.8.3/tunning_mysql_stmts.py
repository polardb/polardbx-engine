#!env python
#coding: utf-8

import time
from rds.domain.lib.master_dbutils import do_select_fetchall
from rds.base.lib import log
from rds.stat.schedule.jobworker import Worker
from rds.base.const import TIMED_TUNING
from rds.base.lib.thread_pool import TIMEINTERNAL_FETCH_TASK
from rds.stat.dba_tunning.CloudDBA_SDK_Core import RequestInfoDO, ResponseInfoDO, SDKConfigDO, DriverManager
import Queue
import thread
import threading
import socket


CREATOR = 901
LOCAL_IP = "127.0.0.1"
LOCAL_PORT = 8108
DATASOURCE_KEY = "RDS_MONITOR_DS"



def split_str(string, delimiter, begin_end = " "):  
    string = string.strip(begin_end)  
    a = string.find(delimiter) 
    if a != -1:
        first_word = string[:a]
    else:
        first_word = string
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


def get_ip_port_info(ip_port_str, ins_type, custins_id):    
    master_ip = None
    master_port = None
    slave_ip = None
    slave_port = None
    ip_port = split_str(ip_port_str, ',')
    if len(ip_port) != 2:
        log.warn("Miss slave ip and port")
        if len(ip_port) == 1 and ins_type ==3:
            log.info("Miss slave ip and port, because of readonly instance")
            master_ip_port = split_str(ip_port[0], '#')
            master_ip = master_ip_port[1]
            master_port = master_ip_port[2]
            slave_ip,slave_port = get_readonly_primary_slave_ip_port(custins_id)  
    else:    
        ip_port1 = split_str(ip_port[0], '#')
        ip_port2 = split_str(ip_port[1], '#')
        if len(ip_port1) == 3 and len(ip_port2) == 3:
            if ip_port1[0] == '0':
                master_ip = ip_port1[1]
                master_port = ip_port1[2]
                slave_ip = ip_port2[1]
                slave_port = ip_port2[2]
            else:
                master_ip = ip_port2[1]
                master_port = ip_port2[2]
                slave_ip = ip_port1[1]
                slave_port = ip_port1[2]
                
    return master_ip,master_port,slave_ip,slave_port 

def get_readonly_primary_slave_ip_port(custins_id):
    ip = None
    port = None
    sql_str = """select h.ip, i.port
    from cust_instance ci, instance i, custins_hostins_rel chr, hostinfo h, instance_stat stat
    where ci.primary_custins_id = chr.custins_id
    and chr.hostins_id = i.id
    and stat.ins_id=i.id
    and h.id = i.host_id
    and stat.role=1 and ci.id = '%s' limit 1;"""
    rows = do_select_fetchall(sql_str, custins_id)
    if not rows:
        log.error('there is no readonly_primary_slave_ip_port info.')
    ip = rows[0][0]
    port = rows[0][1]
    return ip,port


class CallSDKThread(threading.Thread):
    def __init__(self, tunningSqlStmts, threadID):
        threading.Thread.__init__(self)
        self.tunningIns = 0;
        self.threadID = DATASOURCE_KEY + "-thread-" + str(threadID) + " ";
        self.tunningSqlStmts = tunningSqlStmts;

    def run(self):
        while self.tunningSqlStmts.custInstances.qsize() > 0 :
            self.tunning(self.tunningSqlStmts.custInstances);
        
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
        self.appThreads = None;
        
    def process(self, job_target):
        """
            Get all instances according to the cluster, Put them into worker
            thread pool. These daemon threads will work until the queue is empty.
        """
        try:
            self.custInstances = Queue.Queue();
            self.appThreads = [];
        
            cluster = job_target.target_id
            log.info("Begin to check ins of cluster:%s..." % cluster)
            self.get_instances(cluster)
            instanceNum = self.custInstances.qsize();
            threadNum = 2
            if (instanceNum < 300):
                threadNum = 2
            elif (instanceNum < 1000):
                threadNum = 3
            elif (instanceNum >= 1000 and instanceNum < 3000):
                threadNum = 6
            elif (instanceNum >= 3000 and instanceNum < 6000):
                threadNum = 12
            else:
                threadNum = 16
                
            """
            init sdk  and init threadNum appThread call doCommand
            """
            sdkConfig = SDKConfigDO(LOCAL_IP, LOCAL_PORT, DATASOURCE_KEY);
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
        """
            Get instances detailed info from metadb according to cluster.
        """
        sql = r'''select ci.ins_name, ci.id, ci.ins_type, GROUP_CONCAT(stat.role,'#', h.ip,'#', i.port), ci.disk_size, ci.db_type from cust_instance ci, instance i, 
            custins_hostins_rel chr, hostinfo h, instance_stat stat 
            where  ci.type = 'x' and 
            ci.is_tmp = 0 and  
            ci.cluster_name = '%s' and 
            ci.id = chr.custins_id and 
            chr.hostins_id = i.id and 
            ci.db_type in ('mysql','mssql') and 
            h.is_deleted = 0 and 
            h.host_name != '' and 
            stat.ins_id=i.id and 
            h.id = i.host_id and 
            (ci.is_deleted = 0 and ci.status in (1, 6, 7) and i.is_deleted =0 and ci.ins_type != 4 and ci.ins_type != 6) group by ci.id;''' % cluster                          
        rows = do_select_fetchall(sql)
        for ins_name, id, ins_type, ip_port_str, disk_size, db_type in rows:
            if not ip_port_str:
                continue
            (master_ip, master_port, slave_ip, slave_port) = get_ip_port_info(ip_port_str, ins_type, id)
            request = RequestInfoDO(cluster, ins_name, id, master_ip, master_port, slave_ip, slave_port, disk_size, db_type);
            request.setCmd(TIMED_TUNING);
            """
            put_nowait is suited to single thread without blocking
            """
            self.custInstances.put_nowait(request);
        log.info('Ins number:%s' % self.custInstances.qsize())
        return self.custInstances
    

def main():

    o = TunningSqlStmts()
    o.process('TBC_DNS01')

if __name__ == "__main__":
    main()
