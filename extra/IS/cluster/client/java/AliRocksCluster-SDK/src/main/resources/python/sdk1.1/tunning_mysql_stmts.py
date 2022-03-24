#!env python
#coding: utf-8

import time
import Queue
import thread
import threading
import socket
import struct


CREATOR = 901
LOCAL_IP = "127.0.0.1"
LOCAL_PORT = 8108


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
        print("Miss slave ip and port")
        if len(ip_port) == 1 and ins_type ==3:
            print("Miss slave ip and port, because of readonly instance")
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


def create_connection(sock_ip, sock_port):
    addr = (sock_ip, sock_port)
    tcpCliSock = None
    try:
        tcpCliSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if tcpCliSock:
            tcpCliSock.connect(addr)
        return tcpCliSock
    except Exception as e:
        log.error('Create connection fail, exception: ' % e)
        return None

def send_message(tcpCliSock, msg):
    requestBytes = msg.encode('utf-8')
    requestLen = len(requestBytes) + 4;
    requestLenBytes = struct.pack("i", requestLen) 
    """
    requestType
    """
    requestType =  3
    requestTypeBytes = struct.pack("i", requestType) 
    
#     tcpCliSock.sendall(msg.encode())
    tcpCliSock.sendall(requestLenBytes[::-1] + requestTypeBytes[::-1] + msg)
    
def recv_message(tcpCliSock, bufSize):
    data = tcpCliSock.recv(bufSize)
    return data



                                               
class TunningSqlStmtsWorker(object):
    def __init__(self, cluster_name, ins_name, id, masterIP, masterPort, ip, port, disk_size, db_type):
        self.cluster_name = cluster_name
        self.ins_name = ins_name
        self.id = id
        self.masterIP = masterIP
        self.masterPort = masterPort
        self.ip = ip
        self.port = port
        self.disk_size = disk_size
        self.db_type = db_type
        self.sock_ip = LOCAL_IP
        self.sock_port = LOCAL_PORT
        self.tcpCliSock = create_connection(self.sock_ip, self.sock_port)
            
    def __del__(self):
        pass
                
    def tune_ins(self, tcpCliSock):
        """
            Connect to server and start the tuning
        """
        try:
            if (tcpCliSock == None):
                print('Socket connect to server is None, reconnect')
                tcpCliSock = create_connection(LOCAL_IP, LOCAL_PORT)
                if (tcpCliSock == None):
                    print('Socket connect to server is None, return')
                    return
 
            msg = '{"CMD":"TIMEDTUNING", "CLUSTERNAME":"%s", "CUSTINSID":"%s", "CUSTINSNAME":"%s", "MASTERIP":"%s", "MASTERPORT":"%s", "SLAVEIP":"%s", "SLAVEPORT":"%s", "DISKSIZE":"%s", "DBTYPE":"%s"}\n' % (
                    self.cluster_name, self.id, self.ins_name, self.masterIP, self.masterPort, self.ip, self.port, self.disk_size, self.db_type)
            
            print('Send message to server: %s' % msg)
            send_message(tcpCliSock, msg)
            tcpCliSock.setblocking(0)
            deadline = time.time() + 3 * 60
            reply = None
            while time.time() < deadline:
                try:
                    reply = recv_message(tcpCliSock, 1024)
                    if reply and len(reply) > 0:
                        print('Get reply from server: %s' % reply)
                        break
                except socket.error:                   
                    time.sleep(5)
            if reply is None or len(reply) == 0:
                print('Tuning server can not finish timed job in 5 minutes. No longer waiting')
        except Exception,e :
            print('Tune_ins fail, Exception: %s' % e)
            
    def get_info(self):
        return dict(cluster_name=self.cluster_name, ins_name=self.ins_name, id=self.id,
                    ip=self.ip, port=self.port)
                  
class TunningSqlStmts():
    lock = threading.Lock()
    
    def __init__(self):
        self._threadpool = None
        self.custInstances = []
        
    def process(self, job_target):
        """
            Get all instances according to the cluster, Put them into worker
            thread pool. These daemon threads will work until the queue is empty.
        """
        try:
            self.custInstances = []
        
            cluster = job_target
            print("Begin to check ins of cluster:%s..." % cluster)
            self.get_instances(cluster)
            
            worker = TunningSqlStmtsWorker('TBC_DNS01', 'rmtestdns01001', 123957L, '10.118.136.207', '3005', '10.118.136.207', '3005', 1234, 'mysql' );
            worker.tune_ins(worker.tcpCliSock);
        
        except Exception, e:
            print(e)
        
    def get_instances(self, cluster):
        for index in range(3):
            #('TBC_DNS01', u'rmtestdns01001', 123957L, '10.118.136.207', '3005', '10.118.136.214', '3005', 20480L, u'mysql')
            master_ip = '10.118.136.207'
            master_port = '3005'
            slave_ip = '10.118.136.214'
            slave_port = '3005'
            ins_name = 'rmtestdns01001'
            id = 123957L
            disk_size = 1234
            db_type = 'mysql'
            self.custInstances.append(TunningSqlStmtsWorker(cluster, ins_name, id, master_ip, master_port, slave_ip, slave_port, disk_size, db_type))
        print('Ins number:%s' % len(self.custInstances))
        return self.custInstances
    


def main():

    o = TunningSqlStmts()
    o.process('TBC_DNS01')

if __name__ == "__main__":
    main()
