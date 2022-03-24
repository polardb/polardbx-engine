#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2015年01月01日
~~~Happy New Year~~
@author: jiasen.xjs
'''
import json
from random import shuffle
import socket
import time

from configuration import settings
from rds.base.const import REAL_TUNING
from rds.base.lib import log
from rds.domain.cloud_dba.abnormal_check import AbnormalCheck
from rds.pengine.pipeline import FixedStepsPipeline, HostRole, PipelineException
from CloudDBA_SDK_Core import RequestInfoDO, ResponseInfoDO, SDKConfigDO, DriverManager
    


try:
    from rds.domain.lib import master_dbutils
    from rds.domain.repo import cust_instance as ci
except:
    pass


APP_KEY = "RDS_MONITOR_DS"
LOCAL_IP = "127.0.0.1"
LOCAL_PORT = 8108

class RealtimeAbnormalCheckPipeline(FixedStepsPipeline):

    @staticmethod
    def is_responsible(task, custins):
        return task.task_key == "abnormal_check"

    def fixed_steps(self):
        steps = []
        steps.append((self.do_init, HostRole.MASTER))
        steps.append((self.do_realtime_tuning_job, HostRole.MASTER))
        steps.append((self.do_abnormal_check, HostRole.MASTER))
        steps.append((self.do_nothing, HostRole.MASTER))
        return steps

    def do_init(self):
        cust_ins = ci.get_cust_instance_detached(self.context.cust_ins.id)
        if cust_ins.ins_type not in (0,2,3) or cust_ins.is_tmp !=0:
            log.warn("custins is not fit to do abnormal check :ins_type %s,is_tmp %s"%(cust_ins.ins_type,cust_ins.is_tmp))
            self.set_next_step(self.do_nothing, HostRole.MASTER)
            return
        user_id,disk_size = cust_ins.user_id,cust_ins.disk_size
        self.context["user_id"] = user_id
        self.context["disk_size"] = disk_size
        
        if cust_ins.is_read_custins():
            self.context["is_read_custins"] = True
            sql_str = """select h.ip,i.port from custins_hostins_rel chr, instance i, hostinfo h,instance_stat ist
            where chr.hostins_id = i.id and h.id = i.host_id and ist.ins_id = i.id and ist.role = 1
            and chr.custins_id = %s;"""
            rows = master_dbutils.do_select_fetchall(sql_str, self.context.cust_ins.primary_custins_id)
            for ip, port in rows:
                self.context["primary_custins_slave_host_ins_ip"] = ip
                self.context["primary_custins_slave_host_ins_port"] = port
        else:
            self.context["is_read_custins"] = False
        
    def do_realtime_tuning_job(self):
        rows = master_dbutils.do_select_fetchall("select ip,port from bakowner where type=%s", settings.SERVER_DBA_TUNNING)
        if not rows:
            raise PipelineException('there is no dba tunning service configured in bakower.')
        ip_list = []
        for ip,port in rows:
            ip_list.append((ip,port))
        shuffle(ip_list)
        tunning_server_ip =  ip_list[0][0]
        tunning_server_port = ip_list[0][1]
        log.info("selected tunning_server (%s,%s)"%(tunning_server_ip,tunning_server_port))
        ##send msg to tuning server
        requestInfo = self._build_tunning_msg()
        log.info("send msg %s to tuning server" % requestInfo.toString())
        
        sdkConfig = SDKConfigDO(LOCAL_IP, LOCAL_PORT, APP_KEY)
        client = DriverManager.getInstance(sdkConfig).getCloudDBAClient()
        try:
            responseInfo = client.doCommand(requestInfo)
        except Exception,error:
            log.error('Exception: %s' % error, exc_info=True)
        finally:
            DriverManager.release()
        
        if  responseInfo:
            log.info("tuning server has finished realtime job.reply:%s" % responseInfo.toString())
        else:
            log.info('tuning server can not finish realtime job in 3 minutes. No longer waiting')
            
    def _build_tunning_msg(self):
        dbType = self.context.cust_ins.db_type
        clusterName = self.context.cust_ins.cluster_name
        custInsID =  str(self.context.cust_ins.id)
        custInsName = self.context.cust_ins.ins_name
        masterIP = self.context.cust_ins.master_host_ins.ip
        masterPort = str(self.context.cust_ins.master_host_ins.port) 
        diskSize = str(self.context["disk_size"])   
        
        if self.context["is_read_custins"] == True:
            slaveIP = self.context["primary_custins_slave_host_ins_ip"]
            slavePort = str(self.context["primary_custins_slave_host_ins_port"])
        else:
            slaveIP = self.context.cust_ins.slave_host_ins.ip
            slavePort = str(self.context.cust_ins.slave_host_ins.port) 
            
        requestInfo = RequestInfoDO(clusterName, custInsName, custInsID, masterIP, masterPort, slaveIP, slavePort, diskSize, dbType)
        requestInfo.setCmd(REAL_TUNING)
        return requestInfo
        
    def do_abnormal_check(self):
        task_begin_time = master_dbutils.do_select_fetchone("select task_begin from task_queue where id=%s", self.task_id)
        checker = AbnormalCheck.new(self.context.cust_ins.db_type,self.context.cust_ins.id,self.context["user_id"],task_begin_time)
        log.info("realtime abnormal check for mysql custins: %s" % (self.context.cust_ins.id))
        checker.do_abnormal_check()
        log.info("finish abnormal check")
            
    def do_nothing(self):
        pass
