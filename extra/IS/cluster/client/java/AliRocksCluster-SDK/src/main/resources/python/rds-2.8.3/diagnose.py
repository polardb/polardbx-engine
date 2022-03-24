from rds.domain.diagnose.inslevel import CustInstanceLevel
from rds.domain.diagnose.inshealth import CustInstanceHealth
from rds.domain.diagnose.insperf import InstancePerfValuesManager, InstancePerf
from rds.domain.diagnose.render import JsonSerializable
from rds.domain.diagnose import constants
from rds.domain.lib import master_dbutils
from rds.domain.lib import drds_dbutils
from rds.domain.meta import cluster_info
from rds.domain.lib import ossutil
from configuration import settings
from datetime import datetime, timedelta
import time
import MySQLdb
from rds.base.lib import log
from rds.domain.diagnose.rules import reloader
from rds.domain.diagnose.hostperf import HostPerfValuesManager
from rds.domain.diagnose.diskperf import DisksizePerf
from rds.domain.diagnose.render import RenderContext, ReportRender
from rds.domain.diagnose.rules.rule import RuleResultManager
from rds.domain.diagnose.snapshot import Snapshot
from rds.domain.diagnose.missindex_holder import MissIndexHolder
from rds.domain.diagnose.mysql_snapshot import Slowlog
from rds.domain.diagnose.mssql_snapshot import MssqlSnapshotDelta
from rds.stat.dba_tunning.tunning_mysql_stmts import split_str, get_ip_port_info
from rds.domain.diagnose.custins_disk_advice import Custins_Disk_Advice
from rds.stat.dba_tunning.CloudDBA_SDK_Core import RequestInfoDO, ResponseInfoDO, SDKConfigDO, DriverManager
import os
import json
import codecs
import sys

reload(sys)
sys.setdefaultencoding('utf8')



def get_master_host_id(host_id_str, ins_type):  
    master_host_id = None
    role_host_id = split_str(host_id_str, ',')
    if len(role_host_id) != 2:
        log.warn("Miss master or slave ip")
        if len(role_host_id) == 1 and ins_type ==3:
            log.info("Miss slave ip because of readonly instance")
            host_id = split_str(role_host_id[0], '#')
            master_host_id = host_id[1]
    else:     
        host_id1 = split_str(role_host_id[0], "#")
        host_id2 = split_str(role_host_id[1], "#")        
        if len(host_id1) == 2 and len(host_id2) == 2:
            if host_id1[0] == 0:
                master_host_id = host_id1[1]
            else:
                master_host_id = host_id2[1]
  
    return master_host_id

class ErrorCounter(object):

    first_error_occur = None
    counter = 0

    EXCEPTION_TARGET_TYPE = 'diagnose:error'

    @staticmethod
    def error():
        now = datetime.now()
        if ErrorCounter.first_error_occur is None:
            ErrorCounter.first_error_occur = now

        if (now - ErrorCounter.first_error_occur).total_seconds() > 120:
            ErrorCounter.first_error_occur = now
            ErrorCounter.counter = 1
        else:
            ErrorCounter.counter += 1
            if ErrorCounter.counter >= 10:
                log.info("too many error, send warning!")
                ErrorCounter._send_warning()
                ErrorCounter.counter = 0
                ErrorCounter.first_error_occur = now

    @staticmethod
    def _send_warning():
        now = datetime.now()
        owner_id = master_dbutils.do_select_fetchone("select bak_id from bakowner where type=7 limit 1")
        master_dbutils.do_dml("delete from exception where target_subtype=%s", ErrorCounter.EXCEPTION_TARGET_TYPE)
        master_dbutils.do_dml("""insert into exception (
                              target_id, target_type, exception,
                              status, creator, modifier,
                              gmt_created, gmt_modified,
                              critical_times, target_subtype)
                              values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              """,
                              (owner_id, 8, "diagnose failed too frequently!", 0, 0, 0, now, now, 5, ErrorCounter.EXCEPTION_TARGET_TYPE))


class Diagnose(JsonSerializable):

    def __init__(self, diagnose_id,custins_id, diagnose_kind):
        """
        Params:
            diagnose_id: A int of the custins_diagnose
            custins_id
        """        
        self.diagnose_id = diagnose_id
        self.custins_id = custins_id
        # default diagnose_kind is 0(timed tuning), this value can be modified by param if diagnose_kind is realtime tuning
        self.diagnose_kind = 0
        self.diagnose_kind = diagnose_kind
        
        (self.user_type,
         self.time_begin, self.time_end,
         self.diagnose_type) = drds_dbutils.do_select_fetchall("""select user_type,
                                                                 time_begin, time_end,
                                                                 diagnose_type
                                                                 from custins_diagnose
                                                                 where custins_id=%s and id=%s""", (self.custins_id,self.diagnose_id))[0]
        # default health score is 100, this value can be modified by a rule.
        self.healthscore = 100
        
        sqlstr = """select ci.db_type, ci.cluster_name, ci.ins_type, ci.is_tmp, ci.ins_name, ci.disk_size,
        GROUP_CONCAT(stat.role,'#', h.ip,'#', i.port), GROUP_CONCAT(stat.role,'#', i.host_id)
        from cust_instance ci, instance i, custins_hostins_rel chr, hostinfo h, instance_stat stat
        where ci.id = chr.custins_id 
        and chr.hostins_id = i.id 
        and stat.ins_id=i.id 
        and h.id = i.host_id 
        and ci.id = %s limit 1; """

        row = master_dbutils.do_select_fetchall(sqlstr,self.custins_id)
        if not row:
            raise Exception("base info of custins %s is error,please check"%self.custins_id)
        
        self.db_type, self.cluster_name,self.ins_type,self.is_tmp,self.ins_name,\
        self.disk_size, ip_port_str, host_id_str = row[0]

        self.master_host_id = get_master_host_id(host_id_str, self.ins_type)        

        if self.diagnose_kind == 1:
            (master_ip, master_port, slave_ip, slave_port) = get_ip_port_info(ip_port_str, self.ins_type, self.custins_id)
            self.msg = RequestInfoDO(self.cluster_name, self.ins_name, self.custins_id, master_ip, master_port, slave_ip, slave_port, self.disk_size, self.db_type);
        else:
            self.msg = None
        
        
        # build instance level, initialized in self._do_diagnose()
        self.ins_level = None

        # build instance health, initialized in self._do_diagnose()
        self.ins_health = None

        # build instance perf, initialized in self._do_diagnose()
        self.ins_perf_values = None
        self.ins_perf = None

        # build host perf values, initialized in self._do_diagnose()
        self.host_perf_values = None

        # build disksize values, initialized in self._do_diagnose()
        self.disksize_perf_values = None

        # build missindex holder, initialized in self._do_diagnose()
        self.missindex_holder = None

        # build instance disk advice
        self.custins_disk_advice = None

        # top size tables, item format is (db_name,table_name,data_size,index_size)
        self.top_size_tables = []

        # myisam tables, item format is (db_name, table_name, engine)
        self.myisam_tables = []

        # snapshot list the element is MysqlSnapshot or MssqlSnapshot object.
        self.last_snapshot = None
        self.first_snapshot = None
        self.slowlogs = []

        # result manager to hold the rules.
        self.rule_results = RuleResultManager()

        # the json data local path
        self.data_file = os.path.join(constants.TMP_DIR, "realtime_data_%s" % (self.diagnose_id))
        self.report_file = os.path.join(constants.TMP_DIR, "realtime_report_%s.html" % (self.diagnose_id))

        # initialize the oss client for report uploading and downloading.
        self.oss_info = cluster_info.get_oss_apiinfo_bycluster(self.cluster_name)
        self.oss_client = ossutil.rdsoss(self.oss_info["host"], self.oss_info["accessid"],
                                         self.oss_info["accesskey"], self.oss_info[constants.REPORT_BUCKET_KEY])

        self._prepare_dir()

    def _prepare_dir(self):
        if not os.path.exists(constants.TMP_DIR):
            os.makedirs(constants.TMP_DIR)

    def _build_snapshots(self):
        if self.diagnose_type == constants.DiagnoseType.NOW:
            log.info("build_snapshots_for_now")
            self._build_snapshots_for_now()
        else:
            log.info("build_snapshots_for_range")
            self._build_snapshots_for_range()
        # get the last snapshot for the database
        
        log.info("get slow_logs for diagnose:%s"% self.diagnose_id)
        self._fillup_mysql_slowlogs()

        self._compute_mssql_snapshot_delta()

        log.info("successfully to download snapshots for diagnose: %s" % self.diagnose_id)

    def _build_snapshots_for_now(self):
        self.last_snapshot = Snapshot.new(self.custins_id,self.cluster_name,self.db_type).snapshot()

        # we just do a snapshot, so the snapshot time is the end time of
        # this diagnose.
        self.time_end = self.last_snapshot.snapshot_time

        rows = drds_dbutils.do_select_fetchall("""select id, snapshot_time
                                               from custins_diagnose_snapshot
                                               where snapshot_time >= %s and snapshot_time < %s
                                               and status=2
                                               and custins_id=%s
                                               order by id desc limit 1
                                               """,
                                               (self.time_end - timedelta(minutes=60 * 2),
                                                self.time_end - timedelta(minutes=20),
                                                self.custins_id))
        if rows:
            """we find the """
            (first_snapshot_id, first_snapshot_time) = rows[0]
            self.first_snapshot = Snapshot.load(self.custins_id,self.db_type, first_snapshot_id)

            # we get a last snapshot, so the time_begin is the last snapshot's
            # snapshot_time.
            self.time_begin = first_snapshot_time

        else:
            if self.db_type == constants.DBTYPE_MSSQL:
                log.info("can not find first snapshot, we wait a minute and then do a new snapshot.")
                time.sleep(60)
                new_snapshot = Snapshot.new(self.custins_id,self.cluster_name,self.db_type).snapshot()
                self.first_snapshot = self.last_snapshot
                self.last_snapshot = new_snapshot

                self.time_begin = self.first_snapshot.snapshot_time
                self.time_end = self.last_snapshot.snapshot_time

            elif self.db_type == constants.DBTYPE_MYSQL:
                self.time_begin = self.time_end - timedelta(minutes=30)

        drds_dbutils.do_dml("""update custins_diagnose set time_begin=%s,
                              time_end=%s where custins_id=%s and id=%s""",
                              (self.time_begin, self.time_end, self.custins_id, self.diagnose_id))

    def _build_snapshots_for_range(self):

        rows = drds_dbutils.do_select_fetchall("""select id, snapshot_time
                                                 from custins_diagnose_snapshot
                                                 where snapshot_time >= %s
                                                 and snapshot_time <= %s
                                                 and status=2
                                                 and custins_id=%s
                                                 order by id""",
                                                 (self.time_begin, self.time_end,
                                                 self.custins_id))

        if not rows or len(rows) < 1:
            raise Exception("custins :%s no snapshot between [%s, %s]" % (self.custins_id,self.time_begin, self.time_end))

        (first_snapshot_id, self.time_begin) = rows[0]
        (last_snapshot_id, self.time_end) = rows[-1:][0]

        self.first_snapshot = Snapshot.load(self.custins_id,self.db_type, first_snapshot_id)
        self.last_snapshot = Snapshot.load(self.custins_id,self.db_type, last_snapshot_id)

        self.time_begin = self.first_snapshot.snapshot_time
        self.time_end = self.last_snapshot.snapshot_time

        drds_dbutils.do_dml("""update custins_diagnose set time_begin=%s,
                              time_end=%s where custins_id=%s and id=%s""",
                              (self.time_begin, self.time_end, self.custins_id, self.diagnose_id))

    def _fillup_mysql_slowlogs(self):
        if self.db_type != constants.DBTYPE_MYSQL:
            return

        self.slowlogs = Slowlog.groupby_formatted_sql(Slowlog.query_from_drds(self.custins_id,
                                                                              self.time_begin,
                                                                              self.time_end))
    def _compute_mssql_snapshot_delta(self):
        if self.db_type != constants.DBTYPE_MSSQL:
            return
        self.snapshot_delta = MssqlSnapshotDelta(self.first_snapshot, self.last_snapshot)
        self.slowlogs = self.snapshot_delta.slowlogs

    def set_healthscore(self, healthscore):
        self.healthscore = healthscore
        drds_dbutils.do_dml("update custins_diagnose set health_score=%s where custins_id=%s and id=%s",
                              (self.healthscore, self.custins_id, self.diagnose_id))

    def inslevel(self):
        return self.ins_level

    def insperfvalues(self):
        return self.ins_perf_values

    def hostperfvalues(self):
        return self.host_perf_values

    def insperf(self):
        return self.ins_perf

    def missindexholder(self):
        return self.missindex_holder

    def custins_disk_advice(self):
        return self.custins_disk_advice

    def inshealth(self):
        return self.ins_health

    def snapshotlist(self):
        """Return the list of Snapshot."""
        return [self.first_snapshot, self.last_snapshot]

    def lastsnapshot(self):
        """Return the last snapshot."""
        return self.last_snapshot

    def slowlog_list(self):
        """Return slowsql from the snapshot."""
        return self.slowlogs

    def ismyisam(self, db_name, table_name):
        for (db, table, _) in self.myisam_tables:
            if db_name == 'db' and table_name == 'table':
                return True

        return False

    def todict(self):
        check = lambda x:x.todict() if x else None
        dict_info = {"healthscore": self.healthscore,
                     "user_type": self.user_type,
                     "time_begin": self.time_begin.strftime('%Y-%m-%d %H:%M:%S'),
                     "time_end": self.time_end.strftime('%Y-%m-%d %H:%M:%S'),
                     "ins_health": check(self.ins_health),
                     "ins_level": check(self.ins_level),
                     "ins_perf_values": check(self.ins_perf_values),
                     "ins_perf": check(self.ins_perf),
                     "disksize_perf_values": check(self.disksize_perf_values),
                     "rules": check(self.rule_results),
                     "snapshot": check(self.last_snapshot),
                     "missindex": check(self.missindex_holder),
                     "disk_advice": check(self.custins_disk_advice),
                     "slowlog_list": [x.todict() for x in self.slowlogs]
                     }

        if self.db_type == constants.DBTYPE_MSSQL:
            dict_info["snapshot_delta"] = self.snapshot_delta.todict()

        return dict_info

    def update_status(self, status, msg=""):
        
        drds_dbutils.do_dml("""update custins_diagnose
                              set status=%s, msg=%s, gmt_modified=%s, modifier=%s
                              where custins_id=%s and id=%s
                              """,
                              (status, msg, datetime.now(), settings.OWNER_ID, self.custins_id, self.diagnose_id))
        
        log.info("end to update_status %s for diagnose: %s" % (status,self.diagnose_id))

    def diagnose(self):
        try:
            self._do_diagnose()
        except Exception, e:
            log.error("error happened when _do_diagnose for cins: %s:error:%s"%(self.custins_id,e),exc_info = True)
            
        try:
            self._build_report()
            self._upload_oss()
            self.update_status(constants.DiagnoseStatus.FINISH)
        except Exception, e:
            self.update_status(constants.DiagnoseStatus.FAIL, str(e))
            log.exception("diagnose(id:%s) for custins: %s failed:--%s" % (self.diagnose_id,self.custins_id,e))
            ErrorCounter.error()
            raise e
        finally:
            try:
                if os.path.exists(self.report_file): 
                    os.remove(self.report_file)
            except Exception, e:
                log.warn("failed to rm data_file:%s"%e)
            try:
                if os.path.exists(self.data_file): 
                    os.remove(self.data_file)
            except Exception, e:
                log.warn("failed to rm data_file:%s"%e)
            try:
                dir_path = "%s/custins%s" % (constants.TMP_DIR, self.custins_id)
                if os.path.exists(dir_path): 
                    os.rmdir(dir_path)
            except Exception, e:
                log.warn("failed to rm tempdir:%s"%e)

    def _do_diagnose(self):
        try:
            self._build_snapshots()
        except Exception, e:
            log.error("failed to build snapshots for cins:%s error:%s"%(self.custins_id,e),exc_info = True)
               
        # build instance level
        try:
            self.ins_level = CustInstanceLevel(self.custins_id)
        except Exception, e:
            log.error("failed to build ins_level for cins:%s error:%s"%(self.custins_id,e),exc_info = True)   

        # build instance health
        try:
            self.ins_health = CustInstanceHealth(self.ins_level)
            self.ins_health.compute_health()
            self.set_healthscore(self.ins_health.score)
        except Exception, e:
            log.error("failed to build ins_health for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  

        # build instance perf
        try:
            self.ins_perf_values = InstancePerfValuesManager(self)
            self.ins_perf = InstancePerf(self.custins_id)
        except Exception, e:
            log.error("failed to build ins_perf for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  

        # build host perf values
        try:
            self.host_perf_values = HostPerfValuesManager.new(self.db_type,self.master_host_id)
        except Exception, e:
            log.error("failed to build host_perf for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  

        # build disksize perf values
        try:
            self.disksize_perf_values = DisksizePerf(self.custins_id, self.time_begin, self.time_end)
        except Exception, e:
            log.error("failed to build disksize_perf for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  

        # build missindex holder
        try:
            self.missindex_holder = MissIndexHolder(self.custins_id, self.diagnose_kind, self.msg, self.ins_type, self.is_tmp)
            self.missindex_holder.do_get_missIndex()
        except Exception, e:
            log.error("failed to build missindex for cins:%s error:%s"%(self.custins_id,e),exc_info = True) 
       

        try:
            self.custins_disk_advice = Custins_Disk_Advice(self.custins_id, self.db_type)
            self.custins_disk_advice.get_disk_size_advice()
        except Exception, e:
            log.error("failed to build disk advice for cins:%s error:%s"%(self.custins_id,e),exc_info = True) 

        #build top size tables and myisam tables
        self._get_top_size_tables_and_myisam_tables()
        
        # dynamically load rules.
        try:
            from rds.domain.diagnose.rules.factory import RuleFactory
            for rule_factory in RuleFactory.factories():
                for rule in rule_factory.create_rules(self):
                    self._apply_rule(rule)
        except Exception, e:
            log.error("failed to apply rules for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  
                    
        log.info("successfully to do_diagnose for diagnose: %s" % self.diagnose_id)
    
    def _get_top_size_tables_and_myisam_tables(self):
        
        try:
            rows = drds_dbutils.do_select_fetchall(
                """
                select db_name,table_name,data_size,index_size,engine
                from tunning_tabledesign
                where custins_id=%s
                and gmt_modified>='%s';                
                """ % (self.custins_id,
                       datetime.now().strftime('%Y-%m-%d')))
            if rows:
                #build top_size_talbes top 20 
                rows.sort(key=lambda x: x[2], reverse=True)
                top_tables = rows[0:constants.NUM_TOP_SIZE_TABLES]
                for db_name,table_name,data_size,index_size,engine in top_tables:
                    self.top_size_tables.append((db_name,table_name,data_size,index_size))
                
                # build myisam_tables
                if self.db_type == constants.DBTYPE_MYSQL:
                    for db_name,table_name,data_size,index_size,engine in rows:
                        if str(engine) in('MyISAM', 'MRG_MYISAM'):
                            self.myisam_tables.append((db_name, table_name, engine))
            
        except Exception, e:
            log.error("failed to build top_size_talbes and myisam_tables for cins:%s error:%s"%(self.custins_id,e),exc_info = True)  
        
    def _apply_rule(self, rule):
        try:
            if rule.apply():
                self.rule_results.add(rule)
        except Exception, e:
            log.exception("failed to apply rule: %s" % e)

    def _build_report(self):
        # generate json data file
        # generate html report file
        context = RenderContext()
        context.add("diagnose", self)
        render = ReportRender(self._get_template(), context)
        render.render(self.report_file)

        out = codecs.open(self.data_file, 'w')
        out.write(json.dumps(self.todict()))
        out.flush()
        out.close()
        log.info("successfully to build_report for diagnose: %s" % self.diagnose_id)

    def _get_template(self):
        if self.db_type == constants.DBTYPE_MYSQL:
            return "mysql_report.html"
        if self.db_type == constants.DBTYPE_MSSQL:
            return "mssql_report.html"

        raise Exception("invalid dbtype: %s" % self.db_type)

    def _upload_oss(self):
        
        drds_dbutils.do_dml("""update custins_diagnose
                              set upload_begin=%s, gmt_modified=%s,
                              modifier=%s
                              where custins_id=%s and id=%s
                              """,
                              (datetime.now(), datetime.now(),
                               settings.OWNER_ID, self.custins_id, self.diagnose_id))

        path = "custins%s" % self.custins_id

        data_target_name = "diagnose_data_%s.json" % (self.diagnose_id)
        data_download_url = "%s/%s" % (path, data_target_name)

        report_target_name = "diagnose_report_%s.html" % (self.diagnose_id)
        report_download_url = "%s/%s" % (path, report_target_name)

        self.oss_client.multi_upload(path, data_target_name, self.data_file, content_type='application/json')
        log.info("successful to upload file: %s to oss." % (self.data_file))

        self.oss_client.multi_upload(path, report_target_name, self.report_file, content_type='text/html')
        log.info("successful to upload file: %s to oss." % (self.report_file))

        drds_dbutils.do_dml("""update custins_diagnose
                              set upload_end=%s, gmt_modified=%s,
                              archive_ownerid=%s, report_download_url=%s,
                              data_download_url=%s, modifier=%s
                              where custins_id=%s and id=%s""",
                              (datetime.now(), datetime.now(),
                               self.oss_info["owner_id"], report_download_url,
                               data_download_url, settings.OWNER_ID, self.custins_id, self.diagnose_id))
        log.info("successfully to upload oss for diagnose: %s" % self.diagnose_id)


custins_id = 246515 # mysqldiagnose
#custins_id = 14996 # sqlserverdiagnose

def test_diagnose_now():
    last_id = drds_dbutils.do_select_fetchone("select id from custins_diagnose where custins_id=%s order by id desc limit 1;"%custins_id)
    if not last_id:
        last_id = 0
    new_id = last_id + 1

    drds_dbutils.do_dml("""insert into custins_diagnose(id, custins_id, task_id, status,
                          gmt_created, gmt_modified, user_type) values(%s, %s, %s, %s, %s, %s, %s)""",
                          (new_id, custins_id, 0, "not_run", datetime.now(), datetime.now(),
                           'dba'))

    d = Diagnose(new_id,custins_id, 1)
    d.diagnose()

def test_diagnose_range():
    last_id = drds_dbutils.do_select_fetchone("select id from custins_diagnose where custins_id=%s order by id desc limit 1;")
    if not last_id:
        last_id = 0
    new_id = last_id + 1

    now = datetime.now()
    drds_dbutils.do_dml("""insert into custins_diagnose(id, custins_id, task_id, status,
                          gmt_created, gmt_modified, user_type, diagnose_type,
                          time_begin, time_end)
                          values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          """,
                          (new_id, custins_id, 0, "not_run", datetime.now(), datetime.now(),
                           'dba', 'range', now - timedelta(hours=1), now))

    d = Diagnose(new_id,custins_id)
    d.diagnose()
    
def test_jr():
    last_id = master_dbutils.do_select_fetchone("select max(id) from custins_diagnose;")
    if not last_id:
        last_id = 0
    new_id = last_id + 1

    master_dbutils.do_dml("""insert into custins_diagnose(id, custins_id, task_id, status,
                          gmt_created, gmt_modified, user_type) values(%s, %s, %s, %s, %s, %s, %s)""",
                          (new_id, custins_id, 0, "not_run", datetime.now(), datetime.now(),
                           'dba'))

    d = Diagnose(new_id,custins_id)
    d.update_status(3,'test jianrong```')
    d._upload_oss()


if __name__ == "__main__":

    settings.LOG_NODE_AGENT_ADDRESS = ""
    settings.LOG_FILE_NAME = "slowlog.log"
    log.init_log(settings)
    #cust_instance.init()
    #custins = cust_instance.get_cust_instance_detached(124381)
    ci.init()

    #test_diagnose_range()
    test_diagnose_now()
    #test_jr()
