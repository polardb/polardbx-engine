
from datetime import datetime, timedelta
from rds.domain.lib import drds_dbutils
from rds.domain.lib import master_dbutils
from rds.domain.diagnose.render import JsonSerializable
from rds.base.lib import log
from configuration import settings
from lib2to3.fixer_util import String
from random import shuffle
from rds.stat.dba_tunning.CloudDBA_SDK_Core import RequestInfoDO, ResponseInfoDO, SDKConfigDO, DriverManager
import socket
import time
import json

def select_tunning_server():
    tunning_server_ip = None
    tunning_server_port = None
    rows = master_dbutils.do_select_fetchall("select ip,port from bakowner where type=%s", settings.SERVER_DBA_TUNNING)
    if not rows:
        log.error('there is no dba tunning service configured in bakower.')
    ip_list = []
    for ip,port in rows:
        ip_list.append((ip,port))
    shuffle(ip_list)
    tunning_server_ip =  ip_list[0][0]
    tunning_server_port = ip_list[0][1]
    return tunning_server_ip,tunning_server_port


def send_msg_to_tunning_server(msg, tunning_server_ip, tunning_server_port):
    sdkConfig = SDKConfigDO(tunning_server_ip, tunning_server_port, DATASOURCE_KEY)
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

class IndexAdvice(JsonSerializable):
    
    def __init__(self):
        self.table_name = ""
        self.equality_columns = ""
        self.create_sql = ""

    def todict(self):
        return {"table_name": self.table_name,
                "equality_columns": self.equality_columns,
                "create_sql": self.create_sql}

class FunctionAdvice(JsonSerializable):

    def __init__(self):
        self.function_advice = ""

    def todict(self):
        return {"function_advice":self.function_advice}

class ImplicitAdvice(JsonSerializable):

    def __init__(self):
        self.implicit_advice = ""

    def todict(self):
        return {"implicit_advice":self.implicit_advice}

class MissIndex(JsonSerializable):

    def __init__(self):
        self.db_name = ""
        self.sql_text = ""
        self.formatted_sql = ""
        self.tuning_advice = ""
        self.index_advice_list = []
        self.function_advice_list = []
        self.implicit_advice_list = []

    def todict(self):
        return {"db_name": self.db_name,
                "sql_text": self.sql_text,
                "formatted_sql": self.formatted_sql,
                "tuning_advice": self.tuning_advice,
                "index_advice_list": [x.todict() for x in self.index_advice_list],
                "function_advice_list": [x.todict() for x in self.function_advice_list],
                "implicit_advice_list": [x.todict() for x in self.implicit_advice_list]}

        
class MissIndexHolder(JsonSerializable):
    
    def __init__(self, custins_id, diagnose_kind, msg, ins_type, is_tmp):
        self.custins_id = custins_id
        self.diagnose_kind = diagnose_kind
        self.msg = msg
        self.ins_type = ins_type
        self.is_tmp = is_tmp
        self.missindex_list = []
        log.info("start diagnose:%s" % custins_id)

    def do_get_missIndex(self):
        if self.diagnose_kind == 1:             
            if self.ins_type not in (0,2,3) or self.is_tmp !=0:
                log.warn("custins is not fit to do realtime check :ins_type %s,is_tmp %s"%(self.ins_type,self.is_tmp))
                return

            server_ip, server_port = select_tunning_server()
            log.info("selected tunning_server (%s,%s)"%(server_ip,server_port))
                        
            timebegin = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))  
            log.info("send msg %s to tuning server at %s"%(self.msg,timebegin))
            send_msg_to_tunning_server(self.msg,server_ip,server_port)

            rows = drds_dbutils.do_select_fetchall("""select db_name,
                                                 table_name, equality_columns,
                                                 sql_text,
                                                 create_sql, tuning_advice from tunning_missindex
                                                 where custins_id=%s
                                                 and gmt_modified>=%s
                                                 """,
                                                (self.custins_id, timebegin))
           
        else:          
            rows = drds_dbutils.do_select_fetchall("""select db_name,
                                                 table_name, equality_columns,
                                                 sql_text,
                                                 create_sql, tuning_advice from tunning_missindex
                                                 where custins_id=%s
                                                 and gmt_modified>=%s
                                                 """,
                                                (self.custins_id, datetime.now() - timedelta(hours=4)))
           
        table_name_list = []
        equality_columns_list  = []
        create_sql_list = []
        tuning_advice_all = {}
        implicit_rule_list = []
        function_rule_list = []

        log.info("put advice into list:%s" % self.custins_id)
        for (db_name, table_name, equality_columns, sql_text, create_sql, tuning_advice) in rows:
            item = MissIndex()
            json_format = True
            item.db_name = db_name
            if equality_columns:
                try:
                    table_name_list = json.loads(table_name)
                    equality_columns_list = json.loads(equality_columns)
                    create_sql_list = json.loads(create_sql)
                except Exception,e:
                    json_format = False
                if (json_format == True):
                    for i in range(len(equality_columns_list)):
                        indexAdvice = IndexAdvice()
                        indexAdvice.table_name = table_name_list[i]
                        indexAdvice.equality_columns = equality_columns_list[i]
                        indexAdvice.create_sql = create_sql_list[i]
                        item.index_advice_list.append(indexAdvice)
                else:
                    indexAdvice = IndexAdvice()
                    indexAdvice.table_name = table_name
                    indexAdvice.equality_columns = equality_columns
                    indexAdvice.create_sql = create_sql
                    item.index_advice_list.append(indexAdvice)
            item.sql_text = sql_text

            if tuning_advice:
                tuning_advice_all = json.loads(tuning_advice)
                if tuning_advice_all.has_key("IMPLICITCASTCHECK"):
                    implicit_rule_list = tuning_advice_all["IMPLICITCASTCHECK"]
                    for implicit_check in implicit_rule_list:
                        implicitAdvice = ImplicitAdvice()
                        format_advice = ""
                        format_advice += u"\n\u8be5SQL\u67e5\u8be2\u5b57\u6bb5\u4e2d\n"
                        format_advice += "\t" + implicit_check[0] + "\n"
                        format_advice += "\t" + u"\n\u4f20\u5165\u7684\u6570\u636e\u7c7b\u578b\u4e0e\u8868\u5b57\u6bb5\u5b9a\u4e49\u4e0d\u4e00\u81f4\uff0c\u4f1a\u5bfc\u81f4\u7d22\u5f15\u5931\u6548\uff0c\u5efa\u8bae\u6539\u4e3a\n"
                        format_advice += "\t" + implicit_check[1] + "\n"
                        implicitAdvice.implicit_advice=format_advice
                        item.implicit_advice_list.append(implicitAdvice)

                if tuning_advice_all.has_key("FUNCTIONCHECK"):
                    function_rule_list = tuning_advice_all["FUNCTIONCHECK"]
                    for functioncheck in function_rule_list:
                        functionAdvice = FunctionAdvice()
                        format_advice = ""
                        format_advice += u"\n\u8be5SQL\u4e2d\u4f7f\u7528\u4e86\u51fd\u6570\n"
                        format_advice += "\t" + functioncheck[0] + "\n"
                        if functioncheck[1]:
                            format_advice += "\t" + functioncheck[1] + "\n"
                        format_advice += "\t" + u"\n\u51fd\u6570\u53ef\u80fd\u4f1a\u4f7f\u5b57\u6bb5\u4e0a\u7684\u7d22\u5f15\u5931\u6548\uff0c\u8bf7\u5c3d\u91cf\u907f\u514d\u4e0d\u5fc5\u8981\u7684\u51fd\u6570\n"
                        functionAdvice.function_advice=format_advice
                        item.function_advice_list.append(functionAdvice)

        #    item.formatted_sql = sqlparser.parse(item.sql_text.replace("?", "1"))

            self.missindex_list.append(item)

    def todict(self):       
        return {"missindex_list": [item.todict() for item in self.missindex_list]}

    def get_missindex(self, db_name, formatted_sql):
        for missindex in self.missindex_list:
            if missindex.db_name == db_name and missindex.formatted_sql == formatted_sql:
                return missindex

        return None

if __name__ == "__main__":
    m = MissIndexHolder(145031, 1)
    m.do_get_missIndex()
    m.todict

