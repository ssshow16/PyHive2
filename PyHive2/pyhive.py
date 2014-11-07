import os
import jpype
import util
import subprocess
import j2p

from pandas import DataFrame

class Constants(object):
    DEFAULT_FS_HOME = "/rhive"
    FS_DEFAULT_CONF = ("fs.default.name", "fs.defaultFS")
    MAPRED_JOB_TRACKER_CONF = "mapred.job.tracker"
    MAPRED_JOB_NAME_CONF = "mapred.job.name"
    MAPRED_CHILD_ENV_CONF = "mapred.child.env"
    MAPRED_MAP_CHILD_ENV_CONF = "mapred.map.child.env"
    MAPRED_REDUCE_CHILD_ENV_CONF = "mapred.reduce.child.env"
    HIVE_WAREHOUSE_DIR_CONF = "hive.metastore.warehouse.dir"
    HIVE_SERVER2_ENABLE_DOAS_CONF = "hive.server2.enable.doAs"
    HIVE_QUERYLOG_LOCATION_CONF = "hive.querylog.location"

class Context(object):
    def __init__(self):
        self.__hiveHome = os.environ.get("HIVE_HOME","")
        self.__hiveLib = os.environ.get("HIVE_LIB","")
        self.__hiveAuxlib = os.environ.get("HIVE_AUXLIB","")
        self.__hiveServer2 = os.environ.get("HIVESERVER2",True)
        self.__hadoopHome = os.environ.get("HADOOP_HOME","")
        self.__hadoopCmd = os.environ.get("HADOOP_CMD","")
        self.__fsHome = os.environ.get("HADOOP_HOME","")
    def setHiveHome(self,hiveHome):
        self.__hiveHome = hiveHome
    def getHiveHome(self):
        return self.__hiveHome
    def setHiveLib(self,hiveLib):
        self.__hiveLib = hiveLib
    def getHiveLib(self):
        return self.__hiveLib
    def setHiveAuxLib(self,hiveAuxLib):
        self.__hiveAuxLib = hiveAuxLib
    def getHiveAuxLib(self):
        return self.__hiveAuxLib
    def setHiveServer2(self,hiveServer2):
        self.__hiveServer2 = hiveServer2
    def getHiveServer2(self):
        return self.__hiveServer2
    def setHadoopHome(self,hadoopHome):
        self.__hadoopHome = hadoopHome
    def getHadoopHome(self):
        return self.__hadoopHome
    def setHadoopCmd(self,hadoopCmd):
        self.__hadoopCmd = hadoopCmd
    def getHadoopCmd(self):
        return self.__hadoopCmd
    def setFsHome(self,fsHome):
        self.__fsHome = fsHome
    def getFsHome(self):
        return self.__fsHome

class HiveInfo(object):
    def __init__(self):
        self.__host = "127.0.0.1"
        self.__port = 10000
        self.__isServer2 = True

    def setHost(self,host):
        self.__host = host
    def getHost(self):
        return self.__host
    def setPort(self,port):
        self.__port = port
    def getport(self):
        return self.__port
    def setServer2(self,isServer2):
        self.__isServer2 = isServer2
    def isServer2(self):
        return self.__isServer2

class HiveConnection(object):
    def __init__(self, hiveJdbcClient):
        self.__hiveJdbcClient = hiveJdbcClient
    def getHiveJdbcClient(self):
        return self.__hiveJdbcClient
    def setFsDefault(self,fsDefault):
        self.__fsDefault = fsDefault
    def getFsDefault(self):
        return self.__fsDefault

def init(hadoopHome, hiveHome):
    pass

def startJVM(context):

    ## load hive lib
    hivelibs = util.listFiles(context.getHiveHome(),"*.jar")

    ## load hadoop lib
    hadoopCmd = context.getHadoopCmd()

    if hadoopCmd == '' or len(hadoopCmd) <= 0:
        if context.getHadoopHome():
            hadoopCmd = os.path.join(context.getHadoopHome(),"bin","hadoop")
            pass
        else:
            raise Exception("Both HADOOP_HOME and HADOOP_CMD are missing. "
                            "Please set HADOOP_CMD using nexr.sys.env.hadoop.cmd()")

    hadoopLibPath = subprocess.check_output(hadoopCmd + " classpath",shell=True)
    hadoopLibPath = hadoopLibPath.replace("*","").split(":")

    classpath = []

    for dir in hadoopLibPath:
        classpath.extend(util.listFiles(dir,"*.jar"))

    classpath.extend(hivelibs)
    classpath.extend(hadoopLibPath)
    classpath.extend([os.path.join(os.path.abspath('./out/production/PyHive2'))])

    classpath = ":".join(classpath)

    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=%s" % classpath)

def shutdownJVM():
    jpype.shutdownJVM()

def closeConnection(connection):
    client = connection.getHiveJdbcClient()
    client.close()

def close():
    shutdownJVM()

def connect(context = Context(), hiveInfo = HiveInfo(), db = "default", user = "", password = "", properties = []):
    startJVM(context)
    jHiveJdbcClient = j2p.createHiveJdbcClient(context)
    jHiveJdbcClient.connect(hiveInfo.getHost(), hiveInfo.getport(), db, user, password)

    hiveConnection = HiveConnection(jHiveJdbcClient)
    ## TODO
    hiveConnection.setFsDefault("hdfs://localhost:9000")

    return hiveConnection

def query(connection, query):
    client = connection.getHiveJdbcClient()
    rs = client.query(query)
    df = convertResultSetToDataFrame(rs)
    rs.close()
    return df

def queryForString(connection, query):
    client = connection.getHiveJdbcClient()
    rs = client.query(query)
    string = convertResultSetToString(rs)
    rs.close()
    return string

def execute(connection, query):
    client = connection.getHiveJdbcClient()
    client.execute(query)

def descTable(connection, tableName, extended = False):

    result = None
    if extended:
        sql = "describe extended %s" % tableName
        result = queryForString(connection, sql)
    else:
        sql = "describe %s" % tableName
        result = query(connection, sql)
        result = result.applymap(util.trim)

    return result

def tableLocation(connection, tableName):
    desc = descTable(connection,tableName,True)
    location = util.searchWithRegex(desc,"location\\s*:\\s*[^,]+",0)
    return location

def existsTable(connection, tableName):
    tables = showTables(connection, "^%s$" % tableName)
    return len(tables.index) == 1

def dropTable(connection, tableNames):
    for tableName in tableNames:
        execute(connection,"drop table if exists %s" % tableName)

# def dataSize(connection, tableName):
#     location = tableLocation(connection,tableName)
#     dataInfo =


def showTables(connection, tableNamePattern=".*"):
    result = query(connection,"show tables")
    return result[result.tab_name.str.match(tableNamePattern)]

def showDatabases(connection):
    result = query(connection,"show databases")
    return result

def useDatabases(connection, database):
    execute(connection,"use %s" % database)

def set(connection, key=None, value=None):
    if value == None:
        if key == None:
            return query(connection,"set -v")
        else:
            return query(connection,"set %s" % key)
    else:
        execute(connection,"set %s=%s" % (key,value))

def convertResultSetToDataFrame(rs):

    colNames = util.getColumnNames(rs)
    colTypes = util.getColumnTypes(rs)
    colCnt = util.getColumnCount(rs)

    rows = []
    while rs.next():
        row = []
        for i in range(colCnt):
            val = util.getColumnValue(rs, i+1, colTypes[i])
            row.extend([val])
        rows.append(row)

    df = DataFrame(rows, columns=colNames)

    return df

def convertResultSetToString(rs):

    colCnt = util.getColumnCount(rs)

    result = ""
    while rs.next():

        if len(result) > 0:
            result += "\n"

        for i in range(colCnt):
            val = util.getColumnValue(rs, i+1, "string")
            if val == None:
                result = result + ""
            else:
                result = result + val

    return result