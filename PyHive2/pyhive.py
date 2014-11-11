import os
import jpype
import util
import subprocess
import j2p
import hdfs
import tempfile
import pandas as pd

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

class SysEnv(object):
    def __init__(self, hiveHome, hiveLib, hiveAuxLib, hiveServer2, hadoopHome, hadoopCmd, fsHome):
        self.__hiveHome = hiveHome
        self.__hiveLib = hiveLib
        self.__hiveAuxlib = hiveAuxLib
        self.__hiveServer2 = hiveServer2
        self.__hadoopHome = hadoopHome
        self.__hadoopCmd = hadoopCmd
        self.__fsHome = fsHome
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
    def __init__(self, hiveJdbcClient, session):
        self.__hiveJdbcClient = hiveJdbcClient
        self.__session = session
    def getHiveJdbcClient(self):
        return self.__hiveJdbcClient
    def setFsDefault(self,fsDefault):
        self.__fsDefault = fsDefault
    def getFsDefault(self):
        return self.__fsDefault
    def getSession(self):
        return self.__session

class HiveSession(object):
    def __init__(self, user, fsTmp, fsUdfs):
        self.__user = user
        self.__pseudoUser = user
        self.__tempDir = tempfile.mkdtemp()
        self.__fsTmp = fsTmp
        self.__fsUdfs = fsUdfs
    def getUser(self):
        return self.__user
    def getTempDir(self):
        return self.__tempDir
    def getFsTmp(self):
        return self.__fsTmp
    def getFsUdfs(self):
        return self.__fsUdfs

class pyhiveGlobalInjector(object):
    pass

def init():
    sysEnv = SysEnv(hiveHome=os.environ.get("HIVE_HOME",""),
                    hiveLib=os.environ.get("HIVE_LIB",""),
                    hiveAuxlib = os.environ.get("HIVE_AUXLIB",""),
                    hiveServer2 = os.environ.get("HIVESERVER2",True),
                    hadoopHome = os.environ.get("HADOOP_HOME",""),
                    hadoopCmd = os.environ.get("HADOOP_CMD",""),
                    fsHome = os.environ.get("HADOOP_HOME",""))



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

    # def fsHome:
    #     if is.empty(configuration@sys.env@fs.home):
    #         return(configuration@constants@DEFAULT.FS.HOME)
    #     else:
    #         return(configuration@sys.env@fs.home)

    hiveConnection = HiveConnection(jHiveJdbcClient)
    ## TODO
    hiveConnection.setFsDefault("hdfs://localhost:9000")

    return hiveConnection

def query(connection, query):
    client = connection.getHiveJdbcClient()
    dfModel = client.query(query)
    df = util.convertDataFrame(dfModel)
    dfModel.close()

    return df

def execute(connection, query):
    client = connection.getHiveJdbcClient()
    dfModel = client.execute(query)
    df = util.convertDataFrame(dfModel)
    dfModel.close()

    return df

def queryForString(connection, query):
    client = connection.getHiveJdbcClient()
    dfModel = client.query(query)
    dfString = dfModel.toString()
    dfModel.close()
    return dfString

def execute(connection, query):
    client = connection.getHiveJdbcClient()
    client.execute(query)

def loadTable(connection,tableName, limit=-1):
    def copyData():
        srcDir = tableLocation(connection,tableName)
        ## TODO
        dstDir = os.getcwd()
        hdfs.dfsGet(connection, srcDir, dstDir)
        dataDir = os.path.join(dstDir,tableName)
        return dataDir

    def loadData(dataDir):
        d = None;
        for base, dirs, files in os.walk(dataDir):
            for file in files:
                path = os.path.join(base,file)
                if os.path.getsize(path) == 0:
                    continue
                if file.endswith(".crc"):
                    continue

                data = pd.read_csv(path,header=None)
                if d == None:
                    d = data
                else:
                    d.append(data,ignore_index=True)

        if d is not None:
            d.columns = tableColumns(connection,tableName)

        return d

    dataDir = copyData()
    df = loadData(dataDir)

    return df

def writeTable(connection, data, tableName, sep = ",", na = ""):
    def writeDataToLocal():
        #file <- wf(connection@session, table.name, postfix = sprintf("_%s", nexr.random.key()))
        file = wf(tableName, postfix="_" + util.randomKeyGen())
        data.to_csv(file, sep = sep, header=None, index = False)
        return file

    # def findColtypes():
    #     type.character <- sapply(data, is.character)
    #     type.numeric   <- sapply(data, is.numeric)
    #     type.integer   <- sapply(data, is.integer)
    #     type.logical   <- sapply(data, is.logical)
    #     type.factor    <- sapply(data, is.factor)
    #
    #     coltypes <- character(length(data))
    #     coltypes[type.character] <- "string"
    #     coltypes[type.numeric] <- "double"
    #     coltypes[type.integer] <- "int"
    #     coltypes[type.logical] <- "boolean"
    #     coltypes[type.factor] <- "string"
    #
    #     names(coltypes) <- names(data)
    #     return(coltypes)

    def loadDataIntoHive(dataFile,coltypes):
        #dst <- hdfs.path(connection@session@fs.tmp, basename(data.file))
        dst = hdfsPath(connection)
        os.path.basename(dataFile)

        dst = ""
        hdfs.dfsPut(connection, dataFile, dst, srcDel = False, overWrite = True)

        colnames <- gsub("[^[:alnum:]_]+", "", names(coltypes))

        # if (any(duplicated(tolower(colnames))) == TRUE) {
        #     stop(paste("Hive doesn't support case-sensitive column-names: ", paste(colnames, collapse = ",")))
        # }

        cols = paste(colnames, coltypes)
        sql = "create table %s ( %s ) row format delimited fields terminated by '%s'" % (tableName, ",".join(cols), sep)
        execute(connection,sql)

        sql = "load data inpath '%s' overwrite into table %s" % (dst,tableName)
        execute(connection, l.qry)

def hdfsPath(a,*p):
    paths = [a]
    for path in p:
        paths.extend(path)
    return "/".join(paths)

def wf(name, prefix=None, postfix=None):
    if prefix is not None:
        name = prefix + name

    if postfix is not None:
        name = name + postfix

    return os.path.join(os.getcwd(),name)

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

def tableColumns(connection, tableName):
    desc = descTable(connection,tableName)
    return desc['col_name'].tolist()

def tableLocation(connection, tableName):
    desc = descTable(connection,tableName,True)

    location = util.searchWithRegex(desc,"location\\s*:\\s*[^,]+",0)
    location = util.replaceWithRegex("^location\\s*:(\\s*)","",location)
    location = util.replaceWithRegex("^[A-Za-z ]*://[^:/]*(:[0-9]+)?","",location)

    return location

def existsTable(connection, tableName):
    tables = showTables(connection, "^%s$" % tableName)
    return len(tables.index) == 1

def dropTable(connection, tableNames):
    for tableName in tableNames:
        execute(connection,"drop table if exists %s" % tableName)


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
