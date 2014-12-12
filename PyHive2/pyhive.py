import os
import jpype
import util
import subprocess
import j2p
import hdfs
import tempfile
import getpass
import pandas as pd
import marshal

## Define Global Variable
globalRef = {}

## Temp dictionary for PyUDF variable
exportVars = {}

## Define Class
class Constants(object):
    DEFAULT_FS_HOME = "/pyhive"
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
    def __init__(self, hiveHome, hiveLib, hiveAuxLib, hiveServer2, hadoopHome, hadoopCmd, hadoopConfDir, fsHome):
        self.__hiveHome = hiveHome
        self.__hiveLib = hiveLib
        self.__hiveAuxlib = hiveAuxLib
        self.__hiveServer2 = hiveServer2
        self.__hadoopHome = hadoopHome
        self.__hadoopCmd = hadoopCmd
        self.__hadoopConfDir = hadoopConfDir
        self.__fsHome = fsHome

    def setHiveHome(self, hiveHome):
        self.__hiveHome = hiveHome

    def getHiveHome(self):
        return self.__hiveHome

    def setHiveLib(self, hiveLib):
        self.__hiveLib = hiveLib

    def getHiveLib(self):
        return self.__hiveLib

    def setHiveAuxLib(self, hiveAuxLib):
        self.__hiveAuxLib = hiveAuxLib

    def getHiveAuxLib(self):
        return self.__hiveAuxLib

    def setHiveServer2(self, hiveServer2):
        self.__hiveServer2 = hiveServer2

    def getHiveServer2(self):
        return self.__hiveServer2

    def setHadoopHome(self, hadoopHome):
        self.__hadoopHome = hadoopHome

    def getHadoopHome(self):
        return self.__hadoopHome

    def setHadoopCmd(self, hadoopCmd):
        self.__hadoopCmd = hadoopCmd

    def getHadoopCmd(self):
        return self.__hadoopCmd

    def setHadoopConfDir(self, hadoopConfDir):
        self.__hadoopConfDir = hadoopConfDir

    def getHadoopConfDir(self):
        return self.__hadoopConfDir

    def setFsHome(self, fsHome):
        self.__fsHome = fsHome

    def getFsHome(self):
        return self.__fsHome


class HiveInfo(object):
    def __init__(self):
        self.__host = "127.0.0.1"
        self.__port = 10000
        self.__isServer2 = True

    def setHost(self, host):
        self.__host = host

    def getHost(self):
        return self.__host

    def setPort(self, port):
        self.__port = port

    def getport(self):
        return self.__port

    def setServer2(self, isServer2):
        self.__isServer2 = isServer2

    def isServer2(self):
        return self.__isServer2


class HiveConnection(object):
    def __init__(self, info, session, client):
        self.__info = info
        self.__session = session
        self.__client = client
        self.__fsDefault = ""
        self.__jobTracker = ""

    def getClient(self):
        return self.__client

    def setFsDefault(self, fsDefault):
        self.__fsDefault = fsDefault

    def getFsDefault(self):
        return self.__fsDefault

    def setJobTracker(self, jobTracker):
        self.__jobTracker = jobTracker

    def getJobTracker(self):
        return self.__jobTracker

    def getSession(self):
        return self.__session


class HiveSession(object):
    def __init__(self, user, pseudoUser, fsTmp, fsUdfs, fsScripts, fsLibs):
        self.__user = user
        self.__pseudoUser = pseudoUser
        self.__tempDir = tempfile.mkdtemp()
        self.__fsTmp = fsTmp
        self.__fsUdfs = fsUdfs
        self.__fsScripts = fsScripts
        self.__fsLibs = fsLibs

    def getUser(self):
        return self.__user

    def getPseudoUser(self):
        return self.__pseudoUser

    def getTempDir(self):
        return self.__tempDir

    def getFsTmp(self):
        return self.__fsTmp

    def getFsUdfs(self):
        return self.__fsUdfs

    def getFsScripts(self):
        return self.__fsScripts

    def getFsLibs(self):
        return self.__fsLibs


class Configuration(object):
    def __init__(self, sysEnv):
        self.__sysEnv = sysEnv
        self.__javaParams = {}
        self.__hiveConf = {}
        self.__logLevel = 'warn'
        self.__opParallelLevel = 3L
        self.__fsParallelLevel = 3L
        self.__bigQuerySize = 1024L * 512L

    def getSysEnv(self):
        return self.__sysEnv

    def getJavaParams(self):
        return self.__javaParams

    def getHiveConf(self):
        return self.__hiveConf

    def getLogLevel(self):
        return self.__logLevel

    def getOpParallelLevel(self):
        return self.__opParallelLevel

    def getFsParallelLevel(self):
        return self.__fsParallelLevel

    def getBigQuerySize(self):
        return self.__bigQuerySize


def setGlobalRef(key, value=None):
    global globalRef
    if value is None:
        return globalRef[key]
    else:
        globalRef[key] = value


def init():
    sysEnv = SysEnv(hiveHome=os.environ.get("HIVE_HOME", ""),
                    hiveLib=os.environ.get("HIVE_LIB", ""),
                    hiveAuxLib=os.environ.get("HIVE_AUXLIB", ""),
                    hiveServer2=os.environ.get("HIVESERVER2", True),
                    hadoopHome=os.environ.get("HADOOP_HOME", ""),
                    hadoopCmd=os.environ.get("HADOOP_CMD", ""),
                    hadoopConfDir=os.environ.get("HADOOP_CONF_DIR", ""),
                    fsHome=os.environ.get("HDFS_HOME", ""))

    setGlobalRef("configuration", Configuration(sysEnv))
    # setGlobalRef("service",Service(sysEnv))  //TODO


def startJVM(configuration):
    ## load hive lib
    hivelibs = util.listFiles(configuration.getSysEnv().getHiveHome(), "*.jar")

    ## load hadoop lib
    hadoopCmd = configuration.getSysEnv().getHadoopCmd()

    if not hadoopCmd:
        if configuration.getSysEnv().getHadoopHome():
            hadoopCmd = os.path.join(configuration.getSysEnv().getHadoopHome(), "bin", "hadoop")
        else:
            raise Exception("Both HADOOP_HOME and HADOOP_CMD are missing. "
                            "Please set HADOOP_CMD using nexr.sys.env.hadoop.cmd()")

    hadoopLibPath = subprocess.check_output(hadoopCmd + " classpath", shell=True)
    hadoopLibPath = hadoopLibPath.replace("*", "").split(":")

    classpath = []

    for dir in hadoopLibPath:
        classpath.extend(util.listFiles(dir, "*.jar"))

    pkgPath = os.path.split(__file__)[0]

    classpath.extend(hivelibs)
    classpath.extend(hadoopLibPath)
    classpath.extend(util.listFiles(os.path.join(pkgPath,"lib"), "*.jar"))
    classpath.extend(configuration.getSysEnv().getHadoopConfDir())
    classpath = ":".join(classpath)

    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=%s" % classpath)


def shutdownJVM():
    jpype.shutdownJVM()

def closeConnection(conn):
    client = conn.getClient()
    client.close()


def close():
    shutdownJVM()


def connect(info=HiveInfo(), db="default", user="", password="", properties={}, updateJar=False):
    configuration = setGlobalRef("configuration")

    startJVM(configuration)
    client = j2p.createHiveJdbcClient(info.isServer2())
    client.connect(info.getHost(), info.getport(), db, user, password)

    def fsHome():
        if not configuration.getSysEnv().getFsHome():
            return Constants.DEFAULT_FS_HOME
        else:
            return configuration.getSysEnv().getFsHome()

    def pseudoUser():
        if not user:
            return "anonymous"
        else:
            return user

    def newSession(user, home):
        return HiveSession(user=getpass.getuser(),
                           pseudoUser=user,
                           fsUdfs=hdfsPath(home, user, "udfs"),
                           fsTmp=hdfsPath(home, user, "tmp"),
                           fsScripts=hdfsPath(home, user, "scripts"),
                           fsLibs=hdfsPath(home, user, "libs"))

    home = fsHome()
    user = pseudoUser()
    session = newSession(user, home)

    conn = HiveConnection(info=info, session=session, client=client)

    def fsDefault():
        for val in Constants.FS_DEFAULT_CONF:
            fs = setHive(conn, val)
            if fs:
                fs = util.replaceWithRegex("(localhost|127.0.0.1)", info.getHost(), fs)
                return fs
        return None

    def jobTracker():
        jt = setHive(conn, Constants.MAPRED_JOB_TRACKER_CONF)
        if jt:
            jt = util.replaceWithRegex("(localhost|127.0.0.1)", info.getHost(), jt)
            return jt
        return None

    conn.setFsDefault(fsDefault())
    conn.setJobTracker(jobTracker())

    def setHiveconf():
        hiveConf = configuration.getHiveConf()
        for key in hiveConf:
            setHive(conn, key, hiveConf[key])

    def setUdfs():
        # TODO make Hive UDF for PyHive2
        execute(conn, "create temporary function %s as \"%s\"" % ("Py", "com.nexr.pyhive.hive.udf.PyUDF"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("PyA","com.nexr.pyhive.hive.udf.RUDAF"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("unfold","com.nexr.pyhive.hive.udf.GenericUDTFUnFold"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("expand","com.nexr.pyhive.hive.udf.GenericUDTFExpand"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("rkey","com.nexr.pyhive.hive.udf.RangeKeyUDF"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("scale","com.nexr.pyhive.hive.udf.ScaleUDF"))
        # execute(conn,"create temporary function %s as \"%s\"" % ("rank_cor","com.nexr.pyhive.hive.udf.GenericUDFRankCorrelation"))
        pass

    def makeHdfsDirs():
        hdfsDirs = [conn.getSession().getFsUdfs(),
                    conn.getSession().getFsTmp(),
                    conn.getSession().getFsScripts(),
                    conn.getSession().getFsLibs()]

        for dir in hdfsDirs:
            if hdfs.dfsExists(conn, dir) == False:
                hdfs.dfsMkdirs(conn, dir)
                hdfs.dfsChmod(conn, "777", dir)

    def addJar(jarPath, hdfsJarPath):
        if updateJar or hdfs.dfsExists(conn, hdfsJarPath) == False:
            hdfs.dfsPut(conn, jarPath, hdfsJarPath, overWrite=True)
        execute(conn, "add jar %s" % hdfsJarPath)

    def setMapredChildEnv():
        udfutils = j2p.JUDFUtils()
        defaultFsEnvName = udfutils.getDefaultFileSystemPropertyName()
        baseDirEnvName = udfutils.getBaseDirectoryPropertyName()

        setHive(conn, Constants.MAPRED_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        setHive(conn, Constants.MAPRED_MAP_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        setHive(conn, Constants.MAPRED_REDUCE_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        setHive(conn, baseDirEnvName, conn.getSession().getFsUdfs())

    setHiveconf()
    makeHdfsDirs()

    # TODO add pyhive version into path
    jarPath = "PyHive2/lib/pyhive.jar"
    hdfsJarPath = hdfsPath(conn.getFsDefault(), conn.getSession().getFsLibs(), "pyhive.jar")
    addJar(jarPath, hdfsJarPath)

    setUdfs()
    setMapredChildEnv()
    debugOff()

    if db != "default":
        useDatabases(conn, db)

    return conn


def query(conn, query):
    client = conn.getClient()
    dfModel = client.query(query)
    df = util.convertDataFrame(dfModel)
    dfModel.close()
    return df


def execute(conn, query):
    client = conn.getClient()
    dfModel = client.execute(query)
    df = util.convertDataFrame(dfModel)
    dfModel.close()
    return df


def queryForString(conn, query):
    client = conn.getClient()
    dfModel = client.query(query)
    dfString = dfModel.toString()
    dfModel.close()
    return dfString


def execute(conn, query):
    client = conn.getClient()
    client.execute(query)


def loadTable(conn, tableName, limit=-1):
    def copyData():
        srcDir = tableLocation(conn, tableName)
        ## TODO
        dstDir = os.getcwd()
        hdfs.dfsGet(conn, srcDir, dstDir)
        dataDir = os.path.join(dstDir, tableName)
        return dataDir

    def loadData(dataDir):
        d = None;
        for base, dirs, files in os.walk(dataDir):
            for file in files:
                path = os.path.join(base, file)

                if os.path.getsize(path) == 0:
                    continue
                if file.endswith(".crc"):
                    continue

                data = pd.read_csv(path, header=None, sep="\001")
                if d == None:
                    d = data
                else:
                    d.append(data, ignore_index=True)

        if d is not None:
            d.columns = tableColumns(conn, tableName)

        return d

    dataDir = copyData()
    df = loadData(dataDir)

    # delete temp directory
    util.unlink(dataDir)
    return df


def writeTable(conn, data, tableName, sep=",", na=""):
    def writeDataToLocal():
        # file <- wf(conn@session, table.name, postfix = sprintf("_%s", nexr.random.key()))
        file = wf(tableName, postfix="_" + util.randomKeyGen())
        data.to_csv(file, sep=sep, header=None, index=False)
        return file

    def findColTypes():
        coltypes = data.dtypes
        coltypes[coltypes == 'int64'] = "int"
        coltypes[coltypes == 'float64'] = "float"
        coltypes[coltypes == 'object'] = "string"
        coltypes[coltypes == 'category'] = "string"
        coltypes[coltypes == 'bool'] = "boolean"

        return coltypes.tolist()

    def loadDataIntoHive(dataFile, coltypes):
        dst = hdfsPath(conn.getSession().getFsTmp(), os.path.basename(dataFile))
        hdfs.dfsPut(conn, dataFile, dst, srcDel=False, overWrite=True)

        colnames = data.columns.tolist()
        colnames = util.replaceWithRegex("[^a-zA-Z0-9]", "", colnames)
        lcolnames = [x.lower() for x in colnames]

        if util.isduplicated(lcolnames):
            raise Exception("Hive doesn't support case-sensitive column-names: %s" % ",".join(colnames))

        cols = []
        for i in xrange(len(colnames)):
            cols.append(colnames[i] + " " + coltypes[i])

        sql = "create table %s ( %s ) row format delimited fields terminated by '%s'" % (tableName, ",".join(cols), sep)
        execute(conn, sql)

        sql = "load data inpath '%s' overwrite into table %s" % (dst, tableName)
        execute(conn, sql)

    file = writeDataToLocal()
    coltypes = findColTypes()
    loadDataIntoHive(file, coltypes)
    # delete local tmp file
    rmf(file)


def dataSize(conn, tableName, summary=False):
    location = tableLocation(conn, tableName)
    dataInfo = hdfs.dfsDiskUsage(conn, location, summary)
    return dataInfo["length"].tolist()


def queryBig(conn, sql, fetchSize=5000L, limit=-1L, saveAs=None):
    tableName = None
    if saveAs == True:
        tableName = saveAs
    else:
        tableName = "rs_%s" % queryId(conn)

    execute(conn, "create table %s as %s" % (tableName, sql))

    if saveAs:
        length = dataSize(conn, tableName)
        return [tableName, length]

    data = loadTable(conn, tableName, limit)
    dropTable(conn, [tableName])

    return data


def hdfsPath(a, *p):
    paths = [a]
    for path in p:
        paths.extend([path])
    return "/".join(paths)


def wf(name, prefix=None, postfix=None):
    if prefix is not None:
        name = prefix + name

    if postfix is not None:
        name = name + postfix

    return os.path.join(os.getcwd(), name)


def rmf(file):
    os.remove(file)


def descTable(connection, tableName, extended=False):
    result = None
    if extended == True:
        sql = "describe extended %s" % tableName
        result = queryForString(connection, sql)
    else:
        sql = "describe %s" % tableName
        result = query(connection, sql)
        result = result.applymap(util.trim)

    return result


def tableColumns(connection, tableName):
    desc = descTable(connection, tableName)
    return desc['col_name'].tolist()


def tableLocation(connection, tableName):
    desc = descTable(connection, tableName, True)

    location = util.searchWithRegex(desc, "location\\s*:\\s*[^,]+", 0)
    location = util.replaceWithRegex("^location\\s*:(\\s*)", "", location)
    location = util.replaceWithRegex("^[A-Za-z ]*://[^:/]*(:[0-9]+)?", "", location)

    return location


def dbName(connection, tableName):
    desc = descTable(connection, tableName, True)

    db = util.searchWithRegex(desc, "dbName\\s*:\\s*[^,]+", 0)
    db = util.splitWithRegex(":", db)[1]
    db = util.replaceWithRegex("^\\s+|\\s+$", "", db)

    return db


def existsTable(connection, tableName):
    tables = showTables(connection, "^%s$" % tableName)
    return len(tables.index) == 1


def dropTable(connection, tableNames):
    for tableName in tableNames:
        execute(connection, "drop table if exists %s" % tableName)


def showTables(connection, tableNamePattern=".*"):
    result = query(connection, "show tables")
    return result[result.tab_name.str.match(tableNamePattern)]


def showDatabases(connection):
    result = query(connection, "show databases")
    return result


def useDatabases(connection, database):
    execute(connection, "use %s" % database)


def setHive(connection, key=None, value=None):
    if value == None:
        if key == None:
            return query(connection, "set -v")
        else:
            result = queryForString(connection, "set %s" % key)
            result = util.splitWithRegex("=", result)

            if len(result) == 2:
                return util.replaceWithRegex("^\\s+|\\s+$", "", result[1])
            else:
                return None;
    else:
        execute(connection, "set %s=%s" % (key, value))


def queryId(conn):
    user = conn.getSession().getPseudoUser()

    id = util.replaceWithRegex("^[A-Za-z ]*://[^:/]*(:[0-9]+)?", "", user)
    id = id.lower()
    if len(id) == 0:
        id = "rs"

    return "%s_%s" % (id, util.randomKeyGen())


def assignVars(name, value):
    exportVars[name] = value

def export(conn, name, func):

    # TODO export variable

    # export function code into HDFS
    funcPath = os.path.join(os.getcwd(), "%s.func" % name)
    data = marshal.dumps(func.func_code)

    file_i = open(funcPath,'w')
    file_i.write(data)
    file_i.close()

    dst = hdfsPath(conn.getSession().getFsUdfs(), "%s.func" % name)
    hdfs.dfsPut(conn,funcPath,dst,overWrite=True)

    return hdfs.dfsExists(conn,dst)

# nexr.query.id <- function(connection) {
# id <- tolower(gsub("[^[:alnum:]]", "", connection@session@pseudo.user))
# if (nchar(id) == 0) {
# id <- "rs"
# }
#
# sprintf("%s_%s", id, nexr.random.key())
# }

def debugOff():
    # TODO load log4j and set log level as "ERROR"
    # j.logger <- j2r.log4j.Logger.class()
    # root.logger <- j.logger$getRootLogger()
    # root.logger$setLevel(j2r.log4j.Level.class()$ERROR)
    pass
