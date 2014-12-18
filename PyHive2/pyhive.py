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

## Define Class
class Constants(object):
    """
    Constants class
    """
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
    """
    System Environment class.

    This class contains the following system environments.
     - HIVE_HOME        : hive home path
     - HIVE_LIB         : hive lib path
     - HIVE_AUXLIB      : hive aux lib path
     - HIVESERVER2      : hiveserver2 use
     - HADOOP_HOME      : hadoop home path
     - HADOOP_CMD       : hadoop cmd path
     - HADOOP_CONF_DIR  : hadoop configuration path
     - HDFS_HOME        : pyhive home path on HDFS
    """

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
    """
    Hive Server Information
    """
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
    """
    Hive session information
    """
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
    """
    Configuration class
    """
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


def _set_global_ref(key, value=None):
    global globalRef
    if value is None:
        return globalRef[key]
    else:
        globalRef[key] = value


def init():
    """
    Load System environment and create Configuration with these values
    """
    sysEnv = SysEnv(hiveHome=os.environ.get("HIVE_HOME", ""),
                    hiveLib=os.environ.get("HIVE_LIB", ""),
                    hiveAuxLib=os.environ.get("HIVE_AUXLIB", ""),
                    hiveServer2=os.environ.get("HIVESERVER2", True),
                    hadoopHome=os.environ.get("HADOOP_HOME", ""),
                    hadoopCmd=os.environ.get("HADOOP_CMD", ""),
                    hadoopConfDir=os.environ.get("HADOOP_CONF_DIR", ""),
                    fsHome=os.environ.get("HDFS_HOME", ""))

    _set_global_ref("configuration", Configuration(sysEnv))
    # _set_global_ref("service",Service(sysEnv))  //TODO


def start_jvm(configuration):
    """ make classpath list and init JVM
    :param configuration: Configuration
        get HIVE_HOME, HADOOP_HOME from this
    """
    ## load hive lib
    hivelibs = util.list_files(configuration.getSysEnv().getHiveHome(), "*.jar")

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
        classpath.extend(util.list_files(dir, "*.jar"))

    pkgPath = os.path.split(__file__)[0]

    classpath.extend(hivelibs)
    classpath.extend(hadoopLibPath)
    classpath.extend(util.list_files(os.path.join(pkgPath, "lib"), "*.jar"))
    classpath.extend(configuration.getSysEnv().getHadoopConfDir())
    classpath = ":".join(classpath)

    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=%s" % classpath)


def shutdown_jvm():
    """stop VM
    TODO how to manage multi JVM
    """
    jpype.shutdownJVM()


def close_connection(conn):
    """ close connection
    :param conn: Connection
    """
    client = conn.getClient()
    client.close()


def close():
    """close pyhive
    """
    shutdown_jvm()


def connect(info=HiveInfo(), db="default", user="", password="", properties={}, updateJar=False):
    """ connect to Hive

    :param info: HiveInfo
    :param db: string
        Hive Database Name
    :param user: string
        Hive User
    :param password: string
        Hive Password
    :param properties: dictionary
        JDBC URL Properties
    :param updateJar: boolean
        if update jar on HDFS

    :return: Connection
    """
    configuration = _set_global_ref("configuration")

    start_jvm(configuration)
    client = j2p.create_JHiveJdbcClient(info.isServer2())
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
                           fsUdfs=hdfs_path_join(home, user, "udfs"),
                           fsTmp=hdfs_path_join(home, user, "tmp"),
                           fsScripts=hdfs_path_join(home, user, "scripts"),
                           fsLibs=hdfs_path_join(home, user, "libs"))

    home = fsHome()
    user = pseudoUser()
    session = newSession(user, home)

    conn = HiveConnection(info=info, session=session, client=client)

    def fsDefault():
        for val in Constants.FS_DEFAULT_CONF:
            fs = execute_set_hive(conn, val)
            if fs:
                fs = util.replace_with_regex("(localhost|127.0.0.1)", info.getHost(), fs)
                return fs
        return None

    def jobTracker():
        jt = execute_set_hive(conn, Constants.MAPRED_JOB_TRACKER_CONF)
        if jt:
            jt = util.replace_with_regex("(localhost|127.0.0.1)", info.getHost(), jt)
            return jt
        return None

    conn.setFsDefault(fsDefault())
    conn.setJobTracker(jobTracker())

    def setHiveconf():
        hiveConf = configuration.getHiveConf()
        for key in hiveConf:
            execute_set_hive(conn, key, hiveConf[key])

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
            if hdfs.exists(conn, dir) == False:
                hdfs.mkdirs(conn, dir)
                hdfs.chmod(conn, "777", dir)

    def addJar(jarPath, hdfsJarPath):
        if updateJar or hdfs.exists(conn, hdfsJarPath) == False:
            hdfs.put(conn, jarPath, hdfsJarPath, overWrite=True)
        execute(conn, "add jar %s" % hdfsJarPath)

    def setMapredChildEnv():
        udfutils = j2p.get_JUDFUtils()
        defaultFsEnvName = udfutils.getDefaultFileSystemPropertyName()
        baseDirEnvName = udfutils.getBaseDirectoryPropertyName()

        execute_set_hive(conn, Constants.MAPRED_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        execute_set_hive(conn, Constants.MAPRED_MAP_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        execute_set_hive(conn, Constants.MAPRED_REDUCE_CHILD_ENV_CONF,
                "%s=%s,%s=%s" % (defaultFsEnvName, conn.getFsDefault(), baseDirEnvName, conn.getSession().getFsUdfs()))
        execute_set_hive(conn, baseDirEnvName, conn.getSession().getFsUdfs())

    setHiveconf()
    makeHdfsDirs()

    # TODO add pyhive version into path
    jarPath = "PyHive2/lib/pyhive.jar"
    hdfsJarPath = hdfs_path_join(conn.getFsDefault(), conn.getSession().getFsLibs(), "pyhive.jar")
    addJar(jarPath, hdfsJarPath)

    setUdfs()
    setMapredChildEnv()
    debug_off()

    if db != "default":
        use_databases(conn, db)

    return conn


def query(conn, query):
    """ Execute the query and return the result.

    :param conn: Connection
    :param query: string
        query statement.
    :return: pandas.DataFrame
        the query result
    """
    client = conn.getClient()
    dfModel = client.query(query)
    df = util.convert_data_frame(dfModel)
    dfModel.close()
    return df


def execute(conn, query):
    """ Execute the query

    :param conn: Connection
    :param query: string
        query statement.
    """
    client = conn.getClient()
    dfModel = client.execute(query)
    df = util.convert_data_frame(dfModel)
    dfModel.close()
    return df


def query_for_string(conn, query):
    """ Execute the query and return the result.

    :param conn: Connection
    :param query: string
        query statement.

    :return: string
        the query result
    """
    client = conn.getClient()
    dfModel = client.query(query)
    dfString = dfModel.toString()
    dfModel.close()
    return dfString


# def execute(conn, query):
# client = conn.getClient()
# client.execute(query)


def load_table(conn, tableName, limit=-1):
    """ Load hive table data

    :param conn: Connection
    :param tableName: string
        hive table name
    :param limit: long
        query size limit?

    :return: pandas.DataFrame
        hive table data
    """

    def copy_data():
        srcDir = table_location(conn, tableName)
        ## TODO
        dstDir = os.getcwd()
        hdfs.get(conn, srcDir, dstDir)
        dataDir = os.path.join(dstDir, tableName)
        return dataDir

    def load_data(dataDir):
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
            d.columns = table_columns(conn, tableName)

        return d

    dataDir = copy_data()
    df = load_data(dataDir)

    # delete temp directory
    util.unlink(dataDir)
    return df


def write_table(conn, data, tableName, sep=",", na=""):
    """ write pandas.DataFrame into Hive

    :param conn: Connection
    :param data: pandas.DataFrame
    :param tableName: String
        hive table name
    :param sep: string
        seperate character
    :param na: string
        NA data string
    """

    def write_data_to_local():
        # file <- wf(conn@session, table.name, postfix = sprintf("_%s", nexr.random.key()))
        file = wf(tableName, postfix="_" + util.gen_random_key())
        data.to_csv(file, sep=sep, header=None, index=False)
        return file

    def find_col_types():
        coltypes = data.dtypes
        coltypes[coltypes == 'int64'] = "int"
        coltypes[coltypes == 'float64'] = "float"
        coltypes[coltypes == 'object'] = "string"
        coltypes[coltypes == 'category'] = "string"
        coltypes[coltypes == 'bool'] = "boolean"

        return coltypes.tolist()

    def load_data_into_hive(dataFile, coltypes):
        dst = hdfs_path_join(conn.getSession().getFsTmp(), os.path.basename(dataFile))
        hdfs.put(conn, dataFile, dst, srcDel=False, overWrite=True)

        colnames = data.columns.tolist()
        colnames = util.replace_with_regex("[^a-zA-Z0-9]", "", colnames)
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

    file = write_data_to_local()
    coltypes = find_col_types()
    load_data_into_hive(file, coltypes)
    # delete local tmp file
    rmf(file)


def data_size(conn, tableName, summary=False):
    """ get the size of table
    :param conn: Connection
    :param tableName: string
        the hive table name
    :param summary:
    :return: long
        the size of hive table
    """
    location = table_location(conn, tableName)
    dataInfo = hdfs.du(conn, location, summary)
    return dataInfo["length"].tolist()


def query_big(conn, query, fetchSize=5000L, limit=-1L, saveAs=None):
    """ execute the query for Big Data

    :param conn: Connection
    :param query: string
        query statement
    :param fetchSize:
    :param limit:
    :param saveAs:
    :return: pandas.DataFrame
        the result of query
    """
    tableName = None
    if saveAs == True:
        tableName = saveAs
    else:
        tableName = "rs_%s" % gen_query_id(conn)

    execute(conn, "create table %s as %s" % (tableName, query))

    if saveAs:
        length = data_size(conn, tableName)
        return [tableName, length]

    data = load_table(conn, tableName, limit)
    drop_table(conn, [tableName])

    return data


def hdfs_path_join(a, *p):
    """ make file path for HDFS

    :param a: string
        start path
    :param p: list

    :return: string
        hdfs path

    """
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
    """ remove file

    :param file: string
        file path
    """
    os.remove(file)


def desc_table(connection, tableName, extended=False):
    """ get the description of hive table

    :param connection: Connection
    :param tableName: string
        hive table name
    :param extended:
        if True, get the detail description of hive table. default is False
    :return:
        if extended is True, string including the detail description of hive table.
        Otherwise, pandas.DataFrame including table scheme info.
    """
    result = None
    if extended == True:
        sql = "describe extended %s" % tableName
        result = query_for_string(connection, sql)
    else:
        sql = "describe %s" % tableName
        result = query(connection, sql)
        result = result.applymap(util.trim)

    return result


def table_columns(connection, tableName):
    """ get column names

    :param connection: Connection
    :param tableName: string
        hive table name
    :return: list
        columns
    """
    desc = desc_table(connection, tableName)
    return desc['col_name'].tolist()


def table_location(connection, tableName):
    """ get hdfs path for hive table

    :param connection: Connection
    :param tableName: string
        hive table name
    :return: string
        hdfs path
    """
    desc = desc_table(connection, tableName, True)

    location = util.search_with_regex(desc, "location\\s*:\\s*[^,]+", 0)
    location = util.replace_with_regex("^location\\s*:(\\s*)", "", location)
    location = util.replace_with_regex("^[A-Za-z ]*://[^:/]*(:[0-9]+)?", "", location)

    return location


def dbname(connection, tableName):
    """ get database name where hive table is

    :param connection: Connection
    :param tableName: string
        hive table name
    :return: string
        database name
    """
    desc = desc_table(connection, tableName, True)

    db = util.search_with_regex(desc, "dbName\\s*:\\s*[^,]+", 0)
    db = util.split_with_regex(":", db)[1]
    db = util.replace_with_regex("^\\s+|\\s+$", "", db)

    return db


def exists_table(connection, tableName):
    """ check exists table
    :param connection: Connection
    :param tableName: string
        hive table name
    :return: boolean
        if exists table, return True.
        Otherwise, return False.
    """
    tables = show_tables(connection, "^%s$" % tableName)
    return len(tables.index) == 1


def drop_table(connection, tableNames):
    """ drop table

    :param connection: Connection
    :param tableNames: string
        hive table name
    """
    for tableName in tableNames:
        execute(connection, "drop table if exists %s" % tableName)


def show_tables(connection, tableNamePattern=".*"):
    """ show the table list in current database

    :param connection: Connection
    :param tableNamePattern: String
        table name regex pattern. default is all table

    :return: list
        the table list matching tableNamePattern
    """
    result = query(connection, "show tables")
    return result[result.tab_name.str.match(tableNamePattern)]


def show_databases(connection):
    """ show the database list

    :param connection: Connection
    :return: list
        the database list
    """
    result = query(connection, "show databases")
    return result


def use_databases(connection, database):
    """ change current database as "database"

    :param connection: Connection
    :param database: string
        database name
    """
    execute(connection, "use %s" % database)


def execute_set_hive(connection, key=None, value=None):
    """ execute "SET" query

    :param connection: Connection
    :param key: string
        the key of configuration.
    :param value: string
        the value of configuration.
    :return: list or string or None
        if key and value is None, return all configuration variables
        if value is None, return the value of a particular configuration variable(key)
        if key and value is not None, override the value of a particular configuration variable(key)

    """
    if value == None:
        if key == None:
            return query(connection, "set -v")
        else:
            result = query_for_string(connection, "set %s" % key)
            result = util.split_with_regex("=", result)

            if len(result) == 2:
                return util.replace_with_regex("^\\s+|\\s+$", "", result[1])
            else:
                return None;
    else:
        execute(connection, "set %s=%s" % (key, value))


def gen_query_id(conn):
    """ make unique query id

    :param conn: Connection
    :return: string
        query ID

    make unique query id with the following rule:
    [username | "rs"] + random value
    """
    user = conn.getSession().getPseudoUser()

    id = util.replace_with_regex("^[A-Za-z ]*://[^:/]*(:[0-9]+)?", "", user)
    id = id.lower()
    if len(id) == 0:
        id = "rs"

    return "%s_%s" % (id, util.gen_random_key())


def export(conn, name, func, vars={}):
    """ export python object and function into HDFS for PyUDF

    :param conn: connection
    :param name: string
        PyUDF name
    :param func: python function
    :param vars: dictionary
        python objects
    """
    if len(vars) != 0:
        # export function code into HDFS
        varsPath = os.path.join(os.getcwd(), "%s.var" % name)
        data = marshal.dumps(vars)

        file_i = open(varsPath, 'w')
        file_i.write(data)
        file_i.close()

        dst = hdfs_path_join(conn.getSession().getFsUdfs(), "%s.var" % name)
        hdfs.put(conn, varsPath, dst, overWrite=True)

    # export function code into HDFS
    funcPath = os.path.join(os.getcwd(), "%s.func" % name)
    data = marshal.dumps(func.func_code)

    file_i = open(funcPath, 'w')
    file_i.write(data)
    file_i.close()

    dst = hdfs_path_join(conn.getSession().getFsUdfs(), "%s.func" % name)
    hdfs.put(conn, funcPath, dst, overWrite=True)

    return hdfs.exists(conn, dst)


def debug_off():
    """
    off print java log
    """
    # TODO load log4j and set log level as "ERROR"
    # j.logger <- j2r.log4j.Logger.class()
    # root.logger <- j.logger$getRootLogger()
    # root.logger$setLevel(j2r.log4j.Level.class()$ERROR)
    pass
