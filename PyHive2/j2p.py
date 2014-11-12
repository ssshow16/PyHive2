import jpype

def createHiveJdbcClient(isHiveServer2):
    hivePkg = jpype.JPackage('com.nexr.pyhive.hive') # get the package
    HiveJdbcClient = hivePkg.HiveJdbcClient # get the class
    hiveJdbcClient = HiveJdbcClient(isHiveServer2)  # create an instance of the class

    return hiveJdbcClient

def FileSystemUtils():
    hadoopPkg = jpype.JPackage('com.nexr.pyhive.hadoop') # get the package
    FileSystemUtils = hadoopPkg.FileSystemUtils
    return FileSystemUtils

def JUDFUtils():
    hadoopPkg = jpype.JPackage('com.nexr.pyhive.hive.udf') # get the package
    UDFUtils = hadoopPkg.UDFUtils
    return UDFUtils

def JProperties():
    util = jpype.JPackage('java.util')
    Properties = util.Properties
    return Properties()