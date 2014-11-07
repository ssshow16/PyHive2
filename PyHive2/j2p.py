import jpype

def createHiveJdbcClient(context):
    hivePkg = jpype.JPackage('com.nexr.pyhive.hive') # get the package
    HiveJdbcClient = hivePkg.HiveJdbcClient # get the class
    hiveJdbcClient = HiveJdbcClient(context.getHiveServer2())  # create an instance of the class

    return hiveJdbcClient

def FileSystemUtils():
    hadoopPkg = jpype.JPackage('com.nexr.pyhive.hadoop') # get the package
    FileSystemUtils = hadoopPkg.FileSystemUtils
    return FileSystemUtils