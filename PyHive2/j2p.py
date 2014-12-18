import jpype


def create_JHiveJdbcClient(isHiveServer2):
    """ create HiveJdbcClient java instance

    :param isHiveServer2: boolean
        where hiveserver2 is
    :return: HiveJdbcClient java instance
    """
    hivePkg = jpype.JPackage('com.nexr.pyhive.hive')  # get the package
    HiveJdbcClient = hivePkg.HiveJdbcClient  # get the class
    hiveJdbcClient = HiveJdbcClient(isHiveServer2)  # create an instance of the class

    return hiveJdbcClient


def get_JFileSystemUtils():
    """ get FileSystemUtils java instance

    :return: FileSystemUtils java instance
    """
    hadoopPkg = jpype.JPackage('com.nexr.pyhive.hadoop')  # get the package
    FileSystemUtils = hadoopPkg.FileSystemUtils
    return FileSystemUtils


def get_JUDFUtils():
    """ get UDFUtils instance

    :return: UDFUtils instance
    """
    hadoopPkg = jpype.JPackage('com.nexr.pyhive.hive.udf')  # get the package
    UDFUtils = hadoopPkg.UDFUtils
    return UDFUtils


def create_JProperties():
    """ create Properties java instance

    :return: Properties java instance
    """
    util = jpype.JPackage('java.util')
    Properties = util.Properties
    return Properties()