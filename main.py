__author__ = 'bruceshin'

from PyHive2 import pyhive
from PyHive2 import hdfs

conn = pyhive.connect()
du = hdfs.dfsDiskUsage(conn,"/rhive/data/bruceshin/iris_4de5a5b9c1832fb5ecaa16b79028a3")
print du



