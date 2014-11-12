__author__ = 'bruceshin'

from PyHive2 import pyhive
# from PyHive2 import hdfs

conn = pyhive.connect()
# result = hdfs.dfsExists(conn,"/pyhive/anonymous/tmp")
#
# if result == True:
#     print "ok"
# else:
#     print "fail"
pyhive.closeConnection(conn)