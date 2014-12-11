__author__ = 'bruceshin'

from PyHive2 import pyhive
from PyHive2 import hdfs

conn = pyhive.connect()
# result = hdfs.dfsExists(conn,"/pyhive/anonymous/tmp")
#
# if result == True:
#     print "ok"
# else:
#     print "fail"
# result = pyhive.queryBig(conn,"select count(*) as count, species from iristest group by species")
# result = pyhive.loadTable(conn,"rs_anonymous_20141125140454_88338")
result = pyhive.query(conn,"select * from iris limit 4")
# hdfs.dfsChmod(conn,"777","/idea.properties2")
# hdfs.dfsChown(conn,"bruceshin","/user/bruceshin/x.properties")
# hdfs.dfsChgrp(conn,"supergroup","/user/bruceshin/x.properties")

# result = hdfs.dfsTail(conn,"/idea.properties2")
print result
pyhive.closeConnection(conn)
pyhive.close()