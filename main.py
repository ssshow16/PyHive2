__author__ = 'bruceshin'

from PyHive2 import pyhive

conn = pyhive.connect()

def test(a,b):
    return a+b

pyhive.export(conn,"test",test)
result = pyhive.query(conn,"select Py('test',sepallength,sepalwidth,0.0) from iris")

print result
pyhive.closeConnection(conn)
pyhive.close()