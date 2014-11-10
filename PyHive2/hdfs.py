__author__ = 'bruceshin'

import j2p
import util

def dfsDiskUsage(connection, path="/"):
    fsu = j2p.FileSystemUtils()
    du = fsu.du(path, connection.getFsDefault(),"bruceshin")

    return util.convertDataFrame(du)

def dfsPut(connection,src,dst,srcDel = False, overWrite = False):
    fsu = j2p.FileSystemUtils()
    fsu.copyFromLocal(srcDel,overWrite,src,dst,connection.getFsDefault(),"bruceshin")

def dfsGet(connection, src, dst, srcDel = False):
    fsu = j2p.FileSystemUtils()
    fsu.copyToLocal(srcDel, src,dst,connection.getFsDefault(),"bruceshin")