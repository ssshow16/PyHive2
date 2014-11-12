__author__ = 'bruceshin'

import j2p
import util

def dfsDiskUsage(conn, path="/"):
    fsu = j2p.FileSystemUtils()
    du = fsu.du(path, conn.getFsDefault(),conn.getSession().getPseudoUser())

    return util.convertDataFrame(du)

def dfsPut(conn,src,dst,srcDel = False, overWrite = False):
    fsu = j2p.FileSystemUtils()
    fsu.copyFromLocal(srcDel,overWrite,src,dst,conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsGet(conn, src, dst, srcDel = False):
    fsu = j2p.FileSystemUtils()
    fsu.copyToLocal(srcDel, src,dst,conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsExists(conn, path):
    fsu = j2p.FileSystemUtils()
    isExists = fsu.exists(path,conn.getFsDefault(),conn.getSession().getPseudoUser())
    return isExists == 1

def dfsMkdirs(conn,path):
    fsu = j2p.FileSystemUtils()
    result = fsu.mkdirs(path,conn.getFsDefault(),conn.getSession().getPseudoUser())
    return result == 1

def dfsChmod(conn,option,path,recursive=False):
    fsu = j2p.FileSystemUtils()
    fsu.chmod(path,option,recursive, conn.getFsDefault(),conn.getSession().getPseudoUser())




