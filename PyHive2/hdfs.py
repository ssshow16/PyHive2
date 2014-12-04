__author__ = 'bruceshin'

import j2p
import util

def dfsDiskUsage(conn, path="/", summary=False):
    fsu = j2p.FileSystemUtils()

    du = None
    if summary:
        du = fsu.dus(path, conn.getFsDefault(),conn.getSession().getPseudoUser())
    else:
        du = fsu.du(path, conn.getFsDefault(),conn.getSession().getPseudoUser())

    return util.convertDataFrame(du)

def dfsLs(conn,path = "/"):
    fsu = j2p.FileSystemUtils()
    result = fsu.ls(path,conn.getFsDefault(),conn.getSession().getPseudoUser())
    return util.convertDataFrame(result)

def dfsRm(conn,paths):
    fsu = j2p.FileSystemUtils()

    if isinstance(paths,basestring):
        result = fsu.delete(paths,conn.getFsDefault(),conn.getSession().getPseudoUser())
        return util.convertIntToBoolean(result)
    else:
        results = []
        for path in paths:
            result = fsu.delete(path,conn.getFsDefault(),conn.getSession().getPseudoUser())
            results.extend([util.convertIntToBoolean(result)])
        return results

def dfsRename(conn,src, dst):
    fsu = j2p.FileSystemUtils()
    result = fsu.rename(src,dst,conn.getFsDefault(),conn.getSession().getPseudoUser())
    return util.convertIntToBoolean(result)

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
    return util.convertIntToBoolean(result)

def dfsCat(conn,path):
    fsu = j2p.FileSystemUtils()
    fsu.cat(path,conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsTail(conn,path,kB = 1L):
    fsu = j2p.FileSystemUtils()
    fsu.tail(path,kB,conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsChmod(conn,option,path,recursive = False):
    fsu = j2p.FileSystemUtils()
    fsu.chmod(path,option,recursive, conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsChown(conn, option, path, recursive = False):
    fsu = j2p.FileSystemUtils()
    fsu.chown(path,option,recursive, conn.getFsDefault(),conn.getSession().getPseudoUser())

def dfsChgrp(conn, option, path, recursive = False):
    fsu = j2p.FileSystemUtils()
    fsu.chgrp(path,option,recursive, conn.getFsDefault(),conn.getSession().getPseudoUser())

