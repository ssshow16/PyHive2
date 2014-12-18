__author__ = 'bruceshin'

import j2p
import util


def du(conn, path="/", summary=False):
    """ get length of files.

    :param conn: Connection
    :param path: string
        hdfs path
    :param summary: boolean
        if True, display a summary of file lengths.
        Otherwise, Displays aggregate length of files contained in the directory or
        the length of a file in case its just a file
    :return: pandas.DataFrame
        lengthof files
    """
    fsu = j2p.get_JFileSystemUtils()

    du = None
    if summary:
        du = fsu.dus(path, conn.getFsDefault(), conn.getSession().getPseudoUser())
    else:
        du = fsu.du(path, conn.getFsDefault(), conn.getSession().getPseudoUser())

    return util.convert_data_frame(du)


def ls(conn, path="/"):
    """ For a file returns stat on the file with the following format:
    filename <number of replicas> filesize modification_date modification_time permissions userid groupid
    For a directory it returns list of its direct children as in unix. A directory is listed as:
    sdirname <dir> modification_time modification_time permissions userid groupid

    :param conn: Connection
    :param path: string
        hdfs path
    :return: pandas.DataFrame
        the file or directory list
    """
    fsu = j2p.get_JFileSystemUtils()
    result = fsu.ls(path, conn.getFsDefault(), conn.getSession().getPseudoUser())
    return util.convert_data_frame(result)


def rm(conn, paths):
    """Delete files specified as args. Only deletes non empty directory and files.

    :param conn: Connection
    :param paths: string
        hdfs path
    :return: boolean
        if deleted file, return True.
        Otherwise, return False.
    """
    fsu = j2p.get_JFileSystemUtils()

    if isinstance(paths, basestring):
        result = fsu.delete(paths, conn.getFsDefault(), conn.getSession().getPseudoUser())
        return util.convert_int_to_boolean(result)
    else:
        results = []
        for path in paths:
            result = fsu.delete(path, conn.getFsDefault(), conn.getSession().getPseudoUser())
            results.extend([util.convert_int_to_boolean(result)])
        return results


def rename(conn, src, dst):
    """ Change the name of file or directory

    :param conn: Connection
    :param src: string
        old name of file or directory
    :param dst: string
        new name of file or directory
    :return:
        if renamed file or directory, return True.
        otherwise, return False
    """
    fsu = j2p.get_JFileSystemUtils()
    result = fsu.rename(src, dst, conn.getFsDefault(), conn.getSession().getPseudoUser())
    return util.convert_int_to_boolean(result)


def put(conn, src, dst, srcDel=False, overWrite=False):
    """ Copy single src, or multiple srcs from local file system to the destination filesystem.
    Also reads input from stdin and writes to destination filesystem.

    :param conn: Connection
    :param src: string
        local file system path
    :param dst: string
        hdfs file system path
    :param srcDel: boolean
        whether to delete the src
    :param overWrite: boolean
        whether to overwrite an existing file
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.copyFromLocal(srcDel, overWrite, src, dst, conn.getFsDefault(), conn.getSession().getPseudoUser())


def get(conn, src, dst, srcDel=False):
    """ Copy files to the local file system

    :param conn: Connection
    :param src: string
        hdfs file system path
    :param dst: string
        local file system path
    :param srcDel: boolean
        whether to delete the src
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.copyToLocal(srcDel, src, dst, conn.getFsDefault(), conn.getSession().getPseudoUser())


def exists(conn, path):
    """Check to see if the file exists

    :param conn: Connection
    :param path: string
    :return: boolean
        if file exists, return True
    """
    fsu = j2p.get_JFileSystemUtils()
    isExists = fsu.exists(path, conn.getFsDefault(), conn.getSession().getPseudoUser())
    return isExists == 1


def mkdirs(conn, path):
    """Takes path uri's as argument and creates directories.

    :param conn: Connection
    :param path: string
        hdfs path
    :return:
        if dir created, return True
    """
    fsu = j2p.get_JFileSystemUtils()
    result = fsu.mkdirs(path, conn.getFsDefault(), conn.getSession().getPseudoUser())
    return util.convert_int_to_boolean(result)


def cat(conn, path):
    """ Copies source paths to stdout.

    :param conn: Connection
    :param path: string
        hdfs path
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.cat(path, conn.getFsDefault(), conn.getSession().getPseudoUser())


def tail(conn, path, kB=1L):
    """Displays last kilobyte of the file to stdout.

    :param conn: Connection
    :param path: string
        hdfs path
    :param kB: long
        make length
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.tail(path, kB, conn.getFsDefault(), conn.getSession().getPseudoUser())


def chmod(conn, option, path, recursive=False):
    """Change the owner of files

    :param conn: Connection
    :param option: string
        mode option
    :param path: string
        hdfs path
    :param recursive: boolean
        where to make the change recursively through the directory structure
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.chmod(path, option, recursive, conn.getFsDefault(), conn.getSession().getPseudoUser())


def chown(conn, option, path, recursive=False):
    """ Change the permissions of files

    :param conn: Connection
    :param option: string
        owner
    :param path: string
        hdfs path
    :param recursive: boolean
        where to make the change recursively through the directory structure
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.chown(path, option, recursive, conn.getFsDefault(), conn.getSession().getPseudoUser())


def chgrp(conn, option, path, recursive=False):
    """ Change group association of files

    :param conn: Connection
    :param option: string
        group name
    :param path: string
        hdfs path
    :param recursive: boolean
        where to make the change recursively through the directory structure
    """
    fsu = j2p.get_JFileSystemUtils()
    fsu.chgrp(path, option, recursive, conn.getFsDefault(), conn.getSession().getPseudoUser())

