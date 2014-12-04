package com.nexr.pyhive.hive.udf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class UDFUtils {
    private static final String VARIABLE_EXTENSION = ".var";
    private static final String FUNCTION_EXTENSION = ".func";

    public static Path getFuncPath(String name) {
        return new Path(getBaseDirectory(), getFuncFileName(name));
    }

    public static Path getVarPath(String name) {
        return new Path(getBaseDirectory(), getVarFileName(name));
    }

    public static void registerFunc(String name, String src) throws IOException {
        FileSystem fs = FileSystem.get(getConf());

        boolean delSrc = true;
        boolean overwrite = true;
        Path dst = getFuncPath(name);
        fs.copyFromLocalFile(delSrc, overwrite, new Path(src), dst);
    }

    public static void registerVar(String name, String src) throws IOException {
        FileSystem fs = FileSystem.get(getConf());

        boolean delSrc = true;
        boolean overwrite = true;
        Path dst = getVarPath(name);
        fs.copyFromLocalFile(delSrc, overwrite, new Path(src), dst);
    }

    protected static Configuration getConf() {
        Configuration conf = new Configuration();
        String defaultFS = getDefaultFS();
        if (defaultFS != null) {
            FileSystem.setDefaultUri(conf, defaultFS);
        }

        return conf;
    }

    private static String getDefaultFS() {
        return System.getenv(getDefaultFileSystemPropertyName());
    }

    public static String getDefaultFileSystemPropertyName() {
        return "SESSION_DEFAULT_FS";
    }

    public static String getBaseDirectory() {
        return System.getenv(getBaseDirectoryPropertyName());
    }

    public static String getBaseDirectoryPropertyName() {
        return "SESSION_FS_UDFS";
    }

    public static String getFuncFileName(String name) {
        return String.format("%s%s", name, FUNCTION_EXTENSION);
    }

    public static String getVarFileName(String name) {
        return String.format("%s%s", name, VARIABLE_EXTENSION);
    }

    public static String[] list() throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), PythonFunctionFilter.instance);

        List<String> names = new ArrayList<String>();
        for (int i = 0; i < listStatus.length; i++) {
            FileStatus status = listStatus[i];
            Path path = status.getPath();
            String name = path.getName();

            if (name.endsWith(FUNCTION_EXTENSION)) {
                name = name.substring(0, name.length() - FUNCTION_EXTENSION.length());
                names.add(name);
            }
        }
        return names.toArray(new String[0]);
    }

    public static boolean delete(String name) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), PythonFunctionFilter.instance);

        String funcFileName = getFuncFileName(name);
        String varFileName = getVarFileName(name);

        boolean result = false;
        for (FileStatus status : listStatus) {
            Path path = status.getPath();

            if (funcFileName.equals(path.getName())) {
                result = fs.delete(path, true);
            }

            if (varFileName.equals(path.getName())) {
                result = fs.delete(path, true);
            }
        }

        return result;
    }

    private static String getUserName() {
        return System.getProperty("user.name");
    }

    private static String getUserHome() {
        return System.getProperty("user.home");
    }

    protected static String getTempDirectory() {
        String userName = getUserName();
        String tempDir = System.getProperty("java.io.tmpdir");

        return String.format("%s%s%s", tempDir, File.separator, userName);
    }

    private static class PythonFunctionFilter implements PathFilter {
        private static PythonFunctionFilter instance = new PythonFunctionFilter();

        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return name.endsWith(FUNCTION_EXTENSION) || name.endsWith(VARIABLE_EXTENSION);
        }
    }
}