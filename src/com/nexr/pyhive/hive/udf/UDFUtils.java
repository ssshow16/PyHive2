package com.nexr.pyhive.hive.udf;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class UDFUtils {
	private static final String RENVIRONMENT_EXTENSION = ".env";

	public static Path getPath(String name) {
		return new Path(getBaseDirectory(), getFileName(name));
	}
	
	public static void register(String name, String src) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		
		boolean delSrc = true;
		boolean overwrite = true;
		Path dst = getPath(name);
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
	
	public static String getFileName(String name) {
		return String.format("%s%s", name, RENVIRONMENT_EXTENSION);
	}
	
	public static String[] list() throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), REnvironmentFilter.instance);
		
		String[] names = new String[listStatus.length];
		for (int i = 0; i < names.length; i++) {
			FileStatus status = listStatus[i];
			Path path = status.getPath();
			String name = path.getName();
			name = name.substring(0, name.length() - RENVIRONMENT_EXTENSION.length());
			names[i] = name;
		}
		
		return names;
	}
	
	public static boolean delete(String name) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), REnvironmentFilter.instance);

		String fileName = getFileName(name);
		for(FileStatus status : listStatus) {
			Path path = status.getPath();
			
			if (fileName.equals(path.getName())) {
				return fs.delete(path, true);
			}
		}
		
		return false;
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
	
	private static class REnvironmentFilter implements PathFilter {
		private static REnvironmentFilter instance = new REnvironmentFilter();
		
		@Override
		public boolean accept(Path path) {
			String name = path.getName();
			return name.endsWith(RENVIRONMENT_EXTENSION);
		}
	}
}