package com.nexr.pyhive.hadoop;

import com.nexr.pyhive.model.DataFrameModel;
import com.nexr.pyhive.model.MapModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.*;

public class FileSystemUtils {
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
	public static abstract class CommandPrivilegedExceptionAction<T> implements PrivilegedExceptionAction<T> {
		private String defaultFS;
		private String user;
		
		public CommandPrivilegedExceptionAction(String defaultFS, String user) {
			this.defaultFS = defaultFS;
			this.user = user;
		}
		
		public String getDefaultFS() {
			return defaultFS;
		}

		public String getUser() {
			return user;
		}
		
		public Configuration getConf() {
			return FileSystemUtils.getConf(defaultFS, user);
		}
	}

    public static Configuration getConf(String defaultFS, String user) {
        Configuration conf = new Configuration();

        if (defaultFS != null) {
            FileSystem.setDefaultUri(conf, defaultFS);
        }

        if (user != null) {
            conf.set("hadoop.job.ugi", user);
        }

        return conf;
    }

    public static boolean checkFileSystem(String defaultFS) throws IOException {
        Configuration conf = getConf(defaultFS, null);
        try {
            FileSystem.get(conf);
        } catch (Exception e) {
            return false;
        }

        return true;
    }
	
//	public static class ListFilesCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<REXPList> {
//		private Path path;
//
//		public ListFilesCommandPrivilegedExceptionAction(String src, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(src);
//		}
//
//		@Override
//		public REXPList run() throws Exception {
//			Configuration conf = getConf();
//			List<FileStatus> fileList = getFileList(conf, path);
//
//			RList rList = new RList();
//			String[] permissions = new String[fileList.size()];
//			String[] owners = new String[fileList.size()];
//			String[] groups = new String[fileList.size()];
//			double[] lengths = new double[fileList.size()];
//			String[] modificationTimes = new String[fileList.size()];
//			String[] files = new String[fileList.size()];
//
//			for (int j = 0; j < fileList.size(); j++) {
//				FileStatus fileStatus = fileList.get(j);
//
//				permissions[j] = fileStatus.getPermission().toString();
//				owners[j] = fileStatus.getOwner();
//				groups[j] = fileStatus.getGroup();
//				lengths[j] = fileStatus.getLen();
//				modificationTimes[j] = dateFormat.format(new Date(fileStatus.getModificationTime()));
//				files[j] = fileStatus.getPath().toUri().getPath();
//			}
//
//			rList.put("permission", new REXPString(permissions));
//			rList.put("owner", new REXPString(owners));
//			rList.put("group", new REXPString(groups));
//			rList.put("length", new REXPDouble(lengths));
//			rList.put("modify.time", new REXPString(modificationTimes));
//			rList.put("file", new REXPString(files));
//
//			return new REXPList(rList);
//		}
//
//		private List<FileStatus> getFileList(Configuration conf, Path path) throws IOException {
//			List<FileStatus> list = new ArrayList<FileStatus>();
//
//			FileSystem fs = null;
//			try {
//				fs = path.getFileSystem(conf);
//
//				FileStatus[] fileStatuses = fs.globStatus(path);
//				if (fileStatuses != null) {
//					for (FileStatus fileStatus : fileStatuses) {
//
//						if (!fileStatus.isDir()) {
//							list.add(fileStatus);
//
//						} else {
//							Path dirPath = fileStatus.getPath();
//
//							FileStatus[] files = fs.listStatus(dirPath);
//							if (files != null) {
//								for (FileStatus file : files) {
//									list.add(file);
//								}
//							}
//						}
//					}
//				}
//			} finally {
//				closeFileSystem(fs);
//			}
//
//			return list;
//		}
//	}


	
//	public static REXPList ls(String src, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new ListFilesCommandPrivilegedExceptionAction(src, defaultFS, user));
//	}

	public static class DiskUsageCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<DataFrameModel> {
		private Path path;
		
		public DiskUsageCommandPrivilegedExceptionAction(String src, String defaultFS, String user) {
			super(defaultFS, user);
			this.path = new Path(src);
		}

		@Override
		public DataFrameModel run() throws Exception {

			Configuration conf = getConf();
			
			FileSystem fs = null;
			FileStatus fileStatuses[] = null;
			
			long lens[] = null;
			try {
				fs = path.getFileSystem(conf);
				Path paths[] = FileUtil.stat2Paths(fs.globStatus(path), path);
				fileStatuses = fs.listStatus(paths);
				if (fileStatuses == null) {
					fileStatuses = new FileStatus[0];
				}
				
				lens = new long[fileStatuses.length];
				for (int i = 0; i < fileStatuses.length; i++) {
					lens[i] = fileStatuses[i].isDir() ? fs.getContentSummary(fileStatuses[i].getPath()).getLength() : fileStatuses[i].getLen();
				}
	 
			} finally {
				closeFileSystem(fs);
			}

            List lengths = new ArrayList();
            List files = new ArrayList();

			for (int j = 0; j < fileStatuses.length; j++) {
				FileStatus fileStatus = fileStatuses[j];

                lengths.add(lens[j]);
                files.add(fileStatus.getPath().toUri().getPath());
			}

            List<List> values = new ArrayList<List>();
            values.add(lengths);
            values.add(files);

            DataFrameModel model = new MapModel(
                    new String[]{"length","file"},
                    new String[]{"double","string"},
                    values);

            return model;
		}
	}
	
	public static DataFrameModel du(String src, String defaultFS, String user) throws IOException, InterruptedException {
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
		return ugi.doAs(new DiskUsageCommandPrivilegedExceptionAction(src, defaultFS, user));
	}

//	public static class DiskUsageSummaryCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<REXPList> {
//		private Path path;
//
//		public DiskUsageSummaryCommandPrivilegedExceptionAction(String src, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(src);
//		}
//
//		@Override
//		public ArrayList run() throws Exception {
//			Configuration conf = getConf();
//
//			FileSystem fs = null;
//			FileStatus[] fileStatuses = null;
//
//			long lens[] = null;
//			try {
//				fs = path.getFileSystem(conf);
//				fileStatuses = fs.globStatus(path);
//				if (fileStatuses == null) {
//					fileStatuses = new FileStatus[0];
//				}
//
//				lens = new long[fileStatuses.length];
//				for (int i = 0; i < fileStatuses.length; i++) {
//					lens[i] = fs.getContentSummary(fileStatuses[i].getPath()).getLength();
//				}
//
//			} finally {
//				closeFileSystem(fs);
//			}
//
//			Map<List> rList = Collection.<String, List>emptyMap();
//
//			double[] lengths = new double[fileStatuses.length];
//			String[] files = new String[fileStatuses.length];
//
//			for (int j = 0; j < fileStatuses.length; j++) {
//				FileStatus fileStatus = fileStatuses[j];
//
//				lengths[j] = lens[j];
//				files[j] = fileStatus.getPath().toUri().getPath();
//			}
//
//			rList.put("length", new REXPDouble(lengths));
//			rList.put("file", new REXPString(files));
//
//			return new REXPList(rList);
//		}
//	}
//
//	public static REXPList dus(String src, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new DiskUsageSummaryCommandPrivilegedExceptionAction(src, defaultFS, user));
//	}

	public static class CopyFromLocalCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
		private Path srcPath;
		private Path dstPath;
		private boolean delSrc;
		private boolean overwrite;

		public CopyFromLocalCommandPrivilegedExceptionAction(boolean delSrc, boolean overwrite, String src, String dst, String defaultFS, String user) {
			super(defaultFS, user);
			this.srcPath = new Path(src);
			this.dstPath = new Path(dst);
			this.delSrc = delSrc;
			this.overwrite = overwrite;
		}

		@Override
		public Void run() throws Exception {
			Configuration conf = getConf();

			FileSystem fs = null;
			try {
				fs = FileSystem.get(conf);
				fs.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);
			} finally {
				closeFileSystem(fs);
			}

			return null;
		}
	}

	public static Void copyFromLocal(boolean delSrc, boolean overwrite, String src, String dst, String defaultFS, String user) throws IOException, InterruptedException {
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
		return ugi.doAs(new CopyFromLocalCommandPrivilegedExceptionAction(delSrc, overwrite, src, dst, defaultFS, user));
	}

	public static class CopyToLocalCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
		private Path srcPath;
		private Path dstPath;
		private boolean delSrc;

		public CopyToLocalCommandPrivilegedExceptionAction(boolean delSrc, String src, String dst, String defaultFS, String user) {
			super(defaultFS, user);
			this.srcPath = new Path(src);
			this.dstPath = new Path(dst);
			this.delSrc = delSrc;
		}

		@Override
		public Void run() throws Exception {
			Configuration conf = getConf();

			FileSystem fs = null;
			try {
				fs = FileSystem.get(conf);

//				if (fs instanceof ChecksumFileSystem && delSrc == false) {
//					((ChecksumFileSystem) fs).copyToLocalFile(srcPath, dstPath, false);
//				} else {
					fs.copyToLocalFile(delSrc, srcPath, dstPath);
//				}

			} finally {
				closeFileSystem(fs);
			}

			return null;
		}
	}

	public static Void copyToLocal(boolean delSrc, String src, String dst, String defaultFS, String user) throws IOException, InterruptedException {
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
		return ugi.doAs(new CopyToLocalCommandPrivilegedExceptionAction(delSrc, src, dst, defaultFS, user));
	}

//
//	public static class DeleteCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Boolean> {
//		private Path path;
//
//		public DeleteCommandPrivilegedExceptionAction(String file, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(file);
//		}
//
//		@Override
//		public Boolean run() throws Exception {
//			Configuration conf = getConf();
//
//			FileSystem fs = null;
//			try {
//				fs = FileSystem.get(conf);
//				return fs.delete(path, true);
//			} finally {
//				closeFileSystem(fs);
//			}
//		}
//	}
//
//	public static boolean delete(String file, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new DeleteCommandPrivilegedExceptionAction(file, defaultFS, user));
//	}
//
//
//	public static class RenameCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Boolean> {
//		private Path srcPath;
//		private Path dstPath;
//
//		public RenameCommandPrivilegedExceptionAction(String src, String dst, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.srcPath = new Path(src);
//			this.dstPath = new Path(dst);
//		}
//
//		@Override
//		public Boolean run() throws Exception {
//			Configuration conf = getConf();
//
//			FileSystem fs = null;
//			try {
//				fs = FileSystem.get(conf);
//				return fs.rename(srcPath, dstPath);
//			} finally {
//				closeFileSystem(fs);
//			}
//		}
//	}
//
//	public static boolean rename(String src, String dst, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new RenameCommandPrivilegedExceptionAction(src, dst, defaultFS, user));
//	}
//
//	public static class ExistsCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Boolean> {
//		private Path path;
//
//		public ExistsCommandPrivilegedExceptionAction(String file, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(file);
//		}
//
//		@Override
//		public Boolean run() throws Exception {
//			Configuration conf = getConf();
//
//			FileSystem fs = null;
//			try {
//				fs = path.getFileSystem(conf);
//				return fs.exists(path);
//			} finally {
//				closeFileSystem(fs);
//			}
//		}
//	}
//
//	public static boolean exists(String file, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new ExistsCommandPrivilegedExceptionAction(file, defaultFS, user));
//	}
//
//
//	public static class MakeDirsCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Boolean> {
//		private Path path;
//
//		public MakeDirsCommandPrivilegedExceptionAction(String file, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(file);
//		}
//
//		@Override
//		public Boolean run() throws Exception {
//			Configuration conf = getConf();
//
//			FileSystem fs = null;
//			try {
//				fs = path.getFileSystem(conf);
//				return fs.mkdirs(path);
//			} finally {
//				closeFileSystem(fs);
//			}
//		}
//	}
//
//	public static boolean mkdirs(String file, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new MakeDirsCommandPrivilegedExceptionAction(file, defaultFS, user));
//	}
//
//
//	public static class CatCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private String src;
//
//		public CatCommandPrivilegedExceptionAction(String src, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.src = src;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			Configuration conf = getConf();
//
//			FsShell fsShell = new FsShell(conf);
//			fsShell.run(new String[] { "-cat", src });
//
//			return null;
//		}
//	}
//
//	public static Void cat(String src, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new CatCommandPrivilegedExceptionAction(src, defaultFS, user));
//	}
//
//
//	public static class TailCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private Path path;
//		private int kB;
//
//		public TailCommandPrivilegedExceptionAction(String src, int kB, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.path = new Path(src);
//			this.kB = kB;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			FileSystem fs = null;
//
//			try {
//				fs = path.getFileSystem(getConf());
//
//				FileStatus fileStatus = fs.getFileStatus(path);
//				if (fileStatus.isDir()) {
//					throw new IOException("src must be a file.");
//				}
//
//				long fileSize = fileStatus.getLen();
//				int length = kB * 1024;
//				long offset = fileSize <= length ? 0L : fileSize - length;
//
//				FSDataInputStream in = fs.open(path);
//				in.seek(offset);
//
//				IOUtils.copyBytes(in, System.out, length, false);
//				in.close();
//			} finally {
//				closeFileSystem(fs);
//			}
//
//			return null;
//		}
//
//
//
//	}
//
//	public static Void tail(String src, int kB, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new TailCommandPrivilegedExceptionAction(src, kB, defaultFS, user));
//	}
//
//
//	public static class ChangeModCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private String src;
//		private String option;
//		private boolean recursive;
//
//		public ChangeModCommandPrivilegedExceptionAction(String src, String option, boolean recursive, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.src = src;
//			this.option = option;
//			this.recursive = recursive;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			Configuration conf = getConf();
//
//			FsShell fsShell = new FsShell(conf);
//
//			if (recursive) {
//				fsShell.run(new String[] { "-chmod", "-R", option, src });
//			} else {
//				fsShell.run(new String[] { "-chmod", option, src });
//			}
//
//			return null;
//		}
//	}
//
//	public static Void chmod(String src, String option, boolean recursive, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new ChangeModCommandPrivilegedExceptionAction(src, option, recursive, defaultFS, user));
//	}
//
//
//	public static class ChangeOwnerCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private String src;
//		private String option;
//		private boolean recursive;
//
//		public ChangeOwnerCommandPrivilegedExceptionAction(String src, String option, boolean recursive, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.src = src;
//			this.option = option;
//			this.recursive = recursive;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			Configuration conf = getConf();
//
//			FsShell fsShell = new FsShell(conf);
//
//			if (recursive) {
//				fsShell.run(new String[] { "-chown", "-R", option, src });
//			} else {
//				fsShell.run(new String[] { "-chown", option, src });
//			}
//
//			return null;
//		}
//	}
//
//
//	public static Void chown(String src, String option, boolean recursive, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new ChangeOwnerCommandPrivilegedExceptionAction(src, option, recursive, defaultFS, user));
//	}
//
//
//	public static class ChangeGroupCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private String src;
//		private String option;
//		private boolean recursive;
//
//		public ChangeGroupCommandPrivilegedExceptionAction(String src, String option, boolean recursive, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.src = src;
//			this.option = option;
//			this.recursive = recursive;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			Configuration conf = getConf();
//
//			FsShell fsShell = new FsShell(conf);
//
//			if (recursive) {
//				fsShell.run(new String[] { "-chgrp", "-R", option, src });
//			} else {
//				fsShell.run(new String[] { "-chgrp", option, src });
//			}
//
//			return null;
//		}
//	}
//
//	public static Void chgrp(String src, String option, boolean recursive, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new ChangeGroupCommandPrivilegedExceptionAction(src, option, recursive, defaultFS, user));
//	}
//
//
//	public static class InfoCommandPrivilegedExceptionAction extends CommandPrivilegedExceptionAction<Void> {
//		private String src;
//
//		public InfoCommandPrivilegedExceptionAction(String src, String defaultFS, String user) {
//			super(defaultFS, user);
//			this.src = src;
//		}
//
//		@Override
//		public Void run() throws Exception {
//			Configuration conf = getConf();
//
//			DFSck dfsCk = new DFSck(conf);
//			dfsCk.run(new String[] { src });
//
//			return null;
//		}
//	}
//
//	public static Void info(String src, String defaultFS, String user) throws IOException, InterruptedException {
//		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
//		return ugi.doAs(new InfoCommandPrivilegedExceptionAction(src, defaultFS, user));
//	}
	
	
	private static void closeFileSystem(FileSystem fs) {
		try {
			if (fs != null) {
				fs.close();
			}
		} catch (IOException e) { }
	}
}