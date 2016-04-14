package cn.edu.cqupt.rubic_core.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import cn.edu.cqupt.rubic_framework.service_interface.FileDao;

/**
 * 
 * Title: HDFSFileDaoImpl.java
 * 
 * @description:HDFS文件操作类，通过该类可完成与HDFS文件系统进行交互
 * @author liucx
 * @created 2015-5-19 下午4:27:31
 */
public class HDFSFileDaoImpl implements FileDao {

	// 默认缓冲区大小，暂时写死
	private int buffersize = 1024;

	public boolean isExist(String path) throws IOException {
		FileSystem fs = HDFSConnection.getFileSystem();
		boolean flag = isExist(path, fs);
		HDFSConnection.close(fs);
		return flag;
	}

	// 根据给定的与文件系统的连接判断文件是否存在,不会关闭FileSystem
	private boolean isExist(String path, FileSystem fs) throws IOException {
		return fs.exists(new Path(path));
	}

	public boolean createFile(String dest, byte[] contents) throws IOException {

		FileSystem fs = HDFSConnection.getFileSystem();
		Path path = new Path(dest);
		FSDataOutputStream out = fs.create(path);
		out.write(contents);
		if (out != null)
			out.close();
		HDFSConnection.close(fs);
		return true;
	}

	public boolean rename(String oldName, String newName) throws IOException {
		boolean flag = false;
		FileSystem fs = HDFSConnection.getFileSystem();
		if (isExist(oldName, fs) && isExist(newName, fs)) {
			Path oldPath = new Path(oldName);
			Path newPath = new Path(newName);
			flag = fs.rename(oldPath, newPath);
		}
		HDFSConnection.close(fs);
		return flag;
	}

	// 将待删除的文件加入TreeSet，执行HDFSConnection.close(fs)时才真正删除文件
	public boolean remove(String path) throws IOException {
		FileSystem fs = HDFSConnection.getFileSystem();
		Path movedPath = new Path(path);
		boolean flag = fs.deleteOnExit(movedPath);
		HDFSConnection.close(fs);
		return flag;
	}

	public boolean mkdir(String path) throws IOException {
		boolean flag = false;
		FileSystem fs = HDFSConnection.getFileSystem();
		if (!isExist(path, fs)) {
			Path srcPath = new Path(path);
			flag = fs.mkdirs(srcPath);
		}
		HDFSConnection.close(fs);
		return flag;
	}

	// 还没定格式，先不写
	public String[] listFiles(String path) throws IOException {
		Path f = new Path(path);
		FileSystem fs;
		fs = HDFSConnection.getFileSystem();
		FileStatus[] status = fs.listStatus(f);
		return null;
	}

	public boolean copyFiles(String src, String dest) throws IOException {
		FileSystem fs = HDFSConnection.getFileSystem();
		FSDataInputStream in = fs.open(new Path(src));
		FSDataOutputStream out = fs.create(new Path(dest));

		IOUtils.copyBytes(in, out, buffersize, true);
		in.close();
		out.close();
		HDFSConnection.close(fs);
		return true;
	}

	public OutputStream readFile(String path, OutputStream out)
			throws IOException {
		FileSystem fs = HDFSConnection.getFileSystem();
		InputStream in = fs.open(new Path(path));
		IOUtils.copyBytes(in, out, buffersize, true);
		if (in != null)
			in.close();
		HDFSConnection.close(fs);
		return out;
	}

	public boolean writeFile(String path, InputStream in) throws IOException {
		FileSystem fs = HDFSConnection.getFileSystem();
		OutputStream out = fs.create(new Path(path), null);
		IOUtils.copyBytes(in, out, buffersize, true);
		if (in != null)
			in.close();
		if (out != null)
			out.close();
		HDFSConnection.close(fs);
		return true;
	}
}
