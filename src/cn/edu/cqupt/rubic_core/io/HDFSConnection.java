package cn.edu.cqupt.rubic_core.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Title: HDFSConnection.java
 * 
 * @description:与HDFS建立连接的连接类
 * @author liucx
 * @created 2015-5-19 下午3:39:56
 */
public class HDFSConnection {
	private static Configuration conf = new Configuration();
	static {
		conf.addResource(new Path("core-site.xml"));
		// conf.addResource(new Path("hdfs-site.xml"));
		conf.addResource(new Path("mapred-site.xml"));
	}

	/**
	 * 
	 * @description 获取与hdfs文件系统的连接
	 * @author liucx
	 * @created 2015-5-19 下午3:40:36
	 * @return {@link FileSystem}
	 * @throws IOException
	 */
	public static FileSystem getFileSystem() throws IOException {
		return FileSystem.get(conf);
	}

	/**
	 * 
	 * @description 关闭与hdfs文件系统的连接
	 * @author liucx
	 * @created 2015-5-19 下午3:53:28
	 * @param fs
	 */
	public static void close(FileSystem fs) {
		try {
			if (fs != null)
				fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
