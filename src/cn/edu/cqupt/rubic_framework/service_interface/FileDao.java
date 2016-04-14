package cn.edu.cqupt.rubic_framework.service_interface;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * Title: FileDao.java
 * 
 * @description:文件操作的接口，提供文件的增删改查，写入写出等功能
 * @author liucx
 * @created 2015-5-19 下午4:28:23
 */
public interface FileDao {
	/**
	 * 
	 * @description 判断指定文件或目录是否存在
	 * @author liucx
	 * @created 2015-5-19 下午3:13:21
	 * @param path
	 *            文件或目录的完整路径
	 * @return true：指定文件或目录存在 false：指定文件或目录不存在
	 */
	public boolean isExist(String path) throws IOException;

	/**
	 * 
	 * @description 创建指定目录下的文件
	 * @author liucx
	 * @created 2015-5-19 下午3:12:15
	 * @param dest
	 *            目标文件完整路径
	 * @param contents
	 *            文件内容
	 * @return true：创建文件成功 false：创建文件失败
	 */
	public boolean createFile(String dest, byte[] contents) throws IOException;

	/**
	 * 
	 * @description 文件重命名
	 * @author liucx
	 * @created 2015-5-19 下午3:10:32
	 * @param oldName
	 *            旧文件名（注意需要完整路径）
	 * @param newName
	 *            新文件名（注意需要完整路径）
	 * @return true：重命名成功 false：重命名失败
	 */
	public boolean rename(String oldName, String newName) throws IOException;

	/**
	 * 
	 * @description 删除指定目录下的文件
	 * @author liucx
	 * @created 2015-5-19 下午3:09:52
	 * @param path
	 *            待删除文件路径
	 * @return true：删除成功 false：删除失败
	 */
	public boolean remove(String path) throws IOException;

	/**
	 * 
	 * @description 创建目录
	 * @author liucx
	 * @created 2015-5-19 下午3:08:05
	 * @param path
	 *            目录的路径
	 * @return true:创建目录成功 false:创建目录失败
	 */
	public boolean mkdir(String path) throws IOException;

	/**
	 * 
	 * @description 列举文件系统指定目录下所有文件
	 * @author liucx
	 * @created 2015-5-19 下午3:18:13
	 * @param path
	 *            指定目录的完整路径
	 * @return 返回Rubic系统自定义文件信息列表
	 */
	@Deprecated
	public String[] listFiles(String path) throws IOException;

	/**
	 * 
	 * @description 实现文件之间的拷贝
	 * @author liucx
	 * @created 2015-5-22 下午5:44:54
	 * @param str
	 *            源文件路径
	 * @param dest
	 *            目标文件路径
	 * @return true：拷贝成功 false：拷贝失败
	 * @throws IOException
	 */
	public boolean copyFiles(String src, String dest) throws IOException;

	/**
	 * 
	 * @description 读取文件内容到输出流
	 * @author liucx
	 * @created 2015-5-19 下午3:24:26
	 * @param path
	 *            待读取文件路径
	 * @param out
	 *            输出流，由上层组件决定
	 * @return {@link OutputStream}
	 */
	public OutputStream readFile(String path, OutputStream out)
			throws IOException;

	/**
	 * 
	 * @description 写入内容到文件
	 * @author liucx
	 * @created 2015-5-19 下午3:27:29
	 * @param path
	 *            待写入文件的完整路径
	 * @param in
	 *            {@link InputStream}
	 * @return true:写入成功 false：写入失败
	 */
	public boolean writeFile(String path, InputStream in) throws IOException;

}
