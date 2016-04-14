package cn.edu.cqupt.kmeans.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import cn.edu.cqupt.rubic_core.io.HDFSConnection;

/**
 * 
 * Title: DataSetParser.java
 * 
 * @description:数据集的解析类
 * @author liucx
 * @created 2015-5-14 上午11:35:19
 */
public class DataSetParser {

	/**
	 * 
	 * @description 解析存储在HDFS的聚类中心，聚类中心格式由系统规定 ，为（聚类中心id+空格1,2,3,4,5）
	 * @author liucx
	 * @created 2015-5-14 上午11:35:30
	 * @param splitKey
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static Map<String, ArrayList<Double>> parseGroupCenter(String split,
			String path) throws IOException {

		Map<String, ArrayList<Double>> datas = new HashMap<String, ArrayList<Double>>();
		FileSystem fs = HDFSConnection.getFileSystem();

		FSDataInputStream fsIn = fs.open(new Path(path));
		LineReader lineIn = new LineReader(fsIn);
		Text line = new Text();

		while (lineIn.readLine(line) > 0) {

			StringTokenizer st = new StringTokenizer(line.toString());
			String key = st.nextToken();
			String oneLineNoParse = st.nextToken();
			String[] fields = oneLineNoParse.split(split);

			ArrayList<Double> oneLineData = new ArrayList<Double>();
			for (String tmp : fields) {
				oneLineData.add(Double.parseDouble(tmp));
			}
			datas.put(key, oneLineData);

		}
		fsIn.close();
		return datas;
	}

	/**
	 * 
	 * @description 解析某一个数据集的某一行数据，数据间以split分隔.返回格式为arraylist
	 * @author liucx
	 * @created 2015-5-14 上午11:34:52
	 * @param split
	 * @param line
	 * @return
	 */
	public static List<Double> parseOneLineData(String split, String line) {

		List<Double> oneLineData = new ArrayList<Double>();
		String[] data = line.split(split);
		for (String str : data) {
			oneLineData.add(Double.parseDouble(str));
		}
		return oneLineData;
	}

}
