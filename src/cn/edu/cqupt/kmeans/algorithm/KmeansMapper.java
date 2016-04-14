package cn.edu.cqupt.kmeans.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.cqupt.kmeans.common.KmeansConstant;
import cn.edu.cqupt.kmeans.util.DataSetParser;

/**
 * 
 * Title: KmeansMapper.java
 * 
 * @description:Kmeans算法中的map过程
 * @author liucx
 * @created 2015-5-13 下午3:23:53
 */
public class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Map<String, ArrayList<Double>> datas;
	private String split;
	private Configuration conf;

	/**
	 * 
	 * @description 
	 *              setup函数，在map函数前执行且只运行一次，运行map函数前将用户规定的聚类中心（即旧聚类中心）加载到HDFS并解析,
	 *              存储到成员变量Map<String, ArrayList<Double>>中
	 * @author liucx
	 * @created 2015-5-13 下午3:13:12
	 * @param context
	 *            hadoop系统上下文
	 * @throws IOException
	 * @throws InterruptedException
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		split = conf.get(KmeansConstant.SPLIT_KEY);
		String oldCentersPath = conf.get(KmeansConstant.OLD_CENTER_PATH_KEY);
		datas = DataSetParser.parseGroupCenter(split, oldCentersPath);
		super.setup(context);
	};

	/**
	 * 
	 * @description 从HDFS读入待聚类数据集，依次比较与各聚类中心的距离
	 * @author liucx
	 * @created 2015-5-13 下午3:26:22
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(split);

		Set<String> keys = datas.keySet();
		Iterator<String> iterator = keys.iterator();
		double minDistance = Double.MAX_VALUE;

		String centerKey = null;
		while (iterator.hasNext()) {
			String tmpkey = iterator.next();
			double currentDistance = 0;

			for (int j = 0; j < fields.length; j++) {
				double t1 = Math.abs(datas.get(tmpkey).get(j));
				double t2 = Math.abs(Double.parseDouble(fields[j]));
				currentDistance += Math.pow((t1 - t2) / (t1 + t2), 2);
			}
			if (minDistance > currentDistance) {
				minDistance = currentDistance;
				centerKey = tmpkey;
			}
		}
		context.write(new Text(centerKey), value);
	};
}
