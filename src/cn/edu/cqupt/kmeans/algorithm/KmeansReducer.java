package cn.edu.cqupt.kmeans.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.cqupt.kmeans.common.KmeansConstant;
import cn.edu.cqupt.kmeans.util.DataSetParser;

public class KmeansReducer extends Reducer<Text, Text, Text, Text> {

	private String split;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		split = conf.get(KmeansConstant.SPLIT_KEY);
	};

	/**
	 * 
	 * @description 计算出每一个新类的聚类中心
	 * @author liucx
	 * @created 2015-5-13 下午3:51:34
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// 依次解析输入的数据集中每一行数据，准备计算新的聚类中心
		List<ArrayList<Double>> helpList = new ArrayList<ArrayList<Double>>();
		for (Text val : values) {
			String line = val.toString();

			List<Double> tmpList = DataSetParser.parseOneLineData(split, line);
			helpList.add((ArrayList<Double>) tmpList);
		}

		// 将每一个聚类中心的每一个数据用逗号分割
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < helpList.get(0).size(); i++) {
			double sum = 0;

			for (int j = 0; j < helpList.size(); j++) {
				sum += helpList.get(j).get(i);
			}
			double t = sum / helpList.size();
			if (i == 0)
				sb.append(t);
			else
				sb.append(split).append(t);
		}

		// 上下文输出，格式为key+空格+value
		context.write(key, new Text(sb.toString()));
	};
}
