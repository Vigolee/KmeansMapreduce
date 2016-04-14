package cn.edu.cqupt.kmeans.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import cn.edu.cqupt.kmeans.common.KmeansConstant;
import cn.edu.cqupt.kmeans.util.DataSetParser;
import cn.edu.cqupt.rubic_core.io.HDFSConnection;
import cn.edu.cqupt.rubic_core.io.HDFSFileDaoImpl;
import cn.edu.cqupt.rubic_framework.algorithm_interface.ErrorInputFormatException;
import cn.edu.cqupt.rubic_framework.algorithm_interface.OperationalDataOnHadoop;
import cn.edu.cqupt.rubic_framework.service_interface.FileDao;

/**
 * 
 * Title: KmeansDriver.java
 * 
 * @description:kmeans的驱动类（主类），进行kmeans的配置，初始化job以及job的运行
 * @author liucx
 * @created 2015-5-15 下午5:56:22
 */
public class KmeansDriver implements OperationalDataOnHadoop {

	private static final Log LOG = LogFactory.getLog(KmeansDriver.class);
	private KmeansPrepare kmeansPrepare;

	protected FileDao fileDao;
	protected String dataSource;
	protected String subPath;
	protected double[] parameters;

	protected String outputFolderPath;
	protected String outputFilePath;
	protected String centerPath;

	public void init(String dataSource, String subPath, double... parameters) {
		this.dataSource = dataSource;
		this.subPath = subPath;
		this.parameters = parameters;
		kmeansPrepare = new KmeansPrepare();
		kmeansPrepare.allocatePath();
		fileDao = new HDFSFileDaoImpl();
	}

	/**
	 * 
	 * @description 配置kmeans的参数（包括输入输出的键值、输入输出路径）
	 * @author liucx
	 * @created 2015-5-15 下午5:53:15
	 * @param parameters
	 *            parameters[0]为输入路径，存放数据集，parameters[1]为输出路径，parameters[2]
	 *            为初始聚类中心的路径 ，parameter[3]为jobId
	 * @param isReduce
	 *            是否进行reduce操作，当聚类中心稳定后，只需要再执行一次map函数即可确定聚类中心
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void run(boolean isReduce) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		conf.setStrings(KmeansConstant.OLD_CENTER_PATH_KEY, centerPath);
		conf.setStrings(KmeansConstant.SPLIT_KEY, KmeansConstant.SPLIT);
		Job job = new Job(conf, "Kmeans");
		job.setJarByClass(KmeansDriver.class);
		job.setMapperClass(KmeansMapper.class);
		if (isReduce)
			job.setReducerClass(KmeansReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path input = new Path(dataSource);
		Path output = new Path(outputFolderPath);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	public String run(String dataSource, String subPath, Object configuration,
			double... parameters) throws ErrorInputFormatException {

		init(dataSource, subPath, parameters);
		if (!kmeansPrepare.checkInput()) {
			LOG.error("error input format for mapreduced kmeans algorithm");
			throw new ErrorInputFormatException();
		}

		try {
			kmeansPrepare.generateRawCenters(parameters[0]);
		} catch (IOException e1) {
			LOG.error("cannot read or write files");
			e1.printStackTrace();
		}

		ConvergeCheck convergeCheck = new ConvergeCheck();
		try {
			int index = 0;
			do {
				LOG.info("iteration times:" + index++);
				System.out.println("outputFolderPath: " + outputFolderPath);
				System.out.println("outputFilePath: " + outputFolderPath);
				System.out.println("centerPath: " + outputFolderPath);
				run(true);
			} while (!convergeCheck.isFinished());
			run(false);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return outputFilePath;
	}

	class KmeansPrepare {
		private final String OUTPUT_NAME = "/output";
		private final String CENTER_NAME = "/centers";
		private final String OUTPUT_FILE_NAME = "/part-r-00000";

		// 分配kmeans需要的存储路径
		public void allocatePath() {
			outputFolderPath = subPath + OUTPUT_NAME;
			outputFilePath = outputFolderPath + OUTPUT_FILE_NAME;
			centerPath = subPath + CENTER_NAME;
//			System.out.println("outputFolderPath: " + outputFolderPath);
//			System.out.println("outputFilePath: " + outputFolderPath);
//			System.out.println("centerPath: " + outputFolderPath);
		}

		public boolean checkInput() {
			if (parameters == null || parameters.length != 1)
				return false;

			return true;
		}

		/**
		 * 
		 * @description 选取输入数据及中前k个样本作为初始聚类中心，格式为中心序号+空格+数据1+，+数据2...
		 * @author liucx
		 * @created 2015-5-29 上午10:31:38
		 * @param k
		 *            聚类个数
		 * @throws IOException
		 */
		public void generateRawCenters(double k) throws IOException {
			FileSystem fs = HDFSConnection.getFileSystem();
			FSDataInputStream in = fs.open(new Path(dataSource));
			FSDataOutputStream out = fs.create(new Path(centerPath));
			LineReader lineIn = new LineReader(in);
			Text text = new Text();
			int index = 0;
			while (lineIn.readLine(text) > 0 && index < k) {
				index++;
				StringBuffer sb = new StringBuffer();
				sb.append(index).append(" ").append(text.toString())
						.append("\r\n");
				out.write(sb.toString().getBytes());
			}
			HDFSConnection.close(fs);
		}
	}

	class ConvergeCheck {

		// 数据集解析间隔符，暂时写死
		private String parseSplit = ",";

		/**
		 * 
		 * @description 检查kmeans是否聚类迭代完成（即聚类中心是否稳定）
		 * @author liucx
		 * @created 2015-5-29 上午10:34:55
		 * @return true：聚类完成 false：聚类失败
		 * @throws IOException
		 */
		public boolean isFinished() throws IOException {

			double distance = 0;

			Map<String, ArrayList<Double>> oldCenter = DataSetParser
					.parseGroupCenter(parseSplit, centerPath);
			Map<String, ArrayList<Double>> newCenter = DataSetParser
					.parseGroupCenter(parseSplit, outputFilePath);

			Set<String> keys = oldCenter.keySet();
			Iterator<String> iterator = keys.iterator();
			while (iterator.hasNext()) {
				String tmpKey = iterator.next();
				List<Double> oldLine = oldCenter.get(tmpKey);
				List<Double> newLine = newCenter.get(tmpKey);

				for (int i = 0; i < oldCenter.get(tmpKey).size(); i++) {
					double t1 = Math.abs(oldLine.get(i));
					double t2 = Math.abs(newLine.get(i));
					distance += Math.pow((t1 - t2) / (t1 + t2), 2);
				}
			}
			fileDao.copyFiles(outputFilePath, centerPath);
			fileDao.remove(outputFolderPath);
			return distance == 0;
		}
	}

	public static void main(String[] args) {
		KmeansDriver kd = new KmeansDriver();
		String dataSource = "hdfs://172.22.144.214:9000/kmeans/input/wine.data";
//		String dataSource = "hdfs://localhost:9000/input/wine.txt";
		String subPath = "hdfs://172.22.144.214:9000/kmeans/output";
//		String subPath = "hdfs://localhost:9000/output";
		double parameters = 3;
		try {
			kd.run(dataSource, subPath, null,parameters);
		} catch (ErrorInputFormatException e) {
			e.printStackTrace();
		}
	}
}
