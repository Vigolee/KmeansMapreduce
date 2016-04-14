package cn.edu.cqupt.rubic_framework.algorithm_interface;

public interface OperationalDataOnHadoop {
	/**
	 * 
	 * @description hadoop上的运算过程
	 * @author liucx
	 * @created 2015-5-28 下午4:40:38
	 * @param dataSource
	 *            数据集存放路径
	 * @param subPath
	 *            系统随机分配的路径，可供用户自由分配
	 * @param parameters
	 *            用户输入的基本参数
	 * @return 输出路径
	 * @throws ErrorInputFormatException
	 */
	public String run(String dataSource, String subPath, Object configuration,
			double... parameters) throws ErrorInputFormatException;
}
