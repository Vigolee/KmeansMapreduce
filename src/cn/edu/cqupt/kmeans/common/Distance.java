package cn.edu.cqupt.kmeans.common;

public class Distance {

	/**
	 * @param args
	 * 
	 * 常用两个向量的距离计算
	 */
	/**
	 * 欧式距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double euclideamDistance(String[] a,String[] b){
		double distance=0;
		int k=a.length<b.length?a.length:b.length;
		double[] a1=new double[k];
		double[] b1=new double[k];
		for (int i = 0; i < k; i++) {
			a1[i]=Double.parseDouble(a[i]);
			b1[i]=Double.parseDouble(b[i]);
		}
		distance=euclideamDistance(a1, b1);
		return distance;
	}
	/**
	 * 欧式距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double euclideamDistance(double[] a1, double[] b1){
		double distance=0;
		for (int i = 0; i < b1.length; i++) {
			distance=distance+Math.pow(a1[i]-b1[i], 2);
		}
		return Math.sqrt(distance);
	}
	/**
	 * 曼哈顿距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double manhattanDistance(String[] a,String[] b){
		double distance=0;
		int k=a.length<b.length?a.length:b.length;
		double[] a1=new double[k];
		double[] b1=new double[k];
		for (int i = 0; i < k; i++) {
			a1[i]=Double.parseDouble(a[i]);
			b1[i]=Double.parseDouble(b[i]);
		}
		distance=manhattanDistance(a1, b1);
		return distance;
	}
	/**
	 * 曼哈顿距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double manhattanDistance(double[] a,double[] b){
		double distance=0;
		for (int i = 0; i < b.length; i++) {
			distance=distance+Math.abs(a[i]-b[i]);
		}
		return distance;
	}

	
	/**
	 * 余弦距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double cosineDistance(String[] a,String[] b){
		double distance=0;
		int k=a.length<b.length?a.length:b.length;
		double[] a1=new double[k];
		double[] b1=new double[k];
		for (int i = 0; i < k; i++) {
			a1[i]=Double.parseDouble(a[i]);
			b1[i]=Double.parseDouble(b[i]);
		}
		distance=cosineDistance(a1, b1);
		return distance;
	}
	/**
	 * 余弦距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double cosineDistance(double[] a,double[] b){
		double distance=0;
		double ab=0;
		double aa=0;
		double bb=0;
		for (int i = 0; i < b.length; i++) {
			ab=ab+a[i]*b[i];
			aa=aa+Math.pow(a[i], 2);
			bb=bb+Math.pow(b[i], 2);
		}
		distance=1-((double)ab/(double)(Math.sqrt(aa)*Math.sqrt(bb)));
		return distance;
	}

	/**
	 * 谷本距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double tanimotoDistance(String[] a,String[] b){
		double distance=0;
		int k=a.length<b.length?a.length:b.length;
		double[] a1=new double[k];
		double[] b1=new double[k];
		for (int i = 0; i < k; i++) {
			a1[i]=Double.parseDouble(a[i]);
			b1[i]=Double.parseDouble(b[i]);
		}
		distance=tanimotoDistance(a1, b1);
		return distance;
	}
	/**
	 * 谷本距离
	 * @param a向量，b向量
	 * @return 两个向量的距离
	 * */
	public static double tanimotoDistance(double[] a,double[] b){
		double distance=0;
		double ab=0;
		double aa=0;
		double bb=0;
		for (int i = 0; i < b.length; i++) {
			ab=ab+a[i]*b[i];
			aa=aa+Math.pow(a[i], 2);
			bb=bb+Math.pow(b[i], 2);
		}
		distance=1-((double)ab/(double)(Math.sqrt(aa)+Math.sqrt(bb)-ab));
		return distance;
	}
	/**
	 * 平方欧式距离
	 * */
	public static double squaredEuclideamDistance(String[] a,String[] b){
		double distance=0;
		int k=a.length<b.length?a.length:b.length;
		double[] a1=new double[k];
		double[] b1=new double[k];
		for (int i = 0; i < k; i++) {
			a1[i]=Double.parseDouble(a[i]);
			b1[i]=Double.parseDouble(b[i]);
		}
		distance=squaredEuclideamDistance(a1, b1);
		return distance;
	}
	/**
	 * 平方欧式距离
	 * */
	public static double squaredEuclideamDistance(double[] a1, double[] b1){
		double distance=0;
		for (int i = 0; i < b1.length; i++) {
			distance=distance+Math.pow(a1[i]-b1[i], 2);
		}
		return distance;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
