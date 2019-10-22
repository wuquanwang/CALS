package cloud.workflowScheduling.util;

import java.io.*;
import java.util.*;

import gnu.getopt.*;

/**
 * a random graph generator with a number of input parameters
 * @author wu
 */
public class RandomGraphGenerator {

	//local test
	public static void main(String[] as){
		RandomGraphGenerator g = new RandomGraphGenerator();
		String s = g.generateGraphFile("D:/test.dot","-d -a -n15 -l5 -L0 -c0.5 -v100 -V100 -e100 -E100");

//		String s = g.generateTreeGraph("D:/test.dot",false,5, 4,"-v100 -V100 -e100 -E100");
		GraphViz.writeDOTGraphToImageFile(s, "png", "D:/test.png");
	}
	
	private boolean d = true;	//directed
	private boolean a = false;	//acyclic
	private int s;	//��ͨ��-s������intree��outtree�������ʽ
	private int n;	//vertex number
	private int l;	//level number
	private double L; // StdVar of vertex number in each level
	private double c; // probability of connecting two adjacent vertex
	
	private int v;    // vertex weight mean
	private double V; // vertex weight StdVar
	private int e;	// edge weight mean		�ò����������룬���û��Ȩ��
	private double E;	// edge weight StdVar
	private Random rnd = new Random();
	
	private static final double JUMPTWO = 0.02;	// the probability of crossing two levels ������ĸ���
	private static final double JUMPTHREE = 0.01;
	
	private void parseArguments(String[] args){
		if (args.length <= 1) {
			System.out.println("Parameters are required.");
			return;
		}
		LongOpt[] longopts = new LongOpt[11];//ʹ��LongOpt���ó�ѡ�����
		longopts[0] = new LongOpt("directed", LongOpt.NO_ARGUMENT, null,'d');
		longopts[1] = new LongOpt("acyclic", LongOpt.NO_ARGUMENT,null, 'a');
		longopts[2] = new LongOpt("shape", LongOpt.REQUIRED_ARGUMENT,null, 's');
		longopts[3] = new LongOpt("vertexNumber", LongOpt.REQUIRED_ARGUMENT, null,'n');
		longopts[4] = new LongOpt("levelNumber", LongOpt.REQUIRED_ARGUMENT,null, 'l');
		longopts[5] = new LongOpt("StdVarForVertexInEachLevel", LongOpt.REQUIRED_ARGUMENT,null, 'L');
		longopts[6] = new LongOpt("connectingProbability", LongOpt.REQUIRED_ARGUMENT, null,'c');
		longopts[7] = new LongOpt("vertexWeightMean", LongOpt.REQUIRED_ARGUMENT,null, 'v');
		longopts[8] = new LongOpt("vertexWeightStdVar", LongOpt.REQUIRED_ARGUMENT,null, 'V');
		longopts[9] = new LongOpt("edgeWeightMean", LongOpt.REQUIRED_ARGUMENT,null, 'e');
		longopts[10] = new LongOpt("edgeWeightStdVar", LongOpt.REQUIRED_ARGUMENT,null, 'E');
		Getopt getopt = new Getopt("dsp", args, "das:n:l:L:c:v:V:e:E:", longopts);
		getopt.setOpterr(false);	//�Լ����д�����
		String str = "";
		int ch;
		while((ch = getopt.getopt()) != -1){
			switch(ch){
				case 'd': d = true; break;
				case 'a': a = true; break;
				case 's': str = getopt.getOptarg(); s = Integer.parseInt(str); break;
				case 'n': str = getopt.getOptarg(); n = Integer.parseInt(str); break;
				case 'l': str = getopt.getOptarg(); l = Integer.parseInt(str); break;
				case 'L': str = getopt.getOptarg(); L = Double.parseDouble(str); break;
				case 'c': str = getopt.getOptarg(); c = Double.parseDouble(str); break;
				case 'v': str = getopt.getOptarg(); v = Integer.parseInt(str); break;
				case 'V': str = getopt.getOptarg(); V = Double.parseDouble(str); break;
				case 'e': str = getopt.getOptarg(); e = Integer.parseInt(str); break;
				case 'E': str = getopt.getOptarg(); E = Double.parseDouble(str); break;
				default:System.out.println("Unrecognized parameter");;
			}
		}
	}
	
	public String generateGraphFile(String outFile, String arguments){
		String s = generateGraph(arguments);
		generateDOTFile(outFile, s);
		return s;
	}

	private void generateDOTFile(String outFile, String s) {
		try{
			FileWriter fw = new FileWriter(new File(outFile));
			fw.write(s);
			fw.flush();
			fw.close();
		}catch(Exception e){
			System.out.println("fails");
		}
	}
	
	//DAGģ����
	public String generateGraph(String arguments) {
		parseArguments(arguments.split(" "));
		
		StringBuilder sb = new StringBuilder();
		sb.append(d ? "digraph {\n" : "graph {\n" );
		
		int[][] levels = generateVerticesEachLevel();//��¼ÿ��level����ʼ����յ㣬�յ㲻����(������һ��level��)
		if(v != 0){
			for(int i = 0;i<levels[levels.length-1][1];i++){
				sb.append(i + " [label=\""+i+":"+gaussianWeight(v, V)+"\"];\n");
			}
		}
		
		//���ڲ������һ���ÿ���ڵ㣬��ӱ�
		for (int i = 0; i < levels.length-1; i++) {
			connectVertices(sb, levels, i);
		}
		if(!a){	//	�������޻�ͼ������β����
			connectVertices(sb, levels, levels.length-1);
		}
		sb.append("}\n");
		System.out.println(sb.toString());
		return sb.toString();
	}
	
	// ������������level��vertices������һ�����ʳ���jump�����
	private void connectVertices(StringBuilder sb, int[][] levels, int cur) {
		for (int k = levels[cur][0]; k < levels[cur][1]; k++){			
			int count = 0;
			int next = (cur+1) % levels.length;
			int next2 = (cur+2) % levels.length;
			int next3 = (cur+3) % levels.length;
			for (int j = levels[next][0]; j < levels[next][1]; j++){
				if (rnd.nextDouble() < c){
					addEdge(sb, k, j);
					count++;
				}
			}
			if(rnd.nextDouble()<JUMPTWO && cur+2<levels.length){	
				int t = levels[next2][0] + rnd.nextInt(levels[next2][1]-levels[next2][0]);
				addEdge(sb, k, t);
			}
			if(rnd.nextDouble()<JUMPTHREE && cur+3<levels.length){	
				int t = levels[next3][0] + rnd.nextInt(levels[next3][1]-levels[next3][0]);
				addEdge(sb, k, t);
			}
			if(count ==0){	
				int t = levels[next][0]	+ rnd.nextInt(levels[next][1]-levels[next][0]);
				addEdge(sb, k, t);
			}
		}
	}
	private void addEdge(StringBuilder sb, int j, int k){
		String edge = d ? " -> " : " -- ";
		String label = e==0? "" : " [label="+gaussianWeight(e, E)+"]";
		sb.append(j + edge + k + label + ";\n"); /* An Edge. */
	}
	
	private int gaussianWeight(double mean, double stdVar){
		int edgeWeight = (int)(mean + stdVar * rnd.nextGaussian());
		while(edgeWeight<0)
			edgeWeight = (int)(mean + stdVar * rnd.nextGaussian());
		return edgeWeight;
	}
	
	private int[][] generateVerticesEachLevel(){
		List<Integer> list = new ArrayList<Integer>();
		int residualNum = n;
		while(residualNum>0){
			int numInCurLevel = (int)(1.0*n/l + rnd.nextGaussian()*L);
			if(numInCurLevel>0){
				list.add(numInCurLevel);
				residualNum -= numInCurLevel;
			}
		}
		int[][] a = new int[list.size()][2];
		int sum = 0;
		for(int i= 0;i<a.length;i++){
			a[i][0]= sum;
			a[i][1]= sum + list.get(i);			
			sum += list.get(i);
		}
		return a;
	}
	
	//in-tree��out-tree�����ɣ����Ľ�
	//TASK SCHEDULING FOR PARALLEL SYSTEMSһ���е�6.4.1 ��|��|�� Classification ������intree��outtree�ȵ��ص�
	//TGFF: task graphs for free	Princeton Univ.  CODES/CASHE'98. IEEE, 1998.	Cited by 1104
	public String generateTreeGraph(String outFile, boolean isIn, int number, int level, String arguments){
		parseArguments(arguments.split(" "));
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n");
		for(int i = 0;i < number*level;i++)
			sb.append(i + " [label=\""+i+":"+gaussianWeight(v, V)+"\"];\n");
		for(int k = 1;k<level;k++){
			for(int i = 0; i<number; i++){
				int tmp = rnd.nextInt(number);
				int n1 = (k-1)*number + tmp;
				int n2 = k*number + i;
				if(isIn)
					addEdge(sb, n1, n2);
				else
					addEdge(sb, n2, n1);
			}
		}
		sb.append("}\n");
		System.out.println(sb.toString());
		generateDOTFile(outFile, sb.toString());
		return sb.toString();
	}
	
	//*******************************����Ϊ�ο�����ǰ�����˵�***************************************
	//Below is a simpler generator from https://stackoverflow.com/questions/12790337/generating-a-random-dag
	/* Nodes/Rank: How 'fat' the DAG should be. */
	private static final int MIN_PER_RANK = 1;
	private static final int MAX_PER_RANK = 5;
	/* Ranks: How 'tall' the DAG should be. */
	private static final int MIN_RANKS = 3;
	private static final int MAX_RANKS = 5;
	/* Chance of having an Edge. */
	private static final double PERCENT = 0.3;
	public String generateDAG0() {
		StringBuilder sb = new StringBuilder();
		Random rnd = new Random();
		int i, j, k, nodes = 0;
		int ranks = MIN_RANKS + rnd.nextInt(MAX_RANKS - MIN_RANKS + 1);

		sb.append("digraph {\n");
		for (i = 0; i < ranks; i++) {
			/* New nodes of 'higher' rank than all nodes generated till now. */
			int new_nodes = MIN_PER_RANK + rnd.nextInt(MAX_PER_RANK - MIN_PER_RANK + 1);

			/* Edges from old nodes ('nodes') to new ones ('new_nodes'). */
			for (j = 0; j < nodes; j++)
				for (k = 0; k < new_nodes; k++)
					if (rnd.nextDouble() < PERCENT)
						sb.append(
								j + " -> " + (k + nodes) + ";\n"); /* An Edge. */

			nodes += new_nodes; /* Accumulate into old node set. */
		}
		sb.append("}\n");
		System.out.println(sb.toString());
		return sb.toString();
	}
}
