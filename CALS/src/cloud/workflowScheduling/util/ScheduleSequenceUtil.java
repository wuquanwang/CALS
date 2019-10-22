package cloud.workflowScheduling.util;

import java.util.*;

import cloud.workflowScheduling.setting.*;

/**
 * a utility class to operate on task sequences
 * @author wu
 */
public class ScheduleSequenceUtil {
	private static Random random = new Random();
	
	/**
	 * Build a probabilistic task sequence for @param wf, which is a topological sort. 
	 * The probability is proportionate to @param tctype
	 * 复杂度为 O(n^2): 循环中chooseNextTaskAndRemove部分为O(n), 其他的为遍历边和task。
	 */
	public static int[] probabTaskList(Workflow wf, TProperties.Type tctype){
		int[] taskIndexArr = new int[wf.size()];
		
		List<Task> L = new ArrayList<Task>();	//Empty list that will contain the sorted elements
		//S: Set of all nodes with no incoming edges;  sorted by blevel
		TreeSet<Task> S = new TreeSet<Task>(new TProperties(wf, tctype));
		S.add(wf.getEntryTask());		

		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();
		int tIndex = 0;			//task index in task ordering L
		while(S.size()>0){
			Task task= chooseNextTaskAndRemove(S);// remove a task from S
			
			taskIndexArr[tIndex++] = wf.indexOf(task);
			L.add(task);					// add n to tail of L
    		
			for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
				Task child = e.getDestination();
				Integer count = inEdgeCounts.get(child);
				inEdgeCounts.put(child, count != null ? count+1 : 1);
				if(inEdgeCounts.get(child) == child.getInEdges().size())	//  if m has no other incoming edges then
					S.add(child);					// insert m into S			
			}
		}
		return taskIndexArr;
	}
	
	// Time complexity: O(n)
	private static Task chooseNextTaskAndRemove(TreeSet<Task> S){
		int[] ranks = new int[S.size()];
		double total = 0;
		for(int i = 0;i<S.size();i++){
			ranks[i] = i+1;
			total += ranks[i];
		}
		double fSlice = Math.random() * total;
		double cfTotal = 0;
		
		Task t = null;
		Iterator<Task> iter = S.iterator();
		for (int i = 0; i < S.size(); ++i) {
			cfTotal += ranks[i];
			t = iter.next();
			if (cfTotal > fSlice) {
				iter.remove();
				break;
			}
		}
		return t;
	}
	
	/**
	 * mutate a @param taskIdList for @param wf
	 * 参考tpds, zhu中的Fig.7
	 */
	public static boolean mutate(int[] taskIdList, Workflow wf) {
		int pos = random.nextInt(wf.size() - 2) + 1;	//随机选择一个位置，排除entry和exit
		Task posTask = wf.get(taskIdList[pos]);

		int start = pos, end = pos;
aa:		while(start>=0){
			Task startTask = wf.get(taskIdList[start]);
			for(Edge inEdge : posTask.getInEdges()){
				if(inEdge.getSource() == startTask)
					break aa;
			}
			start--;
		}
bb:		while(end<wf.size()){
			Task endTask = wf.get(taskIdList[end]);
			for(Edge outEdge : posTask.getOutEdges()){
				if(outEdge.getDestination() == endTask)
					break bb;
			}
			end++;
		}
		if(end-start>2){
			int newPos = random.nextInt(end-start-2)+start+1;	//找到的一个新位置
			if(newPos<pos){
				int temp = taskIdList[pos];		//拿出来
				for(int j = pos;j>newPos;j--)
					taskIdList[j] = taskIdList[j-1];
				taskIdList[newPos] = temp;
			}else if(pos < newPos){
				int temp = taskIdList[pos];		//拿出来
				for(int i = pos;i<newPos;i++)
					taskIdList[i] = taskIdList[i+1];
				taskIdList[newPos] = temp;
			}
			return true;
		}
		return false;
	}
	
	/**
	 * crossover * @param X and @param Y
	 */
	public static int[] crossover(int[] X, int[] Y) {
		int size = X.length;
		int pos = random.nextInt(size-2)+1;		//随机选择一个1到n-1之间的位置， cut-off position
		//在list2中把taskIdList1里 0 到 pos位置上的元素都删除掉，然后把list2的剩余元素追加到taskIdList1，形成一个新的task list
		List<Integer> tempList = new ArrayList<Integer>();
		for(int i:Y)
			tempList.add(i);
		for(int i =0;i<=pos;i++)
			tempList.remove(new Integer(X[i]));
		
		int[] S = new int[size];
		for(int i =0;i<size;i++){
			if(i<=pos)
				S[i] = X[i];
			else
				S[i] = tempList.get(i-pos-1);
		}
		return S;
	}
}
