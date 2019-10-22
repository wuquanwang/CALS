package cloud.workflowScheduling.contentionAware;

import java.util.*;
import cloud.workflowScheduling.contentionFree.*;
import cloud.workflowScheduling.setting.*;
import cloud.workflowScheduling.util.*;

/**
 * adapt a contention-free solution to a contention-aware one.
 * Two methods: exclusive, shared
 * A precondition for adapting：duplication is not used in solution
 * @author wu
 */
public class Adaptor {
	/**
	 * build a contention-aware csolution from @param solution for @param wf
	 * via the exclusive manner
	 */
	public CSolution buildFromSolutionExclusive(Solution solution, Workflow wf, TProperties.Type type){
		CSolution csolution = new CSolution();
		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();	//count # of inEdges for each task
		
		//waitingEdges: whose source tasks have finished and waiting for allocating bandwidth resources
		List<EAllocation> waitingEdges = new ArrayList<>();
		List<EAllocation> ingEdges = new ArrayList<>();	//此处用PriorityQueue有bug，因涉及EAllocation对象中的时间修改
		PriorityQueue<TAllocation> ingTasks = new PriorityQueue<>((TAllocation a, TAllocation b) -> {
			return Double.compare(a.getFinishTime(), b.getFinishTime());
		});		
		TAllocation allocEntry = solution.getFirstTA(wf.getEntryTask());
		ingTasks.add(allocEntry);
		csolution.addTaskToVM(allocEntry.getVM(), allocEntry.getTask(), 0); 
		
		double currentTime = 0;	//the current time during simulating execution
		//Each one in ingTasks and ingEdges is in execution
		//In each iteration, an edge or task finishes execution, and updates currentTime to its finish time
		while(ingTasks.size()>0 || ingEdges.size()>0){	
			EAllocation curEAlloc = null;//the executing edge with the earliest finish time
			for(EAllocation tempEA : ingEdges)
				if(curEAlloc == null || tempEA.getFinishTime() < curEAlloc.getFinishTime())
					curEAlloc = tempEA;
			TAllocation curTAlloc = ingTasks.peek();//the executing task with the earliest finish time
			
			//edge or task?
			if(curTAlloc == null || (curEAlloc!=null 
					&& curEAlloc.getFinishTime() < curTAlloc.getFinishTime())){
				ingEdges.remove(curEAlloc);	
				currentTime = curEAlloc.getFinishTime();
				
				Task dTask = curEAlloc.getEdge().getDestination();
				Integer count = inEdgeCounts.get(dTask);
				inEdgeCounts.put(dTask, count != null ? count+1 : 1);
				if(inEdgeCounts.get(dTask) == dTask.getInEdges().size()){//all inEdges of dTask have finished
					VM vm = solution.getFirstTA(dTask).getVM();
					//Note: insert is impossible here. The end of VM is directly used
					double startTime = Math.max(currentTime, csolution.getVMFinishTime(vm));
					TAllocation alloc = csolution.addTaskToVMEnd(vm, dTask, startTime);
					ingTasks.add(alloc);
				}
			}else{
				ingTasks.poll();
				currentTime = curTAlloc.getFinishTime();
				for(Edge outEdge : curTAlloc.getTask().getOutEdges()){
					TAllocation dAlloc = solution.getFirstTA(outEdge.getDestination());
					EAllocation ealloc = new EAllocation(outEdge, curTAlloc.getVM(), dAlloc.getVM());
					waitingEdges.add(ealloc);
				}
				
				final HashMap<Task, Double> props = new TProperties(wf, type);
				Collections.sort(waitingEdges, (EAllocation a, EAllocation b) ->{
					double a1 = props.get(a.getEdge().getDestination());
					double b1 = props.get(b.getEdge().getDestination());
					return  -1*Double.compare(a1, b1);
				});		
			}
			
			Iterator<EAllocation> iter = waitingEdges.iterator();
			//检测waiting edges在currentTime下能否获取带宽资源变成 ingEdges
			//check which from waitingEdges can get bandwidth resources without contention
			while(iter.hasNext()){		
				EAllocation ealloc = iter.next();
				boolean flag = true;	//no contention flag
				List<EAllocation> ealist = csolution.getEAOutList(ealloc.getSourceVM());
				if(ealist != null){
					EAllocation lastEA = ealist.get(ealist.size()-1);
					if(lastEA.getFinishTime() > currentTime+Config.EPS) //放在最后
						flag = false;
				}
				ealist = csolution.getEAInList(ealloc.getDestVM());
				if(ealist != null){
					EAllocation lastEA = ealist.get(ealist.size()-1);
					if(lastEA.getFinishTime()>currentTime+Config.EPS)
						flag = false;
				}
				if(flag || ealloc.getSourceVM() == ealloc.getDestVM()){
					ealloc.setStartTime(currentTime);
					if(ealloc.getSourceVM() == ealloc.getDestVM())
						ealloc.setFinishTime(currentTime);
					csolution.addEdge(ealloc);
					
					iter.remove();
					ingEdges.add(ealloc);		
				}
			}
		}
		return csolution;
	}
	
	/**
	 * build a contention-aware csolution from @param solution for @param wf
	 * via the shared manner
	 */
	public CSolution buildFromSolutionShared(Solution solution, Workflow wf){
		CSolution csolution = new CSolution();
		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();	//count # of inEdges for each task
		
		//PriorityQueue因发现偶有poll返回非最小值的情况，所以使用list；应该是因为EdgeSnapshot的动态修改有关。
		List<EASnapshot> ingEdgeSnaps = new ArrayList<>();	
		PriorityQueue<TAllocation> ingTasks = new PriorityQueue<>((TAllocation a, TAllocation b) -> {
			return Double.compare(a.getFinishTime(), b.getFinishTime());
		});		
		TAllocation allocEntry = solution.getFirstTA(wf.getEntryTask());
		ingTasks.add(allocEntry);
		csolution.addTaskToVM(allocEntry.getVM(), allocEntry.getTask(), 0); 
		
		double currentTime = 0;	//the current time during simulating execution
		//Each one in ingTasks and ingEdgeSnaps is in execution  
		//任务的结束时间是直接确定的，边的结束时间要动态确定——未确定的时候设为最大值
		//In each iteration, an edge or task finishes execution, and updates currentTime to its finish time
		while(ingTasks.size()>0 || ingEdgeSnaps.size()>0){				
			EASnapshot curES = null;
			for(EASnapshot tempES : ingEdgeSnaps)
				if(curES == null || tempES.getRequiredTime() < curES.getRequiredTime())
					curES = tempES;
			TAllocation curTAlloc = ingTasks.peek();
			
			//edgeSnap or task?
			if(curTAlloc == null || (curES!=null 
					&& curES.getRequiredTime() < curTAlloc.getFinishTime() - currentTime)){
				ingEdgeSnaps.remove(curES);	

				double timeDiff = curES.getRequiredTime();
				currentTime += timeDiff;
				Edge curEdge = curES.getEdge();
				
				//add to csolution
				EAllocation ealloc = new EAllocation(curEdge, curES.getsVM(),
						curES.getdVM(), curES.getStartTime(), currentTime);
				csolution.addEdge(ealloc);
				
				//update remDataSize for ingEdgeSnaps 
				for(EASnapshot tempES : ingEdgeSnaps)
					tempES.dataSubstract(timeDiff);
				//re-determine bandwidth for ingEdgeSnaps
				determinePortion(new HashSet<EASnapshot>(ingEdgeSnaps));
				
				Task dTask = curEdge.getDestination();
				Integer count = inEdgeCounts.get(dTask);
				inEdgeCounts.put(dTask, count != null ? count+1 : 1);
				if(inEdgeCounts.get(dTask) == dTask.getInEdges().size()){//all inEdges of dTask have finished
					VM dVM = solution.getFirstTA(dTask).getVM();
					double startTime = Math.max(currentTime, csolution.getVMFinishTime(dVM));
					TAllocation alloc = csolution.addTaskToVMEnd(dVM, dTask, startTime);
					ingTasks.add(alloc);
				}
			}else{
				ingTasks.poll();
				double timeDiff = curTAlloc.getFinishTime() - currentTime;
				currentTime = curTAlloc.getFinishTime();
				//update remDataSize for ingEdgeSnaps
				for(EASnapshot tempES : ingEdgeSnaps)
					tempES.dataSubstract(timeDiff);

				for(Edge outEdge : curTAlloc.getTask().getOutEdges()){
					Task dTask = outEdge.getDestination();
					TAllocation dAlloc = solution.getFirstTA(dTask);
					double dataSize = curTAlloc.getVM() == dAlloc.getVM() ? 0 : outEdge.getDataSize();
					
					EASnapshot snap = new EASnapshot(outEdge, curTAlloc.getVM(), dAlloc.getVM(),
							currentTime, dataSize);
					ingEdgeSnaps.add(snap);
				}
				//re-determine bandwidth for ingEdgeSnaps
				determinePortion(new HashSet<EASnapshot>(ingEdgeSnaps));
			}
		}
		return csolution;
	}

	/**
	 * determine bandwidth allocation for @param ingEASnaps
	 * 大小为0的边必须放入ingEdges当中，因为依赖关系；在带宽分配中则舍去
	 */
	private void determinePortion(HashSet<EASnapshot> ingEASnaps){
		//list of EASnapshot in each VM's incoming / outgoing port
		HashMap<VM, List<EASnapshot>> inMap = new HashMap<>(); 
		HashMap<VM, List<EASnapshot>> outMap = new HashMap<>(); 
		for(EASnapshot es : ingEASnaps){
			VM sVM = es.getsVM(), dVM = es.getdVM();
			if(outMap.get(sVM) == null)
				outMap.put(sVM, new ArrayList<EASnapshot>());
			outMap.get(sVM).add(es);
			if(inMap.get(dVM) == null)
				inMap.put(dVM, new ArrayList<EASnapshot>());
			inMap.get(dVM).add(es);
		}
		
		//all ports with their list of EASnapshot
		PriorityQueue<Port> ports = new PriorityQueue<>((Port a, Port b) ->{
			return -1 * Double.compare(a.getNumber(),b.getNumber());	//逆序
		});
		for(VM vm : inMap.keySet())	
			ports.add(new Port(vm, false, VM.NETWORK_SPEED, inMap.get(vm)));
		for(VM vm : outMap.keySet())
			ports.add(new Port(vm, true, VM.NETWORK_SPEED, outMap.get(vm)));
		
		while(ingEASnaps.size()>0){
			Port port = ports.poll();
			for(EASnapshot es : port.getEsnaps()){
				if(ingEASnaps.remove(es) == false)	//false 表示该传输边的带宽已被分配
					continue;
				if(es.getRemDataSize() == 0 )	//EASnapshot大小为0的，或剩余为0的不参与带宽分配
					continue;

				Port theOtherPort = null;
				VM theOtherVM = port.isOut()?es.getdVM():es.getsVM();
				for(Port tempPort : ports)
					if(tempPort.getVm() == theOtherVM && tempPort.isOut() != port.isOut()){
						theOtherPort = tempPort;
						break;
					}
				
				double tempBand = port.getRemBandWidth() / port.getNumber() ;	
				//because tempBand may be less than theOtherPort.getRemainingBand()
				tempBand = Math.min(tempBand, theOtherPort.getRemBandWidth());
				
				es.setBandwidth(tempBand);
				port.bandSubstract(tempBand);
				theOtherPort.bandSubstract(tempBand);
			}
		}
	}
	
	/**
	 * simply add edge allocations for @param s without considering contention
	 * so it is still an ideal solution. 	这个解仅用于演示，同样也不支持复制情况下的转化。
	 */
	public CSolution convertToIdealCSolution(Solution s){
		CSolution cs = new CSolution();
		for(VM vm : s.getUsedVMSet()){
			List<TAllocation> list = s.getTAListOnVM(vm);
			for(TAllocation alloc : list){
				cs.addTaskToVMEnd(vm, alloc.getTask(), alloc.getStartTime());
				
				for(Edge e :alloc.getTask().getInEdges()){
					TAllocation sAlloc = s.getTAList(e.getSource()).get(0);
					EAllocation ea = new EAllocation(e, sAlloc.getVM(), alloc.getVM(), sAlloc.getFinishTime());
					if(sAlloc.getVM()!= alloc.getVM()){
						cs.addEdge(ea);
					}
				}
			}
		}
		return cs;
	}
	
	//local test
	public static void main(String[] args){
		//F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax
		Workflow wf = new Workflow("F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax");
    	LSUtil lsUtil = new LSUtil(wf);
    	Adaptor adaptor = new Adaptor();

    	Solution solution = lsUtil.getFastSchedule();
		CSolution ideal = adaptor.convertToIdealCSolution(solution);
		
    	ChartUtil.visualizeScheduleNew(solution);
    	ChartUtil.visualizeScheduleNew(ideal);
    	System.out.println(solution.validate(wf)+"\t"+ideal.validate(wf));

	}
}
/**
 * A snapshot for edge allocation. 
 * Its allocated bandwidth may be different in different time.
 */
class EASnapshot{
	private Edge edge;
	private VM sVM, dVM;
	private double startTime;
	private double remDataSize;	//remaining data size
	private double bandwidth = 1;  // avoid denominator=0
	public EASnapshot(Edge edge, VM sVM, VM dVM,
			double startTime, double remDataSize) {
		this.edge = edge;
		this.sVM = sVM;
		this.dVM = dVM;
		this.startTime = startTime;
		this.remDataSize = remDataSize;
	}
	public double getRequiredTime(){
		if(this.bandwidth == 0){	//以上的带宽分配的算法是可能会出现为0的情况的，无法避免
			return Double.MAX_VALUE;
		}
		return this.remDataSize / this.bandwidth;
	}
	private static final double EPS = 0.00001;
	public void dataSubstract(double timeDiff) {
		if(this.remDataSize + EPS < timeDiff * bandwidth)
			throw new RuntimeException("remDataSize is less than 0");
		this.remDataSize -= timeDiff * bandwidth;	
		if(this.remDataSize < EPS)
			this.remDataSize = 0;
	}
	public void setBandwidth(double bandwidth) {	this.bandwidth = bandwidth;}

	//-----------------------------------getters-----------------------------
	public Edge getEdge() {	return edge;}
	public VM getsVM() {return sVM;}
	public VM getdVM() {return dVM;}
	public double getStartTime() {return startTime;}
	public double getRemDataSize() {return remDataSize;}
	public double getBandwidth() {	return bandwidth;	}

	public String toString() {
		return "EdgeSnapshot [" + edge.getSource().getName() + " -> " + edge.getDestination().getName()
				+ ", sVM=" + sVM.getId() + ", dVM=" + dVM.getId()
				+ ", rem:" + remDataSize +", ban:" + bandwidth+", fTime:"+getRequiredTime()+"]";
	}
}
/**
 * Port is an auxiliary class for bandwidth allocation.
 * @author wu
 */
class Port{	
	private VM vm;
	private boolean isOut;
	private double remBandWidth;	//remaining bandwidth resource in this port
	private List<EASnapshot> esnaps;
	private long number;  	// # of EASnapshot(!=0) in this port
	public Port(VM vm, boolean isOut, double remBandWidth, List<EASnapshot> esnaps) {
		this.vm = vm;
		this.isOut = isOut;
		this.remBandWidth = remBandWidth;
		this.esnaps = esnaps;
		this.number = esnaps.stream()
				.filter(es -> es.getRemDataSize() > 0)
				.count();
	}
	public void bandSubstract(double decrement){
		this.remBandWidth -= decrement;
		this.number--;
		if(this.remBandWidth <0 || this.number <0)
			throw new RuntimeException("remBandWidth is less than 0");
	}
	//--------------------------------------getters-----------------------------
	public VM getVm() {	return vm;	}
	public boolean isOut() {	return isOut;	}
	public double getRemBandWidth() {	return remBandWidth;	}
	public List<EASnapshot> getEsnaps() {	return esnaps;}
	public long getNumber() {	return number;	}
	public String toString() {
		return "Port [vm" + vm.getId() + ", " + (isOut ? "out":"in")
				+ ", "+number+", " + remBandWidth + "]";
	}
}