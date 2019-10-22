package cloud.workflowScheduling.contentionFree;

import static java.lang.Math.*;

import java.util.*;

/**
 * It has two concrete sub-classes: TAllocation, EAllocation. 
 * They are used for allocating tasks and edges, respectively
 * 这个类和其两个子类被迫设计成：其中的信息有可能被修改
 * @author wu
 */
public abstract class Allocation implements Comparable<Allocation>{
	protected double startTime;
	protected double finishTime;
	
	public double getStartTime() {
		return startTime;
	}
	public double getFinishTime() {
		return finishTime;
	}
	//used by ProLiS and ICPCP classes
	public void setStartTime(double startTime) {
		this.startTime = startTime;
	}
	//used by Solution when VM is upgraded, by Adaptor
	public void setFinishTime(double finishTime) {
		this.finishTime = finishTime;
	}
	
	public int compareTo(Allocation o) {
		if(this.getStartTime() > o.getStartTime())
			return 1;
		else if(this.getStartTime() < o.getStartTime())
			return -1;
		return 0;
	}
	
	/**
	 * Search the earliest time slot in @param allocList from @param readyTime,
	 * which is no less than @param period
	 * 在allocList上从readytime开始，寻找最早的支持period长度的free time slot
	 */
	public static double searchFreeTimeSlot(List<? extends Allocation> allocList,
			double readyTime, double period ) {
		if(allocList == null || allocList.size() ==0)
			return readyTime;

		if(readyTime + period <= allocList.get(0).getStartTime()){// case1: 插入在最前面
			return readyTime;
		}
		double EST = 0;
		for(int j = allocList.size() - 1; j >= 0 ; j--){
			Allocation alloc = allocList.get(j);//放在alloc的后面
			double startTime = max(readyTime, alloc.getFinishTime()); 
			double finishTime = startTime + period;
			
			if(j == allocList.size() - 1){			// case2: 放在最后面
				EST = startTime;
			}else {								//case3: 插入到中间。alloc不是最后一个，还存在allocNext；需test能否插入
				Allocation allocNext = allocList.get(j+1);
				if(finishTime > allocNext.getStartTime())//存在overlap无法插入
					continue;
				EST = startTime;
			}
			if(readyTime>alloc.getFinishTime())	//终结循环，因为更前面的无需尝试了
				break;
		}
		return EST;
	}
}
