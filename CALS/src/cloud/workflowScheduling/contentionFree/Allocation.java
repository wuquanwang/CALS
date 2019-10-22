package cloud.workflowScheduling.contentionFree;

import static java.lang.Math.*;

import java.util.*;

/**
 * It has two concrete sub-classes: TAllocation, EAllocation. 
 * They are used for allocating tasks and edges, respectively
 * ���������������౻����Ƴɣ����е���Ϣ�п��ܱ��޸�
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
	 * ��allocList�ϴ�readytime��ʼ��Ѱ�������֧��period���ȵ�free time slot
	 */
	public static double searchFreeTimeSlot(List<? extends Allocation> allocList,
			double readyTime, double period ) {
		if(allocList == null || allocList.size() ==0)
			return readyTime;

		if(readyTime + period <= allocList.get(0).getStartTime()){// case1: ��������ǰ��
			return readyTime;
		}
		double EST = 0;
		for(int j = allocList.size() - 1; j >= 0 ; j--){
			Allocation alloc = allocList.get(j);//����alloc�ĺ���
			double startTime = max(readyTime, alloc.getFinishTime()); 
			double finishTime = startTime + period;
			
			if(j == allocList.size() - 1){			// case2: ���������
				EST = startTime;
			}else {								//case3: ���뵽�м䡣alloc�������һ����������allocNext����test�ܷ����
				Allocation allocNext = allocList.get(j+1);
				if(finishTime > allocNext.getStartTime())//����overlap�޷�����
					continue;
				EST = startTime;
			}
			if(readyTime>alloc.getFinishTime())	//�ս�ѭ������Ϊ��ǰ������賢����
				break;
		}
		return EST;
	}
}
