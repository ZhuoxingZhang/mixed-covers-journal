package exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;
import util.Utils;

/**
 * Given an FD set, such as original, non-redundant,... 
 * This experiment computes and saves Key/FD cover, with given FD set as input, such as original, non-redundant, reduced, ...
 * Then, the experiment statistics some properties when computing Key/FD cover. 
 */
public class exp2 {
	/**
	 * Given FDs, compute keyfd cover with different variant.
	 * Then, record some properties of keyfd cover and write keyfd cover into local file.
	 * @param coverType FD cover type
	 * @param para
	 * @param statOutputPath the path of outputting statistics
	 */
	public static void runSingleExp(String coverType, Parameter para,String statOutputPath,String memoryUnit) {
		List<Object> info = Utils.getFDCover(coverType, para);
		if(info == null) {
			System.out.println("name : "+para.dataset.name+" | "+coverType + " keyfd cover | key No. : null, key attribute symbol No. : null"+
					" | FD No. : null, FD attribute symbol No. : null"+
					" | cost : null | max memory use : null");
			
			String stat = para.dataset.name + "," + coverType + " keyfd" + ",null,null,null,null,null,null,null";
			Utils.writeContent(Arrays.asList(stat), statOutputPath, true);//output statistics
			System.out.println("#########################\n");
			return;
		}
		List<String> schema = (List<String>) info.get(0);
		List<FD> FDs = (List<FD>) info.get(1);
		System.out.println("data set : "+para.dataset.name);
		System.out.println("load "+FDs.size()+" FDs successfully!\n");
		
		List<Double> free_mem_list = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem = Utils.getFreeMemory(memoryUnit);
		long startTime = System.currentTimeMillis();
		List<Object> keyfdInfo = Utils.compKeyFDCover(schema, FDs, free_mem_list, memoryUnit);
		long endTime = System.currentTimeMillis();
		free_mem_list.sort(null);//increasing order
		double maxMemUse = start_free_mem - free_mem_list.get(0);//max use of Memory
		long cost = endTime - startTime;//cost of computing keyfd cover
		
		List<Key> minKeys = (List<Key>) keyfdInfo.get(0);
		List<FD> remainFDs = (List<FD>) keyfdInfo.get(1);
		
		
		int keyAttrNumCount = 0;
		for(Key key : minKeys) {
			keyAttrNumCount += key.size();
		}
		int fdAttrNumCount = 0;
		for(FD f : remainFDs) {
			 fdAttrNumCount += f.getLeftHand().size() + f.getRightHand().size();
		}
		
		System.out.println("name : "+para.dataset.name+" | "+coverType + " keyfd cover | key No. : " + minKeys.size()+", key attribute symbol No. : "+keyAttrNumCount+
				" | FD No. : "+remainFDs.size()+ ", FD attribute symbol No. : "+fdAttrNumCount+
				" | cost : "+cost+" ms"+" | max memory use : "+maxMemUse+" "+memoryUnit+"(s)");
		
		String stat = para.dataset.name + "," + coverType + " keyfd" + "," + FDs.size() + "," + minKeys.size() + "," + keyAttrNumCount
				+","+remainFDs.size()+","+fdAttrNumCount+","+cost+","+maxMemUse;
		Utils.writeContent(Arrays.asList(stat), statOutputPath, true);//output statistics
		Utils.writeKeyFDs(schema.size(), keyfdInfo, Utils.getCoverPath(coverType+" keyfd", para));//write keyfd cover into local
		System.out.println("#########################\n");
		
	}
	
	/**
	 * compute keyfd cover of FD of cover type of "original"/"nonredundant"/"reduced"/"canonical"/"minimal"/"reduced minimal"/"optimal",
	 * and record some stats and write keyfd cover into local JSON files.
	 * @param para
	 * @param statOutputPath
	 * @param memoryUnit "GiB"/"MiB"/"KiB"
	 */
	public static void runExps(Parameter para,String statOutputPath,String memoryUnit) {
		for(String coverType : para.FDCoverTypeList) {
			runSingleExp(coverType, para, statOutputPath, memoryUnit);
		}
	}
	
	public static void main(String[] args) {
		Utils.getParameterList(null, DataTypeEnum.NULL_UNCERTAINTY).forEach(para -> runExps(para, para.output_add, "MiB"));
	}

}
