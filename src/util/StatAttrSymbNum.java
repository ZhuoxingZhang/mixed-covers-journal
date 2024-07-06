package util;

import java.util.Arrays;
import java.util.List;

import entity.DataTypeEnum;
import entity.FD;
import entity.Parameter;

/**
 * stat the attribute symbol number of an FD/FD cover
 *
 */
public class StatAttrSymbNum {
	public static void runStat(Parameter para) {
		for(String coverType : para.FDCoverTypeList) {
			List<Object> info = Utils.getFDCover(coverType, para);
			if(info == null) {
				System.out.println("name : "+para.dataset.name+" | "+coverType + " | FD No. : null, FD attribute symbol No. : null");
				String stat = para.dataset.name + "," + coverType  + ",null,null";
				Utils.writeContent(Arrays.asList(stat), para.output_add, true);//output statistics
				System.out.println("#########################\n");
				return;
			}
			List<String> schema = (List<String>) info.get(0);
			List<FD> FDs = (List<FD>) info.get(1);
			System.out.println("data set : "+para.dataset.name);
			System.out.println("load "+FDs.size()+" FDs successfully!\n");
			
			int fdAttrNumCount = 0;
			for(FD f : FDs) {
				 fdAttrNumCount += f.getLeftHand().size() + f.getRightHand().size();
			}
			
			System.out.println("name : "+para.dataset.name+" | "+coverType + " | FD No. : "+FDs.size()+ ", FD attribute symbol No. : "+fdAttrNumCount);
			
			String stat = para.dataset.name + "," + coverType + "," + FDs.size() +","+fdAttrNumCount;
			Utils.writeContent(Arrays.asList(stat), para.output_add, true);//output statistics
			System.out.println("#########################\n");
		}
	}
	public static void main(String[] args) {
		Utils.getParameterList(null, null).forEach(StatAttrSymbNum::runStat);

	}

}
