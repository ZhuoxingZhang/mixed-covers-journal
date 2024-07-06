package exp;

import java.util.List;

import entity.FD;
import entity.Parameter;
import util.Utils;

public class FDCoverInfoExp {

	public static void main(String[] args) {
		//FD cover info over the data sets
		for(Parameter para : Utils.getParameterListV1(null)) {
			String str = "";
			for(String coverType : para.FDCoverTypeList) {
				if(para.dataset.col_num > 18 && coverType.equals("optimal")) 
					continue;
				List<Object> info = Utils.getFDCover(coverType, para);
				List<FD> fdcover = (List<FD>) info.get(1);
				if(!coverType.equals("optimal"))
					str += fdcover.size()+" "+Utils.compFDAttrSymbNum(fdcover)+",";
				else
					str += fdcover.size()+" "+Utils.compFDAttrSymbNum(fdcover);
			}
			System.out.println(para.dataset.name+","+str);
		}

	}

}
