package exp;

import java.util.List;
import entity.FD;
import entity.Key;
import entity.Parameter;
import util.Utils;

public class MixedCoverInfoExp {

	public static void main(String[] args) {
		//mixed cover info over data sets
		for(Parameter para : Utils.getParameterListV1(null)) {
			String str = "";
			for(String coverType : para.KeyFDCoverTypeList) {
				if(para.dataset.col_num > 18 && coverType.equals("optimal keyfd")) 
					continue;
				int index = coverType.indexOf(" keyfd");
				String fd_cover_type = coverType.substring(0, index);
				List<Object> fdinfo = Utils.getFDCover("original", para);
				List<String> R = (List<String>) fdinfo.get(0);
				List<FD> original = (List<FD>) fdinfo.get(1);
				List<Object> info = Utils.comp_Mixed_Cover_Sequential(R, original, fd_cover_type);
				Utils.writeKeyFDs(R.size(), info, Utils.getCoverPath(coverType, para));//write keyfd cover into local
//				List<Object> info = Utils.getKeyFDCover(coverType, para, false);
				List<Key> keys = (List<Key>) info.get(0);
				List<FD> fdcover = (List<FD>) info.get(1);
				if(!coverType.equals("optimal keyfd"))
					str += "("+keys.size()+","+fdcover.size()+") ("+Utils.compKeyAttrSymbNum(keys)+","+Utils.compFDAttrSymbNum(fdcover)+");";
				else
					str += "("+keys.size()+","+fdcover.size()+") ("+Utils.compKeyAttrSymbNum(keys)+","+Utils.compFDAttrSymbNum(fdcover)+")";
			}
			System.out.println(para.dataset.name+";"+str);
		}

	}

}
