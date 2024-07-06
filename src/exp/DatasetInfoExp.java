package exp;

import java.util.List;

import entity.FD;
import entity.Parameter;
import util.Utils;

public class DatasetInfoExp {

	public static void main(String[] args) {
		//data set information
		for(Parameter para : Utils.getParameterListV1(null)) {
			List<Object> info = Utils.getFDCover("original", para);
			List<FD> fdcover = (List<FD>) info.get(1);
			System.out.println(para.dataset.name+","+para.dataset.row_num+","+para.dataset.col_num+","+fdcover.size()+","+Utils.compFDAttrSymbNum(fdcover));
		}

	}

}
