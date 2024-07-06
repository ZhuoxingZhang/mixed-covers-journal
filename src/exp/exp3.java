package exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import entity.FD;
import entity.Key;
import entity.Parameter;
import util.QuineMcCluskey;
import util.Utils;

/**
 * Following up experiment 2, in experiment 3, we compute corresponding FD cover of
 *  \Sigma_FD of key/FD cover of corresponding FD cover type. For example, after we get a key/FD cover of a minimal FD cover,
 *  we get minimal keys \Sigma_k and remaining FDs \Sigma_FD, and then we only compute the minimal cover of \Sigma_FD.
 *  Finally we keep some stats including corresponding \Sigma_FD's cover's size, attribute symbol number and cost.
 *
 */
public class exp3 {
	/**
	 * according keyfd cover of a fdcover, compute corresponding fd cover of \Sigma_fd of keyfd cover.
	 * e.g. a minimal keyfd cover has keys \Sigma_k and FDs \Sigma_FD, we compute minimal FD cover of \Sigma_FD.
	 * Finally, it computes and saves results.
	 * @param FDCoverType
	 * @param para
	 */
	public static void runSingleExp(String FDCoverType, Parameter para){
		List<Object> keyfdInfo = Utils.getKeyFDCover(FDCoverType+" keyfd", para, false);
		if(keyfdInfo == null) {
			List<String> content = Arrays.asList(para.dataset.name+","+para.dataset.DataType.toString()+","+FDCoverType+" keyfd,null,null,null,null,null");
			Utils.writeContent(content, para.output_add, true);
			return;
		}
		List<String> schema = (List<String>) keyfdInfo.get(0);
		List<Key> keys = (List<Key>) keyfdInfo.get(1);
		List<FD> fds = (List<FD>) keyfdInfo.get(2);
		
		int fdsAttrSymCount = Utils.getFDAttrSymCount(fds);
		
		long start = System.currentTimeMillis();
		List<FD> fdCover = Utils.compFDCover(fds, FDCoverType);
		long end = System.currentTimeMillis();
		long cost = end - start;
		int coverAttrSymCount = Utils.getFDAttrSymCount(fdCover);
		
		List<String> content = Arrays.asList(para.dataset.name+","+para.dataset.DataType.toString()+","+FDCoverType+" keyfd,"+fds.size()+","+
				fdsAttrSymCount+","+fdCover.size()+","+coverAttrSymCount+","+cost);
		Utils.writeContent(content, para.output_add, true);
		
		List<Object> keyfd = new ArrayList<>();
		keyfd.add(keys);
		keyfd.add(fds);
		keyfd.add(fdCover);
		Utils.writeKeyFDs(schema.size(), keyfd, Utils.getCoverPath(FDCoverType+" keyfd", para), FDCoverType);
	}
	
	public static void runExps(Parameter para) {
		for(String FDCoverType : para.FDCoverTypeList) {
			runSingleExp(FDCoverType, para);
		}
	}
	public static void main(String[] args) {
		Utils.getParameterList(null, null).forEach(exp3::runExps);
	}

}
