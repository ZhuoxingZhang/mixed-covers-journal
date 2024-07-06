package exp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import entity.DataTypeEnum;
import entity.FD;
import entity.FuncDepen;
import entity.Parameter;
import util.QuineMcCluskey;
import util.Utils;

/**
 * In this experiment, we compute and save several FD cover with given FD as input.
 * These FD covers include non-redundant, reduced, canonical, minimal and reduced minimal covers.
 * Then, we statistics some properties when computing these FD covers.
 */
public class exp0 {
	/**
	 * compute non-redundant, reduced, canonical, minimal, reduced minimal covers of given data set
	 * @param para
	 * @param memoryUnit "GiB"/"MiB"/"KiB"/"Byte" by default
	 * @throws IOException
	 */
	public static void computeCovers(Parameter para,String memoryUnit) {
		List<Object> schema_info = Utils.readFDs(para.fd_add);
		List<String> R = (List<String>) schema_info.get(0);
		List<FD> FDs = (List<FD>) schema_info.get(1);
		System.out.println("data set : "+para.dataset.name);
		System.out.println("load "+FDs.size()+" FDs successfully!\n");
		
		//compute non-redundant cover
		long start_nonredun = System.currentTimeMillis();
		
		List<Double> free_mem_list_nr = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_nr = Utils.getFreeMemory(memoryUnit);
		
		List<FD> nonRedunCover = Utils.compNonRedundantCover(FDs, free_mem_list_nr, memoryUnit);
		
		free_mem_list_nr.sort(null);//increasing order
		double maxMemUse_nr = start_free_mem_nr - free_mem_list_nr.get(0);//max use of Memory
		
		long end_nonredun = System.currentTimeMillis();
		long cost_nonredun = end_nonredun - start_nonredun;
		System.out.println("non-redundant cover size : " + nonRedunCover.size()+
				" | cost : "+cost_nonredun+" ms"+" | max memory use : "+maxMemUse_nr+" "+memoryUnit+"(s)");
		int count1 = 0;
		for(FD f : nonRedunCover) {
			count1 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count1);
		Utils.writeFDs(R.size(), nonRedunCover, para.fd_nonredundant_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "non-redundant", FDs.size(), nonRedunCover.size(), count1, cost_nonredun, maxMemUse_nr, para.output_add);
		System.out.println("#########################\n");
		
		
		//compute reduced cover
		long start_reduced = System.currentTimeMillis();
		
		List<Double> free_mem_list_reduced = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_reduced =  Utils.getFreeMemory(memoryUnit);
		
		List<FD> redCover = Utils.compReducedCover(FDs, free_mem_list_reduced, memoryUnit);
		
		free_mem_list_reduced.sort(null);//increasing order
		double maxMemUse_reduced = start_free_mem_reduced - free_mem_list_reduced.get(0);//max use of Memory
		
		long end_reduced = System.currentTimeMillis();
		long cost_reduced = end_reduced - start_reduced;
		System.out.println("reduced cover size : " + redCover.size()+
				" | cost : "+cost_reduced+" ms"+" | max memory use : "+maxMemUse_reduced+" "+memoryUnit+"(s)");
		int count2 = 0;
		for(FD f : redCover) {
			count2 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count2);
		Utils.writeFDs(R.size(), redCover, para.fd_reduced_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "reduced", FDs.size(), redCover.size(), count2, cost_reduced, maxMemUse_reduced, para.output_add);
		System.out.println("#########################\n");
		
		
		//compute canonical cover
		long start_canonical = System.currentTimeMillis();
		
		List<Double> free_mem_list_cano = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_cano =  Utils.getFreeMemory(memoryUnit);
		
		List<FD> canonCover = Utils.compCanonicalCover(redCover, free_mem_list_cano, memoryUnit);
		
		free_mem_list_cano.sort(null);//increasing order
		double local_canon = start_free_mem_cano - free_mem_list_cano.get(0);
		double maxMemUse_canon = local_canon > maxMemUse_reduced ? local_canon : maxMemUse_reduced;//max use of Memory
		
		long end_canonical = System.currentTimeMillis();
		long cost_canonical = (end_canonical - start_canonical) + cost_reduced;
		System.out.println("canonical cover size : " + canonCover.size()+
				" | cost : "+cost_canonical+" ms"+" | max memory use : "+maxMemUse_canon+" "+memoryUnit+"(s)");
		int count3 = 0;
		for(FD f : canonCover) {
			count3 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count3);
		Utils.writeFDs(R.size(), canonCover, para.fd_canonical_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "canonical", FDs.size(), canonCover.size(), count3, cost_canonical, maxMemUse_canon, para.output_add);
		System.out.println("#########################\n");
		
		
		//compute minimal cover
		long start_min = System.currentTimeMillis();
		
		List<Double> free_mem_list_mini = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_mini =  Utils.getFreeMemory(memoryUnit);
		
		List<FD> minimalCover = Utils.compMinimalCover(nonRedunCover, free_mem_list_mini, memoryUnit);
		
		free_mem_list_mini.sort(null);//increasing order
		double local_mini = start_free_mem_mini - free_mem_list_mini.get(0);
		double maxMemUse_mini = local_mini > maxMemUse_nr ? local_mini : maxMemUse_nr;//max use of Memory
		
		long end_min = System.currentTimeMillis();
		long cost_minimal = (end_min - start_min) + cost_nonredun;
		System.out.println("minimal cover size : " + minimalCover.size()+
				" | cost : "+cost_minimal+" ms"+" | max memory use : "+maxMemUse_mini+" "+memoryUnit+"(s)");
		int count4 = 0;
		for(FD f : minimalCover) {
			count4 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count4);
		Utils.writeFDs(R.size(), minimalCover, para.fd_minimal_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "minimal", FDs.size(), minimalCover.size(), count4, cost_minimal, maxMemUse_mini, para.output_add);
		System.out.println("#########################\n");
		
		
		//compute reduced minimal cover
		long start_red_min = System.currentTimeMillis();
		
		List<Double> free_mem_list_red_mini = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_red_mini =  Utils.getFreeMemory(memoryUnit);
		
		List<FD> red_minimal_Cover = Utils.compReducedCover(minimalCover, free_mem_list_red_mini, memoryUnit);
		
		free_mem_list_red_mini.sort(null);//increasing order
		double local_red_mini = start_free_mem_red_mini - free_mem_list_red_mini.get(0);
		double maxMemUse_red_mini = local_red_mini > maxMemUse_mini ? local_red_mini : maxMemUse_mini;//max use of Memory
		
		long end_red_min = System.currentTimeMillis();
		long cost_red_min = (end_red_min - start_red_min) + cost_minimal;
		System.out.println("reduced minimal cover size : " + red_minimal_Cover.size()+
				" | cost : "+cost_red_min+" ms"+" | max memory use : "+maxMemUse_red_mini+" "+memoryUnit+"(s)");
		int count5 = 0;
		for(FD f : red_minimal_Cover) {
			count5 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count5);
		Utils.writeFDs(R.size(), red_minimal_Cover, para.fd_reduced_minimal_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "reduced minimal", FDs.size(), red_minimal_Cover.size(), count5, cost_red_min, maxMemUse_red_mini, para.output_add);
		System.out.println("#########################\n");
		
		//compute optimal cover
		long start_opt = System.currentTimeMillis();
				
		List<Double> free_mem_list_opt = new ArrayList<Double>();//record free memory when computing covers
		double start_free_mem_opt =  Utils.getFreeMemory(memoryUnit);
		
		List<FD> fd_set = Utils.splitFDs(FDs);
		List<String> attrMap = Utils.getAttributeMapFromFDs(fd_set);
		List<Integer> minTerms = Utils.compMinTerms(fd_set, attrMap);
		QuineMcCluskey engine = new QuineMcCluskey(minTerms,attrMap.size());
		String boolEx = engine.runQuineMcCluskey(true,free_mem_list_opt,memoryUnit);
		System.out.println("simplified bool expression : "+boolEx);
		List<FD> miniCover = Utils.convertBoolExprToFDs(boolEx,attrMap);
		miniCover = Utils.combineFDs(miniCover);
		List<FD> optimalCover = Utils.compOptimalCover(miniCover,free_mem_list_opt,memoryUnit);
		
		long end_opt = System.currentTimeMillis();
		long cost_opt = end_opt - start_opt;
		free_mem_list_opt.sort(null);//increasing order
		double maxMemUse_opt = start_free_mem_opt - free_mem_list_opt.get(0);//max use of Memory		
		
		System.out.println("optimal cover size : " + optimalCover.size()+
						" | cost : "+cost_opt+" ms"+" | max memory use : "+maxMemUse_opt+" "+memoryUnit+"(s)");
		int count6 = 0;
		for(FD f : optimalCover) {
			count6 += f.getLeftHand().size() + f.getRightHand().size();
		}
		System.out.println("attribute symbol number : "+count6);
		Utils.writeFDs(R.size(), optimalCover, para.fd_optimal_cover_add);
		exp0.outputFDCoverStats(para.dataset.name, "optimal", FDs.size(), optimalCover.size(), count6, cost_opt, maxMemUse_opt, para.output_add);
		System.out.println("#########################\n");
	}
	
	
	/**
	 * stat and write FD cover's property like FD number and attribute symbol number
	 * @param fd_cover_add
	 */
	public static void statFDCoverProperty(String datasetName, String coverType,String fd_cover_add,String output) {
		List<FD> cover = (List<FD>) Utils.readFDs(fd_cover_add).get(1);
		int FD_num = cover.size();
		int attr_symb_num = 0;
		for(FD fd : cover) {
			attr_symb_num += fd.getLeftHand().size() + fd.getRightHand().size();
		}
		
		//output
		try {
			FileWriter fr = new FileWriter(output,true);
			BufferedWriter br = new BufferedWriter(fr);
			br.write(datasetName + "," + coverType + "," + FD_num + "," + attr_symb_num+"\n");
			br.close();
			fr.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void outputFDCoverStats(String datasetName, String coverType,int originFDNum,int FDNum,int attrSymbNum,long costInMs,double usedMemory,String output) {
		//output some properties of FD covers
		try {
			FileWriter fr = new FileWriter(output,true);
			BufferedWriter br = new BufferedWriter(fr);
			br.write(datasetName + "," + coverType + "," + originFDNum + "," + FDNum + "," + attrSymbNum+","+costInMs+","+usedMemory+"\n");
			br.close();
			fr.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) throws IOException {
		//generate different covers of input FDs for each data set
		Utils.getParameterListV1(null).forEach(para -> computeCovers(para, "MiB"));
		
	}

}
