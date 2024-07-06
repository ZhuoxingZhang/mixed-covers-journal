package exp;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import entity.FD;
import entity.Key;
import entity.Parameter;
import util.Utils;
/**
 * runtime analysis of covers over the data sets
 *
 */
public class RuntimeAnalysisExp {
	/**
	 * Algorithm 11
	 * given an FD cover F of type t (minimal-reduced and optimal)
	 * 
	 * three steps:
	 * 1) computing an FD cover G' of type t for F 
	 * 2) compute minimal keys K implied by G'
	 * 3) get FD cover G by removing key-FDs from G'
	 * return mixed cover (K, G)
	 * @param FDs
	 * @param coverType
	 * @return final_cost, cost_f, cost_k, cost_r
	 */
	public static List<Double> Mixed_Sequential(List<String> R, List<FD> F, String coverType){
		long start_f = System.currentTimeMillis();
		List<FD> G_prime = Utils.compFDCover(F, coverType);//FD cover with type-t for FDs
		long end_f = System.currentTimeMillis();
		long cost_f = end_f - start_f;
		
		
		
		long start_k = System.currentTimeMillis();
		List<Key> K = Utils.getMinimalKeys(R, G_prime);
		long end_k = System.currentTimeMillis();
		long cost_k = end_k - start_k;
		
		
		long start_r = System.currentTimeMillis();
		List<FD> removal = new ArrayList<>();//need to remove from G'
		for(FD Y_Z : G_prime) {
			for(Key X : K){
				//if X in K is a subset of Y, need to remove
				List<String> Y = Y_Z.getLeftHand();
				if(Y.containsAll(X.getAttributes())) {
					removal.add(Y_Z);
					break;
				}
			}
		}
		G_prime.removeAll(removal);
		long end_r = System.currentTimeMillis();
		long cost_r = end_r - start_r;
		
		
		
		long final_cost = cost_k + cost_r + cost_f;
		
		List<Double> output = new ArrayList<Double>();
		output.addAll(Arrays.asList((double)final_cost, (double)cost_f, (double)cost_k, (double)cost_r));
		
		return output;
	}
	
	/**
	 * Algorithm 12
	 * given an FD set F of no specific type
	 * 
	 * process in parallel for 1) & 2):
	 * 1) the set K of minimal keys implied by F
	 * 2) the FD cover G' of type-t of F
	 * 
	 * 3) get FD cover G by removing key-FDs from G'
	 * return mixed cover (K, G)
	 * 
	 * @param R
	 * @param F
	 * @param coverType
	 * @return final_cost, cost_f, cost_k, cost_r
	 */
	public static List<Double> Mixed_Parallel(List<String> R, List<FD> F, String coverType) {
		//computing keys and FD cover in parallel
		long start_f = System.currentTimeMillis();
		List<FD> G_prime = Utils.compFDCover(F, coverType);
		long end_f = System.currentTimeMillis();
		long cost_f = end_f - start_f;
		
		
		long start_k = System.currentTimeMillis();
		List<Key> K = Utils.getMinimalKeys(R, F);
		long end_k = System.currentTimeMillis();
		long cost_k = end_k - start_k;
		
		
		long parallel_time = cost_f > cost_k ? cost_f : cost_k;//get max value to get parallel cost
		
		
		//compute G
		long start_r = System.currentTimeMillis();
		List<FD> removal = new ArrayList<>();//need to remove from F_prime
		for(FD Y_Z : G_prime) {
			for(Key X : K){
				//if X in K is a subset of Y, need to remove
				List<String> Y = Y_Z.getLeftHand();
				if(Y.containsAll(X.getAttributes())) {
					removal.add(Y_Z);
					break;
				}
			}
		}
		G_prime.removeAll(removal);
		long end_r = System.currentTimeMillis();
		long cost_r = end_r - start_r;
		
		
		long final_cost = parallel_time + cost_r;
		
		List<Double> output = new ArrayList<Double>();
		output.addAll(Arrays.asList((double)final_cost, (double)cost_f, (double)cost_k, (double)cost_r));
		
		return output;
	}
	
	public static void run(List<String> dataset,int repeat, boolean Repeat4Optimal) {
		//supplement experiments for table 5 of the paper
		//computing time analysis for FD cover and mixed cover
		for(Parameter para : Utils.getParameterListV1(dataset)) {
			List<String> variants = Arrays.asList("nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
//			List<String> variants = Arrays.asList("nonredundant");
			List<Object> info = Utils.getFDCover("original", para);
			List<String> R = (List<String>) info.get(0);
			List<FD> originalFDs = (List<FD>) info.get(1);
			String stat = para.dataset.name+"";
			for(String variant : variants) {
				if(para.dataset.col_num > 18 && variant.contains("optimal")) 
					continue;
				if(variant.contains("optimal") && !Repeat4Optimal) {
					//FD
					long start_fd = System.currentTimeMillis();
					Utils.compFDCover(originalFDs, variant);
					long end_fd = System.currentTimeMillis();
					long cost_fd = end_fd - start_fd;
					
					//mix-sequential
					List<Double> cost_mix_seq = Mixed_Sequential(R, originalFDs, variant);
					
					//mix-parallel
					List<Double> cost_mix_par =Mixed_Parallel(R, originalFDs, variant);
					
					stat += "," + cost_fd + "," + cost_mix_seq.get(0) + "," + cost_mix_seq.get(1) + "," + cost_mix_seq.get(2) + "," + cost_mix_seq.get(3) + "," 
							+ cost_mix_par.get(0) + "," + cost_mix_par.get(1) + "," + cost_mix_par.get(2) + "," + cost_mix_par.get(3);
				}else {
					//repeat
					//FD
					List<Double> fd_costs = new ArrayList<>();
					for(int i = 0;i < repeat;i ++) {
						long start_fd = System.currentTimeMillis();
						Utils.compFDCover(originalFDs, variant);
						long end_fd = System.currentTimeMillis();
						fd_costs.add((double)(end_fd - start_fd));
					}
					double cost_fd = Utils.getAve(fd_costs);
					
					
					//mix-sequential
					List<List<Double>>  mix_seq_costs = new ArrayList<>();
					for(int i = 0;i < repeat;i ++) {
						List<Double> cost_mix_seq = Mixed_Sequential(R, originalFDs, variant);
						mix_seq_costs.add(cost_mix_seq);
					}
					List<Double> cost_mix_seq = Utils.getEachColValue(mix_seq_costs);
					
					//mix-parallel
					List<List<Double>>  mix_par_costs = new ArrayList<>();
					for(int i = 0;i < repeat;i ++) {
						List<Double> cost_mix_par = Mixed_Parallel(R, originalFDs, variant);
						mix_par_costs.add(cost_mix_par);
					}
					List<Double> cost_mix_par = Utils.getEachColValue(mix_par_costs);
					
					stat += "," + String.format("%.2f",cost_fd) + "," + String.format("%.2f",cost_mix_seq.get(0)) + "," + String.format("%.2f",cost_mix_seq.get(1)) + "," + String.format("%.2f",cost_mix_seq.get(2)) + "," + String.format("%.2f",cost_mix_seq.get(3)) + "," 
							+ String.format("%.2f",cost_mix_par.get(0)) + "," + String.format("%.2f",cost_mix_par.get(1)) + "," + String.format("%.2f",cost_mix_par.get(2)) + "," + String.format("%.2f",cost_mix_par.get(3));
//					System.out.println(variant + " | " + stat);
				}
				
			}
			System.out.println(stat);
			Utils.writeContent(Arrays.asList(stat), para.output_add, true);//output
		}
	}
	public static void main(String[] args) {
		boolean Repeat4Optimal = false;
		int repeat = 20;//5000
		RuntimeAnalysisExp.run(Arrays.asList("china_weather"),repeat, Repeat4Optimal);
	}

}
