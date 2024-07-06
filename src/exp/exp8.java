package exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import entity.FD;
import entity.Key;
import entity.Parameter;
import util.Utils;

/**
 * we study the computation problem of mixed cover.
 *
 */
public class exp8 {
	
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
	public static List<Long> Mixed_Sequential(List<String> R, List<FD> F, String coverType){
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
		
		List<Long> output = new ArrayList<Long>();
		output.addAll(Arrays.asList(final_cost, cost_f, cost_k, cost_r));
		
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
	public static List<Long> Mixed_Parallel(List<String> R, List<FD> F, String coverType) {
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
		
		List<Long> output = new ArrayList<Long>();
		output.addAll(Arrays.asList(final_cost, cost_f, cost_k, cost_r));
		
		return output;
	}
	
	
	/**
	 * Algorithm 13
	 * given an FD cover F of type t (minimal-reduced and optimal)
	 * 
	 * two steps:
	 * 1) compute minimal keys K
	 * 2) computing the subset G of F 
	 * with the FDs Y->Z from F such that no X in K is a subset of Y
	 * @param FDs
	 * @param coverType
	 * @return index 0: computation cost, index 1: list of keys, index 2: list of FDs
	 */
	public static List<Object> Mixed(List<String> R, List<FD> F, String coverType){
		long start = System.currentTimeMillis();
		List<Key> K = Utils.getMinimalKeys(R, F);
		
		List<FD> removal = new ArrayList<>();//need to remove from F
		for(FD Y_Z : F) {
			for(Key X : K){
				//if X in K is a subset of Y, need to remove
				List<String> Y = Y_Z.getLeftHand();
				if(Y.containsAll(X.getAttributes())) {
					removal.add(Y_Z);
					break;
				}
			}
		}
		F.removeAll(removal);
		
		
		long end = System.currentTimeMillis();
		long cost = end - start;
		
		List<Object> T_K_G = new ArrayList<>();
		T_K_G.add(cost);
		T_K_G.add(K);
		T_K_G.add(F);
		
		return T_K_G;
	}
	
	/**
	 * given the fds with no specific type
	 * @param R
	 * @param F
	 * @param coverType
	 * @throws InterruptedException 
	 */
	public static void comparison(int repeat, Parameter para, String coverType, String output) throws InterruptedException {
		//get original fd cover type as input
		List<Object> res = Utils.getFDCover("original", para);
		List<String> R = (List<String>) res.get(0);
		List<FD> F = (List<FD>) res.get(1);
		
		long final_cost_n=0, cost_k_n=0, cost_f_n=0, cost_r_n=0;
		int num_k_n=0, size_k_n=0, num_f_n=0, size_f_n=0, num_r_n=0, size_r_n=0;
		for(int i = 0;i < repeat;i ++) {
//			List<FD> F_copy = Utils.deepCopyFDs(F);
//			List<Object> res_n = exp8.Mixed_Sequential(R, F_copy, coverType);
//			final_cost_n += (long) res_n.get(0);
//			cost_k_n += (long) res_n.get(1);
//			cost_f_n += (long) res_n.get(2);
//			cost_r_n += (long) res_n.get(3);
//			num_k_n += (int) res_n.get(4);
//			size_k_n += (int) res_n.get(5);
//			num_f_n += (int) res_n.get(6);
//			size_f_n += (int) res_n.get(7);
//			num_r_n += (int) res_n.get(8);
//			size_r_n += (int) res_n.get(9);
		}
		double avg_final_cost_n = final_cost_n / (double)repeat;
		double avg_cost_k_n = cost_k_n / (double)repeat;
		double avg_cost_f_n = cost_f_n / (double)repeat;
		double avg_cost_r_n = cost_r_n / (double)repeat;
		double avg_num_k_n = num_k_n / (double)repeat;
		double avg_size_k_n = size_k_n / (double)repeat;
		double avg_num_f_n = num_f_n / (double)repeat;
		double avg_size_f_n = size_f_n / (double)repeat;
		double avg_num_r_n = num_r_n / (double)repeat;
		double avg_size_r_n = size_r_n / (double)repeat;
		
		long final_cost_p=0, cost_k_p=0, cost_f_p=0, cost_r_p=0;
		int num_k_p=0, size_k_p=0, num_f_p=0, size_f_p=0, num_r_p=0, size_r_p=0;
		for(int i = 0;i < repeat;i ++) {
//			List<FD> F_copy = Utils.deepCopyFDs(F);
//			List<Object> res_p = exp8.Mixed_Parallel(R, F_copy, coverType);
//			final_cost_p += (long) res_p.get(0);
//			cost_k_p += (long) res_p.get(1);
//			cost_f_p += (long) res_p.get(2);
//			cost_r_p += (long) res_p.get(3);
//			num_k_p += (int) res_p.get(4);
//			size_k_p += (int) res_p.get(5);
//			num_f_p += (int) res_p.get(6);
//			size_f_p += (int) res_p.get(7);
//			num_r_p += (int) res_p.get(8);
//			size_r_p += (int) res_p.get(9);
		}
		double avg_final_cost_p = final_cost_p / (double)repeat;
		double avg_cost_k_p = cost_k_p / (double)repeat;
		double avg_cost_f_p = cost_f_p / (double)repeat;
		double avg_cost_r_p = cost_r_p / (double)repeat;
		double avg_num_k_p = num_k_p / (double)repeat;
		double avg_size_k_p = size_k_p / (double)repeat;
		double avg_num_f_p = num_f_p / (double)repeat;
		double avg_size_f_p = size_f_p / (double)repeat;
		double avg_num_r_p = num_r_p / (double)repeat;
		double avg_size_r_p = size_r_p / (double)repeat;
		
		
		String stat = para.dataset.name + "," + coverType + "," +para.dataset.row_num + "," + para.dataset.col_num + "," + F.size() + "," + Utils.compFDAttrSymbNum(F) + ",["+
				"("+String.format("%.2f",avg_final_cost_n) +":" + String.format("%.2f",avg_final_cost_p) + ")," +
				"("+String.format("%.2f",avg_cost_k_n) +":" + String.format("%.2f",avg_cost_k_p) + ")," +
				"("+String.format("%.2f",avg_cost_f_n) +":" + String.format("%.2f",avg_cost_f_p) + ")," +
				"("+String.format("%.2f",avg_cost_r_n) +":" + String.format("%.2f",avg_cost_r_p) + ")," +
				"("+String.format("%.2f",avg_num_k_n) +":" + String.format("%.2f",avg_num_k_p) + ")," +
				"("+String.format("%.2f",avg_size_k_n) +":" + String.format("%.2f",avg_size_k_p) + ")," +
				"("+String.format("%.2f",avg_num_f_n) +":" + String.format("%.2f",avg_num_f_p) + ")," +
				"("+String.format("%.2f",avg_size_f_n) +":" + String.format("%.2f",avg_size_f_p) + ")," +
				"("+String.format("%.2f",avg_num_r_n) +":" + String.format("%.2f",avg_num_r_p) + ")," +
				"("+String.format("%.2f",avg_size_r_n) +":" + String.format("%.2f",avg_size_r_p) + ")" +
				"]";
		Utils.writeContent(Arrays.asList(stat), output, true);//output
	}
	
	
	public static void runExps(int repeat, Parameter para, String output) throws InterruptedException {
		exp8.comparison(repeat, para, "reduced minimal", output);
		if(para.dataset.col_num <= 18)
			exp8.comparison(repeat, para, "optimal", output);
	}
	
	public static void main(String[] args) throws InterruptedException {
		int repeat = 5;
		for(Parameter para : Utils.getParameterListV1(null)) {
			System.out.println("current data name: "+para.dataset.name);
			exp8.runExps(repeat, para, para.output_add);
		}
	}

}
