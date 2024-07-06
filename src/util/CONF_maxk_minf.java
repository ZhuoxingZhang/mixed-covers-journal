package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;
import entity.Schema3NF;
import entity.SchemaBCNF;
import entity.SchemaInfo;
import entity.SchemaInfoCritical;
import util.Utils;

/**
 * CONF with optimizing 3NF schemata
 * 1) computing mixed cover in 3NF subschemata
 * 2) removing redundant 3NF with less keys(maximize keys)
 * 3) if there having equal keys for 3NF, removing redundant with more FDs firstly(minimize FDs)
 *
 */
public class CONF_maxk_minf {
	private long exe_time;
	private int num_of_all_schemata;
	private double ave_key_num_on_BCNF_schemata;
	private int num_of_BCNF_schemata;
	private int num_of_3NF_schemata;
	private double ave_optim_target_on_3NF_schemata;
	private double ave_key_num_on_3NF_schemata;
	private HashMap<Integer,Integer> map_BCNF_keynum_to_num;//in BCNF, the number of N-CONF
	private HashMap<Integer,List<Schema3NF>> map_3NF_optim_target_to_num;//in 3NF, key: optimization target number, value: the sub-schemata with the optimization target
	private long time_fds;//runtime for part fds
	private long time_critical_optimize;//runtime for part critical optimize
	private long time_keys;//run time for part keys
	private long time_noncritical_optimize;//runtime for part non-critical optimize
	private long time_subset;//runtime for part subset
	private long time_lossless;//runtime for lossless
	
	
	public CONF_maxk_minf() {
		this.exe_time = 0l;
		this.num_of_all_schemata = 0;
		this.num_of_BCNF_schemata = 0;
		this.ave_key_num_on_BCNF_schemata = 0d;
		this.num_of_3NF_schemata = 0;
		this.ave_optim_target_on_3NF_schemata = 0;
		this.ave_key_num_on_3NF_schemata = 0;
		this.map_BCNF_keynum_to_num = new HashMap<Integer,Integer>();
		this.map_3NF_optim_target_to_num = new HashMap<Integer,List<Schema3NF>>();
		this.time_fds = 0l;
		this.time_critical_optimize = 0l;
		this.time_keys = 0l;
		this.time_noncritical_optimize = 0l;
		this.time_subset = 0l;
		this.time_lossless = 0l;
	}
	
	
	public void output_results(Parameter para, String coverType, String optimTarget) throws IOException {
		File f = new File(para.output_add);
		FileWriter fw = new FileWriter(f,true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String dataset_name = para.dataset.name;
		
		Iterator<Integer> iter_bcnf = this.map_BCNF_keynum_to_num.keySet().iterator();
		String bcnf_dis = "[";//key number distributions in BCNF
		List<String> bcnf_dis_list = new ArrayList<String>();
		while(iter_bcnf.hasNext()) {
			int key = iter_bcnf.next();
			int value = this.map_BCNF_keynum_to_num.get(key);
			bcnf_dis_list.add(key+" : "+value);
		}
		Collections.sort(bcnf_dis_list,new Comparator<String>() {//decreasing order of n_KEY

			@Override
			public int compare(String o1, String o2) {
				int key1 = Integer.parseInt(o1.split(" : ")[0]);
				int key2 = Integer.parseInt(o2.split(" : ")[0]);
				if(key1 < key2)
					return 1;
				else if(key1 > key2)
					return -1;
				else
					return 0;
			}
			
		});
		for(int i = 0;i < bcnf_dis_list.size();i ++) {
			String s = bcnf_dis_list.get(i);
			if(i != bcnf_dis_list.size() - 1)
				bcnf_dis += "{ " + s + " } ";
			else
				bcnf_dis += "{ " + s + " }";
		}
		bcnf_dis += "]";
		
		Iterator<Integer> iter_3nf = this.map_3NF_optim_target_to_num.keySet().iterator();
		String thirdnf_dis = "[";//FD set's fd number distributions in 3NF
		List<String> thirdnf_dis_list = new ArrayList<String>();
		while(iter_3nf.hasNext()) {
			int key = iter_3nf.next();//optimization target value
			List<Schema3NF> value = this.map_3NF_optim_target_to_num.get(key);//sub-schemata that has the same optimization target value
			for(Schema3NF s : value) {//compute keys on 3NF
				List<Key> keys = Utils.getMinimalKeys(s.getAttr_set(), s.getFd_set());
				this.ave_key_num_on_3NF_schemata += keys.size();
			}
			if(!coverType.contains("keyfd")) {
				thirdnf_dis_list.add(key+" : "+value.size());
			}else {//mixed cover
				int num = value.size();
				if(optimTarget.equals("FDN")) {
					int total_card_num = 0;//key number + fd number
					int total_key_card_num = 0;//key number
					for(Schema3NF s3nf : value) {
						total_card_num += s3nf.getMin_key_list().size() + s3nf.getFd_set().size();
						total_key_card_num += s3nf.getMin_key_list().size();
					}
					thirdnf_dis_list.add(key+" : ("+String.format("%.2f",total_card_num/(double)num)+" "+String.format("%.2f",total_key_card_num/(double)num)+") : "+num);
				}
				if(optimTarget.equals("ASN")) {
					int total_attr_symb_num = 0;
					int total_key_attr_symb_num = 0;
					for(Schema3NF s3nf : value) {
						int key_attr_symb_num = Utils.compKeyAttrSymbNum(s3nf.getMin_key_list());
						total_attr_symb_num += key_attr_symb_num + Utils.compFDAttrSymbNum(s3nf.getFd_set());
						total_key_attr_symb_num += key_attr_symb_num;
					}
					thirdnf_dis_list.add(key+" : ("+String.format("%.2f",total_attr_symb_num/(double)num)+" "+String.format("%.2f",total_key_attr_symb_num/(double)num)+") : "+num);
				}
			}
			
			
		}
		this.ave_key_num_on_3NF_schemata /= (double)this.num_of_3NF_schemata;
		Collections.sort(thirdnf_dis_list,new Comparator<String>() {//decreasing order of fd number

			@Override
			public int compare(String o1, String o2) {
				int key1 = Integer.parseInt(o1.split(" : ")[0]);
				int key2 = Integer.parseInt(o2.split(" : ")[0]);
				if(key1 < key2)
					return 1;
				else if(key1 > key2)
					return -1;
				else
					return 0;
			}
			
		});
		for(int i = 0;i < thirdnf_dis_list.size();i ++) {
			String s = thirdnf_dis_list.get(i);
			if(i != thirdnf_dis_list.size() - 1)
				thirdnf_dis += "{ " + s + " } ";
			else
				thirdnf_dis += "{ " + s + " }";
		}
		thirdnf_dis += "]";
		
		
		//a line of results's column names:
		//algorithm,dataset_name,data type,cover type,runtime
		//runtime for part fds,runtime for part critical optimize,runtime for part keys,runtime for part non-critical optimize,runtime for part subset, runtime for part lossless
		//number of all schemata
		//number of BCNF schemata,average key number on BCNF schemata,distributions of key number in BCNF
		//number of 3NF schemata,average FD number on 3NF schemata,distributions of FD number in 3NF
		String result = "CONF_maxk_minf,"+dataset_name+","+para.dataset.DataType+","+coverType+","+this.exe_time+","
		+this.time_fds+","+this.time_critical_optimize+","+this.time_keys+","+this.time_noncritical_optimize+","+this.time_subset+","+this.time_lossless+","
		+this.num_of_all_schemata+","
		+this.num_of_BCNF_schemata+","+String.format("%.2f",this.ave_key_num_on_BCNF_schemata)+","+bcnf_dis+","
		+this.num_of_3NF_schemata+","+String.format("%.2f",this.ave_key_num_on_3NF_schemata)+","+String.format("%.2f",this.ave_optim_target_on_3NF_schemata)+","+thirdnf_dis;
		
		bw.write(result+"\n");
		
		bw.close();
		fw.close();
	}
	
	/**
	 * 
	 * @param R
	 * @param Sigma_a_bar an atomic cover of Sigma
	 * @return a list of sub-schemata
	 * @throws Exception 
	 */
	public  List<Schema> exe_decomp(String optimTarget, Parameter para, String coverType, List<String> R, List<FD> Sigma_a_bar) throws Exception {
		List<Schema> D = new ArrayList<Schema>();
		List<FD> Sigma_a = new ArrayList<FD>(Sigma_a_bar);//Sigma_a
		List<FD> Sigma_c = new ArrayList<>();//store critical schemata
		List<FD> Sigma_nc = new ArrayList<>();//store non-critical schemata
		
		//part FDs
		long start_fds = System.currentTimeMillis();
		Map<Set<String>, List<FD>> map_schema_to_fdcover = new HashMap<>();//key : schema; value : fd cover
		Map<Set<String>, List<Key>> map_schema_to_keys = new HashMap<>();//key : schema; value : keys
		//increasing order of key number (decreasing order of FD number if they tie for 3NF)
		List<MySchemaInfo> sigma_c_with_inc_k_dec_f = new ArrayList<>();
		for(FD X_A : Sigma_a_bar) {
			List<String> XA = Utils.getFDAttributes(X_A);
			for(FD Y_B : Sigma_a_bar) {//get Y -> B
				List<String> YB = Utils.getFDAttributes(Y_B);
				if(XA.containsAll(YB) && !Utils.getAttrSetClosure(Y_B.getLeftHand(), Sigma_a_bar).containsAll(XA)) {
					//X -> A is critical
					Sigma_c.add(X_A);//record the critical FD
					List<FD> XA_cover = null;//fd cover/fds of mixed cover variant
					List<Key> keys = null;
					Set<String> XA_schema = new HashSet<>(XA);
					if(map_schema_to_fdcover.containsKey(XA_schema)) {
						XA_cover = map_schema_to_fdcover.get(XA_schema);
						keys = map_schema_to_keys.get(XA_schema);
					}else {
						List<FD> XA_projection = Utils.getProjection(Sigma_a_bar, XA);
						if(coverType.contains("keyfd")) {//compute mixed cover
							int idx = coverType.indexOf(" keyfd");
							String fdcover1 = coverType.substring(0, idx);//get corresponding fd cover
							List<Object> info = Utils.compKeyFDCover(XA, Utils.compFDCover(XA_projection, fdcover1));
							XA_cover = (List<FD>) info.get(1);
							keys = (List<Key>) info.get(0);
							XA_cover = Utils.compFDCover(XA_cover, fdcover1);//compute the FD cover of the remaining FDs
							map_schema_to_fdcover.put(XA_schema, XA_cover);
							map_schema_to_keys.put(XA_schema, keys);
						}else {//compute fd cover
							XA_cover = Utils.compFDCover(XA_projection, coverType);
							map_schema_to_fdcover.put(XA_schema, XA_cover);
//							keys = new ArrayList<>();
							keys = Utils.getMinimalKeys(XA, XA_projection);
							map_schema_to_keys.put(XA_schema, keys);
						}
					}
					if(optimTarget.equals("FDN"))
						sigma_c_with_inc_k_dec_f.add(new MySchemaInfo(XA_schema, X_A, keys.size(), XA_cover.size()));//optimization target
					else if(optimTarget.equals("ASN"))
						sigma_c_with_inc_k_dec_f.add(new MySchemaInfo(XA_schema, X_A, Utils.compKeyAttrSymbNum(keys), Utils.compFDAttrSymbNum(XA_cover)));
					else
						throw new Exception("optimization target does not exist!");
					break;
				}
			}
		}
		long end_fds = System.currentTimeMillis();
		this.time_fds = end_fds - start_fds;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part fds : "+this.time_fds);
		
		//sigma_nc = sigma_a - sigma_c
		for(FD fd : Sigma_a) {
			if(!Sigma_c.contains(fd))
				Sigma_nc.add(fd);
		}
		//increasing order of key number/key-attr-symb-num (decreasing order of FD number/key-attr-symb-num if they tie for 3NF)
		Collections.sort(sigma_c_with_inc_k_dec_f, new Comparator<MySchemaInfo>() {
            @Override
            public int compare(MySchemaInfo info1, MySchemaInfo info2) {
                if (info1.keyNum != info2.keyNum) {
                    return Integer.compare(info1.keyNum, info2.keyNum);
                } else {
                    return Integer.compare(info2.FDNum, info1.FDNum);
                }
            }
        });
		
		//part Critical Optimize
		long start_critical_opt = System.currentTimeMillis();
		for(MySchemaInfo critical : sigma_c_with_inc_k_dec_f) {
			FD X_A = critical.fd;
			List<String> XA = Utils.getFDAttributes(X_A);
			List<FD> Sigma_a_NO_XA = Utils.getFDsWithoutSpecificFD(Sigma_a, X_A);
			List<String> X_closure = Utils.getAttrSetClosure(X_A.getLeftHand(), Sigma_a_NO_XA);
			if(X_closure.containsAll(X_A.getRightHand())){//Sigma_a - X -> A implies X -> A 
				Sigma_a.remove(X_A);//Sigma_a removes X -> A
			}else {//D = D U {(XA,Sigma_a_bar[XA])}
				List<FD> projection_XA = Utils.getProjection(Sigma_a_bar, XA);//get projection Sigma_a_bar[XA]
				Schema schema = new Schema(XA,projection_XA);
				if(!D.contains(schema))
					D.add(schema);
			}
		}
		long end_critical_opt = System.currentTimeMillis();
		this.time_critical_optimize = end_critical_opt - start_critical_opt;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part critical optimize : "+this.time_critical_optimize);
		
		//part Keys
		long start_keys = System.currentTimeMillis();
		List<SchemaInfo> Sigma_nc_with_dec_n = new ArrayList<SchemaInfo>();//decreasing order of n_key
		for(FD X_A : Sigma_nc) {//compute sub-schema{XA}'s number of minimal keys
			List<String> XA = Utils.getFDAttributes(X_A);
			List<FD> Sigma_a_bar_XA_projection = Utils.getProjection(Sigma_a_bar, XA);//get XA projection
			int n_XA = 0;//key number of sub-schema XA
			List<Key> minKeys = Utils.getMinimalKeys(XA, Sigma_a_bar_XA_projection);
			n_XA = minKeys.size();
			Sigma_nc_with_dec_n.add(new SchemaInfo(X_A,XA,n_XA));
		}
		long end_keys = System.currentTimeMillis();
		this.time_keys = end_keys - start_keys;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part keys : "+this.time_keys);
		
		Collections.sort(Sigma_nc_with_dec_n,new Comparator<SchemaInfo>() {//decreasing order of n_KEY
			@Override
			public int compare(SchemaInfo o1, SchemaInfo o2) {
				if(o1.getN_key() < o2.getN_key())
					return 1;
				else if(o1.getN_key() > o2.getN_key())
					return -1;
				else
					return 0;
			}	
		});
		
		//part Non-critical Optimize
		long start_noncritical_optimize = System.currentTimeMillis();
		for(SchemaInfo schemainfo : Sigma_nc_with_dec_n) {//schema info (FD,subschema,n_key)
			FD X_A = schemainfo.getFd();
			List<String> XA = schemainfo.getSubschema();
			List<FD> Sigma_a_NO_XA = Utils.getFDsWithoutSpecificFD(Sigma_a, X_A);
			List<String> X_closure = Utils.getAttrSetClosure(X_A.getLeftHand(), Sigma_a_NO_XA);
			if(X_closure.containsAll(X_A.getRightHand())){//Sigma_a - X -> A implies X -> A 
				Sigma_a.remove(X_A);//Sigma_a removes X -> A
			}else {//D = D U {(XA,Sigma_a_bar[XA])}
				List<FD> projection_XA = Utils.getProjection(Sigma_a_bar, XA);//get projection Sigma_a_bar[XA]
				Schema schema = new Schema(XA,projection_XA);
				if(!D.contains(schema))
					D.add(schema);
			}
		}
		long end_noncritical_optimize = System.currentTimeMillis();
		this.time_noncritical_optimize = end_noncritical_optimize - start_noncritical_optimize;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part non-critical optimize : "+this.time_noncritical_optimize);
		
		//part Subset
		long start_subset = System.currentTimeMillis();
		ArrayList<Schema> removal = new ArrayList<Schema>();//need to remove from D
		for(int i = 0;i < D.size();i ++) {
			Schema schema = D.get(i);
			List<String> S = schema.getAttr_set();
			for(int j = 0;j < D.size();j ++) {
				if(i == j)
					continue;
				Schema schema2 = D.get(j);
				List<String> S_prime = schema2.getAttr_set();
				if(S_prime.containsAll(S)) {
					removal.add(schema);
					break;
				}
			}
		}
		for(Schema del : removal) {
			D.remove(del);
		}
		long end_subset = System.currentTimeMillis();
		this.time_subset = end_subset - start_subset;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part subset : "+this.time_subset);
		
		//part Lossless
		long start_lossless = System.currentTimeMillis();
		boolean exist = false;//whether existing or not (R',Sigma') in D, such that R' -> R in closure of Sigma^+
		for(Schema schema : D) {
			List<String> R_prime = schema.getAttr_set();
			List<String> R_prime_closure = Utils.getAttrSetClosure(R_prime, Sigma_a_bar);
			if(R_prime_closure.containsAll(R)) {
				exist = true;
				break;
			}
		}
		if(!exist) {
			Key K = Utils.getRefinedMinKey(Sigma_a_bar, new Key(R), R);
			List<FD> K_projection = Utils.getProjection(Sigma_a_bar, K.getAttributes());
			D.add(new Schema(K.getAttributes(),K_projection));//D = D U {(K,Sigma_a[K])}
		}
		long end_lossless = System.currentTimeMillis();
		this.time_lossless = end_lossless - start_lossless;
		System.out.println("\n=============================\n");
		System.out.println("runtime for part lossless : "+this.time_lossless);
		System.out.println("\ndecomposition finished!\n\n");
		
		return D;
		
	}
	
	/**
	 * 
	 * @param para
	 * @param R
	 * @param Sigma_a_bar an atomic cover
	 * @throws Exception 
	 * @throws SQLException
	 */
	public List<Object> decomp_and_output(String optimTarget, Parameter para, String coverType, List<String> R, List<FD> Sigma_a_bar) throws Exception {
	    long start = System.currentTimeMillis();
		List<Schema> D = exe_decomp(optimTarget, para, coverType, R, Sigma_a_bar);
		long end = System.currentTimeMillis();
		this.exe_time = end - start;
		
		List<SchemaBCNF> schema_BCNF = new ArrayList<>();
		List<Schema3NF> schema_3NF = new ArrayList<>();
		for(Schema schema : D) {
			if(schema.isBCNF()) {
				SchemaBCNF schemabcnf = schema.toBCNF();
				schema_BCNF.add(schemabcnf);
				int key_num = schemabcnf.getMin_key_list().size();
				this.ave_key_num_on_BCNF_schemata += key_num;
				if(this.map_BCNF_keynum_to_num.containsKey(key_num)) {
					int num = this.map_BCNF_keynum_to_num.get(key_num);
					this.map_BCNF_keynum_to_num.put(key_num, ++num);
				}else
					this.map_BCNF_keynum_to_num.put(key_num, 1);
			}else {
				Schema3NF schema3nf = null;
				if(coverType.contains("keyfd"))//mixed cover
					schema3nf = schema.to3NFWithMixedCover(coverType);
				else//fd cover
					schema3nf = schema.to3NF(coverType);
				schema_3NF.add(schema3nf);
				//stat optimization target number
				int optim_target_num;
				if(optimTarget.equals("ASN"))//attribute symbol number
					optim_target_num = Utils.compFDAttrSymbNum(schema3nf.getFd_set());
				else if(optimTarget.equals("FDN"))//FD number
					optim_target_num = schema3nf.getFd_set().size();
				else 
					optim_target_num = 0;
				this.ave_optim_target_on_3NF_schemata += optim_target_num;
				if(this.map_3NF_optim_target_to_num.containsKey(optim_target_num)) {
					List<Schema3NF> list = this.map_3NF_optim_target_to_num.get(optim_target_num);
					list.add(schema3nf);
				}else {
					List<Schema3NF> list = new ArrayList<>();
					list.add(schema3nf);
					this.map_3NF_optim_target_to_num.put(optim_target_num, list);
				}
			}
		}
		this.ave_key_num_on_BCNF_schemata /= (double)schema_BCNF.size();
		this.ave_optim_target_on_3NF_schemata /= (double)schema_3NF.size();
		this.num_of_all_schemata = D.size();
		this.num_of_BCNF_schemata = schema_BCNF.size();
		this.num_of_3NF_schemata = schema_3NF.size();
		
//		System.out.println("BCNF schemata as follows: ");
//		for(SchemaBCNF schema : schema_BCNF) {
//			System.out.println(schema.toString());
//		}
//		System.out.println("###################\n");
//		System.out.println("3NF schemata as follows: ");
//		for(Schema3NF schema : schema_3NF) {
//			System.out.println(schema.toString());
//		}
		
		//output results into file
		this.output_results(para, coverType, optimTarget);
		
		List<Object> result = new ArrayList<>();
		result.add(schema_BCNF);
		result.add(schema_3NF);
		return result;
	}

	class MySchemaInfo {
		Set<String> subschema;
		FD fd;
		int keyNum;
		int FDNum;
		public MySchemaInfo() {}
		public MySchemaInfo(Set<String> subschema, FD fd, int keyNum, int FDNum) {
			this.subschema = subschema;
			this.fd = fd;
			this.keyNum = keyNum;
			this.FDNum = FDNum;
		}
		
		
	}
}
