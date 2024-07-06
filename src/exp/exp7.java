package exp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import entity.Cardinality;
import entity.DataTypeEnum;
import entity.Dataset;
import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;
import entity.Schema3NF;
import entity.SchemaBCNF;
import util.CONF;
import util.CONF_maxk_minf;
import util.CONF_minf_maxk;
import util.CONF_minf_mink;
import util.DBUtils;
import util.DecompAlg4;
import util.Utils;
import util.iCONF;

/**
 * In this experiment, we from data sets select some meaningful FDs and do following experiments
 *
 */
public class exp7 {
	
	public static Map<String, List<String>> getDatasetAttributesMap(String path) throws IOException{
		Map<String, List<String>> map = new HashMap<>();//key = dataset name, value is a list of attributes of the dataset
		FileReader fr = new FileReader(path);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			String[] a = line.split(":");
			String ds_name = a[0];
			String[] attrs = a[1].split(",");
			map.put(ds_name, Arrays.asList(attrs));
		}
		br.close();
		fr.close();
		return map;
	}
	
	/**
	 * In original FD results, we use index of attributes of datasets to represent attributes.
	 * Here we replace it by the real attributes
	 * @param datasetName
	 * @param FDPath
	 * @param map
	 * @throws IOException 
	 */
	public static void copyAndWriteFDsWithAttrName(String datasetName,String FDPath, String newFDPath, Map<String, List<String>> map) throws IOException {
		String content = "";
		FileReader fr = new FileReader(FDPath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			content += line+"\n";
		}
		br.close();
		fr.close();
		
		List<String> dataset_attributes_list = map.get(datasetName);
		for(int i = dataset_attributes_list.size() - 1;i > -1;i --) {
			String attribute = dataset_attributes_list.get(i);
			content = content.replace(i+"", attribute);
		}
		//write
		File f = new File(newFDPath);
		if(!f.exists())
			f.createNewFile();
		FileWriter fw = new FileWriter(newFDPath);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();
		fw.close();
	}
	
	public static void copyAndWriteFDsWithAttrNameV2(String datasetName,String FDPath, String newFDPath, Map<String, List<String>> map) throws IOException {
		List<String> lines = new ArrayList<String>();
		FileReader fr = new FileReader(FDPath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			lines.add(line);
		}
		br.close();
		fr.close();
		
		List<String> dataset_attributes_list = map.get(datasetName);
		List<String> res = new ArrayList<String>();
		for(String row : lines) {
			String a = null;
			String b = null;
			String[] str = row.split(":");
			a = str[0];
			b = str[1];
			for(int i = dataset_attributes_list.size() - 1;i > -1;i --) {
				String attribute = dataset_attributes_list.get(i);
				a = a.replace(i+"", attribute);
			}
			res.add(a+":"+b);
		}
		
		//write
		Utils.writeContent(res, newFDPath, false);
	}
	
	public static List<FD> readFDFromTxt(String FDPath) throws IOException{
		List<FD> fds = new ArrayList<>();
		FileReader fr = new FileReader(FDPath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			String[] left = line.split("->")[0].split(",");
			String right = line.split("->")[1];
			//each column index - 1
			List<String> l = new ArrayList<>();
			List<String> r = new ArrayList<>();
			for(String a : left) {
				l.add((Integer.parseInt(a) - 1)+"");
			}
			r.add((Integer.parseInt(right) - 1)+"");
			FD fd = new FD(l, r);
			fds.add(fd);
		}
		fds = Utils.combineFDs(fds);
		br.close();
		fr.close();
		return fds;
	}
	
	public static List<FD> readFDFromTxtV2(String FDPath) throws IOException{
		List<FD> fds = new ArrayList<>();
		FileReader fr = new FileReader(FDPath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			String[] left = line.split("->")[0].split(",");
			String right = line.split("->")[1];
			List<String> l = new ArrayList<>();
			List<String> r = new ArrayList<>();
			for(String a : left) {
				l.add(a);
			}
			r.add(right);
			FD fd = new FD(l, r);
			fds.add(fd);
		}
//		fds = Utils.combineFDs(fds);
		br.close();
		fr.close();
		return fds;
	}
	
	public static int computeFDRedunCard(FD fd, String table) throws SQLException {
		Connection conn = DBUtils.connectDB();
		String left = "";
		for(int i = 0;i < fd.getLeftHand().size();i ++) {
			if(i != fd.getLeftHand().size() - 1)
				left += "`"+fd.getLeftHand().get(i) +"`,";
			else
				left += "`"+fd.getLeftHand().get(i)+"`";
		}
		String sql = "SELECT COUNT(*) FROM `"+table+"` GROUP BY "+left+" ORDER BY COUNT(*) DESC";
		System.out.println("\n======================");
		System.out.println(sql);
		System.out.println("======================\n");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int card = Integer.parseInt(rs.getString(1));
		stmt.close();
		conn.close();
		return card;
	}
	
	/**
	 * compute FD's redundancy in its dataset
	 * get max redundancy value of each FD as its redundancy
	 * @param fds
	 * @return a list of string with fd and its card decreasingly
	 * @throws SQLException 
	 */
	public static List<Cardinality> computeFDCardDesc(List<FD> fds, String table, Boolean write, String outputPath) throws SQLException{
		List<Cardinality> fd_card_list = new ArrayList<>();//fd with cardinality
		for(FD fd : fds) {
			Cardinality FDCard = new Cardinality(fd, computeFDRedunCard(fd, table));
			fd_card_list.add(FDCard);
		}
		fd_card_list.sort(new Comparator<Cardinality>() {
			//decreasing order
			@Override
			public int compare(Cardinality o1, Cardinality o2) {
				return o2.getCard() - o1.getCard();
			}
			
		});
		//output
		List<String> fd_with_card = new ArrayList<>();
		fd_card_list.forEach(c->fd_with_card.add(c.toString()));
		if(write) {
			Utils.writeContent(fd_with_card, outputPath, false);
		}
		//print
		fd_card_list.forEach(c->System.out.println(c.toString()));
		return fd_card_list;
	}
	
	public static List<Cardinality> selectTopKFDsWithCard(List<Cardinality> fd_with_card_dec, double topK){
		List<Cardinality> topK_fds = new ArrayList<>();
		int total = fd_with_card_dec.size();
		int select_num = (int) (topK * total);
		for(int i = 0;i < total; i ++) {
			if(i < select_num)
				topK_fds.add(fd_with_card_dec.get(i));
		}
		return topK_fds;
	}
	
	public static List<Cardinality> selectTopKFDsWithCard(List<Cardinality> fd_with_card_dec, int topK){
		List<Cardinality> topK_fds = new ArrayList<>();
		int total = fd_with_card_dec.size();
		for(int i = 0;i < total; i ++) {
			if(i < topK)
				topK_fds.add(fd_with_card_dec.get(i));
		}
		return topK_fds;
	}
	
	public static List<Cardinality> selectFdsWithCardOne(List<Cardinality> fd_with_card_dec){
		List<Cardinality> card_one_fds = new ArrayList<>();
		for(Cardinality card : fd_with_card_dec) {
			if(card.getCard() == 1)
				card_one_fds.add(card);
		}
		return card_one_fds;
	}
	
	
	
	public static void getTrafficNormalization() throws IOException {
		Schema traffic_schema = exp6.getTrafficSchema();
		List<String> R = traffic_schema.getAttr_set();
		List<FD> FDs = traffic_schema.getFd_set();
//		List<FD> atomic = Utils.computeAtomicCover(R, FDs);
		FDs= Utils.splitFDs(FDs);
		System.out.println("Schema: \n"+R.toString());
		System.out.println("original FD cover:");
		FDs.forEach(fd->System.out.println(fd.toString()));
//		System.out.println("atomic FD cover:");
//		atomic.forEach(a->System.out.println(a.toString()));
		Parameter para = new Parameter(new Dataset("traffic", 7, 100, ",", null, DataTypeEnum.COMPLETE));
		CONF conf = new CONF();
		List<Object> decomp = conf.decomp_and_output("FDN", para, "reduced minimal", R, FDs);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void getiCONFNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws Exception {
		List<FD> fds = Utils.splitFDs(FDs); 
		iCONF iconf = new iCONF();
		List<Object> decomp = iconf.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void getCONFNormalization(String outputPath, List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws IOException {
		List<FD> fds = Utils.splitFDs(FDs); 
		CONF conf = new CONF();
		List<Object> decomp = conf.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		List<String> opt = new ArrayList<>();
		opt.add("BCNF subschemta:");
		schemaBCNFList.forEach(e->opt.add(e.toString()));
		opt.add("3NF subschemta:");
		schema3NFList.forEach(e->opt.add(e.toString()));
		Utils.writeContent(opt, outputPath, false);
	}
	
	public static void getCONFNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws IOException {
		List<FD> fds = Utils.splitFDs(FDs); 
		//shuffle
//		Collections.shuffle(fds);
//		System.out.println("shuffled fds:");
//		Utils.printFDs(fds);
		CONF conf = new CONF();
		List<Object> decomp = conf.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void get3NFNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws IOException {
		List<FD> fds = Utils.splitFDs(FDs); 
		DecompAlg4 ThirdNF = new DecompAlg4();
		List<Object> decomp = ThirdNF.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void getCONFMaxKMinFNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws Exception {
		List<FD> fds = Utils.splitFDs(FDs); 
		CONF_maxk_minf d = new CONF_maxk_minf();
		List<Object> decomp = d.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void getCONFMinFMaxKNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws Exception {
		List<FD> fds = Utils.splitFDs(FDs); 
		CONF_minf_maxk d = new CONF_minf_maxk();
		List<Object> decomp = d.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	public static void getCONFMinFMinKNormalization(List<FD> FDs, List<String> R, Parameter para, String subschema_cover_type) throws Exception {
		List<FD> fds = Utils.splitFDs(FDs); 
		CONF_minf_mink d = new CONF_minf_mink();
		List<Object> decomp = d.decomp_and_output("FDN", para, subschema_cover_type, R, fds);
		List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
		System.out.println("BCNF subschemta:");
		schemaBCNFList.forEach(e->System.out.println(e.toString()));
		System.out.println("3NF subschemta:");
		schema3NFList.forEach(e->System.out.println(e.toString()));
	}
	
	
	public static void run(String root, Parameter para, int topK, List<String> fdcover_list, List<String> mixedcover_list
			, List<String> fdcover_list_4_norm, List<String> mixedcover_list_4_norm) throws IOException, SQLException {
		//read FDs from txt and compute FD cardinality with decreasing order
		String name = para.dataset.name;
		String outputPath = root + "\\"+name+"-stats.txt";
		String fdPath = root + "\\"+name+"-fds.txt";
		List<FD> fds = exp7.readFDFromTxt(fdPath);
		List<Cardinality> fd_with_card = exp7.computeFDCardDesc(fds, name, true, outputPath);
		
		//replace number to attribute of FDs
		Map<String, List<String>> map =getDatasetAttributesMap("C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\dataset-attributes.txt");
		exp7.copyAndWriteFDsWithAttrNameV2(name, root+"\\"+name+"-stats.txt", root+"\\"+name+"-stats-1.txt", map);
		
		
		//select topK cardinality FDs and all Fds with cardinality one
		List<Cardinality> fd_with_card_topK = null;
		List<Cardinality> fd_with_card_one = null;
		List<FD> selectedFDs = new ArrayList<>();
		if(topK > 0) {
			fd_with_card_topK = exp7.selectTopKFDsWithCard(fd_with_card, topK);
			fd_with_card_one = exp7.selectFdsWithCardOne(fd_with_card);
			for(Cardinality c : fd_with_card_topK) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
			for(Cardinality c : fd_with_card_one) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
		}else {//if topK is negative, all FDs are included
			for(Cardinality c : fd_with_card) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
		}
		
		
		
		//normalization and output
		int attrNum = para.dataset.col_num;
		List<String> R = new ArrayList<>();
		for(int i = 0;i < attrNum;i ++) {
			R.add(i+"");
		}
		for(String cover_type : fdcover_list_4_norm) {
			String optPath;
			if(topK > 0)
				optPath = root +"\\Top"+topK+"+Keys\\CONF_Top"+topK+"_"+cover_type+".txt";
			else
				optPath = root +"\\TopAll+Keys\\CONF_TopAll_"+cover_type+".txt";
			exp7.getCONFNormalization(optPath,selectedFDs, R, para, cover_type);
		}
		for(String cover_type : mixedcover_list_4_norm) {
			String optPath;
			if(topK > 0)
				optPath = root +"\\Top"+topK+"+Keys\\CONF_Top"+topK+"_"+cover_type+".txt";
			else
				optPath = root +"\\TopAll+Keys\\CONF_TopAll_"+cover_type+".txt";
			exp7.getCONFNormalization(optPath,selectedFDs, R, para, cover_type);
		}
		
		
		//compute non-normalized FD cover/mixed cover
		for(String cover_type : fdcover_list) {
			List<String> opt = new ArrayList<>();
			List<FD> fdcover = Utils.compFDCover(selectedFDs, cover_type);
			fdcover.forEach(f -> opt.add(f.toString()));
			int fd_num = fdcover.size();
			int attr_num = Utils.compFDAttrSymbNum(fdcover);
			opt.add("cover type : "+cover_type+" | FD No. : "+fd_num+" | FD attr symbol No. : "+attr_num);
			String optPath;
			if(topK > 0)
				optPath = root +"\\Top"+topK+"+Keys\\Top"+topK+"_"+cover_type+".txt";
			else
				optPath = root +"\\TopAll+Keys\\TopAll_"+cover_type+".txt";
			Utils.writeContent(opt, optPath, false);
		}
		
		for(String cover_type : mixedcover_list) {
			List<String> opt = new ArrayList<>();
			int idx = cover_type.indexOf(" keyfd");
			List<FD> fdcover = Utils.compFDCover(selectedFDs, cover_type.substring(0, idx));
			List<Object> mixedcoverinfo = Utils.compKeyFDCover(R, fdcover);
			List<Key> keys = (List<Key>) mixedcoverinfo.get(0);
			List<FD> remainingFDs = (List<FD>) mixedcoverinfo.get(1);
			opt.add("Keys: ");
			keys.forEach(k -> opt.add(k.toString()));
			opt.add("FDs: ");
			remainingFDs.forEach(f -> opt.add(f.toString()));
			int fd_num = remainingFDs.size();
			int fd_attr_num = Utils.compFDAttrSymbNum(remainingFDs);
			int key_num = keys.size();
			int key_attr_num = Utils.compKeyAttrSymbNum(keys);
			opt.add("cover type : "+cover_type+" | Key No. : "+key_num+" | Key attr symbol No. : "+key_attr_num);
			opt.add("cover type : "+cover_type+" | FD No. : "+fd_num+" | FD attr symbol No. : "+fd_attr_num);
			String optPath;
			if(topK > 0)
				optPath = root +"\\Top"+topK+"+Keys\\Top"+topK+"_"+cover_type+".txt";
			else
				optPath = root +"\\TopAll+Keys\\TopAll_"+cover_type+".txt";
			Utils.writeContent(opt, optPath, false);
		}
	}
	
	public static List<Double> runSingleExp(String outputPath, int topK, Boolean Normalized,String FDCoverType, Parameter para,int repeat, Schema schema, List<Integer> insert_row_num_list) throws SQLException, IOException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		String OriginTable = para.dataset.name;
		String ProjTable = OriginTable+"_proj";
		
		List<String> sub_schema = schema.getAttr_set();
		
		//create projection table on database
		DBUtils.createTable(ProjTable,sub_schema);
		
		//get projection table
		
		List<List<String>> proj_table_dataset = exp5.get_projection_on_subschema(sub_schema,OriginTable);
		
		//insert projection rows
		exp5.insert_data(ProjTable,proj_table_dataset);
		
		//add unique constraints if exists
		List<String> all_uc_id = new ArrayList<String>();//record all unique constraint names
		int uc_id = 0;
		for(Key k : schema.getMin_key_list()) {
			String uc_name = "uc_"+ProjTable+"_"+uc_id ++;
			DBUtils.addUnique(k,ProjTable,uc_name);
			all_uc_id.add(uc_name);
		}
		
		//add FD triggers if exists
		String triggerID = "tri_"+ProjTable;
		if(!schema.getFd_set().isEmpty()) {
			DBUtils.addTrigger(schema.getFd_set(),ProjTable,triggerID);
		}
		
		
		//update experiments
		for(int row_num : insert_row_num_list) {
			List<Double> cost_list = new ArrayList<Double>();
			List<List<String>> inserted_data = exp5.gen_inserted_dataset(row_num,proj_table_dataset.get(0).size());
			for(int i = 0;i < repeat;i ++) {
				double cost = exp5.insert_data(ProjTable,inserted_data);
				cost_list.add(cost);
				exp5.delete_data(ProjTable,"`id` > "+proj_table_dataset.size());
			}
			result.add(Utils.getAve(cost_list));
			result.add(Utils.getMedian(cost_list));
		}
		
		//drop the table
		DBUtils.dropTable(ProjTable);
		
		//output
		String stat = "";
		for(double a : result) {
			stat += ","+a;
		}
		String top;
		if(topK <= 0)
			top = "all";
		else
			top = "top"+topK;
		String res = para.dataset.name+","+top+","+Normalized+","+FDCoverType+","
				+schema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(schema.getMin_key_list())
				+","+schema.getFd_set().size()+","+Utils.compFDAttrSymbNum(schema.getFd_set())+","+proj_table_dataset.size()+stat;
		Utils.writeContent(Arrays.asList(res), outputPath, true);
		
		System.out.println(res.replace(",", " | "));
		System.out.println("###############################\n");
		
		return result;
	}
	
	public static void runExps(String root, Parameter para, int topK, List<String> fdcover_list, List<String> mixedcover_list, List<String> fdcover_list_4_norm, List<String> mixedcover_list_4_norm,int repeat, List<Integer> insert_row_num_list) throws IOException, SQLException {
		//read FDs from txt and compute FD cardinality with decreasing order
		String name = para.dataset.name;
		String fdPath = root + "\\"+name+"-fds.txt";
		String outputPath = root + "\\"+name+"-results.txt";
		List<FD> fds = exp7.readFDFromTxt(fdPath);
		List<Cardinality> fd_with_card = exp7.computeFDCardDesc(fds, name, false, "");
		
		//schema
		int attrNum = para.dataset.col_num;
		List<String> R = new ArrayList<>();
		for(int i = 0;i < attrNum;i ++) {
			R.add(i+"");
		}
		
		//select topK cardinality FDs and all Fds with cardinality one
		List<Cardinality> fd_with_card_topK = null;
		List<Cardinality> fd_with_card_one = null;
		List<FD> selectedFDs = new ArrayList<>();
		if(topK > 0) {
			fd_with_card_topK = exp7.selectTopKFDsWithCard(fd_with_card, topK);
			fd_with_card_one = exp7.selectFdsWithCardOne(fd_with_card);
			for(Cardinality c : fd_with_card_topK) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
			for(Cardinality c : fd_with_card_one) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
		}else {//if topK is negative, all FDs are included
			for(Cardinality c : fd_with_card) {
				FD f = c.getFd();
				if(!selectedFDs.contains(f))
					selectedFDs.add(f);
			}
		}
		
		//compute non-normalized FD cover/mixed cover
		for(String cover_type : fdcover_list) {
			List<FD> fdcover = Utils.compFDCover(selectedFDs, cover_type);
			Schema schema = new Schema(R, fdcover);//R & FDs
			exp7.runSingleExp(outputPath, topK,false, cover_type, para, repeat, schema, insert_row_num_list);
		}
		
		for(String cover_type : mixedcover_list) {
			int idx = cover_type.indexOf(" keyfd");
			List<FD> fdcover = Utils.compFDCover(selectedFDs, cover_type.substring(0, idx));
			List<Object> mixedcoverinfo = Utils.compKeyFDCover(R, fdcover);
			List<Key> keys = (List<Key>) mixedcoverinfo.get(0);
			List<FD> remainingFDs = (List<FD>) mixedcoverinfo.get(1);
			Schema schema = new Schema(R, remainingFDs, keys);//R & FDs & Keys
			exp7.runSingleExp(outputPath, topK,false, cover_type, para, repeat, schema, insert_row_num_list);
		}
		
		
		//normalization
		for(String cover_type : fdcover_list_4_norm) {
			CONF conf = new CONF();
			List<Object> decomp = conf.decomp_and_output("FDN", para, cover_type, R, Utils.splitFDs(selectedFDs));
			List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
			List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
			for(SchemaBCNF s : schemaBCNFList) {
				Schema schema = new Schema();
				schema.setAttr_set(s.getAttr_set());
				schema.setMin_key_list(s.getMin_key_list());
				schema.setFd_set(new ArrayList<>());
				exp7.runSingleExp(outputPath, topK,true, cover_type, para, repeat, schema, insert_row_num_list);
			}
			for(Schema3NF s : schema3NFList) {
				Schema schema = new Schema(s.getAttr_set(), s.getFd_set());
				exp7.runSingleExp(outputPath, topK,true, cover_type, para, repeat, schema, insert_row_num_list);
			}
		}
		for(String cover_type : mixedcover_list_4_norm) {
			CONF conf = new CONF();
			List<Object> decomp = conf.decomp_and_output("FDN", para, cover_type, R, Utils.splitFDs(selectedFDs));
			List<SchemaBCNF> schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
			List<Schema3NF> schema3NFList = (List<Schema3NF>) decomp.get(1);
			for(SchemaBCNF s : schemaBCNFList) {
				Schema schema = new Schema();
				schema.setAttr_set(s.getAttr_set());
				schema.setMin_key_list(s.getMin_key_list());
				schema.setFd_set(new ArrayList<>());
				exp7.runSingleExp(outputPath, topK,true, cover_type, para, repeat, schema, insert_row_num_list);
			}
			for(Schema3NF s : schema3NFList) {
				Schema schema = new Schema(s.getAttr_set(), s.getFd_set(), s.getMin_key_list());
				exp7.runSingleExp(outputPath, topK,true, cover_type, para, repeat, schema, insert_row_num_list);
			}
		}
	}
	
	
	public static Schema getExampleSchema() {
		List<String> R = Arrays.asList("A", "B", "C", "D", "E");
		List<FD> fds = Arrays.asList(new FD(Arrays.asList("A","B","D"), Arrays.asList("E")),
				new FD(Arrays.asList("B", "C", "D"), Arrays.asList("A")),
				new FD(Arrays.asList("B","C","D"), Arrays.asList("E")),
				new FD(Arrays.asList("A","B"), Arrays.asList("C")),
				new FD(Arrays.asList("E","C"), Arrays.asList("D")),
				new FD(Arrays.asList("E","D"), Arrays.asList("C")),
				new FD(Arrays.asList("D","E"), Arrays.asList("A")));
		//compute atomic cover
		List<FD> atomic = exp7.getAtomicClosure(fds);
		Collections.shuffle(atomic);
		return new Schema(R, atomic);
	}
	
	public static List<FD> getAtomicClosure(List<FD> fds){
		List<FD> canonical = Utils.compFDCover(fds, "canonical");
		List<FD> atomic = Utils.deepCopyFDs(canonical);
		int index = 0;
		while(index < atomic.size()) {
			FD Y_B = atomic.get(index);
			for(int i = 0;i < canonical.size();i ++) {
				FD X_A = canonical.get(i);
				if(Y_B.getLeftHand().containsAll(X_A.getRightHand()) && !X_A.getLeftHand().containsAll(Y_B.getRightHand())) {
					//A IN Y && B NOT IN X
					Set<String> XY_minus_A = new HashSet<>();
					XY_minus_A.addAll(X_A.getLeftHand());
					XY_minus_A.addAll(Y_B.getLeftHand());
					XY_minus_A.removeAll(X_A.getRightHand());
					//XY\A -> B, and reduce left hand side
					FD cand = new FD(new ArrayList<>(XY_minus_A), new ArrayList<>(Y_B.getRightHand()));
					List<String> left_tmp = new ArrayList<>(cand.getLeftHand());
					for(String a : left_tmp) {
						List<String> LHS_no_a = new ArrayList<String>();
						LHS_no_a.addAll(cand.getLeftHand());
						LHS_no_a.remove(a);
						FD f = new FD(LHS_no_a,cand.getRightHand());
						if(Utils.isImplied(fds, f)) {
							cand.getLeftHand().remove(a);//remove redundant attribute from X
						}
					}
					if(!atomic.contains(cand))
						atomic.add(cand);
				}
			}
			index ++;
		}
		System.out.println("atomic closure: ");
		Utils.printFDs(atomic);
		return atomic;
	}

	public static void main(String[] args) throws Exception {
		//copy and replace column index by actual attribute name
//		for(Parameter para : Utils.getParameterList(Arrays.asList("hepatitis"), DataTypeEnum.NULL_UNCERTAINTY)) {
//			Map<String, List<String>> map =getDatasetAttributesMap(para.root_path+"\\dataset-attributes.txt");
//			String path = Utils.getCoverPath("original", para);
//			String newPath = para.root_path+"\\"+para.dataset.name+"-attr.txt";
//			exp7.copyAndWriteFDsWithAttrName(para.dataset.name, path, newPath, map);
//		}
		
		//replace number to specific attribute of FDs
//		String name = "dblp10k";
//		String root = "C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验";
//		Map<String, List<String>> map =getDatasetAttributesMap(root+"\\dataset-attributes.txt");
//		exp7.copyAndWriteFDsWithAttrNameV2(name, root+"\\meaningful FD\\"+name+"\\"+name+"-stats.txt", root+"\\meaningful FD\\"+name+"\\"+name+"-stats-1.txt", map);
		
		//read FDs from txt and compute FD cardinality with decreasing order
//		String name = "dblp10k";
//		String root = "C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\meaningful FD\\"+name;
//		String outputPath = root + "\\"+name+"-stats.txt";
//		String fdPath = root + "\\"+name+"-fds.txt";
//		List<FD> fds = exp7.readFDFromTxt(fdPath);
////		List<Object> fd_info = Utils.getFDCover("original", new Parameter(new Dataset("bridges", 13, 108, ",", "?", DataTypeEnum.NULL_UNCERTAINTY)));
////		List<FD> fds = (List<FD>) fd_info.get(1);
//		List<Cardinality> fd_with_card = exp7.computeFDCardDesc(fds, name, true, outputPath);
		
		//%---------------------------------------------%
//		getTrafficNormalization();
		
		//select topK cardinality FDs and all Fds with cardinality one
//		int topK = 10;
//		List<Cardinality> fd_with_card_topK = exp7.selectTopKFDsWithCard(fd_with_card, topK);
//		List<Cardinality> fd_with_card_one = exp7.selectFdsWithCardOne(fd_with_card);
		
		//normalization
//		String name = "claims";
//		Parameter para = new Parameter(new Dataset("claims", 13, 96388, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
//		String name = "t_biocase_identification_r91800_c38";
//		Parameter para = new Parameter(new Dataset("t_biocase_identification_r91800_c38", 38, 91779, ",", null, DataTypeEnum.NULL_UNCERTAINTY));
//		String name = "dblp10k";
//		Parameter para = new Parameter(new Dataset("dblp10k", 34, 4837, ",", null, DataTypeEnum.NULL_UNCERTAINTY));
//		String name = "bridges";
//		Parameter para = new Parameter(new Dataset("bridges", 13, 108, ",", "?", DataTypeEnum.NULL_UNCERTAINTY));
//		String name = "routes";
//		Parameter para = new Parameter(new Dataset("routes", 9, 67663, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
//		Parameter para = new Parameter(new Dataset("hospital", 15, 114919, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
//		String fdPath = "C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\meaningful FD\\"+name+"\\"+name+"-fds.txt";
//		List<FD> fds = exp7.readFDFromTxt(fdPath);
//		List<Cardinality> fd_with_card = exp7.computeFDCardDesc(fds, name, false, "");
//		//select topK cardinality FDs and all Fds with cardinality one
//		List<Cardinality> fd_with_card_topK = null;
//		List<Cardinality> fd_with_card_one = null;
//		int topK = -1;
//		List<FD> selectedFDs = new ArrayList<>();
//		if(topK > 0) {
//			fd_with_card_topK = exp7.selectTopKFDsWithCard(fd_with_card, topK);
//			fd_with_card_one = exp7.selectFdsWithCardOne(fd_with_card);
//			for(Cardinality c : fd_with_card_topK) {
//				FD f = c.getFd();
//				if(!selectedFDs.contains(f))
//					selectedFDs.add(f);
//			}
//			for(Cardinality c : fd_with_card_one) {
//				FD f = c.getFd();
//				if(!selectedFDs.contains(f))
//					selectedFDs.add(f);
//			}
//		}else {//if topK is negative, all FDs are included
//			for(Cardinality c : fd_with_card) {
//				FD f = c.getFd();
//				if(!selectedFDs.contains(f))
//					selectedFDs.add(f);
//			}
//		}
//		int attrNum = para.dataset.col_num;
//		List<String> R = new ArrayList<>();
//		for(int i = 0;i < attrNum;i ++) {
//			R.add(i+"");
//		}
//		String subschema_cover_type = "optimal keyfd";
//		exp7.get3NFNormalization(selectedFDs, R, para, subschema_cover_type);
//		exp7.getCONFNormalization(selectedFDs, R, para, subschema_cover_type);
//		exp7.getCONFMaxKMinFNormalization(selectedFDs, R, para, subschema_cover_type);
//		exp7.getCONFMinFMaxKNormalization(selectedFDs, R, para, subschema_cover_type);
//		exp7.getCONFMinFMinKNormalization(selectedFDs, R, para, subschema_cover_type);
		
		//example
//		Schema s = exp7.getExampleSchema();
//		List<FD> fds = exp7.readFDFromTxtV2("C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\meaningful FD\\running example\\atomic closure.txt");
//		System.out.println("shuffled fds:");
////		Utils.printFDs(s.getFd_set());
//		Utils.printFDs(fds);
//		Parameter para = new Parameter(new Dataset("example", 13, 96388, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
//		exp7.getCONFMaxKMinFNormalization(fds, s.getAttr_set(), para, "optimal keyfd");
//		exp7.getCONFMinFMaxKNormalization(fds, s.getAttr_set(), para, "optimal keyfd");
////		exp7.getCONFMinFMinKNormalization(s.getFd_set(), s.getAttr_set(), para, "optimal keyfd");
//		exp7.getCONFNormalization(fds, s.getAttr_set(), para, "optimal keyfd");
////		exp7.get3NFNormalization(s.getFd_set(), s.getAttr_set(), para, "optimal keyfd");
		
		
		//compute FD cover/mixed cover
//		String coverType = "reduced minimal";
//		System.out.println(coverType);
//		List<FD> fdcover = Utils.compFDCover(fds, coverType);
//		fdcover.forEach(f -> System.out.println(f.toString()));
//		int fd_num = fdcover.size();
//		int attr_num = Utils.compFDAttrSymbNum(fdcover);
//		System.out.println("FD No. : "+fd_num+" | FD attr symbol No. : "+attr_num);
		
//		String coverType = "reduced minimal keyfd";
//		System.out.println(coverType);
//		List<Object> fdcoverinfo = Utils.compKeyFDCover(R, fds);
//		List<Key> keys = (List<Key>) fdcoverinfo.get(0);
//		List<FD> remainingFDs = (List<FD>) fdcoverinfo.get(1);
//		System.out.println("Keys: ");
//		keys.forEach(k -> System.out.println(k.toString()));
//		System.out.println("FDs: ");
//		remainingFDs.forEach(f -> System.out.println(f.toString()));
//		int fd_num = remainingFDs.size();
//		int fd_attr_num = Utils.compFDAttrSymbNum(remainingFDs);
//		int key_num = keys.size();
//		int key_attr_num = Utils.compKeyAttrSymbNum(keys);
//		System.out.println("Key No. : "+key_num+" | Key attr symbol No. : "+key_attr_num);
//		System.out.println("FD No. : "+fd_num+" | FD attr symbol No. : "+fd_attr_num);
		
//		//read FDs from Json
//		List<Object> fd_info = Utils.getFDCover("original", new Parameter(new Dataset("adult", 15, 32561, ";", null, DataTypeEnum.COMPLETE)));
//		List<FD> fds = (List<FD>) fd_info.get(1);
//		fds = Utils.splitFDs(fds);
//		List<String> opt = new ArrayList<String>();
//		for(FD fd : fds) {
//			String str = "";
//			for(int i = 0;i < fd.getLeftHand().size();i ++) {
//				String a = fd.getLeftHand().get(i);
//				int a1 = Integer.parseInt(a) + 1;
//				if(i != fd.getLeftHand().size() - 1)
//					str += a1+",";
//				else
//					str += a1;
//			}
//			str += "->";
//			for(int i = 0;i < fd.getRightHand().size();i ++) {
//				String a = fd.getRightHand().get(i);
//				int a1 = Integer.parseInt(a) + 1;
//				if(i != fd.getRightHand().size() - 1)
//					str += a1+",";
//				else
//					str += a1;
//			}
//			opt.add(str);
//		}
//		Utils.writeContent(opt, "C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\meaningful FD\\adult\\adult-fds.txt", false);
		
		String name = "claims";
		Parameter para = new Parameter(new Dataset("claims", 13, 96388, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
//		Parameter para = new Parameter(new Dataset("bridges", 13, 108, ",", "?", DataTypeEnum.NULL_UNCERTAINTY));
//		Parameter para = new Parameter(new Dataset("routes", 9, 67663, ",", "", DataTypeEnum.NULL_UNCERTAINTY));
		List<String> fdcoverlist = Arrays.asList("optimal");
		List<String> mixedcoverlist = Arrays.asList("optimal keyfd");
		List<String> fdcoverlist4norm = Arrays.asList();//"reduced minimal","optimal"
		List<String> mixedcoverlist4norm = Arrays.asList();//"reduced minimal keyfd","optimal keyfd"
		int topK = -1;//negative means all FDs as input
		int repeat = 1;
		List<Integer> insert_row_list = Arrays.asList(50, 100, 150);
		String root = "C:\\Users\\freem\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\meaningful FD\\"+name;
		exp7.runExps(root, para, topK, fdcoverlist, mixedcoverlist, fdcoverlist4norm, mixedcoverlist4norm, repeat, insert_row_list);
	}

}
