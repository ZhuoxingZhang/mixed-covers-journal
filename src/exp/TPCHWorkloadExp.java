package additional;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import entity.FD;
import entity.Key;
import util.DBUtils;
import util.Utils;
import entity.Schema3NF;
import entity.SchemaBCNF;


/**
 * In this experiments, we use TPC-H benchmark to do workload experiments under various key numbers and FDs
 *
 */
public class TPCHWorkloadExp {
	/**
	 * get 22 sqls from local file
	 * @param path
	 * @return a map which key = sql id, value = sql
	 * @throws IOException 
	 */
	public static void get_query_sqls_map(String root_dir,Map<String,String> query_sqls_map) throws IOException{
		File f = new File(root_dir);
		if(f.isDirectory()) {
			for(File sql_file : f.listFiles()) {
				if(sql_file.isFile()) {
					String sql_id = sql_file.getName().split("\\.")[0];//q1-q22
					String sql = read_content_from_local(sql_file.getAbsolutePath());
					query_sqls_map.put(sql_id, sql);
				}else {
					get_query_sqls_map(sql_file.getAbsolutePath(),query_sqls_map);
				}
			}
		}else {
			String sql_id = f.getName().split(".")[0];//q1-q22
			String sql = read_content_from_local(f.getAbsolutePath());
			query_sqls_map.put(sql_id, sql);
		}
	}
	
	public static String read_content_from_local(String path) throws IOException {
		String output = "";
		FileReader fr = new FileReader(path);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			output += line+"\n";
		}
		br.close();
		fr.close();
		return output;
	}
	
	/**
	 * for TPC-H, we have 22 query sqls.
	 * @param sql id (1-22)
	 * @param sql
	 * @return query execution time
	 * @throws SQLException
	 */
	public static double query4TPCH(String DBName, String sql_id,String sql) throws SQLException {
		Connection conn = DBUtils.connectDB(DBName);
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		System.out.println("\n==================");
		System.out.println("executing sql with id ["+sql_id+"]");
		System.out.println(sql);
		
		long start = System.currentTimeMillis();
		String[] sqls = sql.split(";");
		for(String s : sqls) {
			String empty_str_test = s.replace(" ", "").replace("\n", "");;
			if(!"".equals(empty_str_test))
				stmt.execute(s);
		}
		long end = System.currentTimeMillis();
		
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		return (double)(end - start);
	}
	
	/**
	 * the refresh function is like :
	 * LOOP (SF * 1500) TIMES
	 * 	INSERT a new row into the ORDERS table
	 * 	LOOP RANDOM(1, 7) TIMES
	 * 		INSERT a new row into the LINEITEM table
	 * 	END LOOP
	 * END LOOP
	 * But for better showing influence of key number, we SF and second loop time as variable
	 * SF : scale factor that shows the whole data quantity, its value like 1G,2G,...
	 * @param first_loop_time
	 * @param second_loop_time
	 * @param orders_data all needed inserted data into orders
	 * @param orders_attr_type if it is string we set "string" in relative position,if int we set "int"
	 * for a tuple ['a',2] the attribute_type should be ['string','int']
	 * @param lineitem_data 
	 * @param lineitem_attr_type
	 * @return insertion execute time
	 * @throws SQLException
	 */
	public static double refreshFunction(int first_loop_time, int second_loop_time,List<List<String>> orders_data,List<String> orders_attr_type,List<List<String>> lineitem_data,List<String> lineitem_attr_type) throws SQLException {
		System.out.println("\n==================");
		System.out.println("executing refresh function(RF1)...");
		
		long start = System.currentTimeMillis();
		int count = -1;
		for(int i = 0;i < first_loop_time;i ++) {
			insert_one_tuple("tpch","orders",orders_data.get(i),orders_attr_type);
			for(int j = 0;j < second_loop_time;j ++) {
				count ++;
				insert_one_tuple("tpch","lineitem",lineitem_data.get(count),lineitem_attr_type);
			}
		}
		long end = System.currentTimeMillis();
		
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		return (double)(end - start);
	}
	
	public static double refreshFunctionV2(int first_loop_time, int second_loop_time,List<List<String>> orders_data,List<List<String>> lineitem_data) throws SQLException {
		System.out.println("\n==================");
		System.out.println("executing refresh function(RF1)...");
		
		long start = System.currentTimeMillis();
		DBUtils.insertDataWithoutID("tpch", "orders", orders_data);
		DBUtils.insertDataWithoutID("tpch", "lineitem", lineitem_data);
		long end = System.currentTimeMillis();
		
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		return (double)(end - start);
	}
	
	/**
	 * just insert one tuple
	 * @param DBName
	 * @param tableName
	 * @param row
	 * @param attribute_type if it is string we set "string" in relative position,if int we set "int",if date fromat we set "date"
	 * for a tuple ['a',2] the attribute_type should be ['string','int']
 	 * @return cost time for inserting records(ms)
	 * @throws SQLException
	 */
	public static double insert_one_tuple(String DBName, String tableName,List<String> row,List<String> attribute_type) throws SQLException {
		if(row == null)
			return -1;
		if(row.isEmpty())
			return -1;
		Connection conn = DBUtils.connectDB(DBName);
		String insertSql = "INSERT INTO `"+tableName + "` VALUES (";
		for(int i = 0;i < row.size();i ++) {
			if(i != (row.size() - 1))
				insertSql += (attribute_type.get(i).equals("int") ? row.get(i) : "'"+row.get(i)+"'")+" ,";
			else
				insertSql += (attribute_type.get(i).equals("int") ? row.get(i) : "'"+row.get(i)+"'")+" )";
		}
		Statement Stmt = conn.createStatement();
		
		long start = System.currentTimeMillis();
		Stmt.executeUpdate(insertSql);
		long end = System.currentTimeMillis();
		Stmt.close();
		conn.close();
		return (double)(end - start);
	}
	
	public static String nextDay(String dt) {
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		 Calendar c = Calendar.getInstance();
		 try {
			c.setTime(sdf.parse(dt));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 c.add(Calendar.DATE, 1);  // number of days to add
		 dt = sdf.format(c.getTime());  // dt is now the new date
		 return dt;
	}
	/**
	 * 
	 * @param col_num a data's column number
	 * @param row_num data number of the data set
	 * @param attr_type
	 * @return a data set with fixed col_num and row_num
	 */
	public static List<List<String>> gen_inserted_dataset_for_customer(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string"))
					data.add(i+"_"+j);
				else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(j == 2){//nation key [0,24]
						Random rand = new Random();
						int rand_num = rand.nextInt(25);
						data.add(rand_num+"");
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	/**
	 * 
	 * @param col_num a data's column number
	 * @param row_num data number of the data set
	 * @param attr_type
	 * @return a data set with fixed col_num and row_num
	 */
	public static List<List<String>> gen_inserted_dataset_for_nation(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string"))
					data.add(i+"_"+j);
				else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(j == 2){//region key [0,4]
						Random rand = new Random();
						int rand_num = rand.nextInt(5);
						data.add(rand_num+"");
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	public static List<List<String>> gen_inserted_dataset_for_part(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string")) {
						data.add(i+"_"+j);
				}else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	public static List<List<String>> gen_inserted_dataset_for_partsupp(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		int partkey_count = 1;
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string"))
					data.add(i+"_"+j);
				else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(j == 0) {//ps_parkey [1,200000]
//						Random rand = new Random();
//						int rand_num = rand.nextInt(200000) + 1;
						if(dataset.isEmpty()) {
							data.add("1");
						}else if (partkey_count < 5){
							int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
							data.add(last_tuple_value + "");
						}else {
							partkey_count = 1;
							int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
							data.add((last_tuple_value + 1) + "");
						}
						partkey_count ++;
						continue;	
					}
					if(j == 1) {//ps_suppkey [1,10000]
						if(dataset.isEmpty()) {
							String partkey_v = data.get(0);
							data.add(partkey_v);
						}else {
							int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
							if(partkey_count == 2){
								if(Integer.parseInt(data.get(0)) > 10000) {
									if(Integer.parseInt(data.get(0))%1000 == 0)
										data.add((Integer.parseInt(data.get(0))%1000 + 2)+"");
									else
										data.add((Integer.parseInt(data.get(0))%1000)+"");
								}else
									data.add(data.get(0));
							}else if((last_tuple_value + 1000) > 10000 && partkey_count <= 5){
								if((last_tuple_value + 1000 - 10000) == 0)
									data.add((last_tuple_value + 1000 - 10000 + 2)+"");
								else
									data.add((last_tuple_value + 1000 - 10000)+"");
							}else {
								data.add((last_tuple_value + 1000)+"");
							}
						}
						
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	public static List<List<String>> gen_inserted_dataset_for_region(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string")) {
					data.add(i+"_"+j);
				}else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	public static List<List<String>> gen_inserted_dataset_for_supplier(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string")) {
					data.add(i+"_"+j);
				}else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value 
					if(j == 1) {//nation key [0-24]
						Random rand = new Random();
						int rand_num = rand.nextInt(25);
						data.add(rand_num+"");
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	/**
	 * 
	 * @param col_num a data's column number
	 * @param row_num data number of the data set
	 * @param attr_type
	 * @return a data set with fixed col_num and row_num
	 */
	public static List<List<String>> gen_inserted_dataset_for_orders(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string")) {
					if(j == 6)
						data.add(new Random().nextInt(10)+"");//satisfy char with length 1
					else
						data.add(i+"_"+j);
				}else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(j == 2){//orders table's o_custkey should betweent 1-1000
						Random rand = new Random();
						int rand_num = rand.nextInt(1000) + 1;
						data.add(rand_num+"");
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	public static List<List<String>> gen_inserted_dataset_for_lineitem(int row_num,int col_num,List<String> attr_type){
		List<List<String>> dataset = new ArrayList<List<String>>();
		String currentDay = "2023-05-24";
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				if(attr_type.get(j).equals("string")) {
					if(j == 6 || j == 8)
						data.add(new Random().nextInt(10)+"");//satisfy char with length 1
					else
						data.add(i+"_"+j);
				}else if(attr_type.get(j).equals("date")) {
					currentDay = nextDay(currentDay);
					data.add(currentDay);
				}else {//if column value should be integer, we start to generate value from -1, then -2,-3,...
					if(j == 7){//lineitem table's l_partkey should be integer between 1-250
//						Random rand = new Random();
//						int rand_num = rand.nextInt(250) + 1;
						data.add("1");
						continue;
					}
					if(j == 4) {//lineitem table's l_suppkey
						List<Integer> set = Arrays.asList(2,2502,5002,7502);
						Random rand = new Random();
						int rand_i = rand.nextInt(set.size());
						data.add(set.get(rand_i)+"");
						continue;
					}
					if(j == 1){//l_orderkey value should be in value domain of orders table's o_orderkey
						data.add("6000001");
						continue;
					}
					if(dataset.isEmpty()) {
						data.add("6000001");
					}else {
						int last_tuple_value = Integer.parseInt(dataset.get(dataset.size() - 1).get(j));
						data.add((last_tuple_value + 1) + "");
					}
				}
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	
	/**
	 * 
	 * @param round
	 * @param first_loop_time
	 * @param second_loop_time
	 * @param orders_attr_type
	 * @param lineitem_attr_type
	 * @param mostKeyNum
	 * @param allTables
	 * @param table_key_map
	 * @param query_sqls_map
	 * @param query_names
	 * @return
	 * @throws SQLException
	 */
	public static List<Double> runSingleExp(String outputPathSchema,String outputPath, int repeat,List<Integer> insert_tuples, Map<String,List<String>> attr_type_map,
			List<String> allTables,Map<String,Schema3NF> table_schema_map, int first_loop_time, List<Integer> second_loop_time,Map<String, String> query_sqls_map,List<String> query_names) throws SQLException {
		for(String table : allTables) {
			Schema3NF schema = table_schema_map.get(table);
			List<Key> keys = schema.getMin_key_list();
			List<FD> fds = schema.getFd_set();
			List<String> R = schema.getAttr_set();
			//record the 3NF schema
			String str = "Key Num: "+keys.size()+" | FD Num: "+fds.size()+"\ntable: "+table+"\nR: "+R.toString()+"\n";
			if(!keys.isEmpty())
				str += getKeysString(keys);
			str += getFDsString(fds);
			str += "----------------------------\n\n";
			Utils.writeContent(Arrays.asList(str), outputPathSchema, true);
		}
		Utils.writeContent(Arrays.asList("###############################\n\n\n"), outputPathSchema, true);
		System.out.println("##############");
		
		//add keys in form of unique constraints for each table
		Map<String,List<String>> table_uc_map = new HashMap<String,List<String>>();//we record all unique index names for each table
		Iterator<String> iter_unique = table_schema_map.keySet().iterator();
		while(iter_unique.hasNext()) {
			String t = iter_unique.next();//specific table
			Schema3NF schema = table_schema_map.get(t);
			List<Key> t_keys_for_exp = schema.getMin_key_list();
			List<String> uc_for_table = new ArrayList<String>();
			System.out.println("adding unique constraint for table : "+t+" | uc num : "+t_keys_for_exp.size());
			for(int i = 0;i < t_keys_for_exp.size();i ++) {
				Key key = t_keys_for_exp.get(i);
				String ucName = "uc_"+t+"_"+i;
				uc_for_table.add(ucName);
				DBUtils.addUnique("tpch", key, t, ucName);
			}
			if(t_keys_for_exp.size() != 0)
				table_uc_map.put(t, uc_for_table);
		}
		
		//add FD in form of triggers
		Map<String,String> table_trigger_map = new HashMap<String,String>();//key=table name, value=trigger id
		Iterator<String> iter_trigger = table_schema_map.keySet().iterator();
		while(iter_trigger.hasNext()) {
			String t = iter_trigger.next();
			Schema3NF schema = table_schema_map.get(t);
			List<FD> t_fds_for_exp = schema.getFd_set();
			if(t_fds_for_exp.size() != 0) {
				System.out.println("adding fd trigger for table : "+t+" | fd num : "+t_fds_for_exp.size());
				String trigger_id = t + "_trigger";
				DBUtils.addTrigger4TPCH("tpch", t_fds_for_exp, t, trigger_id, new Key(schema.getAttr_set()));
				table_trigger_map.put(t, trigger_id);
			}
		}
		
		
		//stat
		List<Double> result_list = new ArrayList<Double>();
		
		//query experiment
		for(String q_name : query_names) {
			String sql = query_sqls_map.get(q_name);//getting query sql with query name,we start from q1 to q22
			double q_time = query4TPCH("tpch",q_name,sql);
			result_list.add(q_time);
		}
		
		//refresh function experiment
		for(int second_loop_t : second_loop_time) {
			//first loop times : first_loop_time; second loop times : second_loop_t
			//then we generate data for orders table : first_loop_time rows
			//we generate data for lineitem table : first_loop_time * second_loop_t
			List<List<String>> orders_data = gen_inserted_dataset_for_orders(first_loop_time,9,attr_type_map.get("orders"));
			List<List<String>> lineitem_data = gen_inserted_dataset_for_lineitem(first_loop_time * second_loop_t,16,attr_type_map.get("lineitem"));
			double rf_time = refreshFunctionV2(first_loop_time, second_loop_t,orders_data,lineitem_data);
			result_list.add(rf_time);
			//delete all inserted records
			DBUtils.deleteData("tpch","orders"," `o_orderkey` > 6000000");
			DBUtils.deleteData("tpch","lineitem"," `l_orderkey` > 6000000");
		}
		
		
		//insertion experiment
		//insert tuples for each table
		List<Double> insert_time = new ArrayList<Double>();
		for(int insertion_row : insert_tuples) {
			double sum = 0;
			for(int i = 0;i < repeat;i ++) {
				List<List<String>> orders_data = gen_inserted_dataset_for_orders(insertion_row,9,attr_type_map.get("orders"));
				List<List<String>> lineitem_data = gen_inserted_dataset_for_lineitem(insertion_row,16,attr_type_map.get("lineitem"));
				List<List<String>> customer_data = gen_inserted_dataset_for_customer(insertion_row,8,attr_type_map.get("customer"));
				List<List<String>> nation_data = gen_inserted_dataset_for_nation(insertion_row,4,attr_type_map.get("nation"));
				List<List<String>> part_data = gen_inserted_dataset_for_part(insertion_row,9,attr_type_map.get("part"));
				List<List<String>> partsupp_data = gen_inserted_dataset_for_partsupp(insertion_row,5,attr_type_map.get("partsupp"));
				List<List<String>> region_data = gen_inserted_dataset_for_region(insertion_row,3,attr_type_map.get("region"));
				List<List<String>> supplier_data = gen_inserted_dataset_for_supplier(insertion_row,7,attr_type_map.get("supplier"));
				Map<String,List<List<String>>> table_data = new HashMap<String,List<List<String>>>();
				table_data.put("orders", orders_data);
				table_data.put("lineitem", lineitem_data);
				table_data.put("customer", customer_data);
				table_data.put("nation", nation_data);
				table_data.put("part", part_data);
				table_data.put("partsupp", partsupp_data);
				table_data.put("region", region_data);
				table_data.put("supplier", supplier_data);
				
				long start = System.currentTimeMillis();
				for(String table : allTables) {
					DBUtils.insertDataWithoutID("tpch", table, table_data.get(table));
				}
				long end = System.currentTimeMillis();
				
				sum += (double)(end-start);
				
				//deleting inserted tuples
				DBUtils.deleteData("tpch", "orders"," `o_orderkey` > 6000000");
				DBUtils.deleteData("tpch", "lineitem"," `l_orderkey` > 6000000");
				DBUtils.deleteData("tpch", "customer"," `c_custkey` > 6000000");
				DBUtils.deleteData("tpch", "nation"," `n_nationkey` > 6000000");
				DBUtils.deleteData("tpch", "part"," `p_partkey` > 6000000");
				DBUtils.deleteData("tpch", "partsupp"," `ps_availqty` > 6000000");
				DBUtils.deleteData("tpch", "region"," `r_regionkey` > 6000000");
				DBUtils.deleteData("tpch", "supplier"," `s_suppkey` > 6000000");
			}
			insert_time.add(sum/repeat);
			
		}
		
		//delete all unique constraints
		System.out.println("deleting uniques ...");
		Iterator<String> iter_table = table_uc_map.keySet().iterator();
		while(iter_table.hasNext()) {
			String t = iter_table.next();//table
			List<String> uc_for_t = table_uc_map.get(t);
			for(String uc_name : uc_for_t) {
				DBUtils.removeUnique("tpch", t, uc_name);
			}
		}
		System.out.println("--------------------------");
		
		//delete all triggers
		System.out.println("deleting triggers ...");
		Iterator<String> iter_table2 = table_trigger_map.keySet().iterator();
		while(iter_table2.hasNext()) {
			String table = iter_table2.next();
			DBUtils.removeTrigger("tpch", table, table_trigger_map.get(table));
		}
		System.out.println("--------------------------");
		
		
		for(int i = 0;i < insert_time.size();i ++) {
			result_list.add(insert_time.get(i));
		}
		
		System.out.println("##############");
		return result_list;
	}
	
	/**
	 * this function generates keys for each table.
	 * it accumulates keys for each table based on last key set by append one different key if possible.
	 * e.g. key number = 2, we have <k1, k2>, while key number = 3 we have <k1, k2, k4>
	 * And each key's size is not more than a specific number
	 * 
	 * each key not being guaranteed in same key size
	 * @param allTables
	 * @param table_key_map
	 * @param key_num_list
	 * @param each_key_size_list
	 * @return
	 */
	public static Map<String,Map<Integer, Map<Integer, List<Key>>>> generateKeysMap(List<String> allTables, Map<String,List<Key>> table_key_map, List<Integer> key_num_list, List<Integer> each_key_size_list){
		Map<String,Map<Integer, Map<Integer, List<Key>>>> selected_table_key_map = new HashMap<>();
		for(String table : allTables) {//append one key if available
			List<Key> all_table_keys = table_key_map.get(table);
			all_table_keys.sort(new Comparator<Key>() {
				@Override
				public int compare(Key o1, Key o2) {//decreasing order
					return o2.size() - o1.size();
				}
			});
			Map<Integer, Map<Integer, List<Key>>> keynum_keysize_map = null;//map that stores info with a specific table
			if(selected_table_key_map.containsKey(table)) {
				keynum_keysize_map = selected_table_key_map.get(table);
			}else {
				keynum_keysize_map = new HashMap<>();
				selected_table_key_map.put(table, keynum_keysize_map);
			}
			for(int keyNum : key_num_list) {
				Map<Integer, List<Key>> keysize_map = null;
				if(keynum_keysize_map.containsKey(keyNum)) {
					keysize_map = keynum_keysize_map.get(keyNum);//info of the specific number of keys of a specific table
				}else {
					keysize_map = new HashMap<>();
					keynum_keysize_map.put(keyNum, keysize_map);
				}
				for(int maxEachKeySize : each_key_size_list) {//each key of specific number of keys has key size not more than specific number
					List<Key> cand_keys = new ArrayList<>();
					for(Key k : all_table_keys) {
						if(k.size() <= maxEachKeySize && cand_keys.size() < keyNum) {
							cand_keys.add(k);
						}
					}
					keysize_map.put(maxEachKeySize, cand_keys);
				}
			}
		}
		return selected_table_key_map;
	}
	
	/**
	 * this function generates keys for each table.
	 * it accumulates keys for each table based on last key set by append one different key if possible.
	 * e.g. key number = 2, we have <k1, k2>, while key number = 3 we have <k1, k2, k4>
	 * And each key's size is not more than a specific number
	 * 
	 * each key being guaranteed in same key size of specific key size environment
	 * @param allTables
	 * @param table_key_map
	 * @param key_num_list
	 * @param each_key_size_list
	 * @return
	 */
	public static Map<String,Map<Integer, Map<Integer, List<Key>>>> generateGuaranteedKeysMap(List<String> allTables, Map<String,List<Key>> table_key_map, List<Integer> key_num_list, List<Integer> each_key_size_list){
		Map<String,Map<Integer, Map<Integer, List<Key>>>> selected_table_key_map = new HashMap<>();
		for(String table : allTables) {
			List<Key> all_table_keys = table_key_map.get(table);
			Map<Integer, List<Key>> table_keysize_keys_dict = new HashMap<>();//key=key size, value=keys with same key size
			for(Key k : all_table_keys) {
				int keySize = k.size();
				if(table_keysize_keys_dict.containsKey(keySize)) {
					table_keysize_keys_dict.get(keySize).add(k);
				}else {
					List<Key> l = new ArrayList<>();
					l.add(k);
					table_keysize_keys_dict.put(keySize, l);
				}
			}
			List<String> table_attrs = get_table_attr_map().get(table);//current table's schemata
			
			Map<Integer, Map<Integer, List<Key>>> keynum_keysize_map = null;//map that stores info with a specific table
			if(selected_table_key_map.containsKey(table)) {
				keynum_keysize_map = selected_table_key_map.get(table);
			}else {
				keynum_keysize_map = new HashMap<>();
				selected_table_key_map.put(table, keynum_keysize_map);
			}
			for(int keyNum : key_num_list) {
				Map<Integer, List<Key>> keysize_map = null;
				if(keynum_keysize_map.containsKey(keyNum)) {
					keysize_map = keynum_keysize_map.get(keyNum);//info of the specific number of keys of a specific table
				}else {
					keysize_map = new HashMap<>();
					keynum_keysize_map.put(keyNum, keysize_map);
				}
				for(int maxEachKeySize : each_key_size_list) {//each key of specific number of keys has key size not more than specific number
					Random rand = new Random();
					if(keysize_map.containsKey(maxEachKeySize - 1)){//if current key number and previous key size in map
						List<Key> previous_keys = keynum_keysize_map.get(keyNum).get(maxEachKeySize - 1);
						List<Key> current_keys = new ArrayList<>();
						for(Key k : previous_keys) {//add previous keys with same key number and for each key to add one attribute
							List<String> remainingAttrs = new ArrayList<>();//remaining attributes that is not in key
							for(String a : table_attrs) {
								if(!k.getAttributes().contains(a))
									remainingAttrs.add(a);
							}
							if(remainingAttrs.size() == 0) {
								List<String> k_list = new ArrayList<>(k.getAttributes());
								current_keys.add(new Key(k_list));
								continue;
							}
							String attr_to_append = remainingAttrs.get(rand.nextInt(remainingAttrs.size()));//randomly select one attribute to  append
							List<String> k_list = new ArrayList<>(k.getAttributes());
							k_list.add(attr_to_append);
							current_keys.add(new Key(k_list));
						}
						int key_num_gap = keyNum - current_keys.size();//add remaining keys to satisfy key number
						for(int i = 0;i < key_num_gap;i ++) {
							finish:
							for(int j = maxEachKeySize; j >= 1;j --) {
								List<Key> keys_with_size = table_keysize_keys_dict.get(j);
								if(keys_with_size != null) {
									for(Key k1 : keys_with_size) {//keys with size
										List<String> new_k_list = new ArrayList<>(k1.getAttributes());
										int size_gap = maxEachKeySize - k1.size();//need to size_gap attribute to satisfy the specific key size
										for(int x = 0;x < size_gap;x ++) {
											for(String a : table_attrs) {
												if(!new_k_list.contains(a)) {
													new_k_list.add(a);//append a new attribute
													break;
												}
											}
										}
										Key cand_key = new Key(new_k_list);
										if(!current_keys.contains(cand_key)) {
											current_keys.add(cand_key);
											break finish;
										}
										
									}
								}
							}
						}
						keysize_map.put(maxEachKeySize, current_keys);//add new key set with specific key number and each key size
					}else {
						if(keynum_keysize_map.containsKey(keyNum - 1)) {
							List<Key> previous_keys = keynum_keysize_map.get(keyNum - 1).get(maxEachKeySize);//
							List<Key> current_keys = new ArrayList<>();
							if(previous_keys != null) {
								for(Key k : previous_keys) {
									current_keys.add(new Key(new ArrayList<>(k.getAttributes())));
								}
							}
							int key_num_gap = keyNum - current_keys.size();
							for(int n = 0; n < key_num_gap; n ++) {
								//add addition new key with specific key size
								finish:
								for(int i = maxEachKeySize;i >= 1;i --){
									if(table_keysize_keys_dict.containsKey(i)) {//find a key with same size
										for(Key k1 : table_keysize_keys_dict.get(i)) {//keys with size i
											List<String> new_k_list = new ArrayList<>(k1.getAttributes());
											int size_gap = maxEachKeySize - k1.size();//need to size_gap attribute to satisfy the specific key size
											for(int j = 0;j < size_gap;j ++) {
												for(String a : table_attrs) {
													if(!new_k_list.contains(a)) {
														new_k_list.add(a);//append a new attribute
														break;
													}
												}
											}
											Key cand_key = new Key(new_k_list);
											if(!current_keys.contains(cand_key)) {
												current_keys.add(cand_key);
												break finish;
											}
											
										}
									}
								}
							}
							if(!current_keys.isEmpty())
								keysize_map.put(maxEachKeySize, current_keys);//add new key set with specific key number and each key size
						}else {//no keys with keyNum-1 and maxEachKeySize-1,i.e key num = 1,key size = 1
							List<Key> current_keys = new ArrayList<>();
							List<Key> keys_with_size = table_keysize_keys_dict.get(maxEachKeySize);
							if(keys_with_size == null)
								continue;
							Key k = keys_with_size.get(rand.nextInt(keys_with_size.size()));
							current_keys.add(new Key(new ArrayList<>(k.getAttributes())));
							keysize_map.put(maxEachKeySize, current_keys);//add new key set with specific key number and each key size
						}
					}
				}
			}
		}
		return selected_table_key_map;
	}
	
	/**
	 * this function generates fds for each table.
	 * it accumulates fds for each table based on last fd set by append one different fd if possible.
	 * e.g. fd number = 2, we have <f1, f2>, while fd number = 3 we have <f1, f2, f4>
	 * @param allTables
	 * @param table_fd_map
	 * @param maxFDNum
	 * @return
	 */
	public static Map<String, Map<Integer, List<FD>>> generateFDsMap(List<String> allTables, Map<String,List<FD>> table_fd_map, int maxFDNum){
		Map<String,Map<Integer, List<FD>>> selected_table_fd_map = new HashMap<>();
		Random rand = new Random();
		for(int fdNum = 1;fdNum <= maxFDNum;fdNum ++) {
			for(String table : allTables) {//append one key if available
				List<FD> remainingFDs = table_fd_map.get(table);
				if(selected_table_fd_map.containsKey(table)) {
					Map<Integer, List<FD>> fd_map = selected_table_fd_map.get(table);//key = fd number, value = fds which number not more than the number
					List<FD> lastFDs = fd_map.get(fdNum - 1);//based on last fds, we append one fd if available
					List<FD> currentFDs = new ArrayList<>(lastFDs);
					if(!remainingFDs.isEmpty()) {//having at lease one key to append
						FD f = remainingFDs.remove(rand.nextInt(remainingFDs.size()));
						currentFDs.add(f);
					}
					fd_map.put(fdNum, currentFDs);
				}else {
					Map<Integer, List<FD>> fd_map = new HashMap<>();//key = fd number, value = fds which number not more than the number
					List<FD> currentFDs = new ArrayList<>();
					if(!remainingFDs.isEmpty()) {//having at lease one fd to append
						FD f = remainingFDs.remove(rand.nextInt(remainingFDs.size()));
						currentFDs.add(f);
					}
					fd_map.put(fdNum, currentFDs);
					selected_table_fd_map.put(table, fd_map);
				}
			}
		}
		return selected_table_fd_map;
	}
	
	public static Schema3NF read3NFSchema(String dataset, String file_name, String root) throws IOException{
		FileReader fr = new FileReader(root + "\\"+file_name+".txt");
		BufferedReader br = new BufferedReader(fr);
		String line;
		List<Key> keys = new ArrayList<>();
		List<FD> fds = new ArrayList<>();
		while((line = br.readLine()) != null) {
			if(line.contains("->")){//fd
				String[] a = line.split("->");
				String[] left = a[0].split(",");
				String[] right = a[1].split(",");
				List<String> lhs = new ArrayList<>(Arrays.asList(left));
				List<String> rhs = new ArrayList<>(Arrays.asList(right));
				FD fd = new FD(lhs, rhs);
				fds.add(fd);
			}
			if(line.contains("[")) {//key
				String[] a = line.replace("[", "").replace("]", "").replace(" ", "").split(",");
				Key k = new Key(Arrays.asList(a));
				keys.add(k);
			}
		}
		br.close();
		fr.close();
		if(keys.isEmpty())
			return new Schema3NF(get_table_attr_map().get(dataset), fds);
		if(fds.isEmpty()) {
			Schema3NF s = new Schema3NF();
			s.setAttr_set(get_table_attr_map().get(dataset));
			s.setMin_key_list(keys);
			return s;
		}
		return new Schema3NF(get_table_attr_map().get(dataset), fds, keys);
	}
	
	/**
	 * 
	 * @param outputPath
	 * @param repeat
	 * @param insert_tuples
	 * @param attr_type_map
	 * @param mostKeyNum
	 * @param mostFDNum
	 * @param allTables
	 * @param table_key_map
	 * @param table_fd_map
	 * @throws SQLException
	 * @throws IOException 
	 */
	public static void runExps(String outputPathSchema,String outputPath, int repeat, List<Integer> insert_tuples,Map<String,List<String>> attr_type_map,List<String> fd_cover_list,List<String> fd_percentage_list, 
			List<String> allTables, int first_loop_time, List<Integer> second_loop_time, Map<String, String> query_sqls_map,List<String> query_names) throws SQLException, IOException{
		//generate keys and fds for experiments
		String root = "C:\\Users\\zzha969\\OneDrive - The University of Auckland\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\Exp Results\\vldb revision exps\\TPCH\\TPCH-fd cover";
		for(String percentage : fd_percentage_list) {//25%, 50%, 75%, 100% FDs
			for(String cover_type : fd_cover_list) {//optimal cover/optimal mixed cover
				Map<String,Schema3NF> table_map = new HashMap<>();
				for(String t : allTables) {
					Schema3NF schema = read3NFSchema(t, t+"-"+cover_type+"-"+percentage, root);
					table_map.put(t, schema);
					/**
					 * print number/size of FDs/Keys
					 */
//					System.out.println(percentage+","+cover_type+","+t+","+schema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(schema.getMin_key_list())+","
//							+schema.getFd_set().size()+","+Utils.compFDAttrSymbNum(schema.getFd_set()));
				}
				List<Double> res = runSingleExp(outputPathSchema,outputPath,repeat,insert_tuples,attr_type_map,allTables,table_map,first_loop_time,second_loop_time,query_sqls_map,query_names);
				System.out.println("******************\n");
				String result = "";
				result += percentage + "," + cover_type;
				for(Double avg : res) {
					result += "," + avg;
				}
				Utils.writeContent(Arrays.asList(result), outputPath, true);//output results
				
				System.out.println("\npercentage = " + percentage + " | cover type = " + cover_type + " | avg stats : "+res.toString());
				System.out.println("==========================");
			}
		}
		
		
	}
	
	public static List<FD> readTPCHFDsFromTxt(String path) throws IOException{
		List<FD> fds = new ArrayList<FD>();
		Map<Set<String>, Set<String>> map = new HashMap<>();//key = left, value = right
		FileReader fr = new FileReader(path);
		BufferedReader br = new BufferedReader(fr);
		Map<String,String> id_attr_map = new HashMap<>(); 
		String line;
		boolean filtered = false;//filter first line with whitespace
		while((line = br.readLine()) != null) {
			if(line.contains("#"))
				continue;
			if(!filtered && line.contains("	")) {
				filtered = true;
				continue;
			}
			if(filtered && line.contains("	")) {
				String[] strs = line.split("	");
				String attr = strs[0].split("\\.")[1];
				String id = strs[1];
				id_attr_map.put(id, attr);
				continue;
			}
			if(line.contains("->")) {
				String[] strs = line.split("\\->");
				String[] left = strs[0].split(",");
				String[] right = strs[1].split(",");
				Set<String> LHS = new HashSet<String>();
				for(String a : left) {
					LHS.add(id_attr_map.get(a));
				}
				Set<String> RHS = new HashSet<String>();
				for(String a : right) {
					RHS.add(id_attr_map.get(a));
				}
//				fds.add(new FD(LHS, RHS));
				if(map.containsKey(LHS)) {
					Set<String> r = map.get(LHS);
					r.addAll(RHS);
				}else {
					map.put(LHS, RHS);
				}
			}
		}
		
		for(Map.Entry<Set<String>, Set<String>> entry : map.entrySet()){
			Set<String> L = entry.getKey();
			Set<String> R = entry.getValue();
			fds.add(new FD(new ArrayList<>(L), new ArrayList<>(R)));
		}
		br.close();
		fr.close();
		return fds;
	}
	
	public static Map<String, Schema3NF> readSchemaFromTxt(String path) throws IOException{
		FileReader fr = new FileReader(path);
		BufferedReader br = new BufferedReader(fr);
		String line;
		Map<String, Schema3NF> schemaMap = new HashMap<String, Schema3NF>();
		String currentTable = null;
		List<Key> currentKeys = null;
		List<FD> currentFDs = null;
		while((line = br.readLine()) != null) {
			if(line.contains(":") && !line.contains("Keys:") && !line.contains("FDs:")) {
				if(currentTable != null) {
					Schema3NF schema = new Schema3NF(get_table_attr_map().get(currentTable),currentFDs, currentKeys);
					schemaMap.put(currentTable, schema);
				}
				currentKeys = new ArrayList<>();
				currentFDs = new ArrayList<>();
				currentTable = line.replace(":", "");
			}
			if(line.contains("[") && !line.contains("->")) {//key
				line = line.replace("[", "").replace("]", "").replace(" ", "");
				String[] attr = line.split(",");
				currentKeys.add(new Key(Arrays.asList(attr)));
			}
			if(line.contains("[") && line.contains("->")) {//FD
				String[] f = line.split("->");
				String[] left = f[0].replace("[", "").replace("]", "").replace(" ", "").split(",");
				String[] right = f[1].replace("[", "").replace("]", "").replace(" ", "").split(",");
				currentFDs.add(new FD(Arrays.asList(left), Arrays.asList(right)));
			}
		}
		if(currentTable != null) {
			Schema3NF schema = new Schema3NF(get_table_attr_map().get(currentTable),currentFDs, currentKeys);
			schemaMap.put(currentTable, schema);
		}
		br.close();
		fr.close();
		return schemaMap;
	}
	
	
	public static Map<String,List<String>> get_attr_type_map() {
		List<String> customer_attr_type = Arrays.asList("int","string","int","string","string","string","int","string");
		List<String> orders_attr_type = Arrays.asList("date","int","int","string","int","string","string","int","string");
		List<String> lineitem_attr_type = Arrays.asList("date","int","int","int","int","int","string","int","string","int","date","date","string","int","string","string");
		List<String> nation_attr_type = Arrays.asList("int","string","int","string");
		List<String> part_attr_type = Arrays.asList("int","string","int","string","string","string","string","int","string");
		List<String> partsupp_attr_type = Arrays.asList("int","int","int","int","string");
		List<String> region_attr_type = Arrays.asList("int","string","string");
		List<String> supplier_attr_type = Arrays.asList("int","int","string","string","string","string","int");
		
		Map<String,List<String>> attr_type_map = new HashMap<String,List<String>>();
		attr_type_map.put("customer", customer_attr_type);
		attr_type_map.put("orders", orders_attr_type);
		attr_type_map.put("lineitem", lineitem_attr_type);
		attr_type_map.put("nation", nation_attr_type);
		attr_type_map.put("part", part_attr_type);
		attr_type_map.put("partsupp", partsupp_attr_type);
		attr_type_map.put("region", region_attr_type);
		attr_type_map.put("supplier", supplier_attr_type);
		
		return attr_type_map;
	}
	
	public static Map<String,List<String>> get_table_attr_map() {
		List<String> customer_attr = Arrays.asList("c_custkey","c_mktsegment","c_nationkey","c_name","c_address","c_phone","c_acctbal","c_comment");
		List<String> orders_attr = Arrays.asList("o_orderdate","o_orderkey","o_custkey","o_orderpriority","o_shippriority","o_clerk","o_orderstatus","o_totalprice","o_comment");
		List<String> lineitem_attr = Arrays.asList("l_shipdate","l_orderkey","l_discount","l_extendedprice","l_suppkey","l_quantity","l_returnflag","l_partkey","l_linestatus","l_tax","l_commitdate","l_receiptdate","l_shipmode","l_linenumber","l_shipinstruct","l_comment");
		List<String> nation_attr = Arrays.asList("n_nationkey","n_name","n_regionkey","n_comment");
		List<String> part_attr = Arrays.asList("p_partkey","p_type","p_size","p_brand","p_name","p_container","p_mfgr","p_retailprice","p_comment");
		List<String> partsupp_attr = Arrays.asList("ps_partkey","ps_suppkey","ps_supplycost","ps_availqty","ps_comment");
		List<String> region_attr = Arrays.asList("r_regionkey","r_name","r_comment");
		List<String> supplier_attr = Arrays.asList("s_suppkey","s_nationkey","s_comment","s_name","s_address","s_phone","s_acctbal");
		
		Map<String,List<String>> attr_map = new HashMap<String,List<String>>();
		attr_map.put("customer", customer_attr);
		attr_map.put("orders", orders_attr);
		attr_map.put("lineitem", lineitem_attr);
		attr_map.put("nation", nation_attr);
		attr_map.put("part", part_attr);
		attr_map.put("partsupp", partsupp_attr);
		attr_map.put("region", region_attr);
		attr_map.put("supplier", supplier_attr);
		
		return attr_map;
	}
	
	
	public static List<Key> randomlySelectKeys(List<Key> totalKeys, int number){
		ArrayList<Key> selectKeys = new ArrayList<>();
		Random rand = new Random();
		if(totalKeys.size() <= number) {
			selectKeys.addAll(totalKeys);
		}else {
			selectKeys.addAll(totalKeys);
			for(int i = 0;i < totalKeys.size() - number;i ++){
				selectKeys.remove(rand.nextInt(selectKeys.size()));
			}
		}
		return selectKeys;
	}
	
	/**
	 * from a set of potential keys and fds, select some to synthesis 3NF schema with fixed key number and fd number if possible
	 * @param R
	 * @param finalKeys
	 * @param totalFDs
	 * @param fdNum
	 * @return fds that cannot guarantee to be 3NF
	 */
	public static List<FD> selectFDsFor3NF(List<String> R, List<Key> finalKeys, List<FD> totalFDs, int fdNum) {
//		List<Key> candKeys = randomlySelectKeys(totalKeys, keyNum);
		Set<String> primeAttrs = Utils.getPrimeAttrs(finalKeys);
		Set<String> nonPrimeAttrs = new HashSet<>(R);
		nonPrimeAttrs.removeAll(primeAttrs);
		
		//在所有fds中 找到lhs全是主属性，如果找不到指定数量的fd， 
		//将一个非主属性任意添加到一个key，重复上述步骤，如果最后非主属性集合为空，仍然找不到，则返回当前schema
		List<FD> candFDs = new ArrayList<>();
		for(FD fd : totalFDs) {
			if(primeAttrs.containsAll(fd.getLeftHand())) {
				candFDs.add(fd);
			}
		}
		
		List<FD> finalFDs = new ArrayList<FD>();
		//randomly select fds from candidate FDs
		Random rand = new Random();
		if(candFDs.size() <= fdNum) {
			return candFDs;
		}else {
			for(int i = 0;i < fdNum;i ++) {
				finalFDs.add(candFDs.remove(rand.nextInt(candFDs.size())));
			}
		}
		
		return finalFDs;
	}
	
	public static String getFDsString(List<FD> fds) {
		String str = "FDs:\n";
		for(FD fd : fds) {
			str += fd.toString()+"\n";
		}
		return str;
	}
	
	public static String getKeysString(List<Key> keys) {
		String str = "Keys:\n";
		for(Key key : keys) {
			str += key.toString()+"\n";
		}
		return str;
	}
	
	
	public static void main(String[] args) throws IOException, SQLException {
		List<String> fd_percentage_list = Arrays.asList("25%", "50%", "75%", "100%");
		List<String> fd_cover_list = Arrays.asList("optimal", "optimalmixed");
		
		List<String> allTables = Arrays.asList("orders","customer","lineitem","nation","part","partsupp","region","supplier");
		int repeat = 3;//repeat for insert
		List<Integer> insert_tuples = Arrays.asList(60,120,180);//insertion exp
		
		int first_loop_time = (int) (0.01*1500);//SF = 0.01, RF exp
		List<Integer> second_loop_time = Arrays.asList(1,2,3,4,5,6,7);
		
		Map<String,String> query_sqls_map = new HashMap<String,String>();//query exp
		String root = "C:\\Users\\zzha969\\OneDrive - The University of Auckland\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\Exp Results\\vldb revision exps";
		String sql_path = root + "\\TPCH\\tpch-query-sql";
		get_query_sqls_map(sql_path,query_sqls_map);
		List<String> query_names = new ArrayList<String>();
		for(int i = 1;i <= 22;i ++) {
			query_names.add("q"+i);
		}
		
		Map<String,List<String>> attr_type_map = get_attr_type_map();
		String outputPathSchema = root + "\\TPCH_schemata.txt";
		String output = root + "\\TPCH_results.csv";
		
		//start experiment
		TPCHWorkloadExp.runExps(outputPathSchema,output,repeat,insert_tuples,attr_type_map,fd_cover_list,fd_percentage_list,allTables,first_loop_time,second_loop_time,query_sqls_map,query_names);
		
	}


}
