package exp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;
import util.DBUtils;
import util.LosslessDecompAlg;
import util.Utils;

/**
 * we study subschema performance on updates, which resulted from sub-schemas of lossless decomposition algorithm
 * for each sub-schema (first 10 at most) in 3NF, we get different FD covers, KeyFD covers from original FDs.
 * Then we have some subschema variants with different equivalent FD set,
 * for each subschema variant, we do update experiments.
 *
 */
public class exp5 {
	
	public static Connection connectDB() {
		String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
	    String DB_URL = "jdbc:mysql://localhost:3306/freeman?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
	    		+ "&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=true";
	    String USER = "root";
	    String PASS = "zzxzzx";
	    Connection conn = null;
        try{
            Class.forName(JDBC_DRIVER);
        
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }
        return conn;

	}
	
//	public static void createTable(String tableName,List<String> R) throws SQLException {
//		Connection conn =connectDB();
//		String sql = "CREATE TABLE `"+tableName+"` (  \n";
//		sql += "`id` int NOT NULL AUTO_INCREMENT,\n";
//		for(int i = 0;i< R.size();i ++) {
//			String columnName = R.get(i);
//			sql += "`"+columnName +"` varchar(256),\n";
//		}
//		sql += "PRIMARY KEY (`id`)\n ) CHARSET=utf8mb3";
//		System.out.println("\n======================");
//		System.out.println("creating table with name : "+tableName+" | schema : "+R.toString());
//		System.out.println(sql);
//		System.out.println("======================\n");
//		Statement stmt = conn.createStatement();
//		stmt.executeUpdate(sql);
//		stmt.close();
//		conn.close();
//	}
	
	/**
	 * 
	 * @param tableName
	 * @param dataset
	 * @return cost time for inserting records(ms)
	 * @throws SQLException
	 */
	public static double insert_data(String tableName,List<List<String>> dataset) throws SQLException {
		if(dataset == null)
			return -1;
		if(dataset.isEmpty())
			return -1;
		Connection conn = connectDB();
		conn.setAutoCommit(false);//manual commit
		String insertSql = "INSERT INTO `"+tableName + "` VALUES ( NULL,";//increment column
		for(int i = 0;i < dataset.get(0).size();i ++) {
			if(i != (dataset.get(0).size() - 1))
				insertSql += " ? ,";
			else
				insertSql += " ? )";
		}
		PreparedStatement prepStmt1 = conn.prepareStatement(insertSql);
		System.out.println("\n==================");
		System.out.println("insert "+dataset.size()+" records into table : "+tableName);
		System.out.println(insertSql);
		
		long start = System.currentTimeMillis();
		int count = 0;
		for(List<String> data : dataset) {
			count ++;
			for(int i = 1;i <= data.size();i ++) {
				prepStmt1.setString(i, data.get(i-1));
			}
			prepStmt1.addBatch();//batch process
			if(count % 10000 == 0) {
				prepStmt1.executeBatch();
				conn.commit();
				prepStmt1.clearBatch();
			}
		}
		prepStmt1.executeBatch();
		conn.commit();//commit
		long end = System.currentTimeMillis();
		System.out.println("==================\n");
		prepStmt1.close();
		conn.close();
		return (double)(end - start);
	}
	
	/**
	 * query 1
	 * SELECT R.A1,...,R.An,count(*) FROM R GROUP BY R.A1,...,R.An HAVING COUNT(*) > 1
	 * @param key
	 * @param tableName
	 * @return query time
	 * @throws SQLException
	 */
	public static double query_one(List<String> key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(String attr : key) {
			sql += "`"+tableName+"`.`"+attr+"`, ";
		}
		sql += "COUNT(*) FROM `"+tableName+"` GROUP BY ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "HAVING COUNT(*) > 1";
		
		System.out.println("\n======================");
		System.out.println("executing query 1...");
		System.out.println(sql);
		
		long start = new Date().getTime();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = new Date().getTime();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("======================\n");
		return (double)(end - start);
	}
	
	/**
	 * query 2
	 * SELECT R.A1,...,R.An FROM R, R AS R1 WHERE R.A1 = R1.A1 AND ... AND R.An = R1.An AND R.id <> R1.id
	 * @param key
	 * @param tableName
	 * @return query time
	 * @throws SQLException
	 */
	public static double query_two(List<String> key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "FROM `"+tableName+"` , `"+tableName+"` AS `"+tableName+"_1` WHERE ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"` AND ";
			else
				sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"`";
		}
		sql += " AND `"+tableName+"`.`id` <> `"+tableName+"_1`.`id`";
		
		System.out.println("\n======================");
		System.out.println("executing query 2...");
		System.out.println(sql);
		
		long start = new Date().getTime();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = new Date().getTime();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("======================\n");
		return (double)(end - start);
	}
	
	/**
	 * delete all data from table with specific condition
	 * @param tableName
	 * @param dataset
	 * @param where_condition according condition to delete tuples
	 * @throws SQLException 
	 */
	public static void delete_data(String tableName,String where_condition) throws SQLException {
		Connection conn = connectDB();
		System.out.println("\n==================");
		System.out.println("delete inserted records from table : "+tableName);
		String delSql = "DELETE FROM `"+tableName + "` WHERE "+where_condition;//delete records

		System.out.println(delSql);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(delSql);
		
		System.out.println("==================\n");
		stmt.close();
		conn.close();
	}
	
	/**
	 * specify a set of attributes (we call key) as an unique constraint on a table
	 * @param key
	 * @param tableName
	 * @throws SQLException
	 */
	public static void add_unique_constraint(List<String> key,String tableName,String unique_id) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		
		String unique_constraint ="ALTER TABLE `"+tableName+"` ADD UNIQUE `"+unique_id+"` ( ";//create an unique constraint
		for(int i =0;i < key.size();i ++) {
			if(i != (key.size() -1))
				unique_constraint += "`"+key.get(i) + "`,";
			else
				unique_constraint += "`"+key.get(i) + "` )";
		}
		
		System.out.println("\n==================");
		System.out.println("create an unique constarint : ");
		System.out.println(unique_constraint);
		
		stmt.executeUpdate(unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	/**
	 * remove an unique constraint on a table
	 * @param tableName
	 * @param unique_id
	 * @throws SQLException
	 */
	public static void del_unique_constraint(String tableName,String unique_id) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String del_unique_constraint = "DROP INDEX `"+unique_id+"` ON `"+tableName+"`";//delete an unique constraint
		
		System.out.println("\n==================");
		System.out.println("delete an unique constarint : ");
		System.out.println(del_unique_constraint);
		
		stmt.executeUpdate(del_unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	
	
	public static List<List<String>> gen_inserted_dataset(int row_num,int col_num){
		List<List<String>> dataset = new ArrayList<List<String>>();
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				data.add(i+"_"+j);
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	
	
	public static List<List<String>> get_projection_on_subschema(List<String> subschema,String OriginTable) throws SQLException{
		List<List<String>> projection = new ArrayList<List<String>>();
		
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
//		List<String> attrs = get_increasing_order_list(subschema);
		
		String str = "";
		for(int i = 0;i < subschema.size();i ++) {
			if(i != subschema.size() - 1)
				str += "`"+subschema.get(i)+"`, ";
			else
				str += "`"+subschema.get(i)+"` ";
		}
		String sql = "SELECT "+str+" FROM `"+OriginTable+"` GROUP BY "+str;
		System.out.println("\n======================");
		System.out.println("get projection on "+subschema.toString()+" from "+OriginTable);
		System.out.println(sql);
		
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			int col_num = rs.getMetaData().getColumnCount();
			List<String> row = new ArrayList<String>();
			for(int i = 1;i <= col_num;i ++) {
				row.add(rs.getString(i));
			}
			projection.add(row);
		}
		System.out.println("projection row num : "+projection.size());
		System.out.println("======================\n");
		return projection;
	}
	
	
	
	
	/**
	 * execute single update experiment on a given schema object that
	 * includes schema R, keys , FDs(original, non-redundant,...,optimal keyfd);
	 * @param count i-th subschema from decomposition of original schema
	 * @param FDCoverType FDs' type like original, non-redundant,...,optimal keyfd
	 * @param para
	 * @param repeat repeat time for update
	 * @param schema 
	 * @param insert_row_num_list
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<Double> runSingleExp(int count,String FDCoverType, Parameter para,int repeat, Schema schema, List<Integer> insert_row_num_list) throws SQLException, IOException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		String OriginTable = DBUtils.getDBTableName(para);
		String ProjTable = OriginTable+"_proj";
		
		List<String> sub_schema = schema.getAttr_set();
		
		//create projection table on database
		DBUtils.createTable(ProjTable,sub_schema);
		
		//get projection table
		
		List<List<String>> proj_table_dataset = get_projection_on_subschema(sub_schema,OriginTable);
		
		//insert projection rows
		insert_data(ProjTable,proj_table_dataset);
		
		//add unique constraints if exists
		List<String> all_uc_id = new ArrayList<String>();//record all unique constraint names
		int uc_id = 0;
		for(Key k : schema.getMin_key_list()) {
			String uc_name = "uc_"+ProjTable+"_"+uc_id ++;
			add_unique_constraint(k.getAttributes(),ProjTable,uc_name);
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
			List<List<String>> inserted_data = gen_inserted_dataset(row_num,proj_table_dataset.get(0).size());
			for(int i = 0;i < repeat;i ++) {
				double cost = insert_data(ProjTable,inserted_data);
				cost_list.add(cost);
				delete_data(ProjTable,"`id` > "+proj_table_dataset.size());
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
		String res = para.dataset.name+","+para.dataset.DataType.toString()+","+count+","+FDCoverType+","
				+schema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(schema.getMin_key_list())
				+","+schema.getFd_set().size()+","+Utils.compFDAttrSymbNum(schema.getFd_set())+","+proj_table_dataset.size()+stat;
		Utils.writeContent(Arrays.asList(res), para.output_add, true);
		
		System.out.println(res.replace(",", " | "));
		System.out.println("###############################\n");
		
		return result;
	}
	
	
	public static void runExps(int maxSubschNum, Parameter para, int repeat, List<Integer> insert_row_num_list) throws SQLException, IOException {
		//load original schema and FDs
		List<Object> schema_info = Utils.readFDs(para.fd_add);
		List<String> R = (List<String>) schema_info.get(0);
		List<FD> FDs = (List<FD>) schema_info.get(1);
		List<FD> atomicCover = new ArrayList<>();
		for(FD fd : FDs) {
			if(fd.getRightHand().size() == 1) {
				if(!atomicCover.contains(fd))
					atomicCover.add(fd);
			}else {
				for(String attr : fd.getRightHand()) {
					FD f = new FD(new ArrayList<String>(fd.getLeftHand()),new ArrayList<String>(Arrays.asList(attr)));
					if(!atomicCover.contains(f))
						atomicCover.add(f);
				}
			}
		}
		System.out.println("data set : "+para.dataset.name);
		System.out.println("load "+atomicCover.size()+" FDs successfully!\n");
		
		//we get sub-schemas in 3NF from input FDs(atomic cover)
		LosslessDecompAlg alg2 = new LosslessDecompAlg();
		List<Schema> subschemas = alg2.decomp_and_output(para, R, atomicCover);
		
		//for each subschema, we get FD set variant based on original,
		//like FD covers, KeyFD covers,
		//then for each type of FD set, we do update experiment
		//10 subschemas at most
		int count = 0;
		for(Schema original : subschemas) {
			if(count >= maxSubschNum)
				break;
			
			List<List<FD>> fdcover_list = new ArrayList<>();
			for(String FDcover : para.FDCoverTypeList) {
				//original, non-redundant cover,....,optimal cover
				List<FD> sigma = Utils.compFDCover(original.getFd_set(), FDcover);
				Schema newSchema = new Schema(original.getAttr_set(),sigma);
				fdcover_list.add(sigma);
				runSingleExp(count, FDcover, para, repeat, newSchema, insert_row_num_list);
				
//				String res = para.dataset.name+","+para.dataset.DataType.toString()+","+count+","+FDcover+","
//						+newSchema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(newSchema.getMin_key_list())
//						+","+newSchema.getFd_set().size()+","+Utils.compFDAttrSymbNum(newSchema.getFd_set());
//				System.out.println(res);
			}
			for(int i = 0;i < fdcover_list.size();i ++) {
				String KeyFDCover = para.KeyFDCoverTypeList.get(i);
				List<FD> fd_cover = fdcover_list.get(i);
				//original keyfd cover, ... , optimal keyfd cover
				List<Object> keyfdcover = Utils.compKeyFDCover(original.getAttr_set(), fd_cover);
				List<Key> sigma_k = (List<Key>) keyfdcover.get(0);
				List<FD> sigma_f = (List<FD>) keyfdcover.get(1);
				Schema newSchema = new Schema(original.getAttr_set(), sigma_f, sigma_k);
				runSingleExp(count, KeyFDCover, para, repeat, newSchema, insert_row_num_list);
				
//				String res = para.dataset.name+","+para.dataset.DataType.toString()+","+count+","+KeyFDCover+","
//						+newSchema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(newSchema.getMin_key_list())
//						+","+newSchema.getFd_set().size()+","+Utils.compFDAttrSymbNum(newSchema.getFd_set());
//				System.out.println(res);
			}
			count ++;
		}
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		int maxSubschemaNum = 5;
		int repeat = 1;
		List<Integer> insert_row_num_list = Arrays.asList(100,200,300,400);
		for(Parameter para : Utils.getParameterListV1(Arrays.asList("routes","hospital"))) {
			runExps(maxSubschemaNum,para, repeat, insert_row_num_list);
		}
	}

}
