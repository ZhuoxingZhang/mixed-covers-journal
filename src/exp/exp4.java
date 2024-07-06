package exp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;
import util.DBUtils;
import util.Utils;


/**
 * In this experiment, we investigate the performance of different FD covers of non-normalized schemas,
 * on update tests.
 */
public class exp4 {
	
	/**
	 * run a single experiment on  insertion
	 * @param schema
	 * @param para
	 * @param FDCoverType
	 * @param repeat
	 * @param keys
	 * @param FDs
	 * @param tableName
	 * @param insert_row_num_list
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<Double> runSingleExp(Parameter para, String FDCoverType, int repeat, Schema schema, String tableName, List<Integer> insert_row_num_list) throws SQLException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		List<String> uniqueIDs = new ArrayList<>();
		String triggerID = "tri_"+tableName;
		
		//add unique constraints
		System.out.println("add trigger and unique constraints...");
		int uc_id = 0;
		for(Key k : schema.getMin_key_list()) {
			if(uc_id > 62)//64 keys at most,including primary key
				break;
			String uc_name = "uc_"+tableName+"_"+uc_id ++;
			uniqueIDs.add(uc_name);
			DBUtils.addUnique(k,tableName,uc_name);
		}
		
		//add FD triggers if exists
		if(!schema.getFd_set().isEmpty()) {
			DBUtils.addTrigger(schema.getFd_set(),tableName,triggerID);
		}
		
		//update experiment
		for(int row_num : insert_row_num_list) {
			List<Double> cost_list = new ArrayList<Double>();
			List<List<String>> syntheData = Utils.synthesizeDataset(row_num,para.dataset.col_num);
			for(int i = 0;i < repeat;i ++) {
				double cost = DBUtils.insertData(tableName,syntheData);
				cost_list.add(cost);
				DBUtils.deleteData(tableName,"`id` > "+para.dataset.row_num);
			}
			result.add(Utils.getAve(cost_list));
			result.add(Utils.getMedian(cost_list));
		}
		
		//remove trigger and unique constraints
		System.out.println("remove trigger and unique constraints...");
		for(String uid : uniqueIDs) {
			DBUtils.removeUnique(tableName, uid);
		}
		DBUtils.removeTrigger(tableName, triggerID);
		
		//output
		String stat = "";
		for(double a : result) {
			stat += ","+a;
		}
		String res = para.dataset.name+","+para.dataset.DataType.toString()+","+FDCoverType+","
				+schema.getMin_key_list().size()+","+Utils.compKeyAttrSymbNum(schema.getMin_key_list())+","+schema.getFd_set().size()+","+Utils.compFDAttrSymbNum(schema.getFd_set())+stat;
		Utils.writeContent(Arrays.asList(res), para.output_add, true);
		
		System.out.println(res.replace(",", " | "));
		System.out.println("###############################\n");
		
		return result;
	}
	
	public static void runExps(Parameter para, int repeat, List<Integer> insert_row_num_list) {
		String tableName = DBUtils.getDBTableName(para);
		List<String> FDCoverTypeList = Arrays.asList("original","nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
		List<String> KeyFDCoverTypeList = Arrays.asList("original keyfd","nonredundant keyfd","reduced keyfd","canonical keyfd","minimal keyfd","reduced minimal keyfd","optimal keyfd");
		List<Key> keys = new ArrayList<>();
		List<FD> FDCover = null;
		List<String> R = null;
		
		//FD cover performance tests
		for(String covertype : FDCoverTypeList) {
			System.out.println("table name : "+tableName +" | cover type : "+covertype);
			List<Object> fdInfo = Utils.getFDCover(covertype, para);
			if(fdInfo == null)
				continue;
			if(R == null)
				R = (List<String>) fdInfo.get(0);
			FDCover = (List<FD>) fdInfo.get(1);
					
			try {
				exp4.runSingleExp(para, covertype, repeat,new Schema(R,FDCover,keys), tableName, insert_row_num_list);
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			}
		}
		
		//keyfd cover performance tests
		for(String covertype : KeyFDCoverTypeList) {
			System.out.println("table name : "+tableName +" | cover type : "+covertype);
			List<Object> keyfdInfo = Utils.getKeyFDCover(covertype, para, false);
			if(keyfdInfo == null)
				continue;
			if(R == null) {
				R = (List<String>) keyfdInfo.get(0);
			}
			if(keys.isEmpty()) {
				keys = (List<Key>) keyfdInfo.get(1);
			}
			FDCover = (List<FD>) keyfdInfo.get(2);
			
			try {
				exp4.runSingleExp(para, covertype, repeat, new Schema(R,FDCover,keys), tableName, insert_row_num_list);
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			}
		}
		
	}
	
	public static void main(String[] args) {
		int repeat = 1;
		List<Integer> insert_row_num_list = Arrays.asList(100,200,300,400);
		for(Parameter para : Utils.getParameterListV1(Arrays.asList("routes", "hospital"))) {
			runExps(para, repeat, insert_row_num_list);
		}
	}

}
