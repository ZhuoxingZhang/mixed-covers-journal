package exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;
import util.Utils;

/**
 * In this experiment, we investigate the influence (time required) of different FD covers as input on computing minimal keys.
 * The algorithm to compute minimal keys is a P-time algorithm, proposed by Osborne.
 *
 */
public class exp1 {
	/**
	 * if the key is non-redundant, refine the key to minimal key and add it into minimal key set
	 * @param R
	 * @param Sigma
	 * @param minimalKeys
	 * @param key not minimal key
	 * @return true if key is redundant, false otherwise
	 */
	public static boolean getNonRedundantKeys(List<String> R, List<FD> Sigma, List<Key> minimalKeys,Key key) {
		boolean redundantKey = false;
		for(Key minimalKey : minimalKeys) {
			if(key.contains(minimalKey)) {
				redundantKey = true;
				break;
			}
		}
		if(!redundantKey) {
			Key minKey = Utils.getRefinedMinKey(Sigma, key, R);
			if(!minimalKeys.contains(minKey))
				minimalKeys.add(minKey);
		}
			
		return redundantKey;
	}
	/**
	 * through a minimal key, get all minimal keys in PTIME
	 * @param R schema
	 * @param Sigma FD set
	 * @param firstMinKey
	 * @return all minimal keys(index 0) and times of computing minimal key count(index 1)
	 */
	public static  List<Object> getMinimalKeys(List<String> R, List<FD> Sigma,Key firstMinKey) {
		List<Key> minimalKeys = new ArrayList<Key>();
		minimalKeys.add(firstMinKey);
		Key current_key = firstMinKey;
		int count = 1;//count the times of computing minimal keys
		while(current_key != null) {
			for(FD fd : Sigma) {
				List<String> left = fd.getLeftHand();
				List<String> right = fd.getRightHand();
				List<String> S = new ArrayList<String>();//S = left union (current_key - right)
				S.addAll(left);
				for(String a : current_key.getAttributes()) {
					if(!right.contains(a) && !S.contains(a))
						S.add(a);
				}
				boolean redundant = getNonRedundantKeys(R, Sigma, minimalKeys, new Key(S));
				if(!redundant)
					count ++;
			}
			int index = minimalKeys.indexOf(current_key);
			if(index+1 >= minimalKeys.size())
				current_key = null;
			else
				current_key = minimalKeys.get(index+1);
		}
		List<Object> res = new ArrayList<>();
		res.add(minimalKeys);
		res.add(count);
		return res;
	}
	
	
	/**
	 * 
	 * @param schema
	 * @param FDs
	 * @return the cost time in milliseconds(index 0), minimal keys number(index 1), iteration count of computing minimal keys(index 2),FD cover size(index3)
	 */
	public static List<Long> runSingleExp(List<String> schema,List<FD> FDs) {
		long startTime = System.currentTimeMillis();
		Key firstMinKey = Utils.getRefinedMinKey(FDs, new Key(schema), schema);
		List<Object> info = getMinimalKeys(schema, FDs, firstMinKey);
		long endTime = System.currentTimeMillis();
		List<Key> allMinimalKeys = (List<Key>) info.get(0);
		long compMinKeyCount = (int)info.get(1);
		long cost = endTime - startTime;
		List<Long> res = new ArrayList<Long>();
		res.add(cost);
		res.add((long)allMinimalKeys.size());
		res.add(compMinKeyCount);
		res.add((long)FDs.size());
		return res;
	}
	
	/**
	 * before each repeat experiment, we will shuffle FD sets.
	 * @param coverType "nonredundant"/"reduced"/"canonical"/"minimal"/"reduced minimal"/"optimal"/original FD cover by default
	 * @param repeat repeat time of experiments
	 * @param para
	 * @param outputPath
	 */
	public static void runExps(String coverType,int repeat,Parameter para,String outputPath) {
		List<Object> schemaInfo = Utils.getFDCover(coverType, para);
		if(schemaInfo == null) {
			System.out.println("dataset : "+para.dataset.name+" | FD cover type : "+coverType+" | FD cover size : null | Key num : null | comp. min key count : null | ave cost : null");
			Utils.writeContent(Arrays.asList(para.dataset.name+","+coverType+",null,null,null,null"), outputPath, true);
			return;
		}
		List<String> schema = (List<String>) schemaInfo.get(0);
		List<FD> FDs = (List<FD>) schemaInfo.get(1);
		
		List<Long> info = null;
		long cost = 0l;
		long keyNum = 0;
		long compMinKeyCount = 0;
		long fdCoverSize = 0;
		for(int i = 0;i < repeat;i ++) {
			Collections.shuffle(FDs);//shuffle the FD set
			info = runSingleExp(schema,FDs);
			cost += (long)info.get(0);
			keyNum += (long)info.get(1);
			compMinKeyCount += (long)info.get(2);
			fdCoverSize += (long)info.get(3);
		}
		System.out.println("dataset : "+para.dataset.name+" | FD cover type : "+coverType+" | FD cover size : "+fdCoverSize/(double)repeat+" | Key num : "+keyNum/(double)repeat+" | comp. min key count : "+compMinKeyCount/(double)repeat+" | ave cost : "+cost/(double)repeat);
		Utils.writeContent(Arrays.asList(para.dataset.name+","+coverType+","+fdCoverSize/(double)repeat+","+keyNum/(double)repeat+","+compMinKeyCount/(double)repeat+","+cost/(double)repeat), outputPath, true);
	}
	
	
	public static void main(String[] args) {
		int repeat = 1;
		for(Parameter para : Utils.getParameterList(null, DataTypeEnum.COMPLETE)) {
			for(String coverType : para.FDCoverTypeList) {
				exp1.runExps(coverType, repeat, para, para.output_add);
			}
		}
	}

}
