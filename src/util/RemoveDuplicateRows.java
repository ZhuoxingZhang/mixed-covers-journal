package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * If a relation has duplicate rows,
 * remove them.
 *
 */
public class RemoveDuplicateRows {
	
	public static void remove(String input, String output) {
		List<String> dataset = Utils.readContent(input);
		Map<String,Integer> count = new HashMap<>();//key = row, value = count
		for(String row : dataset) {
			if(count.containsKey(row)) {
				int c = count.get(row);
				count.put(row, ++c);
			}else
				count.put(row, 1);
		}
		
		List<String> newDS = new ArrayList<>();
		for(Map.Entry<String, Integer> rowCount : count.entrySet()) {
			newDS.add(rowCount.getKey());
		}
		
		Utils.writeContent(newDS, output, true);
	}
	
	public static void main(String[] args) {
		remove("C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\3NF\\dataset\\data\\letter.csv","C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\3NF\\dataset\\letter(non-dup).csv");
	}

}
