package additional;
import java.io.*;
import java.util.*;
import entity.FD;
import entity.Key;
import util.Utils;
public class ComputeCovers4TPCH {
	
	public static List<FD> readFDs(String name, String root) throws IOException{
		FileReader fr = new FileReader(root + "\\"+name+".txt");
		BufferedReader br = new BufferedReader(fr);
		String line;
		List<FD> fds = new ArrayList<>();
		while((line = br.readLine()) != null) {
			String[] a = line.split("->");
			String[] left = a[0].split(",");
			String[] right = a[1].split(",");
			List<String> lhs = new ArrayList<>(Arrays.asList(left));
			List<String> rhs = new ArrayList<>(Arrays.asList(right));
			FD fd = new FD(lhs, rhs);
			fds.add(fd);
				
		}
		br.close();
		fr.close();
		return fds;
	}
	
	public static void writeFDs(String name, String root, List<FD> fds, boolean append) throws IOException {
		FileWriter fw = new FileWriter(root +"\\" + name +".txt", append);
		BufferedWriter bw = new BufferedWriter(fw);
		for(FD fd : fds) {
			String l = fd.getLeftHand().toString().replace("[","").replace("]", "").replace(" ", "");
			String r = fd.getRightHand().toString().replace("[","").replace("]", "").replace(" ", "");
			bw.write(l+"->"+r+"\n");
		}
		bw.close();
		fw.close();
	}
	
	public static void writeAllKeys(String dataset, String root) throws IOException {
		List<FD> fds = readFDs(dataset+"-fds",root);
		FileWriter fw = new FileWriter(root + "\\"+dataset+"-keys-100%.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		List<String> R = TPCHWorkloadExp.get_table_attr_map().get(dataset);
		List<Key> keys = Utils.getMinimalKeys(R, fds);
		for(Key k : keys) {
			bw.write(k.toString()+"\n");
		}
		bw.close();
		fw.close();
	}
	
	public static void writeKeys(String name, String root, List<Key> keys, boolean append) throws IOException {
		FileWriter fw = new FileWriter(root + "\\" + name+".txt", append);
		BufferedWriter bw = new BufferedWriter(fw);
		for(Key k : keys) {
			bw.write(k.toString()+"\n");
		}
		bw.close();
		fw.close();
	}
	
	public static List<Key> readKeys(String name, String root) throws IOException{
		FileReader fr = new FileReader(root + "\\"+name+".txt");
		BufferedReader br = new BufferedReader(fr);
		String line;
		List<Key> keys = new ArrayList<>();
		while((line = br.readLine()) != null) {
			String[] a = line.replace("[", "").replace("]", "").replace(" ", "").split(",");
			Key k = new Key(Arrays.asList(a));
			keys.add(k);
				
		}
		br.close();
		fr.close();
		return keys;
	}
	
	public static boolean checkFD(FD fd, List<Key> keys) {
		for(Key k : keys) {
			int difference = 0;
			Set<String> fd_attrs = new HashSet<>();
			fd_attrs.addAll(fd.getLeftHand());
			fd_attrs.addAll(fd.getRightHand());
			for(String a : fd_attrs) {
				if(!k.getAttributes().contains(a))
					difference ++;
			}
			if(difference == 1) {
				return true;
			}
		}
		return false;
	}
	
	public static void selectedFDs(String dataset, String root) throws IOException {
		for(String name : Arrays.asList("25%", "50%", "75%", "100%")) {
			String kname = dataset + "-keys-" + name;
			List<Key> keys = readKeys(kname, root);
			List<FD> allFDs = readFDs(dataset+"-fds", root);
			List<FD> selected = new ArrayList<>();
			for(FD fd : allFDs) {
				//check fd to satisfy
				boolean ok = checkFD(fd, keys);
				if(ok) {
					selected.add(fd);
				}
			}
			writeFDs(dataset+"-fds-"+name, root, selected, false);
		}
	}
	
	public static void computeOptimal(String dataset, String root) throws IOException {
		for(String p : Arrays.asList("25%", "50%", "75%", "100%")) {
			List<FD> fds = readFDs(dataset+"-fds-"+p, root);
			List<String> R = TPCHWorkloadExp.get_table_attr_map().get(dataset);
			List<FD> optimal = Utils.compFDCover(fds, "optimal");
			writeFDs(dataset+"-optimal-"+p, root+"\\a", optimal, false);
			
			List<Object> info = Utils.comp_Mixed_Cover_Sequential(R, optimal, "optimal");
			List<Key> keys = (List<Key>) info.get(0);
			List<FD> optimal_mixed = (List<FD>) info.get(1);
			writeKeys(dataset+"-optimalmixed-"+p, root+"\\a", keys, false);
			writeFDs(dataset+"-optimalmixed-"+p, root+"\\a", optimal_mixed, true);
		}
	}
	
	public static void main(String[] args) throws IOException {
		/**
		 * compute cover for TPCH
		 */
//		for(String dataset : Arrays.asList("orders","customer","lineitem","nation","part","partsupp","region","supplier")) {
//			String root = "C:\\Users\\zzha969\\OneDrive - The University of Auckland\\Desktop\\vldb revision-mixed cover\\TPC-H_revised";
//			System.out.println(dataset);
//			writeAllKeys(dataset, root);
//			
//			List<Key> keys = readKeys(dataset+"-keys-100%", root);
//			List<Double> percentage = Arrays.asList(0.25, 0.5, 0.75);
//			for(Double p : percentage) {
//				int bound = (int)Math.ceil(keys.size() * p);
//				List<Key> ks = new ArrayList<>();
//				for(int i = 0;i < bound;i ++) {
//					ks.add(keys.get(i));
//				}
//				writeKeys(dataset+"-keys-"+(int)(p*100)+"%", root, ks, false);
//			}
			
//			selectedFDs(dataset, root);
			
//			computeOptimal(dataset, root);
//		}
		
		/**
		 * stat original constraint number and size
		 */
		String root = "C:\\Users\\zzha969\\OneDrive - The University of Auckland\\Desktop\\PhD\\Mixed-Cover的相关工作\\FDCover实验\\Exp Results\\vldb revision exps\\TPCH\\all-tpch-fds";
//		for(String p : Arrays.asList("25%", "50%", "75%", "100%")) {
			for(String cover : Arrays.asList("original", "original keyfd")) {
				for(String dataset : Arrays.asList("orders","customer","lineitem","nation","part","partsupp","region","supplier")) {
					List<FD> fds = readFDs(dataset+"-fds", root);
					if(cover.equals("original")) {
						List<FD> original = Utils.compFDCover(fds, cover);
						System.out.println(cover+","+dataset+","+0+","+0+","+original.size()+","+Utils.compFDAttrSymbNum(original));
					}
					if(cover.equals("original keyfd")) {
						List<String> R = TPCHWorkloadExp.get_table_attr_map().get(dataset);
						List<Object> info = Utils.comp_Mixed_Cover_Sequential(R, fds, "original");
						List<Key> keys = (List<Key>) info.get(0);
						List<FD> original_mixed = (List<FD>) info.get(1);
						System.out.println("originalmixed,"+dataset+","+keys.size()+","+Utils.compKeyAttrSymbNum(keys)+","+original_mixed.size()+","+Utils.compFDAttrSymbNum(original_mixed));
					}
				}
			}
//		}
	}

}
