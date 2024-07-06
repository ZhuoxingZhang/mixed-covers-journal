package util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import entity.DataTypeEnum;
import entity.Dataset;
import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;
/**
 * running some examples here 
 *
 */
public class RunExamples {
	
	public static void exampleOne() {
		String cs = "CAR-SERIAL#";
		String lic = "LICENSE#";
		String owner = "OWNER";
		String date = "DATE";
		String time = "TIME";
		String tic = "TICKET#";
		String offe = "OFFENSE";
		
		List<String> R = Arrays.asList(cs,lic,owner,date,time,tic,offe);
		
		//example 5.13 F from Maier book
//		FD fd1 = new FD(Arrays.asList(cs),Arrays.asList(lic, owner));
//		FD fd2 = new FD(Arrays.asList(lic),Arrays.asList(cs));
//		FD fd3 = new FD(Arrays.asList(tic),Arrays.asList(lic, date, time, offe));
//		FD fd4 = new FD(Arrays.asList(lic, date, time),Arrays.asList(tic, offe));
//		List<FD> FDs = Arrays.asList(fd1,fd2,fd3,fd4);
		
		//example 5.13 G
		FD fd1 = new FD(Arrays.asList(cs),Arrays.asList(lic));
		FD fd2 = new FD(Arrays.asList(lic),Arrays.asList(cs,owner));
		FD fd3 = new FD(Arrays.asList(tic),Arrays.asList(cs, owner, date, time));
		FD fd4 = new FD(Arrays.asList(cs, date, time),Arrays.asList(tic, offe));
		List<FD> FDs = Arrays.asList(fd1,fd2,fd3,fd4);
		
		//compute FD cover 
		List<List<FD>> fdcover_list = new ArrayList<>();
		List<String> FDCoverTypeList = Arrays.asList("original","nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
		List<String> KeyFDCoverTypeList = Arrays.asList("original keyfd","nonredundant keyfd","reduced keyfd","canonical keyfd","minimal keyfd","reduced minimal keyfd","optimal keyfd");
		for(String FDcover : FDCoverTypeList) {
			//original, non-redundant cover,....,optimal cover
			List<FD> sigma = Utils.compFDCover(FDs, FDcover);
			
			System.out.println("R : "+R+"\nFD cover : "+FDcover);
			sigma.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
			fdcover_list.add(sigma);
		}
		for(int i = 0;i < fdcover_list.size();i ++) {
			String KeyFDCover = KeyFDCoverTypeList.get(i);
			List<FD> fd_cover = fdcover_list.get(i);
			//original keyfd cover, ... , optimal keyfd cover
			List<Object> keyfdcover = Utils.compKeyFDCover(R, fd_cover);
			List<Key> sigma_k = (List<Key>) keyfdcover.get(0);
			List<FD> sigma_f = (List<FD>) keyfdcover.get(1);
			
			System.out.println("R : "+R+"\nmixed cover : "+KeyFDCover+"\nkeys : ");
			sigma_k.forEach(key -> System.out.println(key.toString()));
			System.out.println("fds : ");
			sigma_f.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
		}
	}
	
	/**
	 * 3NF schemata : [A, B, C, D, E, F]
	 * FD set : 
	 * FD [A, B, C, D, F] -> [E]
	 * FD [A, B, C, E] -> [F]
	 * FD [A, C, D, E] -> [B, F]
	 * FD [C, D, E, F] -> [A, B]
	 */
	public static void exampleTwo() {
		List<String> R = Arrays.asList("A","B","C","D","E","F");
		FD fd1 = new FD(Arrays.asList("A","B","C","D","F"), Arrays.asList("E"));
		FD fd2 = new FD(Arrays.asList("A","B","C","E"),Arrays.asList("F"));
		FD fd3 = new FD(Arrays.asList("A","C","D","E"),Arrays.asList("B","F"));
		FD fd4 = new FD(Arrays.asList("C","D","E","F"),Arrays.asList("A","B"));
		List<FD> FDs = Arrays.asList(fd1, fd2, fd3, fd4);
		//compute FD cover 
		List<List<FD>> fdcover_list = new ArrayList<>();
		List<String> FDCoverTypeList = Arrays.asList("original","nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
		List<String> KeyFDCoverTypeList = Arrays.asList("original keyfd","nonredundant keyfd","reduced keyfd","canonical keyfd","minimal keyfd","reduced minimal keyfd","optimal keyfd");
		for(String FDcover : FDCoverTypeList) {
			//original, non-redundant cover,....,optimal cover
			List<FD> sigma = Utils.compFDCover(FDs, FDcover);
					
			System.out.println("R : "+R+"\nFD cover type : "+FDcover);
			System.out.println("FD No. : "+sigma.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma));
			sigma.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
			fdcover_list.add(sigma);
		}
		for(int i = 0;i < fdcover_list.size();i ++) {
			String KeyFDCover = KeyFDCoverTypeList.get(i);
			List<FD> fd_cover = fdcover_list.get(i);
			//original keyfd cover, ... , optimal keyfd cover
			List<Object> keyfdcover = Utils.compKeyFDCover(R, fd_cover);
			List<Key> sigma_k = (List<Key>) keyfdcover.get(0);
			List<FD> sigma_f = (List<FD>) keyfdcover.get(1);
					
			System.out.println("R : "+R+"\nmixed cover type : "+KeyFDCover);
			System.out.println("Key No. : "+sigma_k.size()+"\nKey attr symb No. : "+Utils.compKeyAttrSymbNum(sigma_k)+"\nkeys : ");
			sigma_k.forEach(key -> System.out.println(key.toString()));
			System.out.println("fds : ");
			System.out.println("FD No. : "+sigma_f.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma_f));
			sigma_f.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
		}

	}
	
	public static void exampleThree() {
		List<String> R = Arrays.asList("F","L","I","E","S");
		FD fd1 = new FD(Arrays.asList("F","L"), Arrays.asList("E"));
		FD fd2 = new FD(Arrays.asList("E"),Arrays.asList("F","L"));
		FD fd3 = new FD(Arrays.asList("E","S"),Arrays.asList("I"));
		FD fd4 = new FD(Arrays.asList("I"),Arrays.asList("F","L"));
		List<FD> FDs = Arrays.asList(fd1, fd2, fd3, fd4);
		//compute FD cover 
		List<List<FD>> fdcover_list = new ArrayList<>();
		List<String> FDCoverTypeList = Arrays.asList("original","nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
		List<String> KeyFDCoverTypeList = Arrays.asList("original keyfd","nonredundant keyfd","reduced keyfd","canonical keyfd","minimal keyfd","reduced minimal keyfd","optimal keyfd");
		for(String FDcover : FDCoverTypeList) {
			//original, non-redundant cover,....,optimal cover
			List<FD> sigma = Utils.compFDCover(FDs, FDcover);
					
			System.out.println("R : "+R+"\nFD cover type : "+FDcover);
			System.out.println("FD No. : "+sigma.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma));
			sigma.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
			fdcover_list.add(sigma);
		}
		for(int i = 0;i < fdcover_list.size();i ++) {
			String KeyFDCover = KeyFDCoverTypeList.get(i);
			List<FD> fd_cover = fdcover_list.get(i);
			//original keyfd cover, ... , optimal keyfd cover
			List<Object> keyfdcover = Utils.compKeyFDCover(R, fd_cover);
			List<Key> sigma_k = (List<Key>) keyfdcover.get(0);
			List<FD> sigma_f = (List<FD>) keyfdcover.get(1);
					
			System.out.println("R : "+R+"\nmixed cover type : "+KeyFDCover);
			System.out.println("Key No. : "+sigma_k.size()+"\nKey attr symb No. : "+Utils.compKeyAttrSymbNum(sigma_k)+"\nkeys : ");
			sigma_k.forEach(key -> System.out.println(key.toString()));
			System.out.println("fds : ");
			System.out.println("FD No. : "+sigma_f.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma_f));
			sigma_f.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
		}

	}
	
	public static void alg2Decomp(Parameter para) throws SQLException, IOException {
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
		
	}
	
	public static void exampleFour() {
		List<String> R = Arrays.asList("B1","B2","B3","B4","B5","I1","I2","I3","I4","I5","E","S");
		FD fd1 = new FD(Arrays.asList("B1","B2","B3","B4","B5"), Arrays.asList("E"));
		FD fd2 = new FD(Arrays.asList("E"),Arrays.asList("B1","B2","B3","B4","B5"));
		FD fd3 = new FD(Arrays.asList("E","S"),Arrays.asList("I1","I2","I3","I4","I5"));
		FD fd4 = new FD(Arrays.asList("I1"),Arrays.asList("B1","B2","B3","B4","B5"));
		FD fd5 = new FD(Arrays.asList("I2"),Arrays.asList("B1","B2","B3","B4","B5"));
		FD fd6 = new FD(Arrays.asList("I3"),Arrays.asList("B1","B2","B3","B4","B5"));
		FD fd7 = new FD(Arrays.asList("I4"),Arrays.asList("B1","B2","B3","B4","B5"));
		FD fd8 = new FD(Arrays.asList("I5"),Arrays.asList("B1","B2","B3","B4","B5"));
		List<FD> FDs = Arrays.asList(fd1, fd2, fd3, fd4, fd5, fd6, fd7, fd8);
		//compute FD cover 
		List<List<FD>> fdcover_list = new ArrayList<>();
		List<String> FDCoverTypeList = Arrays.asList("original","nonredundant","reduced","canonical","minimal","reduced minimal","optimal");
		List<String> KeyFDCoverTypeList = Arrays.asList("original keyfd","nonredundant keyfd","reduced keyfd","canonical keyfd","minimal keyfd","reduced minimal keyfd","optimal keyfd");
		for(String FDcover : FDCoverTypeList) {
			//original, non-redundant cover,....,optimal cover
			List<FD> sigma = Utils.compFDCover(FDs, FDcover);
					
			System.out.println("R : "+R+"\nFD cover type : "+FDcover);
			System.out.println("FD No. : "+sigma.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma));
			sigma.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
			fdcover_list.add(sigma);
		}
		for(int i = 0;i < fdcover_list.size();i ++) {
			String KeyFDCover = KeyFDCoverTypeList.get(i);
			List<FD> fd_cover = fdcover_list.get(i);
			//original keyfd cover, ... , optimal keyfd cover
			List<Object> keyfdcover = Utils.compKeyFDCover(R, fd_cover);
			List<Key> sigma_k = (List<Key>) keyfdcover.get(0);
			List<FD> sigma_f = (List<FD>) keyfdcover.get(1);
					
			System.out.println("R : "+R+"\nmixed cover type : "+KeyFDCover);
			System.out.println("Key No. : "+sigma_k.size()+"\nKey attr symb No. : "+Utils.compKeyAttrSymbNum(sigma_k)+"\nkeys : ");
			sigma_k.forEach(key -> System.out.println(key.toString()));
			System.out.println("fds : ");
			System.out.println("FD No. : "+sigma_f.size());
			System.out.println("FD attr symb No. : "+Utils.compFDAttrSymbNum(sigma_f));
			sigma_f.forEach(fd -> System.out.println(fd.toString()));
			System.out.println("################\n");
		}

	}


	public static void main(String[] args) throws SQLException, IOException {
//		exampleTwo();
		exampleFour();

//		for(Parameter para : Utils.getParameterList(Arrays.asList("bridges"), DataTypeEnum.NULL_UNCERTAINTY)) {
//			alg2Decomp(para);
//		}
	}

}
