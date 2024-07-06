package util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;
import entity.Schema;

/**
 * SOTA ALGORITHM SO FAR for lossless decomposition of schemas
 * atomic cover of FDs as input
 */
public class LosslessDecompAlg {
	
	/**
	 * 
	 * @param R
	 * @param Sigma functional dependency set 
	 * @param Sigma_a_bar an atomic cover of Sigma
	 * @return
	 */
	public  ArrayList<Schema> exe_decomp(Parameter para,List<String> R,List<FD> Sigma,List<FD> Sigma_a_bar) {
		System.out.println("FD : "+para.fd_add);
		
		ArrayList<FD> Sigma_a = new ArrayList<FD>();
		for(FD fd : Sigma_a_bar) {
			if(!Sigma_a.contains(fd))
				Sigma_a.add(fd);
		}

		for(FD X_A : Sigma_a_bar) {
			ArrayList<String> XA = new ArrayList<String>();
			for(String a : X_A.getLeftHand()) {//get XA set
				if(!XA.contains(a))
					XA.add(a);
			}
			for(String a : X_A.getRightHand()) {//get XA set
				if(!XA.contains(a))
					XA.add(a);
			}
			for(FD Y_B : Sigma_a_bar) {//get Y -> B
				ArrayList<String> YB = new ArrayList<String>();
				for(String a : Y_B.getLeftHand()) {//get YB set
					if(!YB.contains(a))
						YB.add(a);
				}
				for(String a : Y_B.getRightHand()) {//get YB set
					if(!YB.contains(a))
						YB.add(a);
				}
				
				if(XA.containsAll(YB) && !Utils.getAttrSetClosure(Y_B.getLeftHand(), Sigma_a_bar).containsAll(XA)) {
					//X->A is critical
					List<FD> Sigma_a_NO_XA = new ArrayList<FD>();
					for(FD fd : Sigma_a) {
						if(!fd.equals(X_A))
							Sigma_a_NO_XA.add(fd);
					}
					List<String> X_closure = Utils.getAttrSetClosure(X_A.getLeftHand(), Sigma_a_NO_XA);
					if(X_closure.containsAll(X_A.getRightHand()))//Sigma_a - X -> A implies X -> A 
						Sigma_a.remove(X_A);//Sigma_a removes X -> A
				}
			}
		}
		
		
		
		ArrayList<Schema> D = new ArrayList<Schema>();

		long start_time_line_9_to_14 = new Date().getTime();
		ArrayList<FD> Sigma_a_temp = new ArrayList<FD>();
		Sigma_a_temp.addAll(Sigma_a);
		for(FD X_A : Sigma_a_temp) {//pair (FD,subschema,n_key)
			List<String> XA = new ArrayList<String>();
			for(String a : X_A.getLeftHand()) {
				if(!XA.contains(a))
					XA.add(a);
			}
			for(String a : X_A.getRightHand()) {
				if(!XA.contains(a))
					XA.add(a);
			}
			List<FD> Sigma_a_NO_XA = new ArrayList<FD>();
			for(FD fd : Sigma_a) {
				if(!fd.equals(X_A))
					Sigma_a_NO_XA.add(fd);
			}
			
			
			List<String> X_closure = Utils.getAttrSetClosure(X_A.getLeftHand(), Sigma_a_NO_XA);
			
			
			if(X_closure.containsAll(X_A.getRightHand())){//Sigma_a - X -> A implies X -> A 
				Sigma_a.remove(X_A);//Sigma_a removes X -> A
			}else {//D = D U X->A
				List<FD> projection_XA = Utils.getProjection(Sigma_a_bar, XA);//get projection Sigma_a_bar[XA]
				Schema p = new Schema(XA,projection_XA);
				
				if(!D.contains(p))
					D.add(p);
			}
		}
		
		ArrayList<Schema> removal = new ArrayList<Schema>();//need to remove from D
		for(int i = 0;i < D.size();i ++) {
			Schema pair1 = D.get(i);
			List<String> S = pair1.getAttr_set();
			for(int j = 0;j < D.size();j ++) {
				if(i == j)
					continue;
				Schema pair2 = D.get(j);
				List<String> S_prime = pair2.getAttr_set();
				if(S_prime.containsAll(S)) {
					removal.add(pair1);
					break;
				}
			}
		}
		for(Schema p : removal) {
			D.remove(p);
		}
		
		
		boolean exist = false;
		for(Schema p : D) {
			List<String> R_prime = p.getAttr_set();
			List<String> R_prime_closure = Utils.getAttrSetClosure(R_prime, Sigma_a_bar);
			if(R_prime_closure.containsAll(R)) {
				exist = true;
				break;
			}
		}
		if(!exist) {
			Key K = Utils.getRefinedMinKey(Sigma, new Key(R), R);
			List<FD> K_projection = Utils.getProjection(Sigma_a_bar, K.getAttributes());
			D.add(new Schema(K.getAttributes(),K_projection));
		}
		
		
		return D;
		
	}
	
	/**
	 * 
	 * @param para
	 * @param R
	 * @param Sigma_a
	 * @return only return sub-schemas in 3NF
	 * @throws IOException
	 * @throws SQLException
	 */
	public List<Schema>  decomp_and_output(Parameter para, List<String> R,List<FD> Sigma_a) throws IOException, SQLException {
	    //execute decomposition
		List<Schema> D = exe_decomp(para, R, Sigma_a, Sigma_a);
		
		List<Schema> BCNF_schemata = new ArrayList<Schema>();//BCNF databases of all decomposition results
		List<Schema> thirdNF_schemata = new ArrayList<Schema>();//3NF databases of all decomposition results, not BCNF
		
		for(Schema p : D) {
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			boolean isBCNF = Utils.isBCNF(schema,fds);
			if(isBCNF)
				BCNF_schemata.add(p);
			else
				thirdNF_schemata.add(p);
			
		}

		System.out.println("BCNF schemata as follows :\n");
		for(Schema p : BCNF_schemata) {
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			System.out.println("BCNF schemata : "+schema +"\nFD set : ");
			for(FD f : fds) {
				System.out.println(f.toString());
			}
			System.out.println("########################\n");
		}
		
		System.out.println("\n=============================\n");
		System.out.println("3NF (not BCNF) schemata as follows :\n");
		for(Schema p : thirdNF_schemata) {
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			System.out.println("3NF schemata : "+schema +"\nFD set : ");
			for(FD f : fds) {
				System.out.println(f.toString());
			}
			System.out.println("########################\n");
		}
		
		
		return thirdNF_schemata;//return only 3NF sub-schematas
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		for(Parameter para : Utils.getParameterList(Arrays.asList("echo"), DataTypeEnum.NULL_EQUALITY)) {
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
			
			//we get sub-schemas in 3NF from input schema
			LosslessDecompAlg alg2 = new LosslessDecompAlg();
			List<Schema> subschemas = alg2.decomp_and_output(para, R, atomicCover);
		}
		
	}


}
