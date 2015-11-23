package ut.distcomp.bayou;

import java.util.HashMap;

import ut.distcomp.bayou.Utils.TransactionType;

public class SessionManager {

	
	public String ExecuteTransaction(TransactionType delete, String songName,
			String string) {
		/*
		 * if(transaction type = read)
		 * 		call read gurantees
		 * else
		 * 		call write gurantees 
		 */
		return null;
	}
	
	private boolean ReadGuarantee(){
		return false;
	}
	
	private boolean WriteGuarantee(){
		return false;
	}
	
	// private HashMap<WriteID, Integer> readSet;
	// private HashMap<WriteID, Integer> writeSet;

}
