package ut.distcomp.bayou;

import java.util.HashMap;

import ut.distcomp.bayou.Operation.OperationType;

public class SessionManager {

	public SessionManager(){
		readSet = new HashMap<>();
		writeSet = new HashMap<>();
	}
	
	public String ExecuteTransaction(OperationType op, String songName,
			String string) {
		if(op == OperationType.GET){
			
		}else{
			
		}
		return null;
	}
	
	private boolean ReadGuarantee(){
		return false;
	}
	
	private boolean WriteGuarantee(){
		return false;
	}
	
	private final HashMap<ServerId, Integer> readSet;
	private final HashMap<ServerId, Integer> writeSet;

}
