package edu.berkeley.cs162;

public class Client1 {

	public static final String addr = "localhost";
	public static final int portNum = 8080;

	public static KVClient<String, String> initClient (){
		return new KVClient<String, String>(addr, portNum);
	}

	public static void main(String[] args){
		KVClient<String, String> client = initClient();
		try {
		
			client.put("Test 0", "Test 1");
			client.put("Test 2", "Test 3");
			client.put("Test 4", "Test 5");
			System.out.println("put successful");
			client.del("Test 4");
			System.out.println("del successful");
			
		} catch (KVException e) {
			e.printStackTrace();
			System.out.println(e.getMsg().getMessage());
		}
	}
}