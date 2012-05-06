package edu.berkeley.cs162;

public class Client2 {

	public static final String addr = "localhost";
	public static final int portNum = 8080;

	public static KVClient<String, String> initClient (){
		return new KVClient<String, String>(addr, portNum);
	}

	public static void main(String[] args){
		KVClient<String, String> client = initClient();
		try {
			String first = client.get("Test 0");
			String second = client.get("Test 2");
			String third = null;
			try {
				third = client.get("Test 4");
			} catch (KVException e){
				System.out.println("Did not 'get' deleted value");
			}
			System.out.println("First string successfully returned? " + first.equals("Test 1"));
			System.out.println("Second string successfully returned? " + second.equals("Test 3"));
			if (third == null) {
				System.out.println("String successfully deleted");
			} else {
				System.out.println("Delete failed");
			}
		} catch (KVException e) {
			e.printStackTrace();
			System.out.println(e.getMsg().getMessage());
		}
	}
}
