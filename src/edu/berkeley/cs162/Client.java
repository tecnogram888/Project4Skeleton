package edu.berkeley.cs162;

public class Client {

	public static final String addr = "localhost";
	public static final int portNum = 8080;

	public static KVClient<String, String> initClient (){
		return new KVClient<String, String>(addr, portNum);
	}

	public static void main(String[] args){
		KVClient<String, String> client = initClient();
		try {
			int i = 0;
			client.put("Test 0", "Test 1");
			//			while (true) {
			System.out.println("put successful");
			//client.del("Test 0");
			//System.out.println("del successful");
			try {
				System.out.println(client.get("Test 0"));
			} catch (KVException e) {
				System.out.println(((KVException) e).getMsg().getMessage());
				System.out.println("get returned null");
			}
			System.out.print("Did it retrieve the right string? " + client.get("Test 0").equals("Test 1"));
			/*			
			System.out.println("put Testi, Testi+1: " + client.put("Test" + i, "Test" + (i+1)));
			System.out.println("put Testi, Testi+2: " + client.put("Test" + i, "Test" + (i+2)));
			try {
				System.out.println("second get Testi: " + client.get("Test" + i));
			} catch (Exception e) {
				System.out.println("Second get returned null");
			}
			System.out.println("try del(Testi)");
			client.del("Test" + i);
//			System.out.println("try del(Testi+2)");
//			client.del("Test" + (i + 2));
			System.out.println("end");
			i++;
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			}*/
		} catch (KVException e) {
			e.printStackTrace();
			System.out.println(e.getMsg().getMessage());
		}
	}
}