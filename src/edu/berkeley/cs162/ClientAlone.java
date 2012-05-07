package edu.berkeley.cs162;

public class ClientAlone {
	public static void main(String[] args){
		KVClient<String, String> client = Client.initClient();
		int i = 1;
		while (true){
			try {
				System.out.println(i);
				client.put("test1", "Test2");
				System.out.println("cool");
				client.ignoreNext((long)1);
				try{
					System.out.println("Should be False: " + client.put("test1", "Test3"));
				} catch (KVException e) {
					System.out.println("Yay! Correct Error: " + e.getMsg().getMessage());
				}
				System.out.println("Should be False: " + client.put("luke", "isCool"));
				System.out.println("Should be False: " + client.put("luke", "isCool"));
				System.out.println("Should be False: " + client.put("lukeLu", "isAmazing"));
				System.out.println("Should be Equal to \"Test2\": " + client.get("test1"));
				System.out.println("Should be Equal to \"isCool\": " + client.get("luke"));
				System.out.println("Should be Equal to \"isAmazing\": " + client.get("lukeLu"));
				client.del("luke");
				System.out.println("Delete \"luke\" successful");
				client.del("lukeLu");
				System.out.println("Delete \"lukeLu\" successful");
				client.del("test1");
				System.out.println("Delete \"test1\" successful");
			} catch (KVException e) {
				System.out.println(e.getMsg().getMessage());
			}
			i++;
		}
	}
}