/**
 * 
 * XML Parsing library for the key-value store
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2011, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.w3c.dom.*;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. Data is stored in a 
 * marshalled String format in this object.
 */
public class KVMessage{
	private String msgType = null;
	private String key = null;
	private String value = null;
//	private boolean status = false;
	private String message = null;
//	private boolean isPutResp = false;

	public KVMessage(){
		//do nothing
	}
	
	public KVMessage(String msgType, String key, String value) {
		this.msgType = msgType;
		this.key = key;
		this.value = value;
	}
	
	// added by luke
	Text text;
	
	// key or ignoreNext
	public KVMessage(String msgType, String keyORslaveID) {
		this.msgType = msgType;
		if (msgType.equals("ignoreNext")){
			this.message = keyORslaveID;
		} else{
			this.key = keyORslaveID;
		}
		this.value = null;
	}
	
	public KVMessage(String msgType, String message, boolean ignore){
		this.msgType = msgType;
		this.message = message;
	}
	
	//For constructing error messages and successful put + delete messages
	public KVMessage(String message){
		this.msgType = "resp";
		this.message = message;
	}
	
/*	//For successful put message
	public KVMessage(boolean status, String message){
		this.status = status;
		this.message = message;
		this.msgType = "resp";
		this.isPutResp = true;
	}*/
	
	public String getMsgType(){
		return msgType;
	}
	
	public String getKey(){
		return key;
	}
	
	public String getValue(){
		return value;
	}
	
/*	public boolean getStatus(){
		return status;
	}*/
	
	public String getMessage(){
		return message;
	}
	
/*	public boolean getIsPutResp(){
		return isPutResp;
	}*/
	
	public boolean hasEmptyKey(){
		return (key == null | key.length() == 0 | key.isEmpty());
	}
	public boolean hasEmptyValue(){
		return (value == null| value.length() == 0 | value.isEmpty());
	}
	
	/* Hack for ensuring XML libraries does not close input stream by default.
	 * Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	public String getElementsTag (String tag, Element x){
		NodeList nodeList = x.getElementsByTagName(tag);
		if (nodeList.getLength() != 0){ 
			return getTagValue(tag, x);
		} else {
			return null;
		}
	}

	  private static String getTagValue(String sTag, Element eElement) {
			NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
		 
		        Node nValue = (Node) nlList.item(0);
		 
			return nValue.getNodeValue();
		  }

	
	/**
	 * Sites used:
	 * http://www.developerfusion.com/code/2064/a-simple-way-to-read-an-xml-file-in-java/
	 * http://www.mkyong.com/java/how-to-read-xml-file-in-java-dom-parser/
	 * @param input
	 */	
	public KVMessage(InputStream input) throws KVException, SocketTimeoutException{
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder;
			try {
				docBuilder = docBuilderFactory.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				throw new KVException(new KVMessage("Unknown Error: Invalid parser config"));
			}
			Document doc = null;
			try {
				doc = docBuilder.parse(new NoCloseInputStream(input));
			} catch (SocketTimeoutException e){
				throw e;
			} catch (SAXException e) {
				throw new KVException(new KVMessage("XML Error: Received unparseable message"));
			} catch (IOException e) {
				e.printStackTrace();
				throw new KVException(new KVMessage("XML Error: Received unparseable message"));
			}
			
			doc.getDocumentElement().normalize();
			
			// messageList should be a NodeList with only ONE Node
			NodeList typeList = doc.getElementsByTagName("KVMessage");
			
			Node typeNode = typeList.item(0);
			Element typeElement = (Element) typeNode;

			msgType = typeElement.getAttribute("type");
			
			key = getElementsTag("Key", typeElement);

			value = getElementsTag("Value", typeElement);

			message = getElementsTag("Message", typeElement);
	}
	
	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 */
	public String toXML() throws KVException{
		String rtn = null;
			/////////////////////////////
            //Creating an empty XML Document
            //We need a Document
            DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = null;
			try {
				docBuilder = dbfac.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
				throw new KVException(new KVMessage("Unknown Error: Error occured duringf XML creation"));
			}
            Document doc = docBuilder.newDocument();

        	/////////////////////////////
            //Creating the XML tree
            //create the root element and add it to the document
            Element root = doc.createElement("KVMessage");
            doc.appendChild(root);
            root.setAttribute("type", msgType);
            

            if (key != null){
            	//create child element, add an attribute, and add to root
            	Element keyElement = doc.createElement("Key");
            	root.appendChild(keyElement);
            	//add a text element to the child
            	text = doc.createTextNode(key);
            	keyElement.appendChild(text);
            }
            if (value != null){
            	//create child element, add an attribute, and add to root
                Element valueElement = doc.createElement("Value");
                root.appendChild(valueElement);

                //add a text element to the child
                text = doc.createTextNode(value);
                valueElement.appendChild(text);
            }
/*            if (isPutResp){
            	//create child element, add an attribute, and add to root
                Element valueElement = doc.createElement("Status");
                root.appendChild(valueElement);

                //add a text element to the child
                if (status) text = doc.createTextNode("True");
                else text = doc.createTextNode("False");
                valueElement.appendChild(text);
            }*/
            if (message != null){
            	//create child element, add an attribute, and add to root
                Element valueElement = doc.createElement("Message");
                root.appendChild(valueElement);

                //add a text element to the child
                text = doc.createTextNode(message);
                valueElement.appendChild(text);
            }
            /////////////////////////////
            //Output the XML
            //set up a transformer
            TransformerFactory transfac = TransformerFactory.newInstance();
            Transformer trans = null;
			try {
				trans = transfac.newTransformer();
			} catch (TransformerConfigurationException e) {
				e.printStackTrace();
				throw new KVException(new KVMessage("Unknown Error: Error occured duringf XML creation"));
			}
            trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            trans.setOutputProperty(OutputKeys.INDENT, "yes");

            //create string from xml tree
            StringWriter sw = new StringWriter();
            StreamResult result = new StreamResult(sw);
            DOMSource source = new DOMSource(doc);
            try {
				trans.transform(source, result);
			} catch (TransformerException e) {
				e.printStackTrace();
				throw new KVException(new KVMessage("Unknown Error: Error occured duringf XML creation"));
			}
            String xmlString = sw.toString();
            rtn = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xmlString;

            //print xml
           // System.out.println("Here's the xml:\n\n" + rtn);
            //KVMessage.delay();
		return rtn;
	}
	
	/**
	 * http://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
	 * http://stackoverflow.com/questions/20778/how-do-you-convert-binary-data-to-strings-and-back-in-java
	 * @throws KVException Over Sized Key
	 */
	public static String encodeObject(Object input) throws KVException{
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput oout = new ObjectOutputStream(bos);
			oout.writeObject(input);
			byte[] inputByteArray = bos.toByteArray();
			String marshalled = DatatypeConverter.printBase64Binary(inputByteArray);

			oout.close();
			bos.close();
			return marshalled;
		} catch (IOException i) {
			throw new KVException(new KVMessage("Unknown Error: Could not serialize object"));
		}
	}
	
	/**
	 * Decode base64 String to Object
	 * @param str
	 * @return
	 */
	public static Object decodeObject(String str) throws KVException {
		Object obj = null;
		try{
	        byte[] decoded = DatatypeConverter.parseBase64Binary(str);
	        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(decoded));
	        obj = is.readObject();
	        is.close();
		} catch(IOException e) {
	        throw new KVException(new KVMessage("resp", "Unknown Error: Unable to decode object"));
		}
		catch (ClassNotFoundException e) {
	        throw new KVException(new KVMessage("resp", "Unknown Error: Decoding object class not found"));
		}
		return obj;
	}
	
	/** utility function that sends a KVMessage across a socket
	 * @param socket
	 * @param message
	 */
	public static void sendMessage(Socket connection, KVMessage message){
		String xmlFile = null;
		try {
			xmlFile = message.toXML();
		} catch (KVException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		PrintWriter out = null;
		try {
			out = new PrintWriter(connection.getOutputStream(),true);
		} catch (IOException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		System.out.println("Sending to Client: \n" + xmlFile + "\n\n");
		out.println(xmlFile);
		
		try {
			connection.shutdownOutput();
		} catch (IOException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		//out.close();
	}
	
	/** utility function that receives a KVMessage across a socket
	 * @param socket
	 */
	public static KVMessage receiveMessage(Socket connection) throws KVException, SocketTimeoutException {
		InputStream in = null;
		KVMessage rtn = null;
		
		try {
			in = connection.getInputStream();
			rtn = new KVMessage(in);
			System.out.println("Received from Client: \n" + rtn.toXML() + "\n\n");
			//in.close();
		} catch (IOException e) {
			// should NOT throw an exception here
			e.printStackTrace();
		}
		return rtn;
	}
	
	public static void delay(){
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
