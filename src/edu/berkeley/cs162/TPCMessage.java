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
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. Data is stored in a 
 * marshalled String format in this object...
 */
public class TPCMessage extends KVMessage implements Serializable {
	private static final long serialVersionUID = 6473128480951955693L;

	private String msgType = null;
	private String key = null;
	private String value = null;
	private boolean isPutResp = false;
	// Skeleton: private String status = null;
	private String message = null;
	private String tpcOpId = null;

	// added by luke
	Text text;

	/**
	 *  converts a KVMessage to a TPCMessage
	 * @param KVMessage inputMessage
	 * @param String tpcOpID
	 */
	public TPCMessage(KVMessage inputMessage, String tpcOpID){
		this.msgType = inputMessage.getMsgType();
		this.key = inputMessage.getKey();
		this.value = inputMessage.getValue();
//		this.status = inputMessage.getStatus();
		this.message = inputMessage.getMessage();
//		this.isPutResp = inputMessage.getIsPutResp();
		this.tpcOpId = tpcOpID;
	}

	/**
	 *  for 2PClog put
	 * @param String msgType
	 * @param String key
	 * @param String value
	 * @param String message
	 * @param String tpcOpId
	 */
	public TPCMessage(String msgType, String key, String value, String message, String tpcOpId) {
		this.msgType = msgType;
		this.key = key;
		this.value = value;
		this.message = message;
		this.tpcOpId = tpcOpId;
	}

	/**
	 *  for 2PC Put Requests
	 * @param String msgType
	 * @param String key
	 * @param String value
	 * @param String tpcOpId
	 */
	public TPCMessage(String msgType, String key, String valueORmessage, String tpcOpId) {
		this.msgType = msgType;
		this.key = key;
		this.tpcOpId = tpcOpId;
		if ("putreq".equals(msgType)) {
			this.value = valueORmessage;
		} else if ("ready".equals(msgType)){
			this.message = valueORmessage;
		} else{
			// this should not happen
			System.err.println("Error in TPCMessage 4 String Argument");
			System.exit(1);
		}
	}

	/**
	 *  for 2PC Del Requests and 2PC Abort response
	 * @param String msgType
	 * @param String keyORmessage
	 * @param String tpcOpId
	 * @param boolean isDelRequest
	 */
	public TPCMessage(String msgType, String keyORmessage, String tpcOpId, boolean isDelRequest) {
		this.msgType = msgType;
		this.tpcOpId = tpcOpId;
		if ("delreq".equals(msgType)){
			this.key = keyORmessage;
		} else if ("abort".equals(msgType)){
			this.message = keyORmessage;
		} else {
			System.err.println("msgType not del request or abort");
			TPCMaster.exit();
		}
	}

	/**
	 *  2PC Ready Messages, 2PC Decisions, 2PC Acknowledgement, 2PC IgnoreNext, 2PC Abort Decision,
	 *	Register, Registration ACK, 
	 *	Error Message, Server response, 
	 *	2PCLog abort, 2PCLog commit, and KeyRequest
	 * @param String msgType
	 * @param String tpcOpIdORmessageORkey
	 */
	public TPCMessage(String msgType, String tpcOpIdORmessageORkey) {
		this.msgType = msgType;
		if ("ready".equals(msgType) || "commit/abort".equals(msgType) || "ack".equals(msgType) || "commit".equals(msgType) || "abort".equals(msgType)){
			this.tpcOpId = tpcOpIdORmessageORkey;
		} else if ("register".equals(msgType) || "resp".equals(msgType) || "ignoreNext".equals(msgType)){	
			this.message = tpcOpIdORmessageORkey;
		} else if ("getreq".equals(msgType)){
			this.key = tpcOpIdORmessageORkey;
		}
	}

	/**
	 *  for Encryption Key Request
	 * @param String msgType
	 */
	public TPCMessage(String msgType) {
		this.msgType = msgType;

	}

	/*public TPCMessage(TPCMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
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


	public String getMessage(){
		return message;
	}

	public String getTpcOpId(){
		return tpcOpId;
	}

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

	private static String getTagValue(String sTag, Element eElement) {
		NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();

		Node nValue = (Node) nlList.item(0);

		return nValue.getNodeValue();
	}

	public String getElementsTag (String tag, Element x){
		NodeList nodeList = x.getElementsByTagName(tag);
		if (nodeList.getLength() != 0){ 
			return getTagValue(tag, x);
		} else {
			return null;
		}
	}

	/**
	 * Sites used:
	 * http://www.developerfusion.com/code/2064/a-simple-way-to-read-an-xml-file-in-java/
	 * http://www.mkyong.com/java/how-to-read-xml-file-in-java-dom-parser/
	 * @param input
	 */	
	public TPCMessage(InputStream input) throws KVException, SocketTimeoutException{
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

		tpcOpId = getElementsTag("TPCOpId", typeElement);

		if (msgType == "putreq" && value == null) throw new KVException (new KVMessage("XML Error: Received unparseable message"));
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

		if (message != null){
			//create child element, add an attribute, and add to root
			Element valueElement = doc.createElement("Message");
			root.appendChild(valueElement);

			//add a text element to the child
			text = doc.createTextNode(message);
			valueElement.appendChild(text);
		}

		if (tpcOpId != null){
			//create child element, add an attribute, and add to root
			Element valueElement = doc.createElement("TPCOpId");
			root.appendChild(valueElement);

			//add a text element to the child
			text = doc.createTextNode(tpcOpId);
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
		//System.out.println("Here's the xml:\n\n" + rtn);
		//KVMessage.delay();
		return rtn;
	}

	/**
	 * Encode Object to base64 String 
	 * @param obj
	 * @return
	 */
	public static String encodeObject(Object obj) throws KVException {
		String encoded = null;
		try{
			ByteArrayOutputStream bs = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bs);
			os.writeObject(obj);
			byte [] bytes = bs.toByteArray();
			encoded = DatatypeConverter.printBase64Binary(bytes);
			bs.close();
			os.close();
		} catch(IOException e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: Error serializing object"));
		}
		return encoded;
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
	
	public static TPCMessage sendReceive(Socket connection, TPCMessage message) throws SocketTimeoutException {
		String xmlFile = null;
		
		try {
			xmlFile = message.toXML();
		} catch (KVException e) {
			// this should not happen
			TPCMaster.exit();
		}
		
		PrintWriter out = null;
		InputStream in = null;
		
		try {
			out = new PrintWriter(connection.getOutputStream(),true);
		} catch (IOException e) {
			// this should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}
		out.println(xmlFile);
		
		try {
			connection.shutdownOutput();
		} catch (IOException e) {
			// this should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}
		
		try {
			in = connection.getInputStream();
			message = new TPCMessage(in);
			in.close();
		} catch (SocketTimeoutException e) {
			throw e;
		} catch (IOException e) {
			// this should not happen
			e.printStackTrace();
			TPCMaster.exit();
		} catch (KVException e) {
			// this should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}

		out.close();
		try {
			connection.close();
		} catch (IOException e) {
			// this should not happen
			e.printStackTrace();
			TPCMaster.exit();
		}
		return message;
	}

	/** utility function that sends a TPCMessage across a socket
	 * @param socket
	 * @param message
	 */
	public static void sendMessage(Socket connection, TPCMessage message){
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
		out.println(xmlFile);
		
		try {
			connection.shutdownOutput();
		} catch (IOException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		//out.close();
	}
	
	/** utility function that receives a TPCMessage across a socket
	 * @param socket
	 */
	public static TPCMessage receiveMessage(Socket connection) throws SocketTimeoutException{
		InputStream in = null;
		TPCMessage rtn = null;
		
		try {
			in = connection.getInputStream();
			rtn = new TPCMessage(in);
			//in.close();
		} catch (SocketTimeoutException e){
			throw e;
		} catch (IOException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		} catch (KVException e) {
			// should NOT ever throw exception here
			e.printStackTrace();
		}
		return rtn;
	}
	
	boolean equals(TPCMessage message){
		return ((this.msgType == null && message.msgType == null || this.msgType.equals(message.msgType)) &&
				(this.key == null && message.key == null || this.key.equals(message.key)) &&
				(this.value == null && message.value == null || this.value.equals(message.value)) &&
				(this.message == null && message.message == null || this.message.equals(message.message)) &&
				(this.isPutResp == message.isPutResp) &&
				(this.tpcOpId == null && message.tpcOpId == null || this.tpcOpId.equals(message.tpcOpId)));
	}
}

