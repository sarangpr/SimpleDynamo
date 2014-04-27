package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

@SuppressWarnings("serial")
public class Message implements Serializable {
	private boolean isDelivered = false;
	private String destination_Port=null;
	private String type = null;
	private String key = null;
	private String value = null;
	private long originTime;
	private HashMap<String,String> hashmap;
	public Message(){}
	public Message(String type, String destination){
		this.setType(type);
		this.setDestination_Port(destination);
	}
	public Message(boolean isDelivered){
		this.isDelivered= isDelivered;
	}
	public String getDestination_Port() {
		return destination_Port;
	}
	public void setDestination_Port(String destination_Port) {
		this.destination_Port = destination_Port;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public boolean isDelivered() {
		return isDelivered;
	}
	public void setDelivered(boolean isDelivered) {
		this.isDelivered = isDelivered;
	}
	public long getOriginTime() {
		return originTime;
	}
	public void setOriginTime(long originTime) {
		this.originTime = originTime;
	}
	public HashMap<String,String> getHashmap() {
		return hashmap;
	}
	public void setHashmap(HashMap<String,String> hashmap) {
		this.hashmap = hashmap;
	}
}
