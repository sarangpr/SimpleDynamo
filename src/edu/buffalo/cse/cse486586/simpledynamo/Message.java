package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Message implements Serializable {
	private String destination_Port=null;
	private String type = null;
	private String key = null;
	private String value = null;
	public Message(){}
	public Message(String type, String destination){
		this.setType(type);
		this.setDestination_Port(destination);
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
}
