package test.bcp.core.audit.flume.client.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComplexType implements Serializable {

	private static final long	serialVersionUID	= 1L;

	private Integer				intProp;

	private Float				floatProp;

	private Double				doubleProp;

	private String				stringProp;

	private Object[]			objectsArray;

	private Map<String, Object>	objectsMap			= new HashMap<String, Object>();

	private Set<Object>			objectsSet			= new HashSet<Object>();

	private List<Object>		objectsList			= new ArrayList<Object>(0);

	public Integer getIntProp() {
		return intProp;
	}

	public void setIntProp(Integer intProp) {
		this.intProp = intProp;
	}

	public Float getFloatProp() {
		return floatProp;
	}

	public void setFloatProp(Float floatProp) {
		this.floatProp = floatProp;
	}

	public Double getDoubleProp() {
		return doubleProp;
	}

	public void setDoubleProp(Double doubleProp) {
		this.doubleProp = doubleProp;
	}

	public String getStringProp() {
		return stringProp;
	}

	public void setStringProp(String stringProp) {
		this.stringProp = stringProp;
	}

	public Object[] getObjectsArray() {
		return objectsArray;
	}

	public void setObjectsArray(Object[] objectsArray) {
		this.objectsArray = objectsArray;
	}

	public Map<String, Object> getObjectsMap() {
		return objectsMap;
	}

	public void setObjectsMap(Map<String, Object> objectsMap) {
		this.objectsMap = objectsMap;
	}

	public Set<Object> getObjectsSet() {
		return objectsSet;
	}

	public void setObjectsSet(Set<Object> objectsSet) {
		this.objectsSet = objectsSet;
	}

	public List<Object> getObjectsList() {
		return objectsList;
	}

	public void setObjectsList(List<Object> objectsList) {
		this.objectsList = objectsList;
	}

}
