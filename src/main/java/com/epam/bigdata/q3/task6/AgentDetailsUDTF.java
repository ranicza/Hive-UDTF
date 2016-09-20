package com.epam.bigdata.q3.task6;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import eu.bitwalker.useragentutils.UserAgent;

public class AgentDetailsUDTF extends GenericUDTF{

	private static final String UA_TYPE = "UA_TYPE";
	private static final String UA_FAMILY = "UA_FAMILY";
	private static final String OS_NAME = "OS_NAME";
	private static final String DEVICE = "DEVICE";
	
	private PrimitiveObjectInspector agentDtlOI = null;
	private Object[] fwdObj = null;	
	
	public StructObjectInspector initialize(ObjectInspector[] arg) {
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		agentDtlOI = (PrimitiveObjectInspector) arg[0];
		
		fieldNames.add(UA_TYPE);
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fieldNames.add(UA_FAMILY);
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fieldNames.add(OS_NAME);
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fieldNames.add(DEVICE);
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fwdObj = new Object[4];
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
		
	}
	
	@Override
	public void process(Object[] arg) throws HiveException {
		String agentDtl = agentDtlOI.getPrimitiveJavaObject(arg[0]).toString();
		UserAgent ua = UserAgent.parseUserAgentString(agentDtl);
		
		// UA type
		fwdObj[0] = ua.getBrowser() != null ? ua.getBrowser().getBrowserType().getName() : null;
		// UA family
		fwdObj[1] = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
		// OS name
		fwdObj[2] = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
		// Device		
		fwdObj[3] = ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;

		this.forward(fwdObj);
		
	}
	
	@Override
	public void close() throws HiveException {
		forward(fwdObj);		
	}



}
