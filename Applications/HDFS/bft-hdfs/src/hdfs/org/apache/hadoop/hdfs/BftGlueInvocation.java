//$Id: BftGlueInvocation.java 2286 2009-01-21 07:55:35Z sangmin $

package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/** A method invocation, including the method name and its parameters.*/
public class BftGlueInvocation implements Writable, Configurable {
	private String methodName;
	private Class[] parameterClasses;
	private Object[] parameters;
	private Configuration conf;

	public BftGlueInvocation() {}

	public BftGlueInvocation(Method method, Object[] parameters) {
		this.methodName = method.getName();
		this.parameterClasses = method.getParameterTypes();
		this.parameters = parameters;
	}

	/** The name of the method invoked. */
	public String getMethodName() { return methodName; }

	/** The parameter classes. */
	public Class[] getParameterClasses() { return parameterClasses; }

	/** The parameter instances. */
	public Object[] getParameters() { return parameters; }

	public void readFields(DataInput in) throws IOException {
		methodName = UTF8.readString(in);
		parameters = new Object[in.readInt()];
		parameterClasses = new Class[parameters.length];
		ObjectWritable objectWritable = new ObjectWritable();

		for (int i = 0; i < parameters.length; i++) {
			parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
			parameterClasses[i] = objectWritable.getDeclaredClass();
		}

	}

	public void write(DataOutput out) throws IOException {
		UTF8.writeString(out, methodName);
		out.writeInt(parameterClasses.length);
		for (int i = 0; i < parameterClasses.length; i++) {
			ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
					conf);
		}
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(methodName);
		buffer.append("(");
		if(parameters != null){
			for (int i = 0; i < parameters.length; i++) {
				if (i != 0)
					buffer.append(", ");
				buffer.append(parameters[i]);
			}
		}
		buffer.append(")");
		return buffer.toString();
	}

	public void setConf(Configuration _conf) {
		this.conf = _conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

}

