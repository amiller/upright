/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.hive.metastore.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class Type implements TBase, java.io.Serializable {
  private String name;
  private String type1;
  private String type2;
  private List<FieldSchema> fields;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean name = false;
    public boolean type1 = false;
    public boolean type2 = false;
    public boolean fields = false;
  }

  public Type() {
  }

  public Type(
    String name,
    String type1,
    String type2,
    List<FieldSchema> fields)
  {
    this();
    this.name = name;
    this.__isset.name = true;
    this.type1 = type1;
    this.__isset.type1 = true;
    this.type2 = type2;
    this.__isset.type2 = true;
    this.fields = fields;
    this.__isset.fields = true;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
    this.__isset.name = true;
  }

  public void unsetName() {
    this.__isset.name = false;
  }

  public String getType1() {
    return this.type1;
  }

  public void setType1(String type1) {
    this.type1 = type1;
    this.__isset.type1 = true;
  }

  public void unsetType1() {
    this.__isset.type1 = false;
  }

  public String getType2() {
    return this.type2;
  }

  public void setType2(String type2) {
    this.type2 = type2;
    this.__isset.type2 = true;
  }

  public void unsetType2() {
    this.__isset.type2 = false;
  }

  public int getFieldsSize() {
    return (this.fields == null) ? 0 : this.fields.size();
  }

  public java.util.Iterator<FieldSchema> getFieldsIterator() {
    return (this.fields == null) ? null : this.fields.iterator();
  }

  public void addToFields(FieldSchema elem) {
    if (this.fields == null) {
      this.fields = new ArrayList<FieldSchema>();
    }
    this.fields.add(elem);
    this.__isset.fields = true;
  }

  public List<FieldSchema> getFields() {
    return this.fields;
  }

  public void setFields(List<FieldSchema> fields) {
    this.fields = fields;
    this.__isset.fields = true;
  }

  public void unsetFields() {
    this.fields = null;
    this.__isset.fields = false;
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Type)
      return this.equals((Type)that);
    return false;
  }

  public boolean equals(Type that) {
    if (that == null)
      return false;

    boolean this_present_name = true && (this.name != null);
    boolean that_present_name = true && (that.name != null);
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_type1 = true && (this.__isset.type1) && (this.type1 != null);
    boolean that_present_type1 = true && (that.__isset.type1) && (that.type1 != null);
    if (this_present_type1 || that_present_type1) {
      if (!(this_present_type1 && that_present_type1))
        return false;
      if (!this.type1.equals(that.type1))
        return false;
    }

    boolean this_present_type2 = true && (this.__isset.type2) && (this.type2 != null);
    boolean that_present_type2 = true && (that.__isset.type2) && (that.type2 != null);
    if (this_present_type2 || that_present_type2) {
      if (!(this_present_type2 && that_present_type2))
        return false;
      if (!this.type2.equals(that.type2))
        return false;
    }

    boolean this_present_fields = true && (this.__isset.fields) && (this.fields != null);
    boolean that_present_fields = true && (that.__isset.fields) && (that.fields != null);
    if (this_present_fields || that_present_fields) {
      if (!(this_present_fields && that_present_fields))
        return false;
      if (!this.fields.equals(that.fields))
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case 1:
          if (field.type == TType.STRING) {
            this.name = iprot.readString();
            this.__isset.name = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.STRING) {
            this.type1 = iprot.readString();
            this.__isset.type1 = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3:
          if (field.type == TType.STRING) {
            this.type2 = iprot.readString();
            this.__isset.type2 = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4:
          if (field.type == TType.LIST) {
            {
              TList _list0 = iprot.readListBegin();
              this.fields = new ArrayList<FieldSchema>(_list0.size);
              for (int _i1 = 0; _i1 < _list0.size; ++_i1)
              {
                FieldSchema _elem2 = new FieldSchema();
                _elem2 = new FieldSchema();
                _elem2.read(iprot);
                this.fields.add(_elem2);
              }
              iprot.readListEnd();
            }
            this.__isset.fields = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(TProtocol oprot) throws TException {
    TStruct struct = new TStruct("Type");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.name != null) {
      field.name = "name";
      field.type = TType.STRING;
      field.id = 1;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.name);
      oprot.writeFieldEnd();
    }
    if (this.type1 != null) {
      if (this.__isset.type1) {
      field.name = "type1";
      field.type = TType.STRING;
      field.id = 2;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.type1);
      oprot.writeFieldEnd();
    }
  }
  if (this.type2 != null) {
    if (this.__isset.type2) {
    field.name = "type2";
    field.type = TType.STRING;
    field.id = 3;
    oprot.writeFieldBegin(field);
    oprot.writeString(this.type2);
    oprot.writeFieldEnd();
  }
}
if (this.fields != null) {
  if (this.__isset.fields) {
  field.name = "fields";
  field.type = TType.LIST;
  field.id = 4;
  oprot.writeFieldBegin(field);
  {
    oprot.writeListBegin(new TList(TType.STRUCT, this.fields.size()));
    for (FieldSchema _iter3 : this.fields)    {
      _iter3.write(oprot);
    }
    oprot.writeListEnd();
  }
  oprot.writeFieldEnd();
}
}
oprot.writeFieldStop();
oprot.writeStructEnd();
}

public String toString() {
StringBuilder sb = new StringBuilder("Type(");
sb.append("name:");
sb.append(this.name);
sb.append(",type1:");
sb.append(this.type1);
sb.append(",type2:");
sb.append(this.type2);
sb.append(",fields:");
sb.append(this.fields);
sb.append(")");
return sb.toString();
}

}

