/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;


public class UDFOPNull extends UDF {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.udf.UDFOPNull");

  public UDFOPNull() {
  }

  public Boolean evaluate(Long a)  {
    return Boolean.valueOf(a == null ? true : false);
  }

  public Boolean evaluate(Number a)  {
    return Boolean.valueOf(a == null ? true : false);
  }

  public Boolean evaluate(String a)  {
    return Boolean.valueOf(a == null ? true : false);
  }

}
