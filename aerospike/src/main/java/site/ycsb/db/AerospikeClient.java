/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeClient extends site.ycsb.DB {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";
  private static final String DEFAULT_SET = "usertable";

  private String namespace = null;
  private String setName = null;

  private com.aerospike.client.AerospikeClient client = null;

  private Policy readPolicy = new Policy();
  private WritePolicy insertPolicy = new WritePolicy();
  private WritePolicy updatePolicy = new WritePolicy();
  private WritePolicy deletePolicy = new WritePolicy();
  private BatchPolicy batchPolicy = new BatchPolicy();
  private BatchWritePolicy insertBatchPolicy = new BatchWritePolicy();
  private BatchWritePolicy updateBatchPolicy = new BatchWritePolicy();
  private BatchWritePolicy deleteBatchPolicy = new BatchWritePolicy();

  private int batchSize= 1;

  @Override
  public void init() throws DBException {
    insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updatePolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
    insertBatchPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updateBatchPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);
    setName = props.getProperty("as.set", DEFAULT_SET);

    if (namespace.trim().isEmpty()) {
      throw new DBException("Aerospike namespace must not be empty.");
    }
    if (setName.trim().isEmpty()) {
      throw new DBException("Aerospike set must not be empty.");
    }

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout",
        DEFAULT_TIMEOUT));

    readPolicy.setTimeout(timeout);
    insertPolicy.setTimeout(timeout);
    updatePolicy.setTimeout(timeout);
    deletePolicy.setTimeout(timeout);
    batchPolicy.setTimeout(timeout);

    ClientPolicy clientPolicy = new ClientPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
    }

    try {
      client =
          new com.aerospike.client.AerospikeClient(clientPolicy, host, port);
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike " +
          "client for %s:%d.", host, port), e);
    }

    if (getProperties().containsKey("batchsize")) {
      batchSize =
          Integer.parseInt(getProperties().getProperty("batchsize"));
    }

    if (batchSize <= 0) {
      throw new DBException("batchsize must be greater than zero.");
    }

  }

  @Override
  public void cleanup() throws DBException {
    client.close();
  }

  private Key createKey(String key) {
    return new Key(namespace, setName, key);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      Record record = null;

      if (fields != null) {
        for (int i = 0; i < batchSize; i++) {
          Key k = createKey(key + "_" + i);
          record = client.get(readPolicy, k,
              fields.toArray(new String[fields.size()]));
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          Key k = createKey(key + "_" + i);
          record = client.get(readPolicy, k);
        }
      }

      if (record == null) {
        System.err.println("Record key " + key + " not found (read)");
        return Status.ERROR;
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, WritePolicy writePolicy,
      Map<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      ++index;
    }

    if (batchSize > 1) {
      List<BatchRecord> records = new ArrayList<BatchRecord>(batchSize);
      BatchWritePolicy batchWritePolicy = writePolicy == insertPolicy
          ? insertBatchPolicy : updateBatchPolicy;

      Operation[] operations = new Operation[bins.length];
      for (int i = 0; i < bins.length; i++) {
        operations[i] = Operation.put(bins[i]);
      }

      for (int i = 0; i < batchSize; i++) {
        records.add(new BatchWrite(batchWritePolicy, createKey(key + "_" + i),
            operations));
      }

      try {
        if (!client.operate(batchPolicy, records)) {
          System.err.println("Error while batch writing key " + key);
          return Status.ERROR;
        }
      } catch (AerospikeException e) {
        System.err.println("Error while batch writing key " + key + ": " + e);
        return Status.ERROR;
      }
      return Status.OK;
    }

    Key keyObj;
    for (int i = 0; i < batchSize; i++) {
      keyObj = createKey(key + "_" + i);
      try {
        client.put(writePolicy, keyObj, bins);
      } catch (AerospikeException e) {
        System.err.println("Error while writing key " + key + ": " + e);
        return Status.ERROR;
      }
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    read(table, key, null, null);
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (batchSize > 1) {
        List<BatchRecord> records = new ArrayList<BatchRecord>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          records.add(new BatchWrite(deleteBatchPolicy, createKey(key + "_" + i),
              new Operation[] {Operation.delete()}));
        }

        if (!client.operate(batchPolicy, records)) {
          System.err.println("Error while batch deleting key " + key);
          return Status.ERROR;
        }
        return Status.OK;
      }

      for (int i = 0; i < batchSize; i++) {
        if (!client.delete(deletePolicy, createKey(key + "_" + i))) {
          System.err.println("Record key " + key + " not found (delete)");
          return Status.ERROR;
        }
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
