/*
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

package org.apache.hudi.common.util.collection;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static java.nio.ByteBuffer.allocateDirect;

import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class LmdbMap <K extends Serializable, R extends Serializable> implements Map<K, R>  {

  private final String lmdbDbStoragePath;
  private final Env<ByteBuffer> env;
  private final Dbi<ByteBuffer> db;

  public LmdbMap(String lmdbDbStoragePath) {
    this.lmdbDbStoragePath = lmdbDbStoragePath;
    // We always need an Env. An Env owns a physical on-disk storage file. One
    // Env can store many different databases (ie sorted maps).
    env = Env.create()
        // LMDB also needs to know how large our DB might be. Over-estimating is OK.
        .setMapSize(2_000_000_000)
        // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
        .setMaxDbs(1)
        // Now let's open the Env. The same path can be concurrently opened and
        // used in different processes, but do not open the same path twice in
        // the same process at the same time.
        .open(new File(lmdbDbStoragePath), EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC);
    // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
    // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
    this.db = env.openDbi("hudi-test", MDB_CREATE);
  }

  @Override
  public R get(Object key) {
    //ValidationUtils.checkArgument(!closed);
    R fetchedVal = null;
    try {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        db.get(txn, prepareKey((K) key));
        // The fetchedVal is read-only and points to LMDB memory
        final ByteBuffer fetchBuff = txn.val();
        fetchedVal = (fetchBuff == null) ? null : SerializationUtils.deserialize(fetchBuff.array());
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    return fetchedVal;
  }

  @Override
  public R put(K key, R value) {
    try {
      long time1 = System.currentTimeMillis();
      db.put(prepareKey(key), prepareVal(value));
      long time2 = System.currentTimeMillis();
      if (time2-time1 > 50) {
        System.out.println("db put" + (time2 - time1));
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    return value;
  }

  @Override
  public int size() {
    System.out.println("db OMG");
    return -1;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    // Wont be able to store nulls as values
    return get(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("Not Supported");
  }


  @Override
  public R remove(Object key) {
    R val = null;
    try {
      val = get(key);
      db.delete(prepareKey((K) key));
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    return val;
  }

  @Override
  public void putAll(Map<? extends K, ? extends R> m) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clear() {
    db.close();
  }

  @Override
  public Set<K> keySet() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  @Override
  public Collection<R> values() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  @Override
  public Set<Entry<K, R>> entrySet() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  private ByteBuffer prepareKey(final K key) throws IOException {
    long time1 = System.currentTimeMillis();
    byte[] serializedKey = SerializationUtils.serialize(key);
    long time2 = System.currentTimeMillis();
    final ByteBuffer finalKey = allocateDirect(100);
    long time3 = System.currentTimeMillis();

    finalKey.put(serializedKey).flip();
    long time4 = System.currentTimeMillis();
    if (time4 - time1 > 100) {
      System.out.println("prepareKey" + (time4 - time3) + " " + (time3 - time2) + " " + (time2 - time1));
    }
    return finalKey;
  }

  private ByteBuffer prepareVal(final R value) throws IOException {
    long time1 = System.currentTimeMillis();
    byte[] serializedVal = SerializationUtils.serialize(value);
    long time2 = System.currentTimeMillis();
    final ByteBuffer finalVal = allocateDirect(1024);
    long time3 = System.currentTimeMillis();
    finalVal.put(serializedVal).flip();
    long time4 = System.currentTimeMillis();
    if (time4-time1 > 100) {
      System.out.println("prepareVal" + (time4 - time3) + " " + (time3 - time2) + " " + (time2 - time1));
    }
    return finalVal;
  }
}
