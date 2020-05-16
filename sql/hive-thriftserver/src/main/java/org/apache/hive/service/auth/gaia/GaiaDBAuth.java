/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.auth.gaia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;

public class GaiaDBAuth implements PasswdAuthenticationProvider {
    public static final String AUTH_METADATA_TABLE = "gaia_auth_catalog";
    public static final String AUTH_TABLE_TAIL = "auth_user";
    public static final String AUTH_TABLE_CF = "d";
    public static final String AUTH_TABLE_CELL = "c";
    public static final String HIVE_CONF_ZK = "spark.auth.zk.quorum";
    public static final String HIVE_CONF_AUTH_CATALOG = "spark.auth.catalog";
    public static final String HIVE_CONF_AUTH_GROUP = "spark.auth.group";
    public static final String HIVE_CONF_AUTH_GROUP_KEY = "spark.auth.group.key";

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        if (user == null || password == null) {
            throw new AuthenticationException("auth param is null...");
        }
        Configuration conf = new Configuration(new HiveConf());
        String zk = conf.get(HIVE_CONF_ZK);
        if (zk == null) {
            throw new AuthenticationException("hive [" + HIVE_CONF_ZK + "] config not exist...");
        }
        String catalog = conf.get(HIVE_CONF_AUTH_CATALOG);
        if (catalog == null) {
            throw new AuthenticationException("hive [" + HIVE_CONF_AUTH_CATALOG + "] config not exist...");
        }
        String pwd = null;
        GaiaAuthTool.HBaseMgr mgr = null;
        try {
            mgr = new GaiaAuthTool.HBaseMgr(zk, conf.get(HIVE_CONF_AUTH_GROUP_KEY));
            String tableName = getTableName(catalog);
            if (mgr.existTable(tableName)) {
                Get get = new Get(Bytes.toBytes(user));
                Result result = mgr.getValue(tableName, get);
                if (result != null) {
                    byte[] bb = result.getValue(Bytes.toBytes(AUTH_TABLE_CF), Bytes.toBytes(AUTH_TABLE_CELL));
                    pwd = Bytes.toString(bb);
                }
            } else {
                throw new AuthenticationException("auth error, table [" + AUTH_TABLE_TAIL + "] not exist...");
            }
        } catch (IOException ex) {
            throw new AuthenticationException("auth error...", ex);
        } finally {
            try {
                if (mgr != null) {
                    mgr.close();
                }
            } catch (IOException e) {
            }
        }

        String pwdEnc = GaiaAuthTool.encode(password);
        if (!pwdEnc.equals(pwd)) {
            throw new AuthenticationException("auth check fail...");
        }
    }

    public static String getTableName(String catalog) {
        return catalog + "_" + AUTH_TABLE_TAIL;
    }
}
