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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.security.sasl.AuthenticationException;
import java.io.*;
import java.util.*;

public class GaiaAuthTool {
    private static final String SEPARATOR = "=";

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            System.out.println(
                    "cmd format: \n"
                            + "add <user> <pwd> \n"
                            + "delete <user> \n"
                            + "change <user> <pwd> \n"
                            + "show \n"
                            + "backup <dir> \n"
                            + "recover <path> \n"
            );
            System.exit(1);
        }

        String cmd = args[0];
        if ("add".equalsIgnoreCase(cmd)) {
            if (args.length < 3) {
                System.out.println("cmd format: add <user> <pwd>");
                System.exit(1);
            }
            String user = args[1];
            String pwd = args[2];
            if (user.contains(SEPARATOR)) {
                System.out.println("** user: [" + user + "] not contains '=' character **");
                System.exit(1);
            }
            try {
                new GaiaAuthTool().addUser(user, pwd);
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else if ("delete".equalsIgnoreCase(cmd)) {
            if (args.length < 2) {
                System.out.println("cmd format: delete <user>");
                System.exit(1);
            }
            String user = args[1];
            try {
                new GaiaAuthTool().deleteUser(user);
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else if ("change".equalsIgnoreCase(cmd)) {
            if (args.length < 3) {
                System.out.println("cmd format: change <user> <pwd>");
                System.exit(1);
            }
            String user = args[1];
            String pwd = args[2];
            try {
                new GaiaAuthTool().changePwd(user, pwd);
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else if ("show".equalsIgnoreCase(cmd)) {
            try {
                Map<String, String> users = new GaiaAuthTool().queryUsers();
                System.out.println("** user list: ");
                Iterator<String> it = users.keySet().iterator();
                while (it.hasNext()) {
                    System.out.println("** user: [" + it.next() + "] **");
                }
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else if ("backup".equalsIgnoreCase(cmd)) {
            if (args.length < 2) {
                System.out.println("cmd format: backup <dir>");
                System.exit(1);
            }
            String dir = args[1];
            try {
                new GaiaAuthTool().backupAll(dir);
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else if ("recover".equalsIgnoreCase(cmd)) {
            if (args.length < 2) {
                System.out.println("cmd format: recover <path>");
                System.exit(1);
            }
            String path = args[1];
            try {
                new GaiaAuthTool().recoverAll(path);
                System.out.println("** succeed cmd: [" + cmd + "] **");
            } catch (AuthenticationException ae) {
                ae.printStackTrace();
            }
        } else {
            System.out.println("** unsupported cmd: [" + cmd + "] **");
        }
    }

    void addUser(String userName, String password) throws AuthenticationException {
        String[] cfg = getCfg();
        String catalog = cfg[3];
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                mgr.createTable(GaiaDBAuth.AUTH_METADATA_TABLE, cfg[1], GaiaDBAuth.AUTH_TABLE_CF);
            }
            //check user table
            String tableName = GaiaDBAuth.getTableName(catalog);
            if (!mgr.existTable(tableName)) {
                mgr.createTable(tableName, cfg[1], GaiaDBAuth.AUTH_TABLE_CF);
            }
            //add catalog metadata
            Get get = new Get(Bytes.toBytes(catalog));
            Result r = mgr.getValue(GaiaDBAuth.AUTH_METADATA_TABLE, get);
            if (r == null || r.isEmpty()) {
                Put put = new Put(Bytes.toBytes(catalog));
                put.addColumn(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL), Bytes.toBytes(tableName));
                mgr.setValue(GaiaDBAuth.AUTH_METADATA_TABLE, put);
            }
            //check user
            get = new Get(Bytes.toBytes(userName));
            Result result = mgr.getValue(tableName, get);
            if (result != null && !result.isEmpty()) {
                throw new AuthenticationException("user [" + userName + "] exist...");
            }
            //add user
            String pwdEnc = encode(password);
            Put put = new Put(Bytes.toBytes(userName));
            put.addColumn(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL), Bytes.toBytes(pwdEnc));
            mgr.setValue(tableName, put);
        } catch (IOException ex) {
            throw new AuthenticationException("add user error...", ex);
        } finally {
            close(mgr);
        }
    }

    void deleteUser(String userName) throws AuthenticationException {
        String[] cfg = getCfg();
        String catalog = cfg[3];
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                throw new AuthenticationException("table [" + GaiaDBAuth.AUTH_METADATA_TABLE + "] not exist...");
            }
            //check user table
            String tableName = GaiaDBAuth.getTableName(catalog);
            if (!mgr.existTable(tableName)) {
                throw new AuthenticationException("table [" + tableName + "] not exist...");
            }
            //check user
            Get get = new Get(Bytes.toBytes(userName));
            Result result = mgr.getValue(tableName, get);
            if (result == null || result.isEmpty()) {
                throw new AuthenticationException("user [" + userName + "] not exist...");
            }
            //delete user
            Delete delete = new Delete(Bytes.toBytes(userName));
            mgr.delete(tableName, delete);
        } catch (IOException ex) {
            throw new AuthenticationException("delete user error...", ex);
        } finally {
            close(mgr);
        }
    }

    boolean checkUser(String userName) throws AuthenticationException {
        String[] cfg = getCfg();
        String catalog = cfg[3];
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                return false;
            }
            //check user table
            String tableName = GaiaDBAuth.getTableName(catalog);
            if (!mgr.existTable(tableName)) {
                return false;
            }
            //check user
            Get get = new Get(Bytes.toBytes(userName));
            Result result = mgr.getValue(tableName, get);
            return !(result == null || result.isEmpty());
        } catch (IOException ex) {
            throw new AuthenticationException("check user error...", ex);
        } finally {
            close(mgr);
        }
    }

    void changePwd(String userName, String pwd) throws AuthenticationException {
        String[] cfg = getCfg();
        String catalog = cfg[3];
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                throw new AuthenticationException("table [" + GaiaDBAuth.AUTH_METADATA_TABLE + "] not exist...");
            }
            //check user table
            String tableName = GaiaDBAuth.getTableName(catalog);
            if (!mgr.existTable(tableName)) {
                throw new AuthenticationException("table [" + tableName + "] not exist...");
            }
            //check user
            Get get = new Get(Bytes.toBytes(userName));
            Result result = mgr.getValue(tableName, get);
            if (result == null || result.isEmpty()) {
                throw new AuthenticationException("user [" + userName + "] not exist...");
            }
            //add user
            String pwdEnc = encode(pwd);
            Put put = new Put(Bytes.toBytes(userName));
            put.addColumn(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL), Bytes.toBytes(pwdEnc));
            mgr.setValue(tableName, put);
        } catch (IOException ex) {
            throw new AuthenticationException("change user error...", ex);
        } finally {
            close(mgr);
        }
    }

    Map<String, String> queryUsers() throws AuthenticationException {
        String[] cfg = getCfg();
        String catalog = cfg[3];
        Map<String, String> users = new HashMap<>(2);
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                throw new AuthenticationException("table [" + GaiaDBAuth.AUTH_METADATA_TABLE + "] not exist...");
            }
            //check user table
            String tableName = GaiaDBAuth.getTableName(catalog);
            if (!mgr.existTable(tableName)) {
                throw new AuthenticationException("table [" + tableName + "] not exist...");
            }
            // read and write
            List<Result> results = mgr.scan(tableName, new Scan());
            for (Result r : results) {
                String user = Bytes.toString(r.getRow());
                byte[] bb = r.getValue(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL));
                String pwd = Bytes.toString(bb);
                users.put(user, pwd);
            }
            return users;
        } catch (IOException ex) {
            throw new AuthenticationException("query users error...", ex);
        } finally {
            close(mgr);
        }
    }

    void backupAll(String path) throws AuthenticationException {
        File file = new File(path);
        String filePath = null;
        if (file.isFile()) {
            file.getParentFile().mkdirs();
            filePath = file.getParent();
        } else {
            file.mkdirs();
            filePath = file.getPath();
        }
        filePath += File.separator + GaiaDBAuth.AUTH_METADATA_TABLE + "_" + System.currentTimeMillis() + ".bak";

        String[] cfg = getCfg();
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                throw new AuthenticationException("table [" + GaiaDBAuth.AUTH_METADATA_TABLE + "] not exist...");
            }
            // read and write to file
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
            List<Result> rs = mgr.scan(GaiaDBAuth.AUTH_METADATA_TABLE, new Scan());
            for (Result rr : rs) {
                String catalog = Bytes.toString(rr.getRow());
                byte[] rbb = rr.getValue(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL));
                String tableName = Bytes.toString(rbb);
                if (mgr.existTable(tableName)) {
                    List<Result> results = mgr.scan(tableName, new Scan());
                    for (Result r : results) {
                        String user = Bytes.toString(r.getRow());
                        byte[] bb = r.getValue(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL));
                        String pwd = Bytes.toString(bb);
                        bw.write(catalog + SEPARATOR + tableName + ";" + user + SEPARATOR + pwd);
                        bw.newLine();
                    }
                } else {
                    System.out.println("** back table: [" + tableName + "] not exist... **");
                }
            }
            bw.close();
        } catch (IOException ex) {
            throw new AuthenticationException("backup users error...", ex);
        } finally {
            close(mgr);
        }
    }

    void recoverAll(String path) throws AuthenticationException {
        File file = new File(path);
        if (!file.exists()) {
            throw new AuthenticationException("file [" + path + "] not exist...");
        }

        String[] cfg = getCfg();
        HBaseMgr mgr = null;
        try {
            mgr = new HBaseMgr(cfg[0], cfg[2]);
            //check metadata table
            if (!mgr.existTable(GaiaDBAuth.AUTH_METADATA_TABLE)) {
                mgr.createTable(GaiaDBAuth.AUTH_METADATA_TABLE, cfg[1], GaiaDBAuth.AUTH_TABLE_CF);
            }
            // read from file and write
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] lines = line.split(";");
                if (lines.length != 2) {
                    System.out.println("** recover data [" + line + "] invalid **");
                    continue;
                }
                String[] lines2 = lines[0].split(SEPARATOR);
                if (lines2.length != 2) {
                    System.out.println("** recover data [" + line + "] invalid **");
                    continue;
                }
                String[] lines3 = lines[1].split(SEPARATOR);
                if (lines3.length != 2) {
                    System.out.println("** recover data [" + line + "] invalid **");
                    continue;
                }
                //check catalog
                String catalog = lines2[0];
                String tableName = GaiaDBAuth.getTableName(catalog);
                if (!tableName.equals(lines2[1])) {
                    br.close();
                    throw new AuthenticationException("recover data [" + line + "] invalid...");
                }
                //add catalog metadata
                Get get = new Get(Bytes.toBytes(catalog));
                Result r = mgr.getValue(GaiaDBAuth.AUTH_METADATA_TABLE, get);
                if (r == null || r.isEmpty()) {
                    Put put = new Put(Bytes.toBytes(catalog));
                    put.addColumn(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL), Bytes.toBytes(tableName));
                    mgr.setValue(GaiaDBAuth.AUTH_METADATA_TABLE, put);
                }
                //add user
                if (!mgr.existTable(tableName)) {
                    mgr.createTable(tableName, cfg[1], GaiaDBAuth.AUTH_TABLE_CF);
                }
                Put put = new Put(Bytes.toBytes(lines3[0]));
                put.addColumn(Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CF), Bytes.toBytes(GaiaDBAuth.AUTH_TABLE_CELL), Bytes.toBytes(lines3[1]));
                mgr.setValue(tableName, put);
            }
            br.close();
        } catch (IOException ex) {
            throw new AuthenticationException("recover users error...", ex);
        } finally {
            close(mgr);
        }
    }

    private void close(HBaseMgr mgr) {
        try {
            if (mgr != null) {
                mgr.close();
            }
        } catch (IOException e) {
        }
    }

    private String[] getCfg() throws AuthenticationException {
        Configuration conf = new Configuration(new HiveConf());
        String zk = conf.get(GaiaDBAuth.HIVE_CONF_ZK);
        if (zk == null) {
            throw new AuthenticationException("hive [" + GaiaDBAuth.HIVE_CONF_ZK + "] config not exist...");
        }
        String catalog = conf.get(GaiaDBAuth.HIVE_CONF_AUTH_CATALOG);
        if (catalog == null) {
            throw new AuthenticationException("hive [" + GaiaDBAuth.HIVE_CONF_AUTH_CATALOG + "] config not exist...");
        }
        String group = conf.get(GaiaDBAuth.HIVE_CONF_AUTH_GROUP);
        if (group == null) {
            throw new AuthenticationException("hive [" + GaiaDBAuth.HIVE_CONF_AUTH_GROUP + "] config not exist...");
        }
        String[] result = {zk, group, conf.get(GaiaDBAuth.HIVE_CONF_AUTH_GROUP_KEY), catalog};
        return result;
    }

    public static String encode(String str) {
        return DigestUtils.sha256Hex(str);
    }

    public static class HBaseMgr {
        private Admin admin;
        private Connection conn;
        private String groupKey = "GROUP";

        public HBaseMgr(String zk, String groupKey) throws IOException {
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, zk);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            if (groupKey != null && !groupKey.isEmpty()) {
                this.groupKey = groupKey;
            }
        }

        public void close() throws IOException {
            admin.close();
            conn.close();
        }

        public boolean existTable(String tableName) throws IOException {
            return admin.tableExists(TableName.valueOf(tableName));
        }

        public Table getTable(String tableName) throws IOException {
            return conn.getTable(TableName.valueOf(tableName));
        }

        public void createTable(String tableName, String group, String... cks) throws IOException {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            if (group != null) {
                tableDescriptor.setValue(groupKey, group);
            }
            addColumn(tableDescriptor, cks);
            admin.createTable(tableDescriptor);
        }

        public void deleteTable(String tableName) throws IOException {
            TableName name = TableName.valueOf(tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
        }

        private void addColumn(HTableDescriptor tableDescriptor, String... cks) {
            HColumnDescriptor columnDescriptor = null;
            for (String ck : cks) {
                byte[] bck = Bytes.toBytes(ck);
                if (!tableDescriptor.hasFamily(bck)) {
                    columnDescriptor = new HColumnDescriptor(bck);
                    tableDescriptor.addFamily(columnDescriptor);
                }
            }
        }

        public void setValue(String tableName, Put put) throws IOException {
            Table table = this.getTable(tableName);
            try {
                table.put(put);
            } finally {
                table.close();
            }
        }

        public Result getValue(String tableName, Get get) throws IOException {
            Result result = null;
            Table table = this.getTable(tableName);
            try {
                return table.get(get);
            } finally {
                table.close();
            }
        }

        public void delete(String tableName, Delete delete) throws IOException {
            Table table = this.getTable(tableName);
            try {
                table.delete(delete);
            } finally {
                table.close();
            }
        }

        public List<Result> scan(String tableName, Scan scan) throws IOException {
            List<Result> result = new ArrayList<>(2);
            Table table = this.getTable(tableName);
            try {
                ResultScanner rs = table.getScanner(scan);
                for (Result r : rs) {
                    result.add(r);
                }
                rs.close();
            } finally {
                table.close();
            }
            return result;
        }
    }
}
