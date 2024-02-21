package com.atguigu.gmall.realtime.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

@Slf4j
public class HBaseUtil {


    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);
    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String family) throws IOException {

        if (StringUtils.isBlank(nameSpace) || StringUtils.isBlank(table) || StringUtils.isBlank(family)) {
            log.error("参数为空，不创建表，nameSpace={},table={},family={}", nameSpace, table, family);
            return;
        }
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        // 判断要建的表是否存在
        if (admin.tableExists(tableName)) {
            log.info("表 已经存在，跳过创建表：{}", tableName);
            return;
        }
        // 列族描述器
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
        // 表的描述器
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc) // 给表设置列族
                .build();
        admin.createTable(desc);
        admin.close();
        log.info(nameSpace + " " + table + " 建表成功");
    }

    public static void dropHBaseTable(Connection hbaseConn,
                                      String nameSpace,
                                      String table) throws IOException {

        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.close();
        log.info(nameSpace + " " + table + " 删除成功");

    }
}
