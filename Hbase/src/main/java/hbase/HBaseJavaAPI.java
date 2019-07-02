package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HBaseJavaAPI {

    public static  Connection getCon() throws IOException {
        //读取配置文件信息,设置配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","centos.server1.cao");
        //获取连接HBase
        Connection conn = ConnectionFactory.createConnection(conf);
        return  conn;
    }

    
    public static void main(String[] args) throws IOException {
		getCon();
	}
    
    @Test
    public  void createTable ()throws IOException {
        //获取管理员，执行ddl
        Admin admin = getCon().getAdmin();
        //创建一个命名空间描述器，指定名称为class
        NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create("class").build();
        //创建一个命名空间
        admin.createNamespace(nsDescriptor);
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        //创建列族描述器
        HColumnDescriptor info = new HColumnDescriptor("info");
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        //创建表格描述器,将名称添加进去,将列族添加进去
        HTableDescriptor tbDescriptor = new HTableDescriptor(tableName);
        tbDescriptor.addFamily(info);
        //创建一张表
        admin.createTable(tbDescriptor);
        admin.addColumn(tableName,f1);
    }

    @Test
    public  void dropTable ()throws IOException {
        //获取管理员，执行ddl
        Admin admin = getCon().getAdmin();
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        boolean isDisable = admin.isTableDisabled(tableName);
        if(!isDisable){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.deleteNamespace("class");
    }

    @Test
    public void putData() throws IOException {
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        //获取Table对象，执行DML语言
        Table table = getCon().getTable(tableName);
        //创建put对象
        Put put = new Put("1001".getBytes());
        //获取所有的列族
        HColumnDescriptor[] cfs = table.getTableDescriptor().getColumnFamilies();
        for (HColumnDescriptor s:cfs) {
            //条件判断进行插入数据
            if (s.getNameAsString().equals("info")){
                put.addColumn("info".getBytes(),"name".getBytes(),"beifeng".getBytes());
            }else{
                put.addColumn("f1".getBytes(),"age".getBytes(),"19".getBytes());
            }
        }
        //提交
        table.put(put);
    }

    /**
     * delete可以直接删除一行数据
     * delete可以直接删除整个列族数据
     * delete可以直接列数据
     */
    @Test
    public void delData() throws IOException {
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        //获取Table对象，执行DML语言
        Table table = getCon().getTable(tableName);
        //创建delete
        Delete delete = new Delete("1001".getBytes());
        delete.addFamily("f1".getBytes());
        delete.addColumn("info".getBytes(),"name".getBytes());
        //提交
        table.delete(delete);
    }

    @Test
    public void getData() throws IOException {
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        //获取Table对象，执行DML语言
        Table table = getCon().getTable(tableName);
        //创建get对象
        Get get = new Get("1001".getBytes());
        //get.addFamily("info".getBytes());
        get.addColumn("f1".getBytes(),"age".getBytes());
        //提交
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell:cells) {
            System.out.print("主键："+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
            System.out.print("列族："+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
            System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
            System.out.print("时间戳："+cell.getTimestamp()+"\t");
            System.out.print("值："+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println();
        }
    }

    @Test
    public void scanData() throws IOException {
        //创建TableName，指定表的名称
        TableName tableName = TableName.valueOf("class:test");
        //获取Table对象，执行DML语言
        Table table = getCon().getTable(tableName);
        //创建一个scan对象
        Scan scan = new Scan();

       // scan.setStartRow("1002".getBytes());
       // scan.setStopRow("1003".getBytes());

       // scan.addColumn("info".getBytes(),"name".getBytes());

        scan.setTimeRange(1543030712666l,1543030733828l);
       // scan.setTimeStamp(1543030712666l);

       // scan.addFamily("f1".getBytes());


        ResultScanner scanner = table.getScanner(scan);
        for (Result rs:scanner) {
            Cell[] cells = rs.rawCells();
            for (Cell cell:cells) {
                System.out.print("主键："+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
                System.out.print("列族："+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.print("时间戳："+cell.getTimestamp()+"\t");
                System.out.print("值："+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }

    }
}
