package pa01;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import  org.apache.hadoop.hbase.client.*;



  
public class Operator {  

   
    private Connection connection;
    private Admin admin;
    private String HBASEENCODE = "utf8";// 编码格式
    private static Configuration conf;
    /**
	  * 初始化配置
	 */
	 static {
	    conf = HBaseConfiguration.create();
	   
	 }



    /**
     * 构造函数。初始化，参数是配置文件的路径，初始化之后admin就可以使用了
     * 
     * @param conf
     *            是配置文件的路径，配置文件就是集群Linux目录里面 HBase文件夹下conf里面的hbase-site.xml
     *            将这个文件拷贝到自己的电脑里面，然后参数就是这个文件的路径，打包的时候将这个文件一起打包，所以最好是直接放在这个工程的文件夹下
     *            这个方法是适合各种变化的环境，只要复制hbase-site.xml就可以在任意集群上跑。当然如果集群是不变的，也可以按照上面的写法给写死了
     * @throws IOException
     */
 public Operator()  throws IOException {
    	connection=ConnectionFactory.createConnection(conf);
    	admin=connection.getAdmin();
    }

    /**
     * 创建一个表
     * 
     * @param table_name
     *            表名称
     * @param family_names
     *            列族名称集合
     * @throws IOException
     */
    public void create_table(String table_name, String[] family_names) throws IOException {
        if (!tableExist(table_name)) {
            // 获取TableName
            TableName tableName = TableName.valueOf(table_name);
            // table 描述
            HTableDescriptor htabledes = new HTableDescriptor(tableName);
            for (String family_name : family_names) {
                // column 描述
                HColumnDescriptor family = new HColumnDescriptor(family_name);
                htabledes.addFamily(family);
            }
            admin.createTable(htabledes);
        }
    }

    /**
     * 增加一条记录
     * 
     * @param table_name
     *            表名称
     * @param row
     *            行主键，用|拼接
     * @param columnFamily
     *            列族名称
     * @param column
     *            列名(可以为null)
     * @param value
     *            值
     * @throws IOException
     */
    public void addData(String table_name, String row, String columnFamily, String column, String value)
            throws IOException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(row.getBytes(this.HBASEENCODE));// 指定行
        put.addColumn(columnFamily.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE),
                value.getBytes(this.HBASEENCODE));
        table.put(put);
        
    }

    /**
     * 判断表是否存在
     * 
     * @param table_name
     * @return
     * @throws IOException
     */
    public boolean tableExist(String table_name) throws IOException {
        return admin.tableExists(TableName.valueOf(table_name));
    }

    /**
     * 删除表
     * 
     * @param table_name
     * @throws IOException
     */
    public void deleteTable(String table_name) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除成功");
        }
        else{
        	System.out.println("要删除的表不存在！");
        }
    }

    /**
     * 删除一条记录
     * 
     * @param table_name
     *            表名
     * @param row
     *            行主键
     * @throws IOException
     */
    public void delete_row(String table_name, String row) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Delete del = new Delete(row.getBytes(this.HBASEENCODE));
        table.delete(del);
    }

    /**
     * 删除多条记录
     * 
     * @param table_name
     *            表名
     * @param rows
     *            需要删除的行主键
     * @throws IOException
     */
    public void delMultiRows(String table_name, String[] rows) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        List<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete del = new Delete(row.getBytes(this.HBASEENCODE));
            delList.add(del);
        }
        table.delete(delList);
    }

    /**
     * 查询单个row的记录
     * 
     * @param table_name
     *            表名
     * @param row
     *            行键
     * @param columnfamily
     *            列族，可以为null
     * @param column
     *            列名，可以为null
     * @return
     * @throws IOException
     */
    public Cell[] getRow(String table_name, String row, String columnfamily, String column) throws IOException {

        // table_name和row不能为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(row)) {
            return null;
        }
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(row.getBytes(this.HBASEENCODE));
        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addColumn(columnfamily.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
            get.addFamily(columnfamily.getBytes(this.HBASEENCODE));
        }

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        return cells;
    }

    /**
     * 获取表中的部分记录,可以指定列族,列族成员,开始行键,结束行键.
     * 
     * @param table_name
     *            表名
     * @param family
     *            列簇
     * @param column
     *            列名
     * @param startRow
     *            开始行主键
     * @param stopRow
     *            结束行主键
     * @return
     * @throws Exception
     */
    public ResultScanner scan_part_Table(String table_name, String family, String column, String startRow,
            String stopRow) throws IOException {
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
            scan.addColumn(family.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(family) && StringUtils.isEmpty(column)) {
            scan.addFamily(family.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(startRow)) {
            scan.setStartRow(startRow.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(stopRow)) {
            scan.setStopRow(stopRow.getBytes(this.HBASEENCODE));
        }
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }

    /**
     * 获取表中的全部记录
     * 
     * @param table_name
     *            表名
     * @return  ResultScanner :该对象是表的集合，通过该对象可以得到一个表的对象Result
     * @throws IOException
     */
    public ResultScanner scanTableAllData(String table_name) throws IOException {
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;

    }
    /**
     * 显示表内容
     * 
     * @param Result
     */
    public void showCell(Result result){
    	Cell[] cells = result.rawCells();
    	   for(Cell cell:cells){
    		   System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
    		   System.out.println("Timetamp:"+cell.getTimestamp()+" ");
    		   System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
    		   System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
    		   System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
    		   }
    	   }

    /**
     * 获得HBase里面所有的表名
     * 
     * @return  List<String>
     * @throws IOException
     */
    public List<String> getAllTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] allTable = admin.listTables();
            if (allTable.length > 0) {
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                }
            }
        }
        return tables;
    }

    /**
     * 获得表的描述
     * 
     * @param tableName
     * @return string
     */
    public String describeTable(String tableName) {
        try {
            return admin.getTableDescriptor(TableName.valueOf(tableName)).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }



    /**
     * 
     */
   
	   public void close() throws IOException {
	
	        try {
	            if (admin != null)
	                admin.close();
	            if (connection != null)
	                connection.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }  
/*public static void main(String [] args) throws IOException{
			Operator adm=new Operator();
			String tablename="te";
			String []fam={"name","age","likeblue"};
			adm.create_table(tablename, fam);
			List<String>tablelist=adm.getAllTables();
			for(String i:tablelist){
				System.out.println(adm.describeTable(i));
			}
			
				adm.addData(tablename, "one21", "name", "firstn1a3me", "pe1tter");
				adm.addData(tablename, "one31", "name", "lastna13me", "Pette");
				adm.addData(tablename, "one31", "age", "g", "121");
				adm.addData(tablename, "one31", "likeblue", "hg1", "yel1lo");
			adm.delete_row(tablename, "one");
				ResultScanner hResultScanner=adm.scanTableAllData(tablename);
			
				for(Result i:hResultScanner){
					adm.showCell(i);
					System.out.println("*************\n");
					
				}
				System.out.println(adm.tableExist(tablename));
				adm.deleteTable("test2");
				adm.close();
			  
}*/
}  
