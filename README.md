# JavaApi
elipse to connect to hbase and operate it 
  
1.  /**
     * 判断表是否存在
     * 
     * @param table_name
     * @return
     * @throws IOException
     */
    public boolean tableExist(String table_name) public void create_table(String table_name, String[] family_names)
   
   

2.  /**
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

 


3.  /**
     * 判断表是否存在
     * 
     * @param table_name
     * @return
     * @throws IOException
     */
    public boolean tableExist(String table_name) 



4.   /**
     * 删除表
     * 
     * @param table_name
     * @throws IOException
     */
    public void deleteTable(String table_name) 





5. /**
     * 删除一条记录
     * 
     * @param table_name
     *            表名
     * @param row
     *            行主键
     * @throws IOException
     */
    public void delete_row(String table_name, String row)



6. /**
     * 删除多条记录
     * 
     * @param table_name
     *            表名
     * @param rows
     *            需要删除的行主键
     * @throws IOException
     */
    public void delMultiRows(String table_name, String[] rows)



7.  /**
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
    public Cell[] getRow(String table_name, String row, String columnfamily, String column) 



8. /**
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
    public ResultScanner scan_part_Table(String table_name, String family, String column, String startRow,String stopRow)





9.  /**
     * 获取表中的全部记录
     * 
     * @param table_name
     *            表名
     * @return  ResultScanner :该对象是表的集合，通过该对象可以得到一个表的对象Result
     * @throws IOException
     */
    public ResultScanner scanTableAllData(String table_name)


10./**
     * 显示表内容
     * 
     * @param Result
     */
    public void showCell(Result result)


11. /**
     * 获得HBase里面所有的表名
     * 
     * @return
     * @throws IOException
     */
    public List<String> getAllTables()



12./**
     * 获得表的描述
     * 
     * @param tableName
     * @return
     */
    public String describeTable(String tableName)



13. /**
     * 关闭连接
     * 
     */
   public void close()
