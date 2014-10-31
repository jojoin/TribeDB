/**
 * 数据处理/查询模式
 */


/**
 * 模式定义
 */
var define = [
	'TRIBEDB',
	/**** MySQL 可使用的模式 ****/
	,'MYSQL_DELAYED' //mysql 延迟插入

	/****  ****/

	//分区验证，将会过滤掉sql语句中 通过分区主键映射到的不存在的分区过滤掉
	//注意：在通过表分区主键映射待处理分区时，此模式会在操作之前增加一次 "show tables" 请求！
	//
	,'PARTITION_VERIFY' 

	/**** 数据库查询方式 ****/

	,'SCAN_PARTITION_RANDOW' //随机方式扫描表分区
	,'SCAN_PARTITION_ASC'   //升序方式查询表分区
	,'SCAN_PARTITION_DESC'  //降序方式查询表分区




];


/**************************************/



var mode = {};
for(var k in define){
	mode[define[k]] = k;
}


//处理 
module.exports = mode;

