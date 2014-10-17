/**
 * 创建查询语句
 * 返回一个 SQL 数组，顺序从第一个表分区开始
 */


var os = require('os')
var mysql = require('mysql');

var pool = require('./pool.js');
var partition = require('./partition.js');
var util = require('../util.js');

var config = require('../config.js').config
  , database = config['database']
  , table = config['table'];





/******************************************
 * 生成SQL插入语句
 * @param object 操作数据对象（已验证） 
 */
exports.insert = function(obj, callback){
	// 表名
	var table = obj._table;
	// 获取对应库
	var db = partition.get_db_conf(table);
	if(!db){
		return callback(util.errWrap("Can't find any database , Plasce check the configure!"));
	}
	// 获取表配置
	var tbconf = partition.get_table_conf(table)
	  , partition_index = null;
	// console.log(tbconf);
	// 如果定义了分表字段
	if(tbconf && tbconf.divide){
		// 没有自增值
		var value = obj._data[tbconf.divide];
		if(value>0){
			// 通过表配置计算分区位置索引
			partition_index = partition.get_partition_index(tbconf, value);
			// 添加或验证新的表分区，没有自增值
			partition.append(table, partition_index, db, appendPartition);
		}else{
			// 需要自增值，获取所有表分区，插入最后那一个
			// 插入后获得自增值，然后校验自增值是否超出表分区
			// 如果超出表分区大小，则删除本条插入，创建新分区
			// 然后将数据插入到最新的分区内
			partition.listing(table, db, listingPartition);
		}
	}else{
		// 没有分表， 直接返回执行
		return_query(table);
	}

	// 已经建立了最新的分区
	function appendPartition(err, realtable){
		if(err){
			return callback(err);
		}
		// 可以插入，返回插入语句
		return_query(realtable);
	};

	// 得到所有表分区
	function listingPartition(err, data){
		if(err){
			return callback(err);
		}
		if(!data || !data.length){
			return callback(util.errWrap('Table "'+table+'" have no partition !'));
		}
		partition_index = data.length-1;
		var realtable = data[partition_index];
		// 可以插入，返回插入语句
		return_query(realtable);
	};
	
	// 返回sql语句
	function return_query(realtable){

		var sql = 'INSERT INTO `'+realtable+'` SET ?';

		sql = mysql.format(sql, obj._data);
		callback(null,{
			db: db,
			table: table,
			realtable: realtable,
			partition_index: partition_index,
			sql: sql
		});
	}
}



/******************************************
 * 生成SQL删除语句
 */
exports.delete = function(obj, callback){
	
}



/******************************************
 * 生成SQL修改语句
 */
exports.update = function(obj, callback){
	
}



/******************************************
 * 生成SQL查询语句
 */
exports.select = function(obj, callback){

	//获取所有表分区
	partition.listing('sound', 'dl_fm', getAllPartition);

	function getAllPartition(err, data){
		console.log(err);
		console.log(data);
	};
	
}


