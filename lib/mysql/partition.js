/**
 * 跟分区相关的操作
 */

var mysql = require('mysql');

var pool = require('./pool.js');
var build = require('./build.js');
var util = require('../util.js');

var config = require('../config.js').config
  , database = config['database']
  , table = config['table'];





/**
 * 获取表配置
 */
var get_table_conf = exports.get_table_conf = function(table){
	for(var t in config.table){
		var tb = config.table[t];
		if(tb.table==table){
			return tb;
		}
	}
	return null;
}




/**
 * 通过表名 获取数据库配置
 */
exports.get_db_conf = function(table, slave){
	// console.log(config.database.master);
	var type = slave ? 'slave' : 'master'
	  , tbconf = get_table_conf(table)
	  , dbname = tbconf ? tbconf.db : '';
	// 找不到定义则使用默认数据库
	dbname = dbname || config.database_default;
	// 正式开始寻找
	var list = config.database[type];
	if(dbname && list[dbname] && list[dbname].length>0){
		return slave 
			? util.array_rand(list[dbname])
			: list[dbname][0]; //仅仅一主多从
	}else{
		return null;
	}
}





/**
 * 获取表分区索引
 * @param
 */
var get_partition_index = exports.get_partition_index = function(table, value){
	// 获取表配置
	if(typeof table!='object')
		table = get_table_conf(table);
	// 计算表分区
	var ptt = parseInt( (value-1) / table.section);
	return ptt;
}



/**
 * 获取真实表的自增量
 * @param
 */
exports.get_auto_increment = function(realtable, db, callback){
	var sql = 'SHOW TABLE STATUS LIKE "'+realtable+'"';
	//正式开始查找相关表
	pool.query(sql, db, function(err, data){
		if(err){
			return callback(err);
		}

		console.log(err);
		console.log(data);
		callback(null, data);

	});
}




/**
 * 添加一个最新的表分区
 * @param increment 表示增加自增索引
 */
exports.append = function(table, index, db, callback){
	// 从第一个表分区结构创建最新的分区
	var realtable = table+'_'+index
	  , sql = 'CREATE TABLE IF NOT EXISTS '+realtable+' LIKE '+table+'_0';
	// 这里无需设置自增量，后面拷贝移动数据时，会自动“撑大”自增量！
	// console.log(sql);
	pool.query(sql, db, function(err, data){
		if(err){
			return callback(err);
		}
		// console.log(err);
		// console.log(data);
		callback(null, realtable);
	});
}


/**
 * 获取所有表分区
 * @param
 */
exports.listing = function(table, db, callback){
	var sql = 'SHOW TABLES LIKE "'+table+'\_%"';
	// 查询所有表分区
	pool.query(sql, db, function(err, data){
		if(err){
			return callback(err);
		}
		var tblist = [];
		// console.log(isNaN(0));
		// die();
		if(data && data.length>0){
			tblist.length = data.length;
			for(var d in data){
				for(var n in data[d]){
					// console.log(data[d][n]);
					var tbr = data[d][n].substr(table.length+1)
					  , tbnum = parseInt(tbr);
					// console.log(data[d][n]);
					if(!isNaN(tbr) && tbnum>=0){
						tblist[tbnum] = data[d][n]; //自动排序
					}
					break;
				}
			}
		}
		// console.log(tblist);
		return callback(null, tblist);
	});
}




