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


//已经存在的表分区（缓存）
//用于判断表分区操作是否合法
var ready_partition = {};


/**
 * 过滤有效的表分区
 * parts = string 分区名称
 * parts = array  分区名称数组
 * parts = object 分区做键名的字典
 * opt = {
	ignore: // 不更新数据，忽略没有的
 }
 */
var effective = exports.effective = function(table, parts, callback, ignore){

	// 是否存在没有验证的表分区
	var update = [] //是否需要更新的列表
	  , isarr = (parts instanceof Array)
	  ;

	for(var p in parts){
		var ti = isarr 
			? (parts[p] instanceof Array ? parts[p][0] : parts[p]) 
			: p;
		// 包含没验证的表
		if(!ready_partition[ti]){
			update.push(p);
		}
	}

	// 全部通过验证 返回
	if(0==update.length){
		return callback(null, parts);
	}
	//忽略未通过的（不查库更新）
	if(ignore){
		for(var p in update){
			//挨个删除不存在的
			if(isarr){
				parts.splice(p-1,1);
			}else{
				delete parts[p];
			}
		}
		return callback(null, parts);
	}

	//从数据库获取最新的表分区并缓存至内存
	get_table_partition(table, function(err, data){
		if(err){
			return callback(err);
		}
		//递归调用
		var ignore = true; //不查库更新数据 防止递归死循环
		return effective(table, parts, callback, ignore);
	});
}




/**
 * 获取表配置
 */
var table_conf = {}; //表配置缓存
var get_table_conf = exports.get_table_conf = function(table){
	// 查看缓存
	if(table_conf[table]){
		return table_conf[table];
	}
	// 搜索查找
	for(var t in config.table){
		var tb = config.table[t];
		if(tb.table==table){
			// 缓存
			table_conf[table] = tb;
			// 返回
			return tb;
		}
	}
	return null;
}




/**
 * 通过表名 获取数据库配置
 */
var get_db_conf = exports.get_db_conf = function(table, slave){
	// console.log(config.database.master);
	var type = slave ? 'slave' : 'master'
	  , tbconf = get_table_conf(table)
	  , dbname = tbconf ? tbconf.db : '';
	// 找不到定义则使用默认数据库
	dbname = dbname || config.database_default;
	// 开始搜索从库
	var list = config.database[type];
	if(dbname && list[dbname] && list[dbname].length>0){
		if(slave){
			//随机取一个从库
			var db = util.array_rand(list[dbname]);
			db.is_slave = true; //标记
		}else{
			return list[dbname][0]; //仅仅一主多从
		}
	}
	// 从库未找到 从新搜索主库
	if(slave){
		return get_db_conf(table);
	}
	//未找到
	return null;
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
 * 获取所有表分区，并排序
 * @param
 */
var get_table_partition = exports.get_table_partition = function(table, callback, opt){
	opt = opt || {};
	var db = get_db_conf(table);
	// 未配置或未分区
	if(!db){
		return callback(null, null);
	}
	var tb = get_table_conf(table);
	// 未配置或未分区
	if(!tb || !tb['section']){
		return callback(null, null);
	}
	// 查询所有分区
	listing(table, db, callback, opt);
}



/**
 * 获取所有表分区
 * @param
 */
var listing = exports.listing = function(table, db, callback, opt){
	opt = opt || {};
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
			for(var d in data){
				for(var n in data[d]){
					var tb = data[d][n]+'';
					// console.log(tb);
					// die();
					var tbr = tb.substr(table.length+1)
					  , tbnum = parseInt(tbr);
					// console.log(data[d][n]);
					if(!isNaN(tbr) && tbnum>=0){
						// 自定义区间
						if(opt.min&&tbnum<opt.min  || opt.max&&tbnum>opt.max)  continue;
						tblist[tbnum] = data[d][n]; //自动排序
					}
					break;
				}
			}
		}
		var redata = []; 
		for(var t in tblist){
			var realtb = tblist[t];
			if(realtb){ //删除无效的
				redata.push(realtb);
				// 缓存存在的表分区
				if(!ready_partition[realtb]){
					ready_partition[realtb] = 1;
				}
			}
		}
		// console.log(tblist);
		return callback(null, redata.length ? redata : null);
	});
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
 * 添加一个索引为 index 的表分区
 */
exports.append = function(table, index, db, callback){
	// 从第一个表分区结构创建最新的分区
	// 这里无需设置自增量，后面拷贝移动数据时，会自动“撑大”自增量！
	var roottable = table+'_0'
	  , realtable = table+'_'+index;
	// 根表
	if(realtable==roottable){
		return callback(null, realtable);
	}
	// 从缓存检查表是否已经创建
	if(ready_partition[realtable]){
		return callback(null, realtable);
	}
	// 创建数据库（从根表复制表结构）
	var sql = 'CREATE TABLE IF NOT EXISTS '+realtable+' LIKE '+roottable;
	pool.query(sql, db, function(err, data){
		if(err){
			return callback(err);
		}
		return callback(null, realtable);
	});
}



