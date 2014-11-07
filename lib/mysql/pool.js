/**
 * MySQL 连接池
 */

var mysql = require('mysql');

var util = require('../util.js');
var config = require('../config.js').config
  , database = config['database'];



// 数据库连接池
var poolCluster = mysql.createPoolCluster();

var readyPool = {};


/******************************************
 * 执行 sql 查询语句
 * @param db 用户定义的数据库标识
 */
exports.query = function(sql, db, callback){
	if(typeof db=='function'){
		callback = db;
		db = null;
	}
	callback = callback || function(){};
	if(!sql){
		return callback(util.errWrap('No query to do !'));
	}
	// 获取数据库配置
	db = getDBconf(db);
	// log(db);
	if(!db){
		return callback(util.errWrap('No database, Please check the configuration !'));
	}
	// 新建连接池
	var db_id = db.id;
	if(!readyPool[db_id]){
		delete db.id;
		poolCluster.add(db_id,db);
	};
	// 从池中获取连接 并 执行SQL语句
    poolCluster.getConnection(db_id, function (err, connection){
	    if(err){
	        return callback(err,null);
	    }
	    connection.query(sql, function(err, rows) {
	        // And done with the connection.
	        connection.release();
	        // Don't use the connection here, it has been returned to the pool.
	        return callback(err,rows);
	    });
	});
}

/**
 * 关闭所有连接，清除所有数据
 */
exports.destroy = function(){
	poolCluster.end();
}


/**
 * 通过各种方式获取db
 */
function getDBconf(db){
 	// 默认数据库
 	db = db || database.default;
 	if(!db) return null;
 	// 通过各种方式筛选数据库

 	if(typeof db=='number'){
 		for(var m in database.master){ //查找主库
 			var arr = database.master[m]
 			for(var a in arr){
	 			if(db==arr[a].id){
	 				return getDBconf(arr[a]);
	 			}
 			}
 		}
 		for(var s in database.slave){ //查找从库
 			var arr = database.slave[s]
 			for(var a in arr){
	 			if(db==arr[a].id){
	 				return getDBconf(arr[a]);
	 			}
 			}
 		}
 		// 没找到 则返回默认
 		return getDBconf();
 	}else if(typeof db=='string'){
 		// 通过数据库标志获取，仅获取master
 		// log(database.master[db]);
 		return getDBconf(database.master[db][0]);
 	}else if(typeof db=='object'){
 		// 配置定义对象
 		return {
			id: 'id_'+db['id'],
			host: db['host'],
			port: db['port'],
			user: db['user'],
			password: db['password'],
			database: db['database']
 		}
 	}
}


