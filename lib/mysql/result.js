/**
 * 对查询出的原始数据进行再处理
 * 例如分页排序等
 */

var pool = require('./pool.js');
var partition = require('./partition.js');


/******************************************
 * 生成SQL插入语句
 * @param object 操作数据对象（已验证） 
 */
exports.query = function(obj, type, query, callback){
	// 执行sql请求
	pool.query(query.sql, query.db, function(err, data){
		if(err){ //返回错误
			return callback(err);
		}
		if(!data || !data.insertId>0){
			return callback(util.errWrap('Can\'t insert: '+query.table));
		}

		var tcnf = partition.get_table_conf(obj._table);

	// console.log(obj);
	// console.log(obj._table);

		// 不分表，直接返回
		if(!tcnf || !tcnf.divide){
			return callback(null, data);
		}
		//执行完成，后期处理数据
		process[type](data, obj, query, tcnf, callback);
	});
}


/**
 * 数据后期处理操作
 */
var process = {};


// 插入数据
process.insert = function(data, obj, query, tcnf, callback){
	/*
	{ fieldCount: 0,
		affectedRows: 1,
		insertId: 1,
		serverStatus: 2,
		warningCount: 1,
		message: '',
		protocol41: true,
		changedRows: 0 }
	*/
	console.log(data);
	console.log(tcnf);
	console.log(query);
	// 检查自增值是否超出范围
	var insertId = data.insertId
	  , max = tcnf.section*(query.partition_index+1);
	if(insertId<=max){
		// 没有超过分区容量，直接返回
		return callback(null, data);
	}
	//超出分区容量，删除上条插入，新建分区，重新插入
	partition.append(tcnf.table, query.partition_index+1, tcnf.db, function(err, newtable)
	{
		if(err){
			return callback(err);
		}
		// 转移数据，转移数据时自动修改自增量
		var move = "INSERT INTO "+newtable
			+" SELECT * FROM "+query.realtable
			+" WHERE id="+insertId;

		console.log(move);
		pool.query(move, tcnf.db, function(err, data){
			if(err){
				return callback(err);
			}
			// 数据转移完成，删除旧数据
			pool.query("DELETE FROM "+query.realtable+" WHERE id="+insertId, tcnf.db);
			callback(null, data);
		});
	});
} 

