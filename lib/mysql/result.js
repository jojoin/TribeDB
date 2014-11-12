/**
 * 对查询出的原始数据进行再处理
 * 例如分页排序等
 */

var pool = require('./pool.js');
var partition = require('./partition.js');
var create = require('./create.js');

var util = require('../util.js');



/******************************************
 * 生成SQL插入语句
 * @param obj 操作数据对象（已验证） 
 * @param type 操作类型 insert delete update select
 * @param query 数据库及 SQL 语句
 */
var query = exports.query = function(obj, type, query, callback){

	if(!query || !query.sql){
		return callback(null, null);
	}

	var step = 0
	  , step_total = util.isArray(query.sql) ? query.sql.length : 1 
	  , query_result = undefined // 当前处理结果
	  , join_result = undefined // 联表处理结果
	  ;

	var sqls = step_total>1 ? query.sql : [query.sql];

	//执行请求
	for(var s in sqls){
		if(!sqls[s]){
			return stepDone(null);
		}
		// 执行sql请求
		pool.query(sqls[s], query.db, function(err, data){
			if(err){ //返回错误
				return callback(err);
			}
			// log(data);
			if(type=='insert'){
				// 插入后续处理
				dealInsertQuery(obj, query, data, function(err, result){
					if(err){
						return callback(err);
					}
					// 后续处理完成
					// log(result);
					stepDone(result);
				});
				return
			}
			return stepDone(data);
		});
	}

	//单条 sql 执行完成
	function stepDone(data){
		step++;
		if(!data)
			return complate();
		// log(data);

		if(!query_result){
			query_result = data;
		}else{
			if(type=='select'){
				query_result = query_result.concat(data);
			}else{
				query_result = joinMsg(query_result, data);
				// log(query_result);
			}
		}
		return complate();
	}


	//执行联表操作
	(function join(data){
		queryJoin(obj, type, data, function(err, result){
			if(err){
				return callback(err);
			}
			join_result = result;
			complate();
		});
	})();


	// 检查是否完成
	function complate(){

		if(step<step_total)
			return
		// 获取结果后，再次执行联表操作
		if(join_result===undefined){
			return join(query_result);
		}
		// 无联表操作 
		if(!join_result){
			return callback(null, query_result);
		}
		//合并处理结果
		if(type=='select'){
			// 结果已经在联查的时候合并
			for(var j in join_result){
				var key = j.split(' ',2)[1] // j = "table key"
				query_result = joinData(query_result, join_result[j], key);
			}
			return callback(null, query_result);
		}

		// else if(type=='update' || type=='delete' || type=='insert')

		join_result[obj._table] = query_result;
		return callback(null, join_result);
	}



};



/**
 * 获取联表处理筛选条件
 */
function getJoinWhere(obj, type, data, join){
	// 从结果集中取得条件
	if(data){
		var value = util.listem(data, join.key);
		if(util.isEmpty(value))
			return
		var single = value.length==1 ? true : false
		return {
			prop: join.key,
			operator: single ? '=' : 'IN',
			value: single ? value[0] : value
		}
	}
	// 从where中筛选条件
	return util.array_select(obj._where, {prop: join.key});
}



/**
 * 执行联表处理
 */
function queryJoin(obj, type, result, callback){

	if(!obj._join){ //不联表
		return callback(null, null);
	}

	// 联表插入
	// if(type=='insert'){
	// 	return queryJoinInsert(obj, result, callback)
	// }

	// 联表查询 判断是否需要从查询结果中取得条件
	if(type=='select' && !result){
		// 存在排序分页
		if(obj._limit && obj.order_by){
			return callback(null, undefined);
		}
		// 遍历查询条件
		for(var j in obj._join){
			for(var w in obj._where){
				if(obj._where[w].prop!=obj._join[j].key){
					// 存在非联表字段查询条件
					return callback(null, undefined);
				}
			}
		}
	}

	// 开始联表处理
	var step = 0
	  , step_total = 0
	  , join_result = null;

	for(var i in obj._join){
		step++;
		//一次联表请求
		var join =  obj._join[i]
		  , table = join.table
		  , key = join.key
		  , where = getJoinWhere(obj, type, result, join); //联表条件
		if(!where && type!='insert')
			continue
		var db = create.Create(join.table);
	    // 联表条件
		db._where = where;
		db._start = obj._start;
		db._limit = obj._limit;
		db._data = join.data;
		// 执行
		db[type](function(err, jdata){
			if(err) {
				return callback(err); //返回部分数据
			}
			if(!join_result) join_result = {};
			var key = join.table;
			if(type=='select'){
				key += ' '+join.key;
			}
			join_result[key] = jdata;
			stepDone();
		});
	}

	// 联表操作完成
	function stepDone(){ 
		if(step<step_total) return
		callback(null, join_result);	
	}

}



/**
 * 插入数据 后续处理
 */
function dealInsertQuery(obj, query, result, callback){
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

	// 插入失败
	if(!result){
		return callback(util.errWrap('Can\'t insert data to "'+query.table+'"!'));
	}
	var tcnf = partition.get_table_conf(obj._table);
	// 表不分区
	if(!tcnf
	|| !tcnf.section
	// 没有自增主键
	|| !result.insertId
	// 分表字段已经包含值
	|| obj._data[tcnf.divide]!==undefined
	// 自增没有超过分区容量
	|| result.insertId<=tcnf.section*(query.partition_index+1)
	){
		return join(result);
	}


	//超出分区容量，删除上条插入，新建分区，重新插入
	query.partition_index += 1;
	partition.append(tcnf.table, query.partition_index, tcnf.db, function(err, newtable)
	{
		if(err){
			return callback(err);
		}
		// 转移数据，转移数据时自动修改自增量
		var move = "INSERT INTO "+newtable
			+" SELECT * FROM "+query.realtable
			+" WHERE "+tcnf.divide+"="+result.insertId;
		// console.log(move);
		pool.query(move, tcnf.db, function(err, data){
			if(err){
				return callback(err);
			}
			// 数据转移完成，删除旧数据
			pool.query("DELETE FROM "+query.realtable+" WHERE id="+result.insertId, tcnf.db);
			return join(result);
		});
	});

	// 联合、拆分表插入
	function join(result){
		// 返回真实操作表名称
		result.table = obj._table;
		if(parseInt(query.partition_index)>=0){
			result.table += '_'+query.partition_index;
		}
		// 不联表
		if(!obj._join){
			return callback(null, result);
		}
		// 联表数据
		for(var j in obj._join){
			var jo = obj._join[j]
			  , key = jo.key;

			if(!key || jo.data[key]) // 联表字段已经有值
				continue
			if(obj._data[key]){
				obj._join[j][key] = obj._data[key];
			}else{
				obj._join[j][key] = result.insertId; //自增主键
			}
		}
		// 执行联表插入
		queryJoin(obj, type, null, callback);
	}

} 




/**
 * 拆分表 合并数据
 */
function joinData(data, join, key){

	// console.log(jdata);
	for(var d in data){
		var kv = data[d][key]
		    adi = {};
		adi[key] = kv;
		var jd = util.array_select(join,adi,true);
	// console.log(jd);
		if(!jd) continue;
		for(var j in jd){
			if(typeof jd[j]=='function') continue; //避免函数
			data[d][j] = jd[j]; //合并数据
		}
	}
	return data;
}




/**
 * 合并 update 和 delete 结果数据
 */
function joinMsg(data, ta){
	// 字段预处理
	var deal = ['serverStatus', 'insertId'];
	for(var l in deal){
		var dea = deal[l]
		if(data[dea]!==undefined){
			data[dea] += '';
		}
	}

	// 每一项分别处理
	for(var d in ta){
		var one = ta[d];
		if(data[d]===undefined) continue;
		if(typeof data[d]=='number'){
			data[d] += one;
		}else if(typeof data[d]=='string'){
			data[d] += "\n"+one;
		}else if(typeof data[d]=='boolean'){
			data[d] = data[d] && one;
		}else if(data[d].constructor==Array){
			data[d] = data[d].concat(one);
		}else if(data[d].constructor==Object){
			data[d] = join_result([data[d],one]);
		}
	}
	return data;
}







