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
 * @param object 操作数据对象（已验证） 
 */
var query = exports.query = function(obj, type, query, callback){

	if(!query.sql){
		return callback(null, []);
	}

	var step = 0
	  , single = typeof query.sql=='string' 
	  , datas = []
	  , all = single ? 1 : query.sql.length
	  , sqls = single ? [query.sql] : query.sql
	  , join_data = false //表示未进行联查
	  ;

	//是否可以提前联表查询
	if(type!='insert'){
		var join_adt = check_advance_join(obj);
		if(join_adt){
			log('加一步联查');
			all += 1; //加一步联查
			join_query(obj, type, join_adt, stepback);
		}
	}

	//执行sql请求，并行执行SQL
	for(var s in sqls){ 
		query_one(sqls[s]);
	}

	// 执行一个sql语句
	function query_one(sql){
		if(!sql){
			return stepback(null, []);
		}
		// 执行sql请求
		pool.query(sql, query.db, function(err, data){
			if(err){ //返回错误
				return stepback(err);
			}
			return stepback(null, data);
		});
	}
	// sql请求完成
	// is_join 是否是联查数据
	function stepback(err, data, is_join){
		if(err){ //出错
			return callback(err);
		}
		step++;
		if(is_join){ //null 表示已经进行了联查
			join_data = data ? data : null;
		}else{
			datas.push(data);
		}
		if(step==all){
			// 完成所有请求 集中处理数据
			var tbcnf = partition.get_table_conf(obj._table);
			return process[type](datas, obj, query, tbcnf, callback, join_data);
		}
	}




}


/**
 * 数据后期处理操作
 */
var process = {};


/**
 * 插入数据 后续处理
 */
process.insert = function(datas, obj, query, tcnf, callback){
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

	//插入失败
	if(!datas || !datas[0] || !datas[0].insertId>0){
		return callback(util.errWrap('Can\'t insert: '+query.table));
	}
	data = datas[0];	// 检查自增值是否超出范围
	var insertId = data.insertId
	  , max = tcnf.section*(query.partition_index+1);
	if(insertId<=max){
		// 没有超过分区容量，直接返回
		return join_insert(data);
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
			+" WHERE "+tcnf.divide+"="+insertId;
		// console.log(move);
		pool.query(move, tcnf.db, function(err, data){
			if(err){
				return callback(err);
			}
			// 数据转移完成，删除旧数据
			pool.query("DELETE FROM "+query.realtable+" WHERE id="+insertId, tcnf.db);
			return join_insert(data);
		});
	});

	// 联合、拆分表插入
	function join_insert(){
		var insert = obj._join_insert;
		if(!insert){

			return callback(null, data);
		}

		//联合插入多张表
		var n = 0
		  , num = 0;
		for(var i in insert){
			num++;
			var si = insert[i];
			//拆分表主键赋值
			si.data[si.key] = insertId; 
			var db = create.Create(i).data(si.data)
			db.insert(function(err, data){
				// console.log('join_insert');
				// console.log(err);
				// console.log(data);
				if(err) {
					return callback(err);
				}
				// 完成一次插入
				step();
			});

			// console.log(db);
		}

		function step(){
			n++;
			if(n==num){ //所有联合插入完成
				return callback(null, data);
			}
		}
	}


} 


/**
 * 修改数据 后续处理
 */
process.update = function(datas, obj, query, tcnf, callback, joindata){
	// log(datas);
	if(!datas || !datas[0]){
		return callback(null, datas);
	}

	datas[0].serverStatus += ''; //避免数字相加
	var result = join_result(datas);
	// 没有联表操作
	if(!joindata){
		return callback(null, result);
	};
	//如果有联表操作，则通过表名分组返回数据
	results = {};
	results[obj._table] = result;
	for(var j in joindata){
		var table = j.split(' ',2)[0];
		results[table] = joindata[j];
	}
	return callback(null, results);
}


/**
 * 删除数据 后续处理
 */
process.delete = function(datas, obj, query, tcnf, callback, joindata){
	// log(datas);
	if(!datas || !datas[0]){
		return callback(null, datas);
	}

	datas[0].serverStatus += ''; //避免数字相加
	var result = join_result(datas);
	// 没有联表操作
	if(!joindata){
		return callback(null, result);
	};
	//如果有联表操作，则通过表名分组返回数据
	results = {};
	results[obj._table] = result;
	for(var j in joindata){
		var table = j.split(' ',2)[0];
		results[table] = joindata[j];
	}
	return callback(null, results);
}


/**
 * 查询数据 后续处理
 */
process.select = function(datas, obj, query, tcnf, callback, joindata){
	if(!datas || !datas[0]){
		return callback(null, []);
	}
	//将所有查询到的数据追加到后面
	var data = null;
	for(var d in datas){
		var one = datas[d];
		if(!data){
			data = one;
			continue;
		}
		data = data.concat(one);
	}

	// 无联查
	if(!obj._join_select)
		return callback(null, data);

	//合并联查数据
	if(joindata)
		return deal_join_data(joindata);

	//执行联查
	// log('完成后执行联查');
	join_query(obj, 'select', data, function(err, jd){
		if(err)
			return callback(err);
		deal_join_data(jd);
	});


	//合并联查数据
	function deal_join_data(jd){
		if(util.isEmpty(jd)){
			return callback(null, data);
		}
		for(var j in jd){
			var key = j.split(' ',2)[1] // j = "table key"
			data = join_data(data, jd[j], key);
		}
		return callback(null, data);
	}
}








/**
 * 判断是否可以提前联表查询，并放回联查条件
 */
function check_advance_join(obj){

	//判断是否可以提前进行联表查询
	if(!obj._join || !obj._where)
		return null;
	// where 条件为 = 或 in ，并无分页时 可先查
	var is_join = true;
	for(var j in obj._join){
		var js = obj._join[j]
		  , key = js.key
		  , adt = {
		  	'prop': js.key,
		  	'operator': '='
		  };
		var deng = util.array_select(obj._where, adt, true);
		if(deng){
			var redata = {}
			redata[key] = deng.value;
			return redata;
		}
		adt = {
		  'prop': js.key,
		  'operator': 'IN'
		};
		deng = util.array_select(obj._where, adt, true);
		if(deng){
			var redata = [];
			for(var v in deng.value){
				var oo = {};
				oo[key] = deng.value[v];
				redata.push(oo);
			}
			return redata;
		}
	}
}




/**
 * 拆分表 联表查询
 * @param kvs 联表字段值数组
 */
function join_query(obj, type, data, callback){

	if(!obj._join){ //不联表
		return callback(null, {});
	}

 	// 联合、拆分表请求处理
 	var joindata  = {};

	var n = 0
	  , num = obj._join.length;
	for(var i in obj._join){
		n++;
		//一次联表请求
		query(type, obj._join[i]);
	}

	//执行一次请求
	function query(type, join){
		log(data);
		var table = join.table
		  , key = join.key
		  , adts = util.listem(data,key)
		  , db = create.Create(join.table);

		log('执行一次请求');
		if(type=='select'){ // select
			db.columns(join.columns);
		}else if(type=='update'){ // update
			db.data(join.data);
		}

		// update 和 delete 联表时必须包含相同的 where is 或 where in 条件
		db.where_in(key,adts).limit(join.limit || obj._limit);

		// 结果
		db[type](function(err, jdata){
			if(err) {
				return callback(err); //返回部分数据
			}
			// 完成一次联合查询，
			joindata[join.table+' '+join.key] = jdata;
			arrive();
		});
	}

	function arrive(){
		n++;
		if(n<num) return;
		//数据合并完成
		var is_join = true; //是否为联查
		return callback(null, joindata, is_join);
	}

}


/**
 * 拆分表 合并数据
 */
function join_data(data, join, key){

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
function join_result(datas){
	var data = null;
	for(var d in datas){
		var one = datas[d];
		if(!data){
			data = one;
			continue;
		}
		// 每一项分别处理
		for(var d in data){
			if(one[d]===undefined) continue;
			if(typeof data[d]=='number'){
				data[d] += one[d];
			}else if(typeof data[d]=='string'){
				data[d] += "\n"+one[d];
			}else if(typeof data[d]=='boolean'){
				data[d] = data[d]&&one[d];
			}else if(data[d].constructor==Array){
				data[d] = data[d].concat(one[d]);
			}else if(data[d].constructor==Object){
				data[d] = join_result([data[d],one[d]]);
			}
		}
	}
	return data;
}


