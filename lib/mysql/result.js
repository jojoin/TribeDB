/**
 * 对查询出的原始数据进行再处理
 * 例如分页排序等
 */

var pool = require('./pool.js');
var partition = require('./partition.js');
var create = require('./create.js');

var util = require('../util.js');


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

    var dojo = (obj._join  && obj._join.length>0)
        , join_result; // join操作的结果

    // 仅仅 delete 和 update 操作 可能出现多条 sql 执行

    if(type=='insert'){

        return querySql(query.db, query.sql, function(err, data){
            if(err){ //返回错误
                return callback(err);
            }
            // 检查是否需要移动到新的表分区
            movePartition(obj, query, data, function(err){
                if(err){
                    return callback(err);
                }
                if(dojo){ //获取结果后 执行 join 操作
                    join_result = {};
                    join_result[obj._table] = data;
                    return insertJoin(obj, query, data, join_result, callback);
                }
                return callback(null, data);
            });
        });

    }else if(type=='select'){

        // 并行 join
        querySql(query.db, query.sql, function(err, data){
            if(err){
                return callback(err);
            }
            if(dojo){
                return selectJoin(obj, query, data, callback);
            }
            //log('不 select  join ！');
            return callback(null, data);
        });

    }else if(type=='update' || type=='delete'){

        var sqls = util.isArray(query.sql) ? query.sql : [query.sql]
            , len = sqls.length
            , l = 0
            , sql_result = {};
        if(dojo){
            // join 操作
            updelJoin(obj, query, type, function(err, result){
                //log(result);
                join_result = result;
                done();
            });
        }else{
            join_result = null; //无join
        }
        for(var s in sqls){
            (function(sql){
                querySql(query.db, sql, function(err, data){
                    if(err){
                        return callback(err);
                    }
                    l++;
                    sql_result = joinMsg(sql_result, data);
                    done()
                });
            })(sqls[s]);
        }
        function done(){
            if(l==len&&join_result!==undefined){
                if(join_result){ // 有join 操作
                    join_result[obj._table] = sql_result;
                    return callback(null, join_result);
                }
                // 无 join
                return callback(null, sql_result);
            }

        }


    }




};


/**
 * 执行单条sql
 */
function querySql(db, sql, callback){

    pool.query(sql, db, function(err, data){
        if(err){ //返回错误
            return callback(err);
        }
        return callback(null, data);
    });
}


/**
 * 插入数据 后续处理
 */
function movePartition(obj, query, result, callback){

    // 插入失败
    if(!result){
        return callback(util.errWrap('Can\'t insert data to "'+query.table+'"!'));
    }
    var tcnf = partition.get_table_conf(obj._table);
    // 表不分区
    if(!tcnf
        || !tcnf.section // 没有分区
        || !result.insertId // 没有自增主键
        || obj._data[tcnf.divide]!==undefined  // 分表字段已经包含值
        || result.insertId<=tcnf.section*(query.partition_index+1)  // 自增没有超过分区容量
        ){
        return callback();
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
            return callback();
        });
    });

}


/**
 * join 插入
 */
function insertJoin(obj, query, result, join_result, callback){

    var step = 0;

    for(var j in obj._join){
        var jo = obj._join[j]
            , table = jo.table
            , key = jo.key
            , data =  jo.data;

        if(!data[key]){
            if(obj._data&&obj._data[key]){
                data[key] = obj._data[key];  // 插入数据
            }else if(result && result.insertId){
                data[key] = result.insertId; // 插入自增值
            }
        }

        // 执行 join 插入
        (function(table, data){
            create.Create(table).data(data).insert(function(err, result){
                if(err) {
                    return callback(err); //返回部分数据
                }
                join_result[table] = result;
                done(table, result);
            });
        })(table, data);

    }

    // 一次join完成
    function done(){
        step++;
        if(step==obj._join.length){
            callback(null, join_result);
        }
    }

}




/**
 * join 查询
 */
function selectJoin(obj, query, result, callback){

    var step = 0;

    for(var j in obj._join){
        var jo = obj._join[j];
        // 执行 join 查询
        var db = create.Create(jo.table);
        db._columns = jo.columns;
        db._limit = jo.limit || obj._limit;
        db._where = getJoinWhere(obj, result, jo);
        db.select(function(err, data){
            //log(data);
            if(err){
                return callback(err); //返回部分数据
            }
            result = joinData(result, data,  jo.key);
            done();
        },{
            //obj: true
            //sql: true
        });

    }
    // 一次join完成
    function done(){
        step++;
        if(step==obj._join.length){
            callback(null, result);
        }
    }

}



/**
 * join 更新 删除
 */
function updelJoin(obj, query, type, callback) {

    var step = 0
        , join_result = null;

    for(var j in obj._join){
        var jo = obj._join[j];
        // 执行 join 查询
        var db = create.Create(jo.table);
        db._data = jo.data || null;
        db._limit = jo.limit || obj._limit || null;
        db._where = getJoinWhere(obj, null, jo);
        (function(table){
            db[type](function(err, data){
                if(err){
                    return callback(err); //返回部分数据
                }
                join_result = join_result || {};
                join_result[table] = data;
                done();
            },{
                //obj: true
                //sql: true
            });
        })(jo.table);
    }
    // 一次join完成
    function done(){
        step++;
        if(step==obj._join.length){
            callback(null, join_result);
        }
    }

}




/**
 * 获取联表处理筛选条件
 */
function getJoinWhere(obj, data, join){
    // 从结果集中取得条件
    if(data){
        var value = util.listem(data, join.key);
        if(util.isEmpty(value))
            return;
        var single = value.length==1 ? true : false
        return [{
            prop: join.key,
            operator: single ? '=' : 'IN',
            value: single ? value[0] : value
        }];
    }
    // 从where中筛选条件
    return util.array_select(obj._where, {prop: join.key});
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







