/**
 * 创建查询语句
 * 返回一个 SQL 数组，顺序从第一个表分区开始
 */


var os = require('os')
var mysql = require('mysql')
  , escapeId = mysql.escapeId;

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
	// 获取对应库配置
	var db = partition.get_db_conf(table);
	if(!db){ //配置文件异常
		return callback(util.errWrap("Can't find any database , Plasce check the configure!"));
	}
	// 获取表配置
	var tbconf = partition.get_table_conf(table)
	  , partition_index = null;

	// 如果不分表
	if(!tbconf || !tbconf.divide){
		return complete(table);
	}
	// 指定分表主键
	var value = obj._data[tbconf.divide];
	if(value>0){
		// 通过表配置计算分区位置索引
		partition_index = partition.get_partition_index(tbconf, value);
		// 添加或验证新的表分区
		return partition.append(table, partition_index, db, appendPartition);
	}
	/*需要自增值，获取所有表分区，插入最后那一个
	  插入后获得自增值，然后校验自增值是否超出表分区
	  如果超出表分区大小，则删除本条插入，创建新分区
	  然后将数据插入到最新的分区内
	*/
	partition.listing(table, db, listingPartition);
	

	// 已经建立了最新的分区
	function appendPartition(err, realtable){
		if(err){
			return callback(err);
		}
		// 可以插入，返回插入语句
		return complete(realtable);
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
		return complete(realtable);
	};
	
	// 返回sql语句
	function complete(realtable){

		var sql = 'INSERT INTO `'+realtable+'` SET ?';

		sql = mysql.format(sql, obj._data);
		return callback(null,{
			db: db, 
			table: table,
			realtable: realtable,
			partition_index: partition_index, //使用此字段在数据超出分区时移动数据
			sql: sql
		});
	}
}



/******************************************
 * 生成SQL删除语句
 */
exports.delete = function(obj, callback){
	
	//查询头部
	var tbconf = partition.get_table_conf(obj._table)
	  , queryHead = "DELETE FROM "
	  , sqls = [];

	// 获取将要进行查询的表分区
	effectPartition(obj, partitions);

	// 成功获取所有分区
	function partitions(err, parts){
		if(err){
			return callback(err);
		}
		//如果没有分表，则直接删除
		if(!parts){
			var sql = queryHead+obj._table+buildCondition(obj);
			return complete(sql);
		}
		//分表，采用 union 子查询
		for(var p in parts){
			var one = parts[p]
			  , rtb = one[0]
			  , whe = one[1]
			  , exc = (whe===null ? null : tbconf.divide) //忽略分区键条件
			  , condition = buildWhere(obj, exc, whe)+buildLimit(obj);
			// 单表删除语句
			sqls.push(queryHead+rtb+condition);
		}

		// console.log(unionsql);
		// 返回 sql
		return complete(sqls.length==1?sqls[0]:sqls);
	}

	// 返回sql语句
	function complete(sql){
		return callback(null,{
			db: partition.get_db_conf(obj._table),
			table: obj._table,
			sql: sql
		});
	}

}



/******************************************
 * 生成SQL修改语句
 */
exports.update = function(obj, callback){
	
	//查询头部
	var tbconf = partition.get_table_conf(obj._table)
	  , queryHead = "UPDATE "
	  , sqls = [];

	// 获取将要进行查询的表分区
	effectPartition(obj, partitions);

	// 成功获取所有分区
	function partitions(err, parts){
		if(err){
			return callback(err);
		}
		//如果没有分表，则直接删除
		if(!parts){
			var sql = queryHead+'`'+obj._table+'` SET ?'+buildCondition(obj);
			return complete(mysql.format(sql, obj._data));
		}
		//分表，采用 union 子查询
		for(var p in parts){
			var one = parts[p]
			  , rtb = one[0]
			  , whe = one[1]
			  , exc = (whe===null ? null : tbconf.divide) //忽略分区键条件
			  , condition = buildWhere(obj, exc, whe)+buildLimit(obj)
			  , sql = queryHead+rtb+' SET ?'+condition;
			// 单表删除语句
			sqls.push(mysql.format(sql, obj._data));
		}
		// console.log(unionsql);
		// 返回 sql
		return complete(sqls.length==1?sqls[0]:sqls);
	}

	// 返回sql语句
	function complete(sql){
		return callback(null,{
			db: partition.get_db_conf(obj._table),
			table: obj._table,
			sql: sql
		});
	}
	
}



/******************************************
 * 生成SQL查询语句
 */
exports.select = function(obj, callback){
	//union子查询的limit限制
	var child_limit = '';
	if(obj._limit>0){
		if(obj._start>0){
			child_limit = ' LIMIT '+((obj._start-1)*obj._limit);
		}else{
			child_limit = ' LIMIT '+obj._limit;
		}
	}
	//查询头部
	var queryHead = "SELECT "
	  , columns = obj._columns
	  , cols = [];
	  // log(obj);
	if(util.isEmpty(columns)){
		queryHead += '*';
	}else if(typeof columns=='string'){
		queryHead += columns;
	}else if(columns instanceof Array){
		for(var c in columns){
			cols.push(columns[c]);
		}
		queryHead += cols.join(',');
	}
	queryHead += ' FROM ';

	var tbconf = partition.get_table_conf(obj._table);

	// 获取将要进行查询的表分区
	effectPartition(obj, partitions);

	// 获取请求涉及到的表分区
	function partitions(err, parts){
		if(err){
			return callback(err);
		}
		//如果没有分表
		if(!tbconf || !tbconf.divide || !parts){
			var sql = queryHead+'`'+obj._table+'`'+buildCondition(obj);
			return complete(sql);
		}
		//没有定位到相关分表，查询位置不存在
		if(!parts.length){
			return complete(null);
		}
		//分表，采用 union 子查询
		var childs = [];
		for(var p in parts){
			var one = parts[p]
			  , rtb = one[0]
			  , whe = one[1]
			  , exc = (whe===null ? null : tbconf.divide) //忽略分区键条件
			  , sql = queryHead+escapeId(rtb)
			  	+buildWhere(obj, exc, whe)+child_limit;
			childs.push(sql);
		}
		var uniontail = buildOrderBy(obj) + buildGroupBy(obj) + buildLimit(obj);
		var unionsql = '';
		if(childs.length==1){
			unionsql = childs[0]+uniontail;
		}else{
			//union联合
			unionsql =  "("+childs.join(")\n UNION ALL \n(")+")\n"+ uniontail;
		}
		// console.log(unionsql);
		// 返回 sql
		return complete(unionsql);
	}

	// 返回sql语句
	function complete(sql){
		//首先读取从库设置
		var db = partition.get_db_conf(obj._table, true);
		//是否只使用主库
		if(db && db.is_slave && config.system.master_only){
			var master_only = config.system.master_only;
			if(master_only===true || master_only[db.name]){
				db = partition.get_db_conf(obj._table); //取主库
			}
		}
        if(!db){ //未配置的表，使用默认
            db = config.database.master[config.database.default]
        }

		return callback(null,{
			db: db,
			table: obj._table,
			sql: sql
		});
	}


}



/**
 * 建立子查询
 */
function buildChildQuery(obj, add_where){


}




/**
 * 建立查询头部
 */
function buildQueryHead(obj, type, realtable){
	if(type=='select'){
		var columns = obj._columns;
		var cols = [];
		if(util.isEmpty(columns)){
			for(var c in columns){
				cols.push(escapeId(columns[c]));
			}
			cols = cols.join(',');
		}else{
			cols = '*';
		}
		return "SELECT "+cols+' FROM '+realtable+' ';
	}else if(type=='update'){
		return "SELECT "+cols+' FROM '+realtable+' ';
	}
}




/**
 * 建立查询条件
 */
function buildCondition(obj){
	return buildWhere(obj)
	    + buildOrderBy(obj)
		+ buildGroupBy(obj)
		+ buildLimit(obj);
}




/**
 * 格式化数据
 */
function formatValue(stuff){
	if(typeof stuff=='number'){
		return stuff
	}
	if(typeof stuff=='string'){
		return mysql.escape(stuff); //此函数已经添加引号
	}
	if(stuff instanceof Array){ //数组
		var re = [];
		for(var s in stuff){
			re.push(formatValue(stuff[s]))
		}
		return re.join(' , ');
	}
	if(stuff instanceof Object){ //对象
		var re = [];
		for(var s in stuff){
			re.push(escapeId(s)+' = '+formatValue(stuff[s]))
		}
		return re.join(' , ');
	}
}


/**
 * 建立 LIMIT 语句
 */
function buildLimit(obj){
	var start = obj._start
	  , limit = obj._limit;

	if(limit>0){
		if(start>0){
			return ' LIMIT '+start+','+limit;
		}else{
			return ' LIMIT '+limit;
		}
	}else{
		return '';
	}
}



/**
 * 建立 LIMIT 语句
 */
function buildOrderBy(obj){
	var order_by = obj._order_by
	if(order_by){
		return ' ORDER BY '+order_by;
	}else{
		return '';
	}
}



/**
 * 建立 LIMIT 语句
 */
function buildGroupBy(obj){
	var group_by = obj._group_by;
	if(group_by){
		return ' GROUP BY '+escapeId(group_by);
	}else{
		return '';
	}
}



/**
 * 建立 WHERE 语句
 * @param except 除了prop=except条件
 * @param opt.prefix 是否在字段名前加表明前缀
 * @param opt.pure 不包含 WHERE 字符
 */
function buildWhere(obj, except, add, opt){
	var wh = obj._where
	  , where = [];
	//忽略的条件
	except = except || [];
	opt = opt || {};

	if(typeof except=='string'){
		except = [except];
	}
	//追加的条件
	if(add){
		wh = util.clone(obj._where);
		if(typeof add=='object'){
			for(var a in add){
				wh.unshift({prop:add[a]});
			}
		}else{
			wh.unshift({prop:add});
		}
	}

	//循环处理where语句
	for(var w in wh){
		var one = wh[w]
		  , prop = one.prop
		  , value = one.value
		  , operator = one.operator
		  , type = one.type;

		// console.log(prop);

		//排除查询条件
		if(except.indexOf(prop)>-1){
			continue; //排除条件
		}
		if(where.length>0){
			where.push(type); // AND OR 连接符
		}
		if(!value || !operator){
			where.push(prop); // 单条 where 语句
			continue;
		}

		// 开始循环处理where
		var li = '';
		switch(operator){

			case "IN":
			case "NOT IN":
				li += escapeId(prop)+' '+operator+' (';
				li += formatValue(value);
				li += ')';
				break;
			case "BETWEEN":
			case "NOT BETWEEN":
				li += escapeId(prop)+'` '+operator+' ';
				li += formatValue(value[0])+' AND ';
				li += formatValue(value[1]);
				break;
			case "=":
			case ">":
			case ">=":
			case "<":
			case "<=":
			default:
				li += escapeId(prop)+' '+operator+' '+formatValue(value);
				break;
		}
		where.push(li); // where 条件
	}


	//解析完成 返回数据
	if(where.length>0){
		return (opt.pure?' ':' WHERE ')+where.join(' ')+' ';
	}else{
		return '';
	}

}




/**
 * 通过 where 条件划分到各自的区间
 */
function splitQueryToPartition(obj, callback){

	// 通过分区键划分表分区
	getEffectPartition(obj,function(err, parts){
		if(err){
			return callback(err);
		}
		// console.log(parts);
	});


}
 



/**
 * 通过主键值获取影响到的表分区
 * 并且重写主键查询条件
 * @param opt.prefix 字段名加上表名前缀
 */
function effectPartition(obj, callback){
	var table = obj._table
	  , where = obj._where
	  , tbconf = partition.get_table_conf(table);
	// 未配置 或 未分表
	if(!tbconf || !tbconf['section']){
		return callback(null, null); //null表示使用原始主键条件
	}

	var section = parseInt(tbconf['section'])
	  , divide = tbconf['divide'];

	// 分区主键 筛选条件
	var divides = util.array_select(where,{prop:divide});

	// 分区主键条件: = 
	var equalWhere = util.array_select(divides,{operator:'='},true);
	if(equalWhere){
		var rtb = table+'_'+getPartitionIndex(equalWhere.value);
		var redata = [rtb, escapeId(divide)+' = '+equalWhere.value];
		return complete([redata]);
	}

	// 分区主键条件: in
	var inWhere = util.array_select(divides,{operator:'IN'},true);
	if(inWhere){
		var rtbs = []
		  , rtbids = {}
		  , value = inWhere.value;
		for(var i in value){
			var one = value[i]
			  , rtb = table+'_'+getPartitionIndex(one);
			if(rtbs.indexOf(rtb)==-1){
				rtbs.push(rtb);
				rtbids[rtb] = [];
			}
			rtbids[rtb].push(one);
		}
		//后续处理
		var redata = [];
		for(var r in rtbs){
			var rt = rtbs[r] //单个in条件改成 =
			  , rtv = rtbids[rt]
			  , whe = rtv.length==1
			  	? escapeId(divide)+' = '+rtv[0]
			    : escapeId(divide)+' IN ('+rtv.join(',')+')'
			  ;
			redata.push([rt, whe]);
		}
		return complete(redata);
	}

	// 分区主键条件: between
	var betweenWhere = util.array_select(divides,{operator:'BETWEEN'},true);
	if(betweenWhere){
		var min = Math.min.apply(null,betweenWhere.value)
		  , max = Math.max.apply(null,betweenWhere.value)
		  , minP = getPartitionIndex(min)
		  , maxP = getPartitionIndex(max);
		var redata = [];
		for(var m=minP; m<=maxP; m++){
			var tb = table+'_'+m
			  , whe = '';
			if(min==max){
				whe += escapeId(divide)+' = '+min;
			}else if(m==minP){
				whe += escapeId(divide)+' >= '+min;
				if(m==maxP){ //同一个表分区
					whe += ' AND '+escapeId(divide)+' <= '+max;
				}
			}else if(m==maxP){
				whe += escapeId(divide)+' <= '+max;
			}
			redata.push([tb, whe]);
		}
		return complete(redata);
	}

	// 分区主键条件: <= , <
	var less_where = [];
	var lessWhere1 = util.array_select(divides,{operator:'<='},true)
	  , lessWhere2 = util.array_select(divides,{operator:'<'},true);
	if(lessWhere1 && lessWhere2){
		if(lessWhere2.value<=lessWhere1.value){
			lessWhere1 = null; //使用第二个条件
		}
	}
	var lessWhere = lessWhere1 || lessWhere2;
	if(lessWhere){
		var min = lessWhere.value
		  , minP = getPartitionIndex(min);
		for(var m=0; m<=minP; m++){
			var tb = table+'_'+m
			  , whe = '';
			if(m==minP){
				whe = escapeId(divide)+' '+lessWhere.operator+' '+min;
			}
			less_where.push([tb, whe]);
		}
	}



	// 分区主键条件: >= , >
	var more_where = [];
	var moreWhere1 = util.array_select(divides,{operator:'>='},true)
	  , moreWhere2 = util.array_select(divides,{operator:'>'},true);
	if(moreWhere1 && moreWhere2){
		if(moreWhere2.value>=moreWhere1.value){
			moreWhere1 = null; //使用第二个条件
		}
	}
	var moreWhere = moreWhere1 || moreWhere2;
	if(moreWhere){
		var max = moreWhere.value
		  , maxP = getPartitionIndex(max);
		// 首先获取真实存在的表分区
		return partition.get_table_partition(table, function(err, part){
			if(err){
				return callback(err);
			}
			if(!part || !part.length){
				return callback(util.errWrap('Can\'t find specified partition for table: "'+table+'" !'));
			}
			for(var m=0; m<part.length; m++){
				var whe = '';
				if(m==0){
					whe = escapeId(divide)+' '+moreWhere.operator+' '+max;
				}
				more_where.push([part[m], whe]);
			}
			return complete();
		// min表示获取min以上的分区
		},{min: maxP});
	}

	if(lessWhere){
		// 单独的 <= 或 < 条件结果
		return complete();
	}

	// 不存在通过分区键划分表分区，遍历所有表分区
	// 首先获取真实存在的表分区
	partition.get_table_partition(table,function(err, part){
		if(err){
			return callback(err);
		}
		if(!part || !part.length){
			return callback(util.errWrap('Can\'t find any partition for table: "'+table+'" !'));
		}
		var redata = [];
		for(var m=0; m<part.length; m++){
			redata.push([part[m], null]);
		}
		return complete(redata);
	});

	// 包含
	function getPartitionIndex(value){
		value = parseInt(value);
		return parseInt((value-1) / section);
	}

	// 加一层检查涉及到的表分区是否存在
	function complete(parts){
		if(!parts){
			parts = [];
			//取 >= > 和 <= < 的交集
			if(less_where.length && more_where.length){
				for(var l in less_where){
				for(var m in more_where){
				if(less_where[l][0]==more_where[m][0]){
					var w_m = more_where[m][1]
					  , w_l = less_where[l][1]
					  , wh = w_m || w_l || '';
					if(w_l&&w_m){
						wh = w_m+' AND '+w_l;
					}
					parts.push([less_where[l][0], wh]);
				}
				}
				}
			}else{
				parts = less_where || more_where;
			}
		}
		// log(less_where); 
		// log(more_where); 
		// log('!!!!!!'); 
		// log(parts); 

		partition.effective(table, parts, function(err, tbs){
			// log(tbs);
			return callback(err, tbs);
		});
	}
}
