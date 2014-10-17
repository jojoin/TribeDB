/**
 * 
 */

var mysql = require('mysql');

// 建立sql
var build = require('./build.js');
var result = require('./result.js');
var util = require('../util.js');


/******************************************
 * 建立查询
 */
exports.Create = function(table){
	return new Query(table);
}


/***
 * 查询实例类
 */
function Query(table){
	this.init(table);
}


/**
 * 初始化
 */
Query.prototype.init = function(table){
	// 表名
	this._table = table || this._table || '';
	// 字段
	this._columns = [];
	// 数据
	this._data = {};
	// 筛选条件
	this._where = [];
	// 排序
	this._order_by = null;
	// 分页
	this._start = null;
	this._limit = null;

	return this;
}


/**
 * 选择要操作的表
 */
Query.prototype.table = function(table){
	this._table = table;
	return this;
}


/**
 * 选择目标字段
 */
Query.prototype.columns = function(){
	var cols = []
	  , leg = arguments.length;
	// 展开字段
	for(var i=0; i<leg; i++){
		var arg = arguments[i];
		if(typeof arg == 'string'){
			cols.push(arg);
		}else if(typeof arg == 'object'){
			for(var a in arg){
				cols.push(arg[a]);
			}
		}
    }

    this._columns = cols;

	return this;
}


/**
 * 供操作的数据
 * @param target 要加入的数据，
 * @param append 如果传入此值，则以追加的方式添加数据
 *               此时 target 参数作为 key
 *               如果 append===null 则表示删除 target 字段
 * 说明：当传 append 时 target 必须为有效的数据库字段名称
 *       当不传 append 时 target 必须为字典。
 * 
 */
Query.prototype.data = function(target, append){
	if(!this._data){
		this._data = {};
	}


	// 设置数据
	if(typeof target == 'object'){
		this._data = target;
		return this;
	}

	// 表示删除 target
	if(typeof target == 'string' && append===null){
		delete this._data[target];
		return this;
	}

	// 添加或重设数据
	if(typeof target == 'string' && append!==undefined){
		this._data[target] = append;
		return this;
	}



	return this;
}


/**
 * 查询排序 order_by
 * 暂时只能按一维排序
 */
Query.prototype.order_by = function(column){
	
	if(typeof column == 'string'){
		this._order_by = column;
	}

	return this;
}


/**
 * 查询排序 order_by
 * 暂时只能按一维排序
 */
Query.prototype.limit = function(start, limit){
	start = parseInt(start);
	limit = parseInt(limit);
	// 最前面的
	if(start>=0 && limit==undefined){
		this._start = 0;
		this._limit = start;
		return this;
	}

	// 翻页
	if(start>=0 && limit>=0){
		this._start = start;
		this._limit = limit;
		return this;
	}

	return this;
}



/**
 * 查询更新条件
 */
Query.prototype.where = function(prop, value, operator, type){
	if(prop===null){
		//清除所有条件
		delete this._where;
		this._where = [];
		return this;
	}

	if(!prop || typeof prop != 'string'){
		return this;
	}

	type = type || 'AND';

	prop = prop.split(' ');
	if(prop[1] && !operator){
		// 支持 where('id >',123) 格式
		operator = prop[1];
	}

	// 保存where
	this._where.push({
		prop: prop[0],
		value: value,
		operator: operator,
		type: type
	});

	return this;
}

Query.prototype.or_where = function(prop, value, operator){
	this.where(prop, value, operator, 'OR');
};

Query.prototype.where_in = function(name, value){
	this.where(name, value, 'IN');
};

Query.prototype.where_not_in= function(name, value){
	this.where(name, value, 'NOT IN');
};

Query.prototype.or_where_in = function(name, value){
	this.where(name, value, 'IN', 'OR');
};

Query.prototype.or_where_not_in = function(name, value){
	this.where(name, value, 'NOT IN', 'OR');
};




/**
 * like 查询
 */
Query.prototype.like = function(name, value, mode, not, type){

	if(!name || !value){
		return this;
	}

	mode = mode || ''; //默认用户自己传 % 或 _ 符号
	not = not ? 'NOT' : '';
	type = type || 'both';

	value += '';

	if(!mode){
		// 自定义模式 不转义
		value = value.replace(/\"/g,'\\\"');
	}else{
		value = mysql.escape(value);
	}

	if(mode=='after'){
		value = '%'+value;
	}else if(mode=='before'){
		value = value+'%';
	}else{ //mode==true mode=='both'
		value = '%'+value+'%'; //默认全匹配
	}

	var where = name+' '+not+' LIKE "'+value+'"';//"{$key} {$not} LIKE '{$value}'";

	this.where(where,null,null,type);

	return this;
}

Query.prototype.or_like = function(name, value){
	return this.like(name, value, null, null, 'OR');
};

Query.prototype.not_like = function(name, value){
	return this.like(name, value, null, 'NOT');
};

Query.prototype.or_not_like = function(name, value){
	return this.like(name, value, null, 'NOT', 'OR');
};



/**
 * 检查数据是否准备好（没准备好则返回错误消息）
 */
Query.prototype.__incomplete = function(type){
	if(!this._table){
		return 'You not select any table !';
	}
	if(type=='select'){

	}else if(type=='update'){
		if(!this._data){
			return '"data" is empty !';
		}
	}else if(type=='insert'){
		if(!this._data){
			return '"data" is empty !';
		}
	}else if(type=='delete'){

	}

}

/**
 * 创建查询函数
 */
function create_query(qtype){

	return function(callback, opt){
		opt = opt || {};
		callback = callback || function(){};
		// 是否执行sql语句
		var execution = opt.getsql ? false : true;
		// 检查是否准备充分
		var incom = this.__incomplete(qtype);
		if(incom){
			return callback(util.errWrap(incom));
		}
		if(qtype=='delete'){
			if(!opt.force_delete && !this._where){
				return callback(util.errWrp('The delete action is not safe, param 2 to enforce!'));
			}
		}
		// console.log('!!!!'+qtype);
		// 建立sql查询语句
		var that = this;
		build[qtype](that, function(err, query){
			if(err){
				return callback(err);
			}
			// 执行sql
			if(execution){
				result.query(that, qtype, query, callback);
			}else{
				callback(null, query);
			}
		});
	}
}



Query.prototype.insert =  create_query('insert');
Query.prototype.delete =  create_query('delete');
/*
Query.prototype.delete =  function(callback, opt){
	opt = opt || {};
	callback = callback || function(){};
	// 是否执行sql语句
	var execution = opt.getsql ? false : true;
	// 检查是否准备充分
	var incom = this.__incomplete('delete');
	if(incom){
		return callback(util.errWrap(incom));
	}
	//强制删除所有数据提醒
	if(!opt.force_delete && !this._where){
		return callback(util.errWrp('The delete action is not safe, param 2 to enforce!'));
	}
	// 建立sql查询语句
	build['delete'](this, function(err, sqls){
		if(err){
			return callback(err);
		}
		// 执行sql
		if(execution){
			result.query(this, 'delete', sqls, callback);
		}else{
			callback(null, sqls);
		}
	});
}
*/
Query.prototype.update =  create_query('update');
Query.prototype.select =  create_query('select');




