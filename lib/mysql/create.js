/**
 * 
 */

var mysql = require('mysql');

// 建立sql
var build = require('./build.js');
var result = require('./result.js');
var util = require('../util.js');


var config = require('../config.js').config
  , database = config['database']
  , table = config['table'];


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
	this._columns = null;
	// 数据
	this._data = null;
	// 筛选条件
	this._where = null;
	// 排序
	this._order_by = null;
	this._order_by_desc = false;
	// 分组
	this._group_by = null;
	// 分页
	this._start = null;
	this._limit = null;
	// 拆分插入、删除，修改，查询
	this._join = null;
	// this._join_insert = null;
	// this._join_delete = null;
	// this._join_update = null;
	// this._join_select = null;

	return this;
}


/**
 * 重置查询状态以便重用
 * @param force 是否清除表名
 */
Query.prototype.reset = function(force){

	this.init(force ? null : this._table);
	
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
	var leg = arguments.length;
	if(!leg){
		return this;
	}
	// 字符串
	if(leg==1 && (typeof arguments['0'] == 'string')){
		this._columns = arguments['0'];
		return this;
	}
	var cols = [];
	// 展开参数
	for(var i=0; i<leg; i++){
		var arg = arguments[i];
		if(!arg) continue;
		if(arg instanceof String){
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
 * @param desc 是否按倒序排列
 */
Query.prototype.order_by = function(column, desc){
	if(typeof column == 'string'){
		this._order_by = column;
		if(desc){
			this._order_by += ' DESC'
		}
	}
	return this;
}


/**
 * 查询分组 group_by
 */
Query.prototype.group_by = function(column){
	if(typeof column == 'string'){
		this._group_by = column;
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
	// 翻页
	if(start>0 && limit>0){
		this._start = start;
		this._limit = limit;
		return this;
	}
	// 最前面的
	if(start>0){
		this._start = null;
		this._limit = start;
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
		this._where = null;
		return this;
	}

	if(!prop || typeof prop != 'string'){
		return this;
	}

	if(value!==undefined&&value!==null&&value!==NaN){ // 支持 where('id > ',123) 格式
		var propArr = prop.split(' ',2);
		if(propArr.length==2){
			// 支持 where('id >',123) 格式
			prop = propArr[0]
			operator = propArr[1];
		}
	}
	//初始化
	if(!this._where){
		this._where = [];
	}
	// 默认运算符
	type = type || 'AND';
	operator = operator || '=';
	// 保存where
	this._where.push({
		prop: prop,
		value: value,
		operator: operator.toLocaleUpperCase(), //.replace(/(^\s*)|(\s*$)/g,""), //去掉首尾空格
		type: type.toLocaleUpperCase() //大写
	});

	return this;
}

Query.prototype.or_where = function(prop, value, operator){
	return this.where(prop, value, operator, 'OR');
};

Query.prototype.where_in = function(name, value){
	return this.where(name, value, 'IN');
};

Query.prototype.where_between= function(name, value, value2){
	if(value2){
		value = [value, value2];
	}
	return this.where(name, value, 'BETWEEN');
};

Query.prototype.where_not_between= function(name, value, value2){
	if(value2){
		value = [value, value2];
	}
	return this.where(name, value, 'NOT BETWEEN');
};

Query.prototype.where_not_in= function(name, value){
	return this.where(name, value, 'NOT IN');
};

Query.prototype.or_where_in = function(name, value){
	return this.where(name, value, 'IN', 'OR');
};

Query.prototype.or_where_not_in = function(name, value){
	return this.where(name, value, 'NOT IN', 'OR');
};




/**
 * like 查询
 */
Query.prototype.like = function(name, value, mode, not, type){

	if(!name || !value){
		return this;
	}

	mode = mode || ''; //默认用户自己传 % 或 _ 符号
	not = not ? ' NOT' : '';
	type = type || 'AND';

	value += '';

	if(!mode){
		// 自定义模式 不转义
		value = value.replace(/\"/g,'\\\"');
	}else{
		value = mysql.escape(value);
	}

	if(mode=='right'){
		value = '%'+value;
	}else if(mode=='left'){
		value = value+'%';
	}else if(mode=='both'){
		value = '%'+value+'%';
	}else{
		value = value; //默认全匹配
	}

	var where = name+not+' LIKE "'+value+'"';//"{$key} {$not} LIKE '{$value}'";

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
 * 支持表的“纵拆” 
 *
 */
/**
 * 连接拆分表 join 操作封装
 */
Query.prototype.join = function(p1, p2, p3){
	if(p1===null){ //清除
		this._join = null;
		return this;
	}
	if(!p1 || typeof p1!='string')
		return this;
	if(!this._join){
		this._join = [];
	}
	this._join.push([p1, p2, p3]);

	return this;
}



/**
 * 建立拆分表操作
 */
Query.prototype.build_join = function(type){
	if(!this._join)
		return this;
	var opt = [];

	for(var i in this._join){
		var one = deal(this._join[i]);
		if(one){
			opt.push(one);
		}
	}
	//单个处理
	function deal(one){
		if(!one || !one[0])
			return null;
		if(type=='insert'){ // join(table, data, key)
			if(!one[1] || one[1].constructor!=Object)
				return null;
			return {
				table: one[0],
				data: one[1],
				key: one[2] || 'id',
			}
		}else if(type=='delete'){ // join(table, key, limit)
			return {
				table: one[0],
				key: one[1] || 'id',
				limit: one[2] || null
			}
		}else if(type=='update'){ // join(table, data, key, limit)
			if(!one[1] || one[1].constructor!=Object)
				return null;
			return {
				table: one[0],
				data: one[1],
				key: one[2] || 'id',
				limit: one[3] || null
			}
		}else if(type=='select'){ // join(table, columns, key, limit)
			return {
				table: one[0],
				columns: one[1] || '*',
				key: one[2] || 'id',
				limit: one[3] || null
			}
		}
	}
	//储存操作
	if(opt.length)
		this._join = opt;

	return this;
}





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
		//检查是否是否设置为只读
		if(qtype!='select' && config.system.read_only){
			return callback(util.errWrap("Data cannot be modified, "
				+"the reason is set \"read_only=yes\" in the configuration!"));
		}
		// 检查数据是否准备充分
		var incom = this.__incomplete(qtype);
		if(incom){
			return callback(util.errWrap(incom));
		}
		//强制删除验证
		if(qtype=='delete' && !opt.force_delete && !this._where){
			return callback(util.errWrp('The delete action is not safe, param 2 to enforce!'));
		}
		// 建立join查询操作
		this.build_join(qtype);
		//仅获取查询对象
		if(opt.obj){
			return callback(null, this);
		}
		// 建立sql查询语句
		var that = this;
		build[qtype](that, function(err, query){

			// console.log(query.sql);
			if(err){
				return callback(err);
			}
			// 仅仅获取将要进行查询的 db 
			if(opt.db){
				return result_back(null, query.db);
			}
			// 仅仅获取 sql 语句
			if(opt.sql){
				return result_back(null, query.sql);
			}
			// 执行sql请求
			return result.query(that, qtype, query, result_back);
			
		});
		// 数据查询返回，重置条件
		function result_back(err, data){
			// console.log(data);
			if(err){
				return callback(err);
			}
			//重置查询条件（是否保留条件）
			if(!opt.reserved){
				// log('reset');
				that.reset();
			}
			return callback(null, data);
		}
	}
}



Query.prototype.insert =  create_query('insert');
Query.prototype.delete =  create_query('delete');
Query.prototype.update =  create_query('update');
Query.prototype.select =  create_query('select');




