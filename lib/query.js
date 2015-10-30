/**
 * 查询实例类
 */


var pool = require('./pool.js');
var build = require('./build.js');


function Query(table){
    this.reset(table);
}



/**
 * 重设/初始化
 */
Query.prototype.reset = function(table){
    // 表名
    this._table = table;
    // 字段
    this._field = '*';
    // 数据
    this._data = {};
    // 筛选条件
    this._where = []; // 或为 string
    // 排序
    this._order_by = []; // 或为 string
    // 分组
    this._group_by = '';
    // 分页
    this._start = null;
    this._limit = null;
    // 增加的语句
    this._addition = '';
    // 拆分插入、删除，修改，查询
    this._join = null;
    // this._join_insert = null;
    // this._join_delete = null;
    // this._join_update = null;
    // this._join_select = null;

    return this;
};


/**
 * 选择要操作的表
 */
Query.prototype.table = function(table){
    this._table = table;
    return this;
};


/**
 * 选择目标字段
 */
Query.prototype.field = function(cols)
{
    if(typeof cols=='string'){
        // 
    }else if(cols instanceof Array){
        var col = [];
        for(var c in cols){
            col.push('`'+cols[c]+'`');
        }
        cols = col.join(',');
    }else{
        throw new Error("Param 'cols' is Invalid !");
    }
    this._field = cols;
    return this;
};


/**
 * 设置插入或更新的数据
 */
Query.prototype.data = function(data, value)
{
    if(data instanceof Object){
        this._data = data;
    }else if(typeof data=='string' && value!==undefined){
        this._data[data] = value;
    }else{
        throw new Error("Param 'data' is Invalid !");
    }
    return this;
}



/**
 * limit
 */
Query.prototype.limit = function(start, limit)
{
    if(limit===undefined){
        limit = start;
        start = null;
    }
    this._start = parseInt(start);
    this._limit = parseInt(limit);
    return this;
}



/**
 * where 查询更新条件
 */
Query.prototype.where = function(prop, value, operator, type)
{
    // key value 方式多重设置
    if(prop instanceof Object){
        for(var k in prop){
            this.where(k, prop[k], '=');
        }
        return this;
    }

    // 添加单条语句
    if(typeof prop=='string' && value===undefined){
        this._where.push(prop);
        return this;
    }

    // 默认运算符
    type = type || 'AND';
    operator = operator || '=';
    // 保存where
    this._where.push({
        prop: prop,
        value: value,
        operator: operator,//.toLocaleUpperCase(), //大写
        type: type,//.toLocaleUpperCase() //大写
    });

    return this;
};

Query.prototype.where_in = function(name, value){
    return this.where(name, value, 'IN');
};



/**
 * 查询排序 order_by
 * @param desc 是否按倒序排列
 */
Query.prototype.order_by = function(column, desc)
{
    if(desc){
        column += (desc===true ? 'DESC' : desc);
    }
    this._order_by.push(column);
    return this;
};



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
 * 增加的语句 addition
 */
Query.prototype.addition = function(str){
    this._addition = str;
    return this;
}







// 查询
function create_query(type){
    return function(callback){
        var sql = build.createSql(type, this);
        log(this);
        die(sql);
        pool.query(sql, {
                table: this._table,
                slave: type=='select'
        }, callback);
    }
}
Query.prototype.insert = create_query('insert');
Query.prototype.delete = create_query('delete');
Query.prototype.update = create_query('update');
Query.prototype.select = create_query('select');

module.exports = Query;