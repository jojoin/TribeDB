/**
 * 建立 mysql 语句
 */


var mysql = require('mysql')
  , escapeId = mysql.escapeId
  ;


/**
 * 从 Query 对象建立 mysql 语句
 * type = insert, delete, update, select 
 */
exports.createSql = function(type, obj)
{
    switch(type){
        case 'insert': return insertSql(obj);
        case 'delete': return deleteSql(obj);
        case 'update': return updateSql(obj);
        case 'select': return selectSql(obj);
    }
}


/**
 * 创建 insert 语句
 */
function insertSql(obj)
{
    if(typeof obj._data=='string'){
        return 'UPDATE `'+obj._table+'` SET '+obj._data+condition(obj);
    };
    return mysql.format('INSERT INTO `'+obj._table+'` SET ?', obj._data);
}


/**
 * 创建 delete 语句
 */
function deleteSql(obj)
{
    return 'DELETE FROM `'+obj._table+'`'+condition(obj);
}


/**
 * 创建 update 语句
 */
function updateSql(obj)
{
    if(typeof obj._data=='string'){
        return 'UPDATE `'+obj._table+'` SET '+obj._data+condition(obj);
    };
    return mysql.format('UPDATE `'+obj._table+'` SET ?'+condition(obj), obj._data);
}


/**
 * 创建 select 语句
 */
function selectSql(obj)
{
    return 'SELECT '+obj._field+' FROM `'+obj._table+'` '+condition(obj);
}


/**
 * 建立查询条件
 */
function condition(obj){
    return where(obj)
        + order_by(obj)
        + group_by(obj)
        + limit(obj)
        + obj._addition; // 增加的条件
}





/**
 * 建立 LIMIT 语句
 */
function limit(obj)
{
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
function order_by(obj)
{
    var order_by = obj._order_by
    // 支持原生语句
    if(typeof order_by=='string'){
        return ' ORDER BY ' + order_by;
    }
    if(isArray(order_by) && order_by.length>0){
        return ' ORDER BY ' + order_by.join(', ');
    }
    return '';
}


/**
 * 建立 LIMIT 语句
 */
function group_by(obj)
{
    if(obj._group_by){
        return ' GROUP BY '+obj._group_by;
    }else{
        return '';
    }
}


/**
 * 创建 where 语句
 */
function where(obj)
{
    var wheres = obj._where;

    // 支持原生语句
    if(typeof wheres=='string'){
        return ' WHERE ' + wheres;
    }

    var where = [];

    //循环处理where语句
    for(var w in wheres){

        var one = wheres[w];
        if(typeof one=='string'){
            if(w>0) one = 'AND '+one;
            where.push(one); // where 条件
            continue;
        }

        if(! one instanceof Object){
            continue;
        }

        var prop = one.prop
          , value = one.value
          , operator = one.operator
          , type = one.type;

        // 开始循环处理where
        var li = w>0 ? type+' ' : '';

        switch(operator){
            case "IN":
            case "NOT IN":
                li += escapeId(prop)+' '+operator+'(';
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
            case "!=":
            case "<>":
            default:
                li += escapeId(prop)+operator+formatValue(value);
                break;
        }
        where.push(li); // where 条件
    }

    //解析完成 返回数据
    if(where.length>0){
        return ' WHERE '+where.join(' ');
    }else{
        return '';
    }

}





/**
 * 格式化数据
 */
function formatValue(stuff){
    if(typeof stuff=='number'){
        return stuff;
    }
    if(typeof stuff=='string'){
        return mysql.escape(stuff); //此函数已经添加引号
    }
    if(isArray(stuff)){ //数组
        var re = [];
        for(var s in stuff){
            re.push(formatValue(stuff[s]))
        }
        return re.join(',');
    }
    if(stuff instanceof Object){ //对象
        var re = [];
        for(var s in stuff){
            re.push(escapeId(s)+'='+formatValue(stuff[s]))
        }
        return re.join(',');
    }
}
