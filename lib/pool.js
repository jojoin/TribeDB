/**
 * 维护的 mysql 数据库群连接池
 */


var mysql = require('mysql');

// 配置
var config = require('./config.js').config;



// 数据库连接池
// var poolCluster = mysql.createPoolCluster();
var poolClusters = {/*

*/};



/**
 * 关闭所有连接，清除所有数据
 */
exports.destroy = function(){
    for(var p in poolClusters){
        poolClusters[p].end();
    }
}


/**
 * 执行 sql 请求数据
 * opt={
 *     db: // 使用的库名
 *     table: // 使用的表（自动路由到库）
 *     slave: // 是否优先使用从库
 * }
 */
exports.query = function(sql, opt, callback)
{
    if(opt instanceof Function){
        callback = opt;
        opt = {};
    }
    // 取得连接
    get_connection(opt, function(err, connection){
        if(err){
            return callback(err);
        }
        // 执行请求
        connection.query(sql, function(err, rows, fields){
            callback(err, rows, fields);
            // 释放连接
            connection.release();
        });
    });
}



/**
 * 获取数据库连接连接
 */
function get_connection(opt, callback)
{
    // 无数据库
    if(!config.default_db){
        throw new Error("No database has been set up yet");
    }
    // 得到库名称
    var dbn = opt.db || get_db_name_by_table(opt.table) || config.default_db;
    // 找不到库
    if(!config.databases[dbn]){
        throw new Error("Database '"+dbn+"' not set up yet");
    }
    // 获取库配置
    var dbconf = config.databases[dbn];
    // 获取连接池
    var pool = get_pool_cluster(dbn, dbconf);
    // 选择库
    var key = dbn;
    if(opt.slave&&config.databases[dbn].length>1){
        key += '_slave_*'; // 存在并使用从库
    }else{
        key += '_master'; // 使用主库
    }
    // log(key);
    // 取得连接
    pool.of(key).getConnection(callback);
}



/**
 * 启动或获取连接池
 */
function get_db_name_by_table(table)
{
    if(config.tables[table]){
        return config.tables[table].db;
    }
}



/**
 * 启动或获取连接池
 */
function get_pool_cluster(dbname, conf)
{
    // 返回缓存
    if(poolClusters[dbname]){
        return poolClusters[dbname];
    }

    // 初始化连接池
    var poolCluster = mysql.createPoolCluster();
    for(var d in conf){
        var dbconf = conf[d]
          , key = dbname;
        if(d==0){ // 主库
            key += '_master';
        }else{ // 从库
            key += '_slave_'+d;
        }
        // log(dbconf);
        poolCluster.add(key, dbconf);
    }

    // log(poolCluster);

    // 初始化完成
    return poolClusters[dbname] = poolCluster;
}

