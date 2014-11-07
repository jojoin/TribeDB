/**
 * * * *【简介】* * * *
 * 
 * TribeDB 是一个mysql分布式集群储存系统。
 * 采用分库分表的方式，在处理海量数据时获得更加优越的性能。
 * 
 * Github    ：https://github.com/yangjiePro/TribeDB
 * Home Page ：http://yangjiePro.github.com/TribeDB
 * API docs  ：http://yangjiePro.github.com/TribeDB
 * 
 * * * *【依赖】* * * *
 * 
 * TribeDB 基于 node-mysql 模块
 * Github：https://github.com/felixge/node-mysql
 * 安装：  npm install mysql
 * 
 * * * *【作者】* * * *
 * 
 * 作者   ：杨捷
 * QQ     ：446342398
 * 邮箱   ：yangjie@jojoin.com 
 * 主页   ：http://jojoin.com/user/1
 * Github ：https://github.com/yangjiePro
 * 
 * 欢迎交流讨论或提交新的代码！
 */


// 加载工具方法
var util = require('./lib/util.js');
var config = require('./lib/config.js');

var mode = require('./lib/mode.js');

var create = require('./lib/mysql/create.js');
var pool = require('./lib/mysql/pool.js');
var partition = require('./lib/mysql/partition.js');

// 
//module.exports = require('./lib/build.js');

//版本
exports.version = "0.1.1";

//配置
exports.configure = config.load;

//exports.configure.key = config.set;

//数据处理对象
exports.createQuery = create.Create;

//直接进行 sql 请求
exports.query = pool.query;

//关闭所有连接，清除所有数据
exports.destroy = pool.destroy;

//获得所有表分分区
//exports.get_table_partition = partition.get_table_partition;

//模式
//exports.mode = mode;




/********调试函数*********/


// 全局 die
global.die = function(stuff){
	if(stuff!==undefined){
		console.log(stuff);
	}
	process.exit(1);
}
// 全局 log
global.log = function(stuff){
	console.log(stuff);
}

/***************************/







