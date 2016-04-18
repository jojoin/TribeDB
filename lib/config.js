/**
 * 解析配置文件
 * 配置文件示例： example/test.conf
 */


// 系统模块
var http = require('http');
var fs = require('fs');
var path = require('path');

// 本地
var util = require('./util.js');

var isArray = require('util').isArray;

//解析后的配置
exports.config = {
	system:{ //系统配置

	},
	default_db: '', // 默认库名
	databases: { // 数据库

	},
	tables: { // 表

	} 
};


/**
 //配置格式
{

databases: {
	db1: [ // 第一个为主库master, 后续的为从库slave
		{
			host: '127.0.0.1',
	        user: 'root',
	        password: '',
	        ...
		}, ...
		]
	},
	db2: ...,
	db3: ...
},
tables: { // 未定义的表，属于默认数据库
	user: {
		db: 'db1', //所属数据库
	},
	user_conf: {
		db: 'db1', //所属数据库
	}
} 

	
}

*/


/**
 * 添加数据库
 * conf={...}
 * conf=[] : [host,user,password,port,slave] 
 */
exports.db = function(name, conf)
{
	var def = { // 默认数据库配置
		host: 'localhost',
		user: 'root',
		password: '',
		database: name, // 与库名一致
		port: 3306,
		slave: false // 是否为只查询的从库
	}
	if(isArray(conf)){
		var ns = ['host','user','password','port','slave'];
		for(var c in conf){
			if(c>ns.length) break;
			def[ns[c]] = conf[c]; // 合并
		}
	}else if(typeof conf=='object'){
		util.extend(def,conf); // 合并
	}else  if(conf===undefined){

	}else{
		throw new Error("Param 'conf' is Invalid !");
	}

	// 添加新的主库
	if(!exports.config.databases[name]){
		exports.config.databases[name] = [def];
	// 添加从库
	}else if(def.slave){
		exports.config.databases[name].push(def);
	}else{
		throw new Error("Database '"+name+"' is already exists !");
	}

	// 是否设置为默认数据库
	if(!exports.config.default_db){
		exports.config.default_db = name;
	}
}



/**
 * 设置或获取默认连接的数据库
 */
exports.default_db = function(name)
{
	if(!name){
		return exports.config.default_db;
	}
	exports.config.default_db = name;
}




/**
 * 将一些表分配到指定的库
 */
exports.dist = function(dbname, tables)
{
	if(typeof tables=='string'){
		tables = tables.split(',');
	}else if(isArray(tables)){
		//
	}else{
		throw new Error("Param tables="+tables+" is Invalid !");
	}
	// 开始分配
	for(var t in tables){
		var tbn = util.rtrim(tables[t]);
		if(exports.config.tables[tbn]){
			throw new Error("Table '"+tbn+"' is already dist !");
		}
		exports.config.tables[tbn] = {
			db: dbname
		}
	}
}