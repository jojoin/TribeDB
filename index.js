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


// 配置项
exports.config = require('./lib/config.js');

// 连接池
exports.pool = require('./lib/pool.js');

// 查询对象
exports.Query = require('./lib/query.js');



/********调试函数*********/



// 全局 die
global.die = function(stuff){
    if(stuff!==undefined){
        console.log(stuff);
    }
    process.exit(1);
}

