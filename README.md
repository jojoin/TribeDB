TribeDB - 分布式集群储存框架 - Node.js
=======

[TribeDB](http://yangjiepro.github.io/TribeDB/) 是一个MySQL分表分库数据中间件，实现MySQL数据的分布式集群储存管理。在处理海量数据、高并发访问时，获得更加优越的性能及横向扩展能力。它包含以下主要特性:

1. 可伸缩、高扩展的架构
2. 自动路由分库，维护数据库连接池
3. 支持数据表的“横向”和“纵向”分表
4. 支持“一主多从”式读写分离
5. 分布式并行处理，成倍提升性能
6. 对应用层隐藏数据来源及技术细节

拥有以上特点意味着，可随时通过增加普通级别数据库服务器的方式，方便地扩展整体系统性能，而无需修改业务层架构和代码。理论上[TribeDB](http://yangjiepro.github.io/TribeDB/)的扩展能力上线在于主库单表插入性能和主从数据同步开销。通过合理设计“横向”和“纵向”分表和数据切分粒度，可轻松应对上亿级别的数据量和访问请求。

### 快速上手：

```javascript

var tribe = require('tribedb')
  , Query = tribe.Query;
// 添加一个数据库配置
tribe.config.db('db1',{user:'root'});
// 直接执行sql
tribe.pool.query('SELECT 1+1 AS num', { db: 'db1' }, function(err, rows, fields){
  // 结果
});
//插入封装
new Query('post').data({title:'标题'}).insert(function(err, data){
  console.log(err);
  console.log(data);
});
//查询封装
new Query('post').where('title','标题').order_by('time','DESC').limit(1).select(function(err, data){
  console.log(err);
  console.log(data);
});
    
```

[TribeDB](http://yangjiepro.github.io/TribeDB/)通过全局唯一的表名，自动连接对应的数据库，并通过分表配置，将操作映射到涉及的分表，同时完成读写分离。 一切都由[TribeDB](http://yangjiepro.github.io/TribeDB/)自动完成，业务层不必关心数据的位置。当数据库负载过高需要添加服务器时，只需简单修改配置文件而不必修改业务代码，甚至将整个架构推倒重来。继续阅读本文档详细了解如何使用。

### 安装

推荐采用 NPM 方式安装。也可以 下载源码，但需要自己处理依赖。

```shell
$ npm install tribedb

#tribedb@0.1.1 node_modules\tribedb
#└── mysql@2.5.2 (require-all@0.0.8, bignumber.js@1.4.1, readable-stream@1.1.13)
```

### 文档

阅读详细API文档了解如何使用[TribeDB](http://yangjiepro.github.io/TribeDB/)。中文文档地址：http://yangjiepro.github.io/TribeDB/

### 其它

> @ TribeDB - 2015 

> 作者：杨捷 

> Email：yangjie@jojoin.com 

> HomePage：http://yangjiepro.github.io/TribeDB/

> Github：https://github.com/yangjiePro/TribeDB


