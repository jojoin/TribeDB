/**
 * TribeDB 使用测试
 */


var tribe = require('../index.js');

//载入配置文件
tribe.configure('./test.conf', function(err, conf){

	// console.log(err);
	// console.log(conf);


	var db = tribe.createQuery('part');

	db.data({title:'yangjie2'}).insert(function(err, data){

		console.log(err);
		console.log(data);

	},{
		//getsql: 1
	});


	// var db = tribe.createQuery('part');
	// db.select(function(err, sqls){

	// 	console.log(err);
	// 	console.log(sqls[1]);
	// },{
	// 	getsql: 1
	// });



	// console.log(db);
	// console.log(sqls);


});


