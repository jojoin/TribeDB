/**
 * 文件存取效率 基准测试
 */


var fs = require('fs');

var util = require('../lib/util.js');

// 缓存文件目录
// var path = '/tmp';
var path = 'k:/tmp';

console.log('生成大数组...');
console.time('create');
var arr = [];
for(var i=0; i<1; i++){
	var num = parseInt(Math.random()*100*10000)+10;
	one.push({
		id: i + arrSize*arrNum,
		num: num
	});
}
console.timeEnd('create');