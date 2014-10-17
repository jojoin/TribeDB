/**
 * 测试
 */


//var  ctext = rc4("我是明文","我是密码");

//var text = rc4(ctext, "我是密码");



function rc4(data, key){
	var seq = Array(256);//int
	var das = Array(data.length);//code of data
	for (var i=0; i<256; i++){
		seq[i] = i;
		var j=(j+seq[i]+key.charCodeAt(i % key.length)) % 256;
		var temp = seq[i];
		seq[i] = seq[j];
		seq[j] = temp;
	}
	for(var i=0; i<data.length; i++){
	   das[i] = data.charCodeAt(i)
	}
	for(var x = 0; x < das.length; x++){
		var i = (i+1) % 256;
		var j = (j+seq[i]) % 256;
		var temp = seq[i];
		seq[i] = seq[j];
		seq[j] = temp;
		var k = (seq[i] + (seq[j] % 256)) % 256;
		das[x] = String.fromCharCode( das[x] ^ seq[k]) ;
	}
	return das.join('');
}



var miss = rc4('duole1', 'yjasdfasdgdfgdfghfdgh');

console.log(miss);


var mir = rc4(miss,'');

console.log(mir);




process.exit(1);







// 交换数组下标的值
Array.prototype.swap = function(i, j){
  var temp = this[i];
  this[i] = this[j];
  this[j] = temp;
}

Array.prototype.insert = function(index, item) {
  this.splice(index, 0, item);
};

// 排序算法测试

// 生成大数组

function time(old){
	var cur = new Date().getTime();
	if(!old){
		return cur;
	}else{
		var m = (cur-old)/1000;
		return parseFloat(m).toFixed(2);
	}
}


var curtime = time();
console.log('生成大数组...');

var arrSize = 100*10000;
var arrNum = 5;
var obArr = [];
while(--arrNum+1){
	var one = [];
	for(var i=0; i<arrSize; i++){
		var num = parseInt(Math.random()*100*10000)+10;
		one.push({
			id: i + arrSize*arrNum,
			num: num
		});
	}
	obArr.push(one);
}

var longtime = time(curtime);

console.log('生成数组用时: '+longtime);



// 快速排序

// 生成排序共排序的数据


// 系统排序
function sys_sort(obArr, item){
	obArr.sort(function(a,b){
		return a[item] > b[item] ? 1 : -1;
	});
}


// 快速排序
function quick_sort(obArr, item){
	var numArr = []
	  , leg = obArr.length
	  , i=0;
	// while 循环性能是 for 循环的两倍！
	while(i<leg){
		numArr.push(obArr[i][item]);
		i++;
	}
    quickSort(numArr,0,numArr.length-1);
    return obArr;
    function quickSort(arr,l,r){
        if(l<r){         
            var mid=arr[parseInt((l+r)/2)],i=l-1,j=r+1;         
            while(true){
                while(arr[++i]<mid);
                while(arr[--j]>mid);             
                if(i>=j)break;
                arr.swap(i,j);
                // 操作原始数组
                obArr.swap(i,j);
            }       
            quickSort(arr,l,i-1);
            quickSort(arr,j+1,r);
        }
        return arr;
    }
}

// 堆排序
function heap_sort(obArr, item){
	var numArr = [];
	for(var o in obArr){
		numArr.push(obArr[o][item]);
	}
    return heapSort(numArr);
	function heapSort(arr){
	  for(var i = 1; i < arr.length; ++i){
	    for (var j = i, k = (j - 1) >> 1; k >= 0; j = k, k = (k - 1) >> 1){
	      if(arr[k] >= arr[j]) break;
	      arr.swap(j, k);
          obArr.swap(j, k);
	    }
	  }
	  for(var i = arr.length - 1; i > 0; --i){
	    arr.swap(0, i);
        obArr.swap(0, i);
	    for(var j = 0, k = (j + 1) << 1; k <= i; j = k, k = (k + 1) << 1){
	      if(k == i || arr[k] < arr[k - 1]) --k;
	      if(arr[k] <= arr[j]) break;
	      arr.swap(j, k);
          obArr.swap(j, k);
	    }
	  }
	  return arr;
	}
}




// console.log('快速排序算法开始处理...');
// var curtime = time();
// var list = quick_sort(obArr, 'num');    
// var longtime = time(curtime);
// console.log('快速排序用时: '+longtime);

var curtime = time();
for(var i=0;i<obArr.length;i++){
	obArr[i] = quick_sort(obArr[i], 'num');
}
var longtime = time(curtime);
console.log('分组排序用时: '+longtime);



// 分组合并排序
// desc 是否降序
function group_sort(group, item, desc){
	var big = group[0];
	for(var i=1;i<group.length;i++){
		var one = group[i]
		  , left = 0
		  , one_i = 0
		  , one_leg = one.length;
		while(one_i<one_leg){
			var it = one[one_i];
			while(1){
				if(!big[left] || it[item]<=big[left][item]){
					big.insert(left, it);
					break;
				}
				left++;
			}
			one_i++;
		}
		// 释放内存
		delete one;
		delete group[i];
	}
	return big;
}

//
function group_sort2(group, item){
	var big = []
	  , i = 0
	  , leg = group.length;
	while(leg--){
		big = big.concat(group[leg]);
	}
	return quick_sort(big, item);
}

//
function group_sort3(group, item){
	var big = []
	  , i = 0
	  , leg = group.length;
	while(leg--){
		big = big.concat(group[leg]);
	}
	return quick_sort(big, item);
}





var curtime = time();

var big = group_sort2(obArr,'num');

var longtime = time(curtime);
console.log('合并排序用时: '+longtime);


big.length = 10;
console.log(big);








/*



var tribe = require('../index.js');

// console.log('asdfas'.split(':'));

//载入配置文件
tribe.Configure('http://local.app.duole.com/test.conf', function(err, conf){

	//console.log(err);
	console.log(conf);

});

var db = tribe.Create('user');

db.columns('id','name','title');

console.log(db);

db.data({'name':'title','yy':'阿德噶三代富贵'});

console.log(db);


// db.data('yy',null);

// console.log(db);

db.data('yy', '杨捷');

console.log(db);

*/
