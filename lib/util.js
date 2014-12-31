/**
 * 工具方法
 */


var util = require('util');

/**
 * 深层合并多个对象
 * 最左边的参数被修改  并返回合并后的对象
 */
exports.extend = function () {
    var target = arguments[0] || {},
        i, length = arguments.length,
        options, src, copy, clone, name;
    for (i = 1; i < length; ++i) {
        // Only deal with non-null/undefined values
        if ((options = arguments[i]) !== null) {
            // Extend the base object
            for (name in options) {
                src = target[name];
                copy = options[name];
                // Prevent never-ending loop
                if (target === copy) {
                    continue;
                }
                // Recurse if we're merging objects
                if (typeof(copy) == "object") {
                    clone = (src && typeof(src) == "object" ? src : {});
                    target[name] = exports._extend(clone, copy);
                }
                // Don't bring in undefined values
                else if (copy !== undefined) {
                    target[name] = copy;
                }
            }
        }
    }
    // Return the modified object
    return target;
};



/**
 * 对象的深拷贝！
 */
exports.clone = function(Obj){
    var buf;
    if (Obj instanceof Array) {
        buf = [];
        var i = Obj.length;
        while (i--) {
            buf[i] = arguments.callee(Obj[i]);
        }
        return buf;
    }else if (typeof Obj == "function"){
        return Obj;
    }else if (Obj instanceof Object){
        buf = {};
        for (var k in Obj) {
            buf[k] = arguments.callee(Obj[k]);
        }
        return buf;
    }else{
        return Obj;
    }
};


exports.errWrap = function(error){
    return new Date().format("yyyy-MM-dd hh:mm:ss")+' [TribeDB Error]: '+error;
}


//判断是否为数组
exports.isArray = function(stuff){
    return util.isArray(stuff);
}


//判断是否为空
exports.isEmpty = function(stuff){
	if(!stuff) 
		return true;
	if(typeof stuff=='object'){
		for(var s in stuff){
			return false;
		}
		return true;
	}
	return false;
};


//数组随机取出一项
exports.array_rand = function(arr){
	var n = Math.floor(Math.random() * arr.length + 1)-1;  
	return arr[n];
}

/**
 * 取出数组符合条件的项目
 * @param condition 查询条件
 * @param single 是否取出单条
 * @param del 是否删除符合条件的内容
 */
exports.array_select = function(arr, condition, single, del){
    if(!arr || !condition || typeof condition!='object'){
        return null;
    }
    //开始筛选
    reAry = [];
    for(var a in arr){
    	var one = arr[a];
        //筛选
        var same = true;
        for(var c in condition){
        	var ct = condition[c];
        	if(one[c]!==condition[c]){
                same = false;
                break;
        	}
        }
        if(!same) continue; //条件不满足
        if(del){ //删除
            arr.splice(a,1);
        }
        if(single){ //返回单条
            return one;
        }
        reAry.push(one);
    }
    //返回符合条件的数据
    return reAry.length ? reAry : null;
}




// 取得数组中元素的某个属性 单独返回数组元素
exports.listem = function (ary, key) {
    var reary = []
        , leg = ary.length;
    for(var i=0; i<leg; i++){
        var d = ary[i][key];
        d ? reary.push(d) : null;
    }
    return reary;
};



//去除首尾空格
exports.trim = function(str){
　　 return str.replace(/(^\s*)|(\s*$)/g,"");
}

exports.ltrim = function(str){
    return str.replace(/(^\s*)/g,"");
}

exports.rtrim = function(str){
    return str.replace(/(\s*$)/g,"");
}


// var time1 = new Date().Format("yyyy-MM-dd");
// var time2 = new Date().Format("yyyy-MM-dd HH:mm:ss");  

/**
 * 时间格式化
 */
Date.prototype.format = function(format){

//使用方法
//var now = new Date();
//var nowStr = now.format("yyyy-MM-dd hh:mm:ss");
//使用方法2:
//var testDate = new Date();
//var testStr = testDate.format("YYYY年MM月dd日hh小时mm分ss秒");
//alert(testStr);
//示例：
//alert(new Date().Format("yyyy年MM月dd日"));
//alert(new Date().Format("MM/dd/yyyy"));
//alert(new Date().Format("yyyyMMdd"));
//alert(new Date().Format("yyyy-MM-dd hh:mm:ss"));

    var o = {
        "M+" : this.getMonth()+1, //month
        "d+" : this.getDate(), //day
        "h+" : this.getHours(), //hour
        "m+" : this.getMinutes(), //minute
        "s+" : this.getSeconds(), //second
        "q+" : Math.floor((this.getMonth()+3)/3), //quarter
        "S" : this.getMilliseconds() //millisecond
    };

    if(/(y+)/.test(format)) {
        format = format.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));
    }

    for(var k in o) {
        if(new RegExp("("+ k +")").test(format)) {
            format = format.replace(RegExp.$1, RegExp.$1.length==1 ? o[k] : ("00"+ o[k]).substr((""+ o[k]).length));
        }
    }
    return format;
};

