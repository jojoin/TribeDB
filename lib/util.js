/**
 * 工具方法
 */


exports.errWrap = function(error){
	return '[TribeDB Error]: '+error;
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
exports.array_rand=function(arr){
	var n = Math.floor(Math.random() * arr.length + 1)-1;  
	return arr[n];
}


//去除首尾空格
String.prototype.trim=function(){
　　 return this.replace(/(^\s*)|(\s*$)/g,"");
}

String.prototype.ltrim=function(){
    return this.replace(/(^\s*)/g,"");
}

String.prototype.rtrim=function(){
    return this.replace(/(\s*$)/g,"");
}


