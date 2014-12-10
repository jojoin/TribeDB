/**
 * 解析配置文件
 * 配置文件示例： example/test.conf
 */


var http = require('http');
var fs = require('fs');
var path = require('path');


var util = require('./util.js');

//原始的 配置 字符串
var config_text = '';

//解析后的配置
exports.config = {
	system:{}, //系统配置
	database: {
		default:'', //默认数据库标识 default
		master:{}, //主库
		slave:{}   //从库
	},
	table: []
};


/**
 //配置格式

 { database:
   { db1:
      { host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: '',
        database: 'dl_fm' },
     db2:
      { host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: '',
        database: 'dl_fm2' } },
  table:
   [ { table: 'mytable', divide: null, section: null, db: 'db2' },
     { table: 'mytable', divide: null, section: null, db: 'db1' },
     { table: 'mytable2', divide: 'id', section: '100000', db: 'db1' }
   ] 
 }


*/


/**
 * 判断是否为合法的路径
 */
function isPath(text){
	var first = text.charAt(0);
	if(first=='#'
	 ||first=='\n'
	 ){
	 	return false;
	}
	if(text.indexOf('\n')>-1
	|| text.indexOf('#')>-1
	|| text.indexOf('#')>-1
	){
	 	return false;
	}
	// 可能为路径
	return true;
}


/**
 * 载入配置文件内容
 * @param src 配置文件url、路径或正文
 * @param callback(err, conf) 第一个参数为错误信息 
 */
exports.load = function(src, opt, callback){
	if(src===undefined){
		return config_text; //获取配置文本
	}

	if(!opt){
		opt = {};
	}

	if(typeof opt=='function'){
		callback = opt;
		opt = {};
	}

	callback = callback || function(){};
	
	// 检测配置是否满足要求
	if(typeof src != 'string'){
		return callback(util.errWrap('The config src is invalid !'));
	}

	config_text = src;

	// url配置文件
	var head = src.substr(0,7);
	if(head=='http://' || head=='https:/'){
		// 配置文件url
		http.get(src, function(res){
			// 接收配置文件
			var content = '';
			res.on('data',function(chunk){
		        content += chunk;
		    });
		    res.on('end',function(){
		    	// 解析配置
		    	// console.log(content);
		    	configPretreatment(content, callback);
		    });
		}).on('error', function(e) {
			callback(util.errWrap(e)); // 错误回调
		});
		return
	}

	//是否为一个文件路径
	if(isPath(src)){
		// 同步读取文件内容
		if(opt.sync){
			src = fs.readFileSync(src, {encoding: 'utf8'});
		}else{
			fs.readFile(src, function(err, data){
			  if(err)
			  	throw err;
			  // console.log(data);
			  configPretreatment(data, callback);
			});
			return 
		}
	}

	// 视为文本配置 直接解析

	// 配置正文直接解析
	// if(!src.indexOf('[databases]')>-1){
	// 	return callback(util.errWrap('The config content is invalid !'));
	// }

	// 检测成功 正式开始解析
	return configPretreatment(src, callback);

};







/**
 * 预处理配置文件
 * @param text 配置文件正文
 * @return = int 返回数字表示配置文件错误行数
 * @return = object 返回对象表示解析成功
 */
function configPretreatment(text, callback){
    text = text +'';
	if(!text || text.length < 11){
		return callback("[TribeDB Error]: Configuration is not valid ! \n"+text);
	}

	// 纯净的配置内容
	var config = [];

	// # 注释 ; 和 | 换行预处理
	text = text
		.replace(/\#.*/g,'')     //去除 # 注释
		.replace(/;+/g,"\n")     // ; 分号替换为换行
		.replace(/[\n\r\s]*\|+[\n\r\s]*/g,'');  // | 归并到一行

	//正式开始解析
	var line_num = 0
	  , conf = text.split("\n");

	for(var c in conf){
		line_num++;
		var line = conf[c].replace(/\s*/g,''); //去除空白字符

		if(!line) continue; //忽略空行或注释行

		config.push({
			num:line_num, 
			row:line
		});

	}

	// log(config);
	// 正式开始解析配置
	var result = configParse(config);
	if(parseInt(result)>=0){
		var err = config[result];
		return callback(util.errWrap('Error configuration on line '+err.num+' : "'
			+err.row+'"'));
	}
	// log(result);

	// 解析成功
	exports.config = result;

	callback(null, result);

	// 回调
	return result;
}







/**
 * 解析配置文件
 * @param conf 配置文件正文
 * @return = int 返回数字表示配置文件错误行数
 * @return = object 返回对象表示解析成功
 */
function configParse(conf){

	// 当前配置项目
	var config = exports.config
	  , cur_item = 'system' // system、databases、tables
	  , cur_db_type = 'master' //当前库配置的是主库还是从库
	  , cur_db = 'default' //当前数据库
	  , first_db = '' //第一个数据库，可能被当作默认
	  , process = {}
	  , db_i = 0; //数据库标识

	process.system = function(row){
		var item = row.split('=',2);
		if(item.length<2){
			return true;
		}
		var name = item[0], value = item[1];

		// 设置值为 yes 或 no 的项目
		// var bools = ['read_only','master_only','error_time'];
		if(name=='master_only'){
			if(value=='%'){
				config.system[name] = true;
			}else{
				config.system[name] = {};
				var dbs = value.split(',');
				for(var d in dbs){
					config.system[name][dbs[d]] = true;
				}
			}
			return true;
		}


		return true;
	}

	// 解析一行数据库配置
	// db1 = 127.0.0.1, 3306, root, password, dl_fm
	process.database = function(row){
		if(row=='master:'){
			return cur_db_type = 'master';
		}else if(row=='slave:'){
			return cur_db_type = 'slave';
		}

		var db = row.split('=')
		  , leg = db.length;
		if(leg>2){
			return false; //解析失败
		}
		if(leg==1){
			db.push('');
		}
		var db_id = db[0];
		if(!first_db){
			first_db = db_id;
		}
		db = db[1].split(',');
		if(!db){
			return false; //解析失败
		}
		//存入配置
		if(!config.database[cur_db_type][db_id]){
			config.database[cur_db_type][db_id] = [];
		}
		db_i++;
		config.database[cur_db_type][db_id].push({
			id: db_i, // 数据库唯一id值
			name: db_id, //数据库名称（标识）
			host: db[0] || '127.0.0.1',
			port: db[1] || 3306,
			user: db[2] || 'root',
			password: db[3] || '',
			database: db[4] || db_id
		});

		return true;
	}

	// 解析一行数据表配置
	process.table = function(row){
		//如果行末为冒号: 则设置当前使用数据库
		var colon = row.indexOf(':');
		if(colon>-1){
			if(colon!=row.length-1){
				return false;
			}
			cur_db = row.substr(0,row.length-1);
			return true;
		}
		var tb = row.split('=')
		  , leg = tb.length;
		if(leg>2){
			return false; //解析失败
		}
		if(leg==1){
			tb.push('');
		}
		var table = tb[0];
		tb = tb[1].split(',');
		// 1m = 100w = 1000k = 1000*1000
		section = tb[0] || '';
		sect = parseInt(section) || 0;
		if(section.indexOf('k')>-1) sect *= 1000;
		if(section.indexOf('w')>-1) sect *= 10000;
		if(section.indexOf('m')>-1) sect *= 1000*1000;

		//解析表配置
		var cnf = {
			table: table,
			section: sect || null,
			db: cur_db
		}
		// 默认自增id
		cnf.divide = cnf.section ? (tb[1] || 'id') : null;
		config.table.push(cnf);

		return true;
	}

	// 获取当前环境
	function getCurItem(row){
		for(var item in config){
			if(row=='['+item+']'){
				cur_item = item;
				return item;
			}
		}
	}

	// 正式开始解析
	for(var c in conf){
		var line = conf[c]
		  , num = line.num
		  , row = line.row
		  ;
		if(getCurItem(row))
			continue; //得到当前环境

		//解析配置
		for(var item in config){
			if(item==cur_item){
				var ok = process[item](row);
				if(!ok) return c; // 解析失败返回错误行
				break;
			}
		}
	}

	//设置默认数据库
	if(!config.database['default']){
		//第一个定义的数据库被当成默认的数据库
		config.database['default'] = first_db;
	}else{
		config.database['default'] = 'default';
	}


	return config;
}

