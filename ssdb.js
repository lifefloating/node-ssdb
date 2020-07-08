/**
 * Copyright (c) 2013, ideawu
 * All rights reserved.
 * @author: ideawu
 *
 * SSDB nodejs client SDK.
 */

'use strict';

const net = require('net');
const socket = new net.Socket();

function SSDB() {
    // base
}

function build_buffer(params) {

    var bs = [];
    for(var i=0;i<params.length;i++){
        var p = params[i];
        var len = 0;
        if(!(p instanceof Buffer)){
            p = p.toString();
            bs.push(Buffer.byteLength(p));
        }else{
            bs.push(p.length);
        }
        bs.push('\n');
        bs.push(p);
        bs.push('\n');
    }
    bs.push('\n');
    var req = build_buffer(bs);
    socket.write(req);
}

function send_request(params){
    var bs = [];
    for(var i=0;i<params.length;i++){
        var p = params[i];
        var len = 0;
        if(!(p instanceof Buffer)){
            p = p.toString();
            bs.push(Buffer.byteLength(p));
        }else{
            bs.push(p.length);
        }
        bs.push('\n');
        bs.push(p);
        bs.push('\n');
    }
    bs.push('\n');
    var req = build_buffer(bs);
    socket.write(req);
}

function request(cmd, params, callback) {
    callbacks.push(callback || function(){});
    var arr = [cmd].concat(params);
    send_request(arr);
}

SSDB.prototype.connect = function(host, port, timeout, listener) {
    var recv_buf = new Buffer(0);
	var callbacks = [];
	var connected = false;

	if(typeof(timeout) == 'function'){
		listener = timeout;
		timeout = 0;
	}
	timeout = timeout || 0;
	listener = listener || function(){};

	socket.on('error', function(e){
		if(!connected){
			listener('connect_failed', e);
		}else{
			var callback = callbacks.shift();
			callback(['error']);
		}
    });
    
    sock.on('data', function(data){
		recv_buf = build_buffer([recv_buf, data]);
		while(recv_buf.length > 0){
			var resp = parse();
			if(!resp){
				break;
			}
			resp[0] = resp[0].toString();
			var callback = callbacks.shift();
			callback(resp);
		}
	});

	socket.connect(port, host, function(){
		connected = true;
		socket.setNoDelay(true);
		socket.setKeepAlive(true);
		socket.setTimeout(timeout);
		listener(0);
	});
}

SSDB.prototype.close = function () {
    socket.end
}


SSDB.prototype.get = function(key, callback){
    request('get', [key], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            var val = resp[1];
            callback(err, val);
        }
    });
}

SSDB.prototype.memchr = function(buf, ch, start) {
    start = start || 0;
    ch = typeof(ch) == 'string'? ch.charCodeAt(0) : ch;
    for(var i=start; i<buf.length; i++){
        if(buf[i] == ch){
            return i;
        }
    }
    return -1;
}

SSDB.prototype.parse = function() {
    var ret = [];
    var spos = 0;
    var pos;
    
    while(true){

        pos = SSDB.prototype.memchr(recv_buf, '\n', spos);
        if(pos == -1){

            return null;
        }
        var line = recv_buf.slice(spos, pos).toString();
        spos = pos + 1;
        line = line.replace(/^\s+(.*)\s+$/, '\1');
        if(line.length == 0){
            recv_buf = recv_buf.slice(spos);
            break;
        }
        var len = parseInt(line);
        if(isNaN(len)){
            // error
            console.log('error 1');
            return null;
        }
        if(spos + len > recv_buf.length){
            return null;
        }
        var data = recv_buf.slice(spos, spos + len);
        spos += len;
        ret.push(data);
        pos = SSDB.prototype.memchr(recv_buf, '\n', spos);
        if(pos == -1){
            console.log('error 3');
            return null;
        }
        var cr = '\r'.charCodeAt(0);
        var lf = '\n'.charCodeAt(0);
        if(recv_buf[spos] != lf && recv_buf[spos] != cr && recv_buf[spos+1] != lf){
            console.log('error 4 ' + recv_buf[spos]);
            return null;
        }
        spos = pos + 1;
    }
    return ret;
}


SSDB.prototype.set = function(key, val, callback){
    request('set', [key, val], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err);
        }
    });
}


SSDB.prototype.setx = function(key, val, ttl, callback){
    request('setx', [key, val, ttl], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err, resp[1].toString());
        }
    });
}

SSDB.prototype.ttl = function(key, callback){
    request('ttl', [key], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err,resp[1].toString());
        }
    });
}


const commands = {
    set               : 'int',
    setx              : 'int',
    expire            : 'int',
    ttl               : 'int',
    setnx             : 'int',
    get               : 'str',
    getset            : 'str',
    del               : 'int',
    incr              : 'int',
    exists            : 'bool',
    getbit            : 'int',
    setbit            : 'int',
    countbit          : 'int',
    substr            : 'str',
    strlen            : 'int',
    keys              : 'list',
    scan              : 'list',
    rscan             : 'list',
    multi_set         : 'int',
    multi_get         : 'list',
    multi_del         : 'int',
    hset              : 'int',
    hget              : 'str',
    hdel              : 'int',
    hincr             : 'int',
    hexists           : 'bool',
    hsize             : 'int',
    hlist             : 'list',
    hrlist            : 'list',
    hkeys             : 'list',
    hgetall           : 'list',
    hscan             : 'list',
    hrscan            : 'list',
    hclear            : 'int',
    multi_hset        : 'int',
    multi_hget        : 'list',
    multi_hdel        : 'int',
    zset              : 'int',
    zget              : 'int',
    zdel              : 'int',
    zincr             : 'int',
    zexists           : 'bool',
    zsize             : 'int',
    zlist             : 'list',
    zrlist            : 'list',
    zkeys             : 'list',
    zscan             : 'list',
    zrscan            : 'list',
    zrank             : 'int',
    zrrank            : 'int',
    zrange            : 'list',
    zrrange           : 'list',
    zclear            : 'int',
    zcount            : 'int',
    zsum              : 'int',
    zavg              : 'float',
    zremrangebyrank   : 'int',
    zremrangebyscore  : 'int',
    zpop_front        : 'list',
    zpop_back         : 'list',
    multi_zset        : 'int',
    multi_zget        : 'list',
    multi_zdel        : 'int',
    qsize             : 'int',
    qclear            : 'int',
    qfront            : 'str',
    qback             : 'str',
    qget              : 'str',
    qslice            : 'list',
    qpush             : 'int',
    qpush_front       : 'int',
    qpush_back        : 'int',
    qpop              : 'str',
    qpop_front        : 'str',
    qpop_back         : 'str',
    __qpop_front      : 'list',
    __qpop_back       : 'list',
    qlist             : 'list',
    qrange            : 'list',
    qrlist            : 'list',
    dbsize            : 'int',
    info              : 'list',
    auth              : 'bool'
  };



/*
example:
var SSDB = require('./SSDB.js');
var ssdb = SSDB.connect(host, port, function(err){
	if(err){
		return;
	}
	ssdb.set('a', new Date(), function(){
		console.log('set a');
	});
});
*/

