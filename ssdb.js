'use strict';

const net = require('net');
const socket = new net.Socket();

function SSDB() {
    // base
}

function build_buffer(arr) {
    var bs = [];
    var size = 0;
    for (var i = 0; i < arr.length; i++) {
        var arg = arr[i];
        if (arg instanceof Buffer) {
            // pass
        } else {
            arg = new Buffer(arg.toString());
        }
        bs.push(arg);
        size += arg.length;
    }
    var ret = new Buffer(size);
    var offset = 0;
    for (var i = 0; i < bs.length; i++) {
        bs[i].copy(ret, offset);
        offset += bs[i].length;
    }
    return ret;
}

function send_request(params) {
    var bs = [];
    for (var i = 0; i < params.length; i++) {
        var p = params[i];
        var len = 0;
        if (!(p instanceof Buffer)) {
            p = p.toString();
            bs.push(Buffer.byteLength(p));
        } else {
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
    callbacks.push(callback || function () { });
    var arr = [cmd].concat(params);
    send_request(arr);
}

SSDB.prototype.connect = function (host, port, timeout, listener) {
    var recv_buf = new Buffer(0);
    var callbacks = [];
    var connected = false;

    if (typeof (timeout) == 'function') {
        listener = timeout;
        timeout = 0;
    }
    timeout = timeout || 0;
    listener = listener || function () { };

    socket.on('error', function (e) {
        if (!connected) {
            listener('connect_failed', e);
        } else {
            var callback = callbacks.shift();
            callback(['error']);
        }
    });

    sock.on('data', function (data) {
        recv_buf = build_buffer([recv_buf, data]);
        while (recv_buf.length > 0) {
            var resp = parse();
            if (!resp) {
                break;
            }
            resp[0] = resp[0].toString();
            var callback = callbacks.shift();
            callback(resp);
        }
    });

    socket.connect(port, host, function () {
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


SSDB.prototype.get = function (key, callback) {
    request('get', [key], function (resp) {
        if (callback) {
            var err = resp[0] == 'ok' ? 0 : resp[0];
            var val = resp[1];
            callback(err, val);
        }
    });
}

SSDB.prototype.memchr = function (buf, ch, start) {
    start = start || 0;
    ch = typeof (ch) == 'string' ? ch.charCodeAt(0) : ch;
    for (var i = start; i < buf.length; i++) {
        if (buf[i] == ch) {
            return i;
        }
    }
    return -1;
}

SSDB.prototype.parse = function () {
    var ret = [];
    var spos = 0;
    var pos;

    while (true) {

        pos = SSDB.prototype.memchr(recv_buf, '\n', spos);
        if (pos == -1) {

            return null;
        }
        var line = recv_buf.slice(spos, pos).toString();
        spos = pos + 1;
        line = line.replace(/^\s+(.*)\s+$/, '\1');
        if (line.length == 0) {
            recv_buf = recv_buf.slice(spos);
            break;
        }
        var len = parseInt(line);
        if (isNaN(len)) {
            // error
            console.log('error 1');
            return null;
        }
        if (spos + len > recv_buf.length) {
            return null;
        }
        var data = recv_buf.slice(spos, spos + len);
        spos += len;
        ret.push(data);
        pos = SSDB.prototype.memchr(recv_buf, '\n', spos);
        if (pos == -1) {
            console.log('error 3');
            return null;
        }
        var cr = '\r'.charCodeAt(0);
        var lf = '\n'.charCodeAt(0);
        if (recv_buf[spos] != lf && recv_buf[spos] != cr && recv_buf[spos + 1] != lf) {
            console.log('error 4 ' + recv_buf[spos]);
            return null;
        }
        spos = pos + 1;
    }
    return ret;
}


SSDB.prototype.set = function (key, val, callback) {
    request('set', [key, val], function (resp) {
        if (callback) {
            var err = resp[0] == 'ok' ? 0 : resp[0];
            callback(err);
        }
    });
}


SSDB.prototype.setx = function (key, val, ttl, callback) {
    request('setx', [key, val, ttl], function (resp) {
        if (callback) {
            var err = resp[0] == 'ok' ? 0 : resp[0];
            callback(err, resp[1].toString());
        }
    });
}

SSDB.prototype.ttl = function (key, callback) {
    request('ttl', [key], function (resp) {
        if (callback) {
            var err = resp[0] == 'ok' ? 0 : resp[0];
            callback(err, resp[1].toString());
        }
    });
}

SSDB.prototype.del = function (key, callback) {
    request('del', [key], function (resp) {
        if (callback) {
            var err = resp[0] == 'ok' ? 0 : resp[0];
            callback(err);
        }
    });
}

SSDB.prototype.scan = function(key_start, key_end, limit, callback){
    request('scan', [key_start, key_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length % 2 != 1){
                callback('error');
            }else{
                var data = {index: [], items: {}};
                for(var i=1; i<resp.length; i+=2){
                    var k = resp[i].toString();
                    var v = resp[i+1].toString();
                    data.index.push(k);
                    data.items[k] = v;
                }
                callback(err, data);
            }
        }
    });
}

SSDB.prototype.keys = function(key_start, key_end, limit, callback){
    request('keys', [key_start, key_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            var data = [];
            for(var i=1; i<resp.length; i++){
                var k = resp[i].toString();
                data.push(k);
            }
            callback(err, data);
        }
    });
}


SSDB.prototype.zget = function(name, key, callback){
    request('zget', [name, key], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length == 2){
                var score = parseInt(resp[1]);
                callback(err, score);
            }else{
                var score = 0;
                callback('error');
            }
        }
    });
}

SSDB.prototype.zsize = function(name, callback){
    request('zsize', [name], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length == 2){
                var size = parseInt(resp[1]);
                callback(err, size);
            }else{
                var score = 0;
                callback('error');
            }
        }
    });
}

SSDB.prototype.zset = function(name, key, score, callback){
    request('zset', [name, key, score], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err);
        }
    });
}

SSDB.prototype.zdel = function(name, key, callback){
    request('zdel', [name, key], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err);
        }
    });
}

SSDB.prototype.zscan = function(name, key_start, score_start, score_end, limit, callback){
    request('zscan', [name, key_start, score_start, score_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length % 2 != 1){
                callback('error');
            }else{
                var data = {index: [], items: {}};
                for(var i=1; i<resp.length; i+=2){
                    var k = resp[i].toString();
                    var v = parseInt(resp[i+1]);
                    data.index.push(k);
                    data.items[k] = v;
                }
                callback(err, data);
            }
        }
    });
}

SSDB.prototype.zsum = function(name, score_start, score_end, callback){
    request('zsum', [name,score_start,score_end], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length == 2){
                var size = parseInt(resp[1]);
                callback(err, size);
            }else{
                callback('error');
            }
        }
    });
}

SSDB.prototype.zlist = function(name_start, name_end, limit, callback){
    request('zlist', [name_start, name_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            var data = [];
            for(var i=1; i<resp.length; i++){
                var k = resp[i].toString();
                data.push(k);
            }
            callback(err, data);
        }
    });
}

SSDB.prototype.hset = function(name, key, val, callback){
    request('hset', [name, key, val], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err);
        }
    });
}

SSDB.prototype.hdel = function(name, key, callback){
    request('hdel', [name, key], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            callback(err);
        }
    });
}

SSDB.prototype.hscan = function(name, key_start, key_end, limit, callback){
    request('hscan', [name, key_start, key_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length % 2 != 1){
                callback('error');
            }else{
                var data = {index: [], items: {}};
                for(var i=1; i<resp.length; i+=2){
                    var k = resp[i].toString();
                    var v = resp[i+1].toString();
                    data.index.push(k);
                    data.items[k] = v;
                }
                callback(err, data);
            }
        }
    });
}

SSDB.prototype.hlist = function(name_start, name_end, limit, callback){
    request('hlist', [name_start, name_end, limit], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            var data = [];
            for(var i=1; i<resp.length; i++){
                var k = resp[i].toString();
                data.push(k);
            }
            callback(err, data);
        }
    });
}

SSDB.prototype.hsize = function(name, callback){
    request('hsize', [name], function(resp){
        if(callback){
            var err = resp[0] == 'ok'? 0 : resp[0];
            if(resp.length == 2){
                var size = parseInt(resp[1]);
                callback(err, size);
            }else{
                var score = 0;
                callback('error');
            }
        }
    });
}

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

