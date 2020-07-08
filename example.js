var SSDB = require('./ssdb.js');

var host = '127.0.0.1'
var port = 8888

var ssdb = new SSDB()
ssdb.client(host, port)

ssdb.set('a', new Date(), function(){
	console.log('set a');
});
ssdb.get('a', function(err, val){
	console.log('get a = ' + val);
	ssdb.close();
});