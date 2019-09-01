
// Merge Balancer for a Key-Value store.

var fs = require('fs')
var http = require('http')
var url = require('url')

// A basic Key-Value Store
function KeyValueStore(data_path) {
	var keyToFile = (key) => data_path + '/' + key;

	this.get = (key, callback) => fs.readFile(keyToFile(key), 'utf8', callback);

	this.put = (key, value, callback) => fs.writeFile(keyToFile(key), value, 'utf8', callback);
}


/*************************************** TODO ***********************************/

function MergeBalancerKeyValueStore(db1, db2) {
	var putBalancer = () => true;

	this.get = function(key,callback){
		db1.get(key,function(err,res){
			if(err){
				db2.get(key,function(err,res){
					callback(err,res)
				})
			}
			else{
				callback(err,res)
			}
		})
	};

	this.put = function(key,value,callback){
		if(putBalancer()){
			db1.put(key,value,callback)
		}else{
			db2.put(key,value,callback)
		}
	}

	this.setPutBalancer = function(rr){
		putBalancer = rr
	}
}

var createRoundRobinBalancer = function() {
	var counter = true;
	return function(){
		if(counter == true){
			counter = false;
		}else {
			counter = true;
		}
		return !counter
	}
}


/*************************************** TEST ***********************************/

// Farem servir funcions síncrones per simplificar el test.

// 1)
// Primer li fem un reset a les carpetes de test perque sempre tinguin el que toca.

var files;

files = fs.readdirSync('data_folder1')
files.forEach(file => { try { fs.unlinkSync('data_folder1/' + file) } catch (err) {} });

files = fs.readdirSync('data_folder2')
files.forEach(file => { try { fs.unlinkSync('data_folder2/' + file) } catch (err) {} });

files = fs.readdirSync('data_folder3')
files.forEach(file => { try { fs.unlinkSync('data_folder3/' + file) } catch (err) {} });

fs.writeFileSync('data_folder1/keyA', 'valueA');
fs.writeFileSync('data_folder1/keyB', 'valueB');
fs.writeFileSync('data_folder2/keyC', 'valueC');
fs.writeFileSync('data_folder2/keyD', 'valueD');
fs.writeFileSync('data_folder2/keyE', 'valueE');
fs.writeFileSync('data_folder3/keyF', 'valueF');
fs.writeFileSync('data_folder3/keyG', 'valueG');

// 2) Creem les KeyValueStore

var db1 = new KeyValueStore('data_folder1');
var db2 = new KeyValueStore('data_folder2');
var db3 = new KeyValueStore('data_folder3');

var mdb = new MergeBalancerKeyValueStore(db3, new MergeBalancerKeyValueStore(db2, db1));

mdb.setPutBalancer(createRoundRobinBalancer());

// 3) Test that we can get all key-value pairs.

var letters = ['A', 'B', 'C', 'D', 'E', 'F' ,'G'];

letters.forEach(function(letter) {
	mdb.get('key' + letter, function(e,r) {
		if (e || r != 'value' + letter) {
			console.log('Test fails for key' + letter)
		}
		else{
			console.log('Test is OK for key' + letter)
		}
	});
});

// 5) Test that we can add a new key-values in the right places

mdb.put('newKey1', 'newValue1', function(err) {
mdb.put('newKey2', 'newValue2', function(err) {
mdb.put('newKey3', 'newValue3', function(err) {
mdb.put('newKey4', 'newValue4', function(err) {
				
	var folders = ['data_folder1', 'data_folder2', 'data_folder3']

	console.log('############################## TEST ##############################');

	folders.forEach(folder => {
		var files = fs.readdirSync(folder)

		files.forEach(file => { 
			console.log(folder + '/' + file + '=' + fs.readFileSync(folder + '/' + file))
		})
	})

	console.log('##################################################################');

});
});
});
});



