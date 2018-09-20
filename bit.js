const zmq = require('zeromq');
const RpcClient = require('bitcoind-rpc');
const bton = require('bton')
const pLimit = require('p-limit');
const pQueue = require('p-queue');
const Config = require('./config.json')
const queue = new pQueue({concurrency: Config.rpc.limit});
const mingo = require('mingo')

const Filter = require('./bitdb.json')
const Encoding = require('./encoding')
var Db;
var Info;
var rpc;
var filter;

const init = function(db, info) {
  return new Promise(function(resolve, reject) {
    Db = db;
    Info = info;

    if (Filter.filter) {
      let q;
      if (Filter.filter.encoding) {
        q = Encoding(Filter.filter.find, Filter.filter.encoding);
      } else {
        q = Filter.filter.find;
      }
      console.log('shard filter = ', q)
      filter = new mingo.Query(q)
    } else {
      filter = null;
    }

    rpc = new RpcClient(Config.rpc)
    resolve();
  })
};
const request = {
  block: function(block_index) {
    return new Promise(function(resolve, reject) {
      rpc.getBlockHash(block_index, function(err, res) {
        if (err) {
          console.log("Err = ", err)
        } else {
          rpc.getBlock(res.result, function(err, block) {
            resolve(block)
          })
        }
      })
    })
  },
  /**
  * Return the current blockchain height
  */
  height: function() {
    return new Promise(function(resolve, reject) {
      rpc.getBlockCount(function(err, res) {
        if (err) {
          console.log(err)
        } else {
          resolve(res.result)
        }
      })
    })
  },
  tx: async function(hash) {
    // Only index output for now
    let content = await bton.fromHash(hash)
    return content.filter(function(c) {
      return c.type === 'o'
    })
  },
  mempool: function() {
    return new Promise(function(resolve, reject) {
      rpc.getRawMemPool(async function(err, ret) {
        if (err) {
          console.log("Err", err)
        } else {
          let tasks = []
          const limit = pLimit(Config.rpc.limit)
          let txs = ret.result;
          console.log("txs = ", txs.length)
          for(let i=0; i<txs.length; i++) {
            tasks.push(limit(async function() {
              let content = await request.tx(txs[i]).catch(function(e) {
                console.log("Error = ", e)
              })
              return content;
            }))
          }
          let xputs = await Promise.all(tasks)
          console.log("xputs = ", xputs)
          let tx_contents = []
          for(let i=0; i<xputs.length; i++) {
            tx_contents = tx_contents.concat(xputs[i])
          }
          resolve(tx_contents)
        }
      })
    })
  }
}
const crawl = async function(block_index) {
  let block_content = await request.block(block_index)
  let block_hash = block_content.result.hash;
  let block_time = block_content.result.time;

  if (block_content && block_content.result) {
    let txs = block_content.result.tx;
    let tasks = []
    const limit = pLimit(Config.rpc.limit)
    for(let i=0; i<txs.length; i++) {
      tasks.push(limit(async function() {
        let content = await request.tx(txs[i]).catch(function(e) {
          console.log("Error = ", e)
        })
        content.forEach(function(t) {
          t.block_index = block_index;
          t.block_hash = block_hash;
          t.block_time = block_time;
        })
        return content;
      }))
    }
    let xputs = await Promise.all(tasks)

    let tx_contents = []
    for(let i=0; i<xputs.length; i++) {
      tx_contents = tx_contents.concat(xputs[i])
    }

    
    console.log("Xputs = ", tx_contents.length);
    if (filter) {
      tx_contents = tx_contents.filter(function(row) {
        return filter.test(row)
      })
      console.log("Filtered Xputs = ", tx_contents.length);
    }

    console.log("Block " + block_index + " : " + txs.length + "txs | " + tx_contents.length + " outputs")
    return tx_contents;
  } else {
    return []
  }
}
const listen = function() {
	let sock = zmq.socket('sub');
	sock.connect('tcp://' + Config.zmq.incoming.host + ':' + Config.zmq.incoming.port);
	sock.subscribe('hashtx');
	sock.subscribe('hashblock');
	console.log('Subscriber connected to port ' + Config.zmq.incoming.port);

  let outsock = zmq.socket('pub');
  outsock.bindSync('tcp://' + Config.zmq.outgoing.host + ':' + Config.zmq.outgoing.port);
  console.log('Started publishing to ' + Config.zmq.outgoing.host + ":" + Config.zmq.outgoing.port);

  // Listen to ZMQ
	sock.on('message', async function(topic, message) {
		if (topic.toString() === 'hashtx') {
			let hash = message.toString('hex')
      console.log("New mempool hash from ZMQ = ", hash)
      let m = await sync("mempool", hash)
      outsock.send(['mempool', m]);
		} else if (topic.toString() === 'hashblock') {
			let hash = message.toString('hex')
      console.log("New block hash from ZMQ = ", hash)
      let m = await sync("block")
      // get mempool
      // sync mempool db
      if (m) {
        // if the there was a new block, send message
        outsock.send(['block', m]);
      }
		}
	});

  // Don't trust ZMQ. Try synchronizing every 1 minute in case ZMQ didn't fire
  setInterval(async function() {
    let m = await sync("block")
    if (m) {
      // if the there was a new block, send message
      outsock.send(['block', m]);
    }
  }, 60000)

}

const sync = async function(type, hash) {
  if (type === 'block') {
    const lastSynchronized = await Info.checkpoint()
    const currentHeight = await request.height()
    console.log("Last Synchronized = ", lastSynchronized)
    console.log("Current Height = ", currentHeight)

    try {
      for(let index=lastSynchronized+1; index<=currentHeight; index++) {
        console.log("RPC BEGIN " + index, new Date().toString())
        console.time("RPC END " + index)
        let content = await crawl(index)
        console.timeEnd("RPC END " + index)
        console.log(new Date().toString())
        console.log("DB BEGIN " + index, new Date().toString())
        console.time("DB Insert " + index)

        await Db.block.insert(content, index)

        await Info.updateTip(index)
        console.timeEnd("DB Insert " + index)
        console.log("------------------------------------------")
        console.log("\n")
      }

      // clear mempool and synchronize
      if (lastSynchronized < currentHeight) {
        console.log("Clear mempool and repopulate")
        let items = await request.mempool()
        await Db.mempool.sync(items)
      }

    } catch (e) {
      console.log("Error", e)
      console.log("Shutting down Bitdb...", new Date().toString())
      await Db.exit()
      process.exit()
    }

    if (lastSynchronized === currentHeight) {
      console.log("no update")
      return null;
    } else {
      console.log("[finished]")
      return currentHeight;
    }
  } else if (type === 'mempool') {
    queue.add(async function() {
      let content = await request.tx(hash)
      await Db.mempool.insert(content)
      console.log("# Q inserted [size: " + queue.size + "]",  hash)
      console.log(content)
    })
    return hash
  }
}
const run = async function() {

  // initial block sync
  await sync("block")

  // initial mempool sync
  console.log("Clear mempool and repopulate")
  let items = await request.mempool()
  await Db.mempool.sync(items)
}
module.exports = {
  init: init, crawl: crawl, listen: listen, sync: sync, run: run
}
