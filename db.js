const MongoClient = require('mongodb').MongoClient;
var db;
var mongo;
var config;
var init = function(_config) {
  config = _config;
  return new Promise(function(resolve, reject) {
    MongoClient.connect(_config.url, {useNewUrlParser: true}, function(err, client) {
      if (err) console.log(err)
      db = client.db(_config.name)
      mongo = client;
      resolve()
    })
  })
}
var exit = function() {
  return new Promise(function(resolve, reject) {
    mongo.close()
    resolve()
  })
}
var mempool =  {
  insert: async function(item) {
    await db.collection("unconfirmed").insertMany(item).catch(function(err) {
      console.log("## ERR ", err, item)
    })
  },
  reset: async function() {
    await db.collection("unconfirmed").deleteMany({}).catch(function(err) {
      console.log("## ERR ", err)
    })
  },
  sync: async function(items) {
    await db.collection("unconfirmed").deleteMany({}).catch(function(err) {
      console.log("## ERR ", err)
    })
    let index = 0;
    while (true) {
      let chunk = items.splice(0, 1000)
      if (chunk.length > 0) {
        await db.collection("unconfirmed").insertMany(chunk, { ordered: false }).catch(function(err) {
          console.log("## ERR ", err, items)
        })
        console.log("..chunk " + index + " processed ...", new Date().toString())
        index++;
      } else {
        break;
      }
    }
    console.log("Mempool synchronized with " + items.length + " items")
  }
}
var block = {
  reset: async function() {
    await db.collection("confirmed").deleteMany({}).catch(function(err) {
      console.log("## ERR ", err)
    })
  },
  replace: async function(items, block_index) {
    console.log("Deleting all blocks greater than or equal to", block_index)
    await db.collection("confirmed").deleteMany({
      block_index: {
        $gte: block_index
      }
    }).catch(function(err) {
      console.log("## ERR ", err)
    })
    console.log("Updating block", block_index, "with", items.length, "items")
    let index = 0;
    while (true) {
      let chunk = items.splice(0, 1000)
      if (chunk.length > 0) {
        await db.collection("confirmed").insertMany(chunk, { ordered: false }).catch(function(err) {
          console.log("## ERR ", err, items)
        })
        console.log("\tchunk " + index + " processed ...")
        index++;
      } else {
        break;
      }
    }
  },
  insert: async function(items, block_index) {
    let index = 0;
    try {
      while (true) {
        let chunk = items.splice(0, 1000)
        if (chunk.length > 0) {
          await db.collection("confirmed").insertMany(chunk, { ordered: false })
          console.log("..chunk " + index + " processed ...")
          index++;
        } else {
          break;
        }
      }
    } catch (e) {
      console.log("## ERR ", e, items, block_index)
      throw e;
    }
    console.log("Block " + block_index + " inserted ")
  },
  index: async function() {
    console.log("* Indexing MongoDB...")
    console.time("TotalIndex")
    if (config.index && config.index.keys) {
      console.log("Indexing keys...")
      let keys = config.index.keys;
      for(let i=0; i<keys.length; i++) {
        let o = {}
        o[keys[i]] = 1;
        console.time("Index:" + keys[i])
        await db.collection("confirmed").createIndex(o).catch(function(e) {
          console.log("index " + keys[i] + " already exists")
        })
        console.log("* Created index for ", keys[i])
        console.timeEnd("Index:" + keys[i])
      }
    }
    if (config.index && config.index.fulltext) {
      console.log("Creating full text index...")
      let fulltext = {};
      config.index.fulltext.forEach(function(key) {
        fulltext[key] = "text";
      })
      await db.collection("confirmed").createIndex(fulltext, {
        name: "fulltext"
      }).catch(function(e) {
        console.log("text search index already exists")
      })
    }
    console.log("* Finished indexing MongoDB...")
    console.timeEnd("TotalIndex")

    let result = await db.collection("confirmed").indexInformation({full: true}).catch(function(e) {
      console.log("* Error indexing ", e)
    })
    console.log("* Result = ", result)
  }
}
module.exports = {
  init: init, exit: exit, block: block, mempool: mempool
}
