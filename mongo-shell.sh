kubectl exec -it mongodb-rs0-0 -- mongosh -u databaseAdmin -p databaseAdmin123456 --authenticationDatabase admin

show dbs

show collections

db.prices.findOne()

db.prices.countDocuments()

db.prices.find().sort({event_time: -1}).limit(5)

db.prices_downsampled.find().sort({event_time: -1}).limit(5)