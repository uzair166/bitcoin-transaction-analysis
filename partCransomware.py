import pyspark
from pyspark import SparkContext

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')

def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields) != 4:
            return False
        return True
    except:
        return False

def cryptolocker_vout(line):
        fields = line.split(',')
        if fields[3] == "{1AEoiHY23fbBn8QiJ5y6oAjrhRY1Fb85uc}":
            return True
        return False

def clean_vin(line):
    try:
        fields = line.split(',')
        if len(fields) != 3:
            return False
        return True
    except:
        return False

sc = pyspark.SparkContext()
#laod vout file and filter for rows containing cryptolocker public key
vout = sc.textFile("/data/bitcoin/vout.csv")
vout_cryptolocker = vout.filter(clean_vout).filter(cryptolocker_vout)
#cryptolocker_f: hash, value, n, publickey
cryptolocker_f = vout_cryptolocker.map(lambda line: line.split(","))

numberoftracsntions = cryptolocker_f.count()
print(numberoftracsntions)
print("NUMBER OF TRANSACTIONS")

btcreceived = cryptolocker_f.map(lambda f: ("totalbtc", float(f[1])))
totalbtc = btcreceived.reduceByKey(lambda a,b: a+b)
totalbtc.saveAsTextFile("totalbtc")

cryptolocker_join = cryptolocker_f.map(lambda f: ((f[0], f[2]) , (f[1])))
#cryptolocker_join: key(hash, n)     value(value-amount of btc)

vin = sc.textFile("/data/bitcoin/vin.csv").filter(clean_vin)
vin_f = vin.map(lambda line: line.split(","))
#vin_f: txid (hash of next vout), tx_hash(hash of prev vout), vout (n from prev)
vin_join = vin_f.map(lambda f: ((f[1],f[2]),   (f[0])))
#vin_join: key(prevHash, vout-prevN)    value(nextHash)
#vin_join.saveAsTextFile("vin_join")

joined_data = cryptolocker_join.join(vin_join)
#=============key===========
#joined_data[0][0] = cryptolocker.hash, vin.tx_hash
#joined_data[0][1] = cryptolocker.n, vin
#=============values=========
#joined_data[1][0] =  cryptolocker.values
#joined_data[1][1] = vin.txid (nexthash)

joined_data1 = joined_data.map(lambda f: ((f[1][1]),   (f[1][0])))
#joined_data1: key(vin.txid - nexthash)  value(cryptolocker.value - amount of btc)

vout_f = vout.filter(clean_vout).map(lambda l: l.split(","))
#vout_f: hash, value, n, publickey
vout_join = vout_f.map(lambda f: ((f[0]),   (f[3])))
#vout_join: key(hash)   value(public key)

new_joined_data = joined_data1.join(vout_join)
#=============key===========
#new_joined_data[0] = cryptolocker.hash, vin.tx_hash
#=============values=========
#new_joined_data[1][0] =  cryptolocker.values
#new_joined_data[1][1] = vout.publickey (nexthash)

relevant_data = new_joined_data.map(lambda f: ((f[1][1]),  (float(f[1][0]))))


groupbyaddress = relevant_data.reduceByKey(lambda a,b: a+b)
sorted = groupbyaddress.sortBy(lambda f: -f[1])
sorted.saveAsTextFile("sorted addresses")

print(sorted.count())
print("COUNT OF DESTINATIONS")
