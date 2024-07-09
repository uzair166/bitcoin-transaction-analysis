import pyspark
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')

#ignore lines that may not be valid
def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False
        return True

    except:
        return False

def clean_vin(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False
        return True

    except:
        return False

#select lines with the wikileaks address
def wikileaks_vout(line):
    fields = line.split(',')
    if fields[3] == "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}":
        return True
    return False

sc = pyspark.SparkContext()
#load vout file and filter for wikileans public key
vout = sc.textFile("/data/bitcoin/vout.csv")
voutwikileaks = vout.filter(clean_vout).filter(wikileaks_vout)
voutwiki_f = voutwikileaks.map(lambda line: line.split(","))
#Define vout hash as join key, and values are amount of btc, n and wallet address
voutwiki_join = voutwiki_f.map(lambda f: (f[0],(f[1],f[2],f[3])))

#load vin file and remove bad lines
vin = sc.textFile("/data/bitcoin/vin.csv")
vin_f = vin.filter(clean_vin).map(lambda line: line.split(","))
#id of transaction coins are going into as key and values are transaction coins are coming from and output id of previous transaction
vin_join = vin_f.map(lambda f: (f[0],(f[1],f[2])))


#======key====== joined_data[0] = vout.hash, vin.txid
#joined_data[1][0][0] = voutwiki.value (amount of btc)
#joined_data[1][0][1] = voutwiki.n (id for output within this transaction)
#joined_data[1][0][2] = voutwiki.publicKey (id of wallet where coins are being sent)
#joined_data[1][1][0] = vin.tx_hash (transaction coins are coming from)
#joined_data[1][1][1] = vin.vout (output id of previous transaction)
joined_data = voutwiki_join.join(vin_join)


#=============key===========
#joined_data[1][1][0] = vin.tx_hash (transaction coins are coming from)
#joined_data[1][1][1] = vin.vout (output id of previous transaction)
#=============values=========
#joined_data[0] = voutwiki.hash, vin.txid
#joined_data[1][0][0] = voutwiki.value (amount of btc)
#joined_data[1][0][1] = voutwiki.n (id for output within this transaction)
#joined_data[1][0][2] = voutwiki.publicKey (id of wallet where coins are being sent)
joined_data1 = joined_data.map(lambda f: ((f[1][1][0],f[1][1][1]),(f[0],f[1][0][0],f[1][0][1],f[1][0][2])))


vout_f = vout.filter(clean_vout).map(lambda line: line.split(','))
# #key is vout.hash, vout.n and values are vout.value and vout.publicKey
vout_join = vout_f.map(lambda fields: ((fields[0],fields[2]),(fields[1],fields[3])))


#=============key===========
#new_joined_data[0][0] = vin.tx_hash, vout.hash
#new_joined_data[0][1] = vin.vout, vout.n
#=============values=========
#new_joined_data[1][0][0] = voutwiki.hash, vin.txid
#new_joined_data[1][0][1] = voutwiki.value (amount of btc)
#new_joined_data[1][0][2] = voutwiki.n (id for output within this transaction)
#new_joined_data[1][0][3] = voutwiki.publicKey (id of wallet where coins are being sent)
#new_joined_data[1][1][0] = vout.value (amount of btc)
#new_joined_data[1][1][1] = vout.publicKey (bitcoin wallet being sent from)**********
new_joined_data = joined_data1.join(vout_join)


#relevant_data = key:publicKey of sender    value:amount of btc
relevant_data = new_joined_data.map(lambda f: (f[1][1][1], float(f[1][1][0])))
groupbydonors = relevant_data.reduceByKey(lambda a,b: a+b)
top10 = groupbydonors.takeOrdered(10, key = lambda pair: -pair[1])
for x in top10: print(x)
