from mrjob.job import MRJob
import datetime

class BtcTrans(MRJob):

    #input: tx_hash,blockhash,time,tx_in_count,tx_out_count

    def mapper(self, _, line):
        try:
            #split line into values
            fields = line.split(",")
            #convert unix timestamp to month and year
            timestamp = fields[2]
            timestamp = int(timestamp)
            dt = datetime.datetime.fromtimestamp(timestamp)
            monthyear = dt.strftime("%Y %b")
            #emit key as each month per year
            #emit value as 1 for each transaction
            yield(monthyear, 1)

        except:
            pass

    def combiner(self, monthyear, counts):
        #sum up all the counts for each month in each year
        total = 0
        for count in counts:
            total+= count
        #output the total transactions for each month
        yield(monthyear, total)

    def reducer(self, monthyear, counts):
        #sum up all the counts for each month in each year
        total = 0
        for count in counts:
            total+= count
        #output the total transactions for each month
        yield(monthyear, total)


if __name__ == '__main__':
    BtcTrans.run()
