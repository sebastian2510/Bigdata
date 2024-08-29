from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

        def reducer(self, key, values):
            yield key, sum(values)

if __name__ == "__main__":
    MRWordFrequencyCount.run()