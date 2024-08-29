from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

        def mapper(self, _, line):
            yield "chars", len(line)
            yield "words", len(line.split())
            yield "lines", 1

if __name__ == "__main__":
    MRWordFrequencyCount.run()