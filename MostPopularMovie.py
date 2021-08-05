from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				reducer=self.reducer)
			]

	def mapper(self, _, line):
		(user:D, movie_ID, rating, timestamp) = line.split('\t')
		yield movie_ID, 1

	def reducer(self, key, values):
		yield key, sum(values)
if __name__=='__main__':
	MostPopularMovie.run()