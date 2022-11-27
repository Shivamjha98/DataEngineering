from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "top_rated_movies")

base_rdd = sc.textFile("/Users/shivi/OneDrive/Desktop/shared/ratings.dat")

mapped_rdd = base_rdd.map(lambda x: (int(x.split("::")[1]), float(x.split("::")[2])))

num_ratings = mapped_rdd.mapValues(lambda x: (x, 1))

num_ratings_given = num_ratings.reduceByKey(lambda x,y: (x[0]+y[0],     x[1]+y[1]))

filtered_ratings = num_ratings_given.filter(lambda x: x[1][1] > 1000)

avg_ratings = filtered_ratings.mapValues(lambda x: x[0] / x[1])

final_movie_ids = avg_ratings.filter(lambda x: x[1]> 4.5)


movies_rdd = sc.textFile("/Users/shivi/OneDrive/Desktop/shared/movies.dat")

mapped_movies = movies_rdd.map(lambda x: (int(x.split("::")[0]), x.split("::")[1]))

resultant_rdd = mapped_movies.join(final_movie_ids)

resultant_rdd_final = resultant_rdd.map(lambda x: x[1][0])

for x in resultant_rdd_final.collect():
    print(x)