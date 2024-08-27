import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,col



def fetch_data():
    filename: str = "AChristmasCarol_CharlesDickens_English.txt"
    with open(filename, "r") as f:
        data: str = f.read()
        data = data.lower().strip('.,?!-:;()[{}"\'').lower()
    return data

def word_count(data: str) -> dict[str, int]:
    words = data.split()
    word_counts = {}
    for word in words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
    return word_counts

def save_data(data):
    with open("output.txt", "w") as f:
        f.write(data)

def main():
    spark = SparkSession.builder.master("local").appName("word_count").getOrCreate()
    sc = spark.SparkContext()
    # Read the input file and Calculating words count
    text_file = sc.textFile("AChristmasCarol_CharlesDickens_English.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)
    output = counts.collect()

    output.display()

    # data = fetch_data()
    # count = word_count(data)
    # saved_data = [f"{word}: {count}" for word, count in count.items()]
    # for data in saved_data:
    #     print(data)
    # print(f"Word count: {len()}")
    # save_data("\n".join(saved_data))

if __name__ == "__main__":
    main()