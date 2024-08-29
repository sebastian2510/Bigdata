from pyspark.sql import SparkSession
import string
import sys

sys.stdout.reconfigure(encoding='utf-16') # Needed for terminal spark-submit (it gave an error...)

def word_count(data: str) -> dict[str, int]:
    translator = str.maketrans('', '', string.punctuation) 
    data = data.translate(translator).lower().strip()
    words = data.split()
    word_counts = {}
    for word in words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
    return word_counts

def save_data(spark, saved_data):
    """
    Save the data to a file called output.txt
    It shall be saved in /input_dir directory in the HDFS
    """
    path = "/input_dir/output.txt"

    # Access the Hadoop FileSystem
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    output_path = spark._jvm.org.apache.hadoop.fs.Path(path)

    if hadoop_fs.exists(output_path):
        hadoop_fs.delete(output_path, True)

    output_stream = hadoop_fs.create(output_path)
    try:
        for data in saved_data:
            output_stream.write(f"{data}\n".encode("utf-8"))
    finally:
        output_stream.close()
    
    


def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    # Read from /input_dir/AChristmasCarol_CharlesDickens_English.txt
    df = spark.read.text("/input_dir/AChristmasCarol_CharlesDickens_English.txt", wholetext=True)
    # The text is stored in the first row and first column
    lines = df.collect()[0][0]
    count = word_count(lines)
    saved_data = [f"{word}: {count}" for word, count in count.items()]
    for new_data in saved_data:
        print(new_data)
        
    print(f"Word count: {len(lines)}")
    save_data(spark, saved_data)
    spark.stop()


if __name__ == "__main__":
    main()