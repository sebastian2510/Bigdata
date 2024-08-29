from pyspark import SparkContext, SparkConf
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

def save_data(sc, saved_data):
    """
    Save the data to a file called output.txt
    It shall be saved in /input_dir directory in the HDFS
    """
    path = "/input_dir/output.txt"

    # Access the Hadoop FileSystem
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    output_path = sc._jvm.org.apache.hadoop.fs.Path(path)

    if hadoop_fs.exists(output_path):
        hadoop_fs.delete(output_path, True)

    output_stream = hadoop_fs.create(output_path)
    try:
        for data in saved_data:
            output_stream.write(f"{data}\n".encode("utf-8"))
    finally:
        output_stream.close()

def main():
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    # Read from /input_dir/AChristmasCarol_CharlesDickens_English.txt
    rdd = sc.textFile("/input_dir/AChristmasCarol_CharlesDickens_English.txt")
    # Collect the text data
    lines = rdd.collect()
    text = " ".join(lines)
    count = word_count(text)
    saved_data = [f"{word}: {count}" for word, count in count.items()]
    for new_data in saved_data:
        print(new_data)
    print(f"Word count: {len(text)}")
    save_data(sc, saved_data)
    sc.stop()

if __name__ == "__main__":
    main()