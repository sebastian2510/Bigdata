from pyspark.sql import SparkSession
import string
import sys
import os

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

def save_data(data):
    if os.path.exists("output.txt"):
        os.remove("output.txt")

    with open("output.txt", "w") as f:
        f.write(data)

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
    save_data(saved_data)
    spark.stop()


if __name__ == "__main__":
    main()