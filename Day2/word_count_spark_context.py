from pyspark import SparkContext, SparkConf
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
    save_data(saved_data)
    sc.stop()

if __name__ == "__main__":
    main()