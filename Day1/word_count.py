import string
def fetch_data():
    filename: str = "AChristmasCarol_CharlesDickens_English.txt"
    with open(filename, "r") as f:
        data: str = f.read()
        translator = str.maketrans('', '', string.punctuation) 
        data = data.translate(translator).lower().strip()
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
    data = fetch_data()
    count = word_count(data)
    saved_data = [f"{word}: {count}" for word, count in count.items()]
    for new_data in saved_data:
        print(new_data)
    print(f"Word count: {len(data)}")
    save_data("\n".join(saved_data))

if __name__ == "__main__":
    main()