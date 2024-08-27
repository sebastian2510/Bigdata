class Data:
    @staticmethod
    def fetch_data():
        filename: str = "AChristmasCarol_CharlesDickens_English.txt"
        with open(filename, "r") as f:
            data: str = f.read()
            data = data.lower()
        return data
    
    @staticmethod
    def word_count(data: str, words: list[str]) -> list[int]:
        return [data.count(word + " ") for word in words]

    
    @staticmethod
    def save_data(data):
        with open("output.txt", "w") as f:
            f.write(data)


def main():
    data = Data.fetch_data()
    words = ["a", "christmas", "carol", "by", "charles", "dickens", "illustrated", "george", "alfred", "williams", "new"]
    word_count = Data.word_count(data, words)
    saved_data = [f"{word}: {count}" for word, count in zip(words, word_count)]
    print(f"Word count: {len(data.split())}")
    for data in saved_data:
        print(data)
    Data.save_data("\n".join(saved_data))

if __name__ == "__main__":
    main()