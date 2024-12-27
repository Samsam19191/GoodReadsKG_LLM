import pandas as pd


class DataProcessor:
    def __init__(self, fileoutput):
        self.fileoutput = fileoutput
        self.df = None
        self.filename = None

    def load_data(self):
        try:
            self.df = pd.read_csv(f"raw_data/{self.filename}.csv")
        except FileNotFoundError:
            print("File not found")

    def clean_book_data(self):
        if self.df is not None:
            # only keep the columns we need
            columns_to_keep = [
                "Id",
                "Name",
                "Authors",
                "Rating",
                "pagesNumber",
                "PublishYear",
                "PublishMonth",
                "PublishDay",
                "Publisher",
                "Language",
            ]
            self.df = self.df[columns_to_keep]

            # fill rows with missing values
            self.df.fillna(
                {
                    "Authors": "Unknown Author",
                    "Publisher": "Unknown Publisher",
                    "Language": "Unknown Language",
                },
                inplace=True,
            )
        else:
            print("Dataframe is not loaded")

    def clean_rating_data(self):
        if self.df is not None:
            # remove useless rows
            self.df = self.df[self.df["Name"] != "Rating"]
        else:
            print("Dataframe is not loaded")

    def save_data(self):
        if self.df is not None:
            file_path = f"processed_data/{self.fileoutput}.csv"
            try:
                with open(file_path, "r") as f:
                    is_empty = f.read(1) == ""
            except FileNotFoundError:
                is_empty = True
            self.df.to_csv(file_path, mode="a", header=is_empty, index=False)
        else:
            print("Dataframe is not loaded")

    # reset fileoutput data
    def reset_data(self):
        file_path = f"processed_data/{self.fileoutput}.csv"
        try:
            with open(file_path, "w") as f:
                f.truncate()
        except FileNotFoundError:
            print("File not found")

    def process_books(self, filename):
        self.filename = filename
        self.load_data()
        self.clean_book_data()
        self.save_data()

    def process_ratings(self, filename):
        self.filename = filename
        self.load_data()
        self.clean_rating_data()
        self.save_data()
