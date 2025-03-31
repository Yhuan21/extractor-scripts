import os
import json
import mariadb
import pandas as pd
import pickle
from datetime import datetime

DIR_BANK_RECON = "T:/bank_recon/GW/GW.pkl"


class Extractor:
    def __init__(self):

        with open("config.json", "r") as f:
            config = json.load(f)

        self.host = config["DATABASE"]["HOST"]
        self.user = config["DATABASE"]["USER"]
        self.password = config["DATABASE"]["PASSWORD"]
        self.database = config["DATABASE"]["DATABASE"]
        self.tables = config["DATA"]["TABLES"]
        self.cols_to_extract = config["DATA"]["COLS_TO_EXTRACT"]
        self.save_directory = os.path.join(
            os.getcwd(), config["DIRECTORY"]["SAVE_DIRECTORY"]
        )
        self.company = config["DIRECTORY"]["COMPANY"]

        self.db_conn = mariadb.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
        )

    def _connect_db(self):
        try:
            self.db_conn = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
            )
        except mariadb.Error as e:
            print(f"Error connecting to database: {e}")
            exit(1)

    def extract_data(self, table):
        sql_data = pd.read_sql(sql=f"select * from {table};", con=self.db_conn)
        dir_path = f"{self.save_directory}/{self.company}"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        sql_data.to_pickle((f"{dir_path}/{table}.pkl"))

    def run_data(self):
        for tbl in self.tables:
            self.extract_data(tbl)

    def create_cv_data(self):
        CVChecks = pickle.load(open("data/GWAS/CVChecks.pkl", "rb"))
        CVEntries = pickle.load(open("data/GWAS/CVEntries.pkl", "rb"))
        CVHeader = pickle.load(open("data/GWAS/CVHeader.pkl", "rb"))

        CVChecks.rename(columns=str.lower, inplace=True)
        CVEntries.rename(columns=str.lower, inplace=True)
        CVHeader.rename(columns=str.lower, inplace=True)

        merged_df1 = CVChecks.merge(CVEntries,
                                    on=["trndate", "trnno"],
                                    how="left"
                                    )
        final_merged_df = merged_df1.merge(
            CVHeader, on=["trndate", "trnno"], how="left"
        )
        return final_merged_df

    def create_or_data(self):
        ORHeader = pickle.load(open("data/GWAS/ORHeader.pkl", "rb"))
        OREntries = pickle.load(open("data/GWAS/OREntries.pkl", "rb"))
        ORHeader.rename(columns=str.lower, inplace=True)
        OREntries.rename(columns=str.lower, inplace=True)
        final_merged_df = pd.merge(
            ORHeader, OREntries, on=["trndate", "trnno"], how="left"
        )
        return final_merged_df

    def create_jv_data(self):
        JVEntries = pickle.load(open("data/GWAS/JVEntries.pkl", "rb"))
        JVHeader = pickle.load(open("data/GWAS/JVHeader.pkl", "rb"))
        JVEntries.rename(columns=str.lower, inplace=True)
        JVHeader.rename(columns=str.lower, inplace=True)
        final_merged_df = pd.merge(
            JVHeader, JVEntries, on=["trndate", "trnno"], how="left"
        )
        return final_merged_df

    def format_cv_data(self, cv_df):
        print(cv_df.columns)
        cv_df["subacct"] = cv_df["subacct_x"].fillna(cv_df["subacct_y"])
        cv_df["other_01_filler"] = cv_df["payee"].fillna(cv_df["name_x"])
        cv_df["other_01"] = cv_df["other_01_filler"].fillna(cv_df["name_y"])
        cv_df["other_03"] = cv_df["checkno"]
        cv_df["dr_amt"] = cv_df["dramt"]
        cv_df["cr_amt"] = cv_df["cramt"]
        cv_df["net"] = cv_df["dramt"] - cv_df["cramt"]
        cv_df["from"] = ""
        cv_df["to"] = ""
        cv_df["ref_1"] = ""
        cv_df["ref_2"] = ""
        cv_df["checkdate"] = cv_df["checkdate"]
        cols_extract = self.cols_to_extract
        cols_extract.append("checkdate")
        return cv_df[cols_extract].copy()

    def format_or_data(self, or_df):
        or_df["other_01"] = or_df["receivedfrom"].fillna(or_df["name"])
        or_df["other_03"] = or_df["remarks_x"].fillna(or_df["remarks_y"])
        or_df["dr_amt"] = or_df["dramt"]
        or_df["cr_amt"] = or_df["cramt"]
        or_df["net"] = or_df["dramt"] - or_df["cramt"]
        or_df["from"] = ""
        or_df["to"] = ""
        or_df["ref_1"] = ""
        or_df["ref_2"] = ""
        or_df = or_df[or_df['acctno'] == "1000"]
        self.cols_to_extract.remove('checkdate')
        return or_df[self.cols_to_extract].copy()

    def format_jv_data(self, jv_df):
        jv_df["other_01"] = jv_df["name"]
        jv_df["other_03"] = ""
        jv_df["dr_amt"] = jv_df["dramt"]
        jv_df["cr_amt"] = jv_df["cramt"]
        jv_df["net"] = jv_df["dramt"] - jv_df["cramt"]
        jv_df["from"] = ""
        jv_df["to"] = ""
        jv_df["ref_1"] = ""
        jv_df["ref_2"] = ""
        return jv_df[self.cols_to_extract].copy()

    def filter_date_period(self, start_date, end_date, df):
        df["trndate"] = pd.to_datetime(df["trndate"])
        df = df[(df["trndate"] >= start_date) & (df["trndate"] <= end_date)]
        df["trndate"] = pd.to_datetime(df["trndate"]).dt.strftime('%m/%d/%Y')
        return df

    def to_csv(self, df, filename):
        df.to_csv(f"{self.save_directory}/"
                  f"{self.company}/"
                  f"{filename}.csv",
                  index=False
                  )

    def process(self, start_date, end_date):
        self.run_data()

        df1 = self.create_cv_data()
        df2 = self.create_or_data()
        df3 = self.create_jv_data()

        cv_df = self.format_cv_data(df1)
        or_df = self.format_or_data(df2)
        jv_df = self.format_jv_data(df3)

        df1 = self.filter_date_period(start_date, end_date, cv_df)
        df2 = self.filter_date_period(start_date, end_date, or_df)
        df3 = self.filter_date_period(start_date, end_date, jv_df)

        formatted_df = pd.concat([df1, df2, df3], ignore_index=True)
        formatted_df = formatted_df.fillna(0)
        formatted_df.to_pickle(DIR_BANK_RECON)
        # self.to_csv(formatted_df, "GWAS_data")

        return formatted_df


if __name__ == "__main__":
    extractor = Extractor()
    curr_date = datetime.now()
    start_date = "2024-01-01"  # change based on end-user req
    end_date = curr_date.strftime("%Y-%m-%d")
    formatted_df = extractor.process(start_date, end_date)
    print(formatted_df)
