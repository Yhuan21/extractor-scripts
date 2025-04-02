import pyodbc
import pandas as pd
import os
import json

with open("config.json", "r") as f:
    config = json.load(f)

DIR_PATH_LOGS = os.path.expanduser(config["DIRECTORY"]["LOGS"])
DIR_PATH = os.path.dirname(DIR_PATH_LOGS)
DIR_BANK_RECON = os.path.expanduser(config["DIRECTORY"]["BANK_RECON"])

if not os.path.exists(DIR_PATH):
    os.makedirs(DIR_PATH)


class Extractor:
    def __init__(self) -> None:
        pass

    def write_msg(self, msg: str) -> None:
        file_path = DIR_PATH
        with open(file_path, "a+") as f:
            f.writelines(msg + "\n")

    def run(
        self, path_string:
            str, file_name:
            str, company: str,
            save_directory: str
    ) -> None:
        pickle_trn_cheque = f"{save_directory}/{company}/TRN_CHEQUE.pkl"
        pickle_trn = f"{save_directory}/{company}/TRN.pkl"

        query_trn_cheque = "SELECT * FROM TRN_CHEQUE;"
        query_trn = "SELECT * FROM TRN;"

        print(path_string, file_name, company, save_directory)
        self.write_msg("directory checking..")
        self.directory_path = path_string
        self.file_name = file_name
        if self.directory_path[:-1] == "//" or self.directory_path[:-1] == "/":
            self.directory_path = self.directory_path[:-1]

        if self.file_name[:1] == "//" or self.file_name[:1] == "/":
            self.file_name = self.file_name[:1]
        self.directory_path.replace("//", "/")
        self.file_name.replace("//", "/")
        self.write_msg("connecting to db..")
        print(
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ="
            + self.directory_path
            + self.file_name
            + ";Trusted_Connection=yes;"
        )

        self.conn = pyodbc.connect(
            "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ="
            + self.directory_path
            + self.file_name
            + ";Trusted_Connection=yes;"
        )
        self.write_msg(f"connected to {company} FCU db..")
        self.write_msg("executing query for TRN_CHEQUE..")
        self.TRN_CHEQUE = pd.read_sql(sql=query_trn_cheque, con=self.conn)
        self.write_msg("saving pickle of TRN_CHEQUE..")
        self.TRN_CHEQUE.to_pickle(pickle_trn_cheque)
        self.write_msg("done making pickle of TRN_CHEQUE..")
        self.write_msg("executing query for TRM..")
        self.TRM = pd.read_sql(sql=query_trn, con=self.conn)
        self.write_msg("saving pickle of TRM..")
        self.TRM.to_pickle(())
        self.write_msg("done making pickle of TRM..")
        self.write_msg("executing query for TRN..")
        self.TRN = pd.read_sql(sql=query_trn, con=self.conn)
        self.write_msg("saving pickle of TRN..")
        self.TRN.to_pickle(pickle_trn)
        self.write_msg("done making pickle of TRN..")


if __name__ == "__main__":
    fe = Extractor()
    fe.write_msg("START OF SCRIPT")
    save_directory = DIR_BANK_RECON
    company_list = {
        "WFC": ("T:\\WESTBROOK\\Database\\", "FCUCV.mdb"),
        "BDC": ("T:\\Database\\FCU\\", "FCUCV.MDB"),
        "GMC": ("T:\\MAHOGANY\\Database\\", "FCUCV.mdb"),
    }
    for key, value in company_list.items():
        fe.run(value[0], value[1], key, save_directory)
        fe.write_msg("END OF SCRIPT")
