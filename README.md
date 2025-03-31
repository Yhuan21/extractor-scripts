# GWASDB_Extractor
***
## Overview
This Python script extracts, processes, and saves financial transaction data from a MariaDB database. The extracted data is transformed and saved as pickle files for further use.
***
## Dependencies
- Python 3
- Required Libraries:
  - `os`
  - `json`
  - `mariadb`
  - `pandas`
  - `pickle`
  - `dotenv`
  - `datetime`
***

## Configuration
- The script loads database credentials from environment variables using `.env`.
- Extracted tables and columns are configured in `config.json`.
- Data is saved in a structured directory using company-specific paths.
***

## Features
### Database Extraction
- Connects to a MariaDB database.
- Extracts data from predefined tables.
- Saves extracted data as `.pkl` files.
***

### Data Merging
- Merges multiple datasets:
  - **CV Data**: Combines `CVChecks`, `CVEntries`, and `CVHeader`.
  - **OR Data**: Combines `ORHeader` and `OREntries`.
  - **JV Data**: Combines `JVHeader` and `JVEntries`.
***

### Data Formatting
- Standardizes column names to lowercase.
- Applies formatting transformations.
- Filters data based on specific account numbers (e.g., `acctno == "1000"`).
***

### Date Filtering
- Converts `trndate` to datetime format.
- Filters data within a specified date range.
- Reformats `trndate` to `MM/DD/YYYY` format.
***

### Saving Processed Data
- Saves the processed dataset to a predefined directory.
- Supports saving in `.csv` and `.pkl` formats.
***

## Execution
```python
if __name__ == "__main__":
    extractor = Extractor()
    curr_date = datetime.now()
    start_date = "2024-01-01"  # Change as needed
    end_date = curr_date.strftime("%Y-%m-%d")
    formatted_df = extractor.process(start_date, end_date)
    print(formatted_df)
```
***



## Notes
- Ensure that the required pickle files exist before running the script.
- Modify `config.json` to customize table and column extraction.
- Use `.env` for secure storage of database credentials.
***

# Bank Recon Scheduler

## Overview
This script extracts financial data from Microsoft Access databases for different companies and saves it as serialized pickle files. It provides modular functionality to connect to the databases, execute SQL queries, and store results for subsequent use.



---

## Requirements

The script requires the following software and libraries:

- **Python**: 3.8 or later
- **Libraries**:
  - `pyodbc`
  - `pandas`
  - `os`
The script requires the Microsoft Access ODBC driver to connect to `.mdb` and `.accdb` files. Ensure the driver is installed on the machine.

---

## File Structure

### Directory and File Paths
- **Log File**: `~/Desktop/cali-helper-functions/logs.txt` - logs runtime messages.
- **Pickle Directory**: Data is saved in the directory `T:\bank_recon\<company>\` in pickle format:
  - `TRN_CHEQUE.pkl`
  - `TRN.pkl`

### Input
- Access database files for companies, organized as defined in a dictionary mapping (`company_list`).

---

## Class and Methods

### Class: `Extractor`
**Purpose**: Handles data extraction, logging, and saving.

#### Methods:

1. **`__init__(self) -> None`**
   - Initializes the `Extractor` object. Currently, no attributes are initialized here.

2. **`write_msg(self, msg: str) -> None`**
   - Writes messages to the log file for debugging and progress tracking.

3. **`run(self, path_string: str, file_name: str, company: str, save_directory: str) -> None`**
   - Connects to the Microsoft Access database for the specified company.
   - Executes queries on the tables and retrieves `TRN` and `TRN_CHEQUE` data.
   - Saves the extracted data in pickle format to the specified save directory.

---

## Usage

1. Ensure Microsoft Access ODBC driver is installed.
2. Set the correct file paths for the databases in the `company_list` dictionary.
3. Run the script:
   ```bash
   python <script_name>.py
   ```

### Example
The script iterates through the `company_list` and extracts the data for each company, saving it as pickle files
