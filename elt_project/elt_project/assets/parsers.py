import pandas as pd
import os
import re
import csv
import gc
from abc import ABC, abstractmethod

class Parser(ABC):
    """
    Abstract base class for all data parsers.
    Defines the interface for parsing various file types.
    """
    @abstractmethod
    def parse(self, file_path: str) -> pd.DataFrame:
        """
        Parses the given file path and returns a pandas DataFrame.
        """
        pass

class CsvParser(Parser):
    """
    Parses CSV files into a pandas DataFrame.
    """
    def parse(self, file_path: str) -> pd.DataFrame:
        # Default CSV parsing, extended to handle boolean interpretation.
        # `true_values` and `false_values` help pandas correctly parse boolean-like columns.
        # Empty strings from the CSV will be treated as False, which is crucial for
        # 'BIT NOT NULL' columns in SQL Server that would otherwise fail on NULL inserts.
        df = pd.read_csv(
            file_path, true_values=['true', 'True', 'TRUE', '1'], false_values=['false', 'False', 'FALSE', '0', '']
        )
        # For any columns that are boolean type but still have NaNs (because the source had something
        # pandas didn't recognize as true/false), fill them with False.
        df.fillna({col: False for col in df.select_dtypes(include='bool').columns}, inplace=True)
        return df

class PsvParser(Parser):
    """
    Parses Pipe-Separated Value (PSV) files into a pandas DataFrame.
    """
    def parse(self, file_path: str) -> pd.DataFrame:
        # PSV parsing, assuming '|' as delimiter
        return pd.read_csv(file_path, sep='|')

class ExcelParser(Parser):
    """
    Parses Excel files (.xlsx, .xls) into a pandas DataFrame.
    By default, it reads the first sheet.
    """
    def parse(self, file_path: str) -> pd.DataFrame:
        # This requires the 'openpyxl' package for .xlsx files.
        # Explicitly specify engine for .xlsx to avoid "format cannot be determined" errors
        # Strip whitespace to ensure extension detection works
        clean_path = file_path.strip()
        ext = os.path.splitext(clean_path)[1].lower()
        if ext in ['.xlsx', '.xlsm', '.xltx']:
            return pd.read_excel(file_path, engine='openpyxl')
        return pd.read_excel(file_path)

class CsvToExcelConverterParser(Parser):
    """
    Parses a CSV file and, as a side effect, saves it as an Excel file
    in the same directory before returning the DataFrame.
    """
    def parse(self, file_path: str) -> pd.DataFrame:
        # Regular expression for illegal XML characters, including control characters.
        # This will match characters that are invalid in XML 1.0.
        # openpyxl writes XML-based formats (.xlsx), so these need to be removed.
        illegal_xml_chars_re = re.compile(
            r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f\ufdd0-\ufddf\ufffe\uffff]'
        )

        def _sanitize_string(s):
            return illegal_xml_chars_re.sub('', s) if isinstance(s, str) else s

        # Read the source CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Construct the output Excel file path
        base_name = os.path.splitext(file_path)[0]
        excel_output_path = f"{base_name}.xlsx"

        # Sanitize all string columns in the DataFrame before writing to Excel
        df = df.applymap(_sanitize_string)

        # Save the DataFrame to an Excel file (without the index)
        df.to_excel(excel_output_path, index=False)

        return df

class ParserFactory:
    """
    A factory for creating parser instances based on file type.
    """
    def __init__(self):
        self._parsers = {}

    def register_parser(self, file_type: str, parser_class: type):
        """
        Registers a parser class with a given file type.
        """
        if not issubclass(parser_class, Parser):
            raise ValueError("Registered class must be a subclass of Parser")
        self._parsers[file_type.lower()] = parser_class

    def get_parser(self, file_type: str) -> Parser:
        """
        Returns an instance of the parser for the specified file type.
        """
        parser_class = self._parsers.get(file_type.lower())
        if not parser_class:
            raise ValueError(f"No parser registered for file type: {file_type}")
        return parser_class()

# Instantiate the factory and register the default parsers
parser_factory = ParserFactory()
parser_factory.register_parser("csv", CsvParser)
parser_factory.register_parser("psv", PsvParser)
parser_factory.register_parser("excel", ExcelParser)
parser_factory.register_parser("csv_to_excel", CsvToExcelConverterParser)
# parser_factory.register_parser("json", JsonParser) # Uncomment and register if you add JsonParser

def stream_excel_to_csv(file_path: str, csv_path: str, logger=None) -> None:
    """
    Converts an Excel file to CSV using a memory-efficient streaming approach.
    Uses openpyxl read-only mode for .xlsx to avoid loading the file into RAM.
    """
    import time
    start_time = time.time()
    
    # Determine engine based on extension
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext in ['.xlsx', '.xlsm', '.xltx']:
        if logger: logger.info(f"Streaming Excel conversion (openpyxl) for: {file_path}")
        import openpyxl
        # read_only=True and data_only=True ensure we don't load the whole file or formulas into RAM
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        try:
            sheet = wb.active
            with open(csv_path, 'w', newline='', encoding='latin1', errors='replace') as f:
                writer = csv.writer(f)
                for row in sheet.values:
                    if row and any(cell is not None for cell in row):
                        writer.writerow(row)
        finally:
            wb.close()
            gc.collect()
    else:
        if logger: logger.info(f"Standard Excel conversion (pandas) for: {file_path}")
        # Fallback for legacy .xls files which require loading into memory
        df = pd.read_excel(file_path)
        df.to_csv(csv_path, index=False, encoding='latin1', errors='replace')
        del df
        gc.collect()
        
    if logger: logger.info(f"Excel to CSV conversion completed in {time.time() - start_time:.2f}s")