from pathlib import Path
from milton.ukb_csv_to_parquet import *

convert_ukb_csv_to_parquet(
    Path('/path/to/ukb/release/main-ukb-file.csv'),
    n_chunks=8,
    output_path=Path(f'/path/to/output/folder/ukb.parquet'))