import pandas as pd
from sqlalchemy import Table, Column, MetaData, Integer, String, Float, inspect, Boolean, DateTime

type_mapping = {
    'int64': Integer,
    'float64': Float,
    'object' : String,
    'bool' : Boolean,
    'datetime64[ns]' : DateTime
}
def load_excel_sheets(file_path):
    xls = pd.ExcelFile(file_path)
    return  {sheet: xls.parse(sheet) for sheet in xls.sheet_names}

def infer_sqlalchemy_type(dtype):
    return type_mapping.get(str(dtype), String)

def create_or_update(engine, sheet_name, df):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    columns = []

    for col_name, dtype in df.dtypes.items():
        col_type = infer_sqlalchemy_type(dtype)
        col = Column(col_name, col_type, primary_key=(col_name == df.columns[0]))
        columns.append(col)
    
    if sheet_name not in metadata.tables:
        table=Table(sheet_name, metadata, *columns)
        table.create(engine)
    else:
        table = metadata.tables[sheet_name]
        existing_cols = table.columns.keys()
        with engine.connect() as conn:
            for col in df.columns:
                if col not in existing_cols:
                    col_type = infer_sqlalchemy_type(df[col].dtype)
                    alter = f'ALTER TABLE"{sheet_name}" ADD COLUMN "{col}" {col_type.__visit_name__.upper()};'
                    conn.execute(alter)

    return table, df