from sqlalchemy import insert, select
from config import engine
from utils import load_excel_sheets, create_or_update

def sync_excel_to_db(path = "sample_datasets.xlsx"):
    sheets = load_excel_sheets(path)


    for sheet_name, df in sheets.items():
        table, df = create_or_update(engine, sheet_name, df)
        pk_column = df.columns[0]

        with engine.begin() as conn:
            for _, row in df.iterrows():
                pk_value = row[pk_column].item() if hasattr(row[pk_column], 'item') else row[pk_column]
                

                exists_stmt = select(table.c[pk_column]).where(table.c[pk_column]==pk_value)
                result = conn.execute(exists_stmt).fetchone()
                if result:
                    continue

                data = {k: (v.item() if hasattr(v, 'item') else v) for k, v in row.to_dict().items()}
                stmt = insert(table).values(**data)
                conn.execute(stmt)

if __name__=="__main__":
    sync_excel_to_db()
