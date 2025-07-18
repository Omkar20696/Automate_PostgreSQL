from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "postgresql+psycopg2://postgres:omkar20696@localhost:5432/Automate"

engine = create_engine(DATABASE_URL)
sessionlocal = sessionmaker(bind=engine)
base = declarative_base()