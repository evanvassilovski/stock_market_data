from sqlalchemy import Table, Column, String, Date, MetaData, Integer, Float, PrimaryKeyConstraint
from db import get_engine

engine = get_engine()

metadata = MetaData()

stocks = Table("stocks", metadata,
    Column("ID", Integer, primary_key=True),
    Column("Symbol", String),
    Column("Desc", String),
    Column("SectorID", Integer)
)

sectors = Table("sectors", metadata,
    Column("ID", Integer, primary_key=True),
    Column("Symbol", String),
    Column("Desc", String)
)

markets = Table("markets", metadata,
    Column("ID", Integer, primary_key=True),
    Column("Symbol", String),
    Column("Desc", String)
)

attributes = Table("attributes", metadata,
    Column("ID", Integer, primary_key=True),
    Column("Attribute", String)
)

yf_data = Table("yf_data", metadata,
    Column("Date", Date, primary_key=True),
    Column("SymbolID", Integer, primary_key=True),
    Column("AttributeID", Integer, primary_key=True),
    Column("Horizon", Integer, primary_key=True),
    Column("Value", Float),
    PrimaryKeyConstraint("Date", "SymbolID", "AttributeID", "Horizon")
)

fred_data = Table("fred_data", metadata,
    Column("Date", Date, primary_key=True),
    Column("AttributeID", Integer, primary_key=True),
    Column("Horizon", Integer, primary_key=True),
    Column("Value", Float),
    PrimaryKeyConstraint("Date", "AttributeID", "Horizon")
)

metadata.create_all(engine)