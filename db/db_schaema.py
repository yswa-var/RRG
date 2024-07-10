from sqlalchemy import Column, ForeignKey, Integer, TIMESTAMP, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()