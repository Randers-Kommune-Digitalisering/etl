from sqlalchemy import Column, DateTime, Integer, String, Float, ForeignKey, Boolean, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


bruger_afdeling = Table(
    'bruger_afdeling', Base.metadata,
    Column('bruger_id', Integer, ForeignKey('Bruger.BrugerID')),
    Column('afdeling_id', Integer, ForeignKey('Afdeling.AfdelingID'))
)


class Afdeling(Base):
    __tablename__ = 'Afdeling'
    AfdelingID = Column(Integer, primary_key=True, autoincrement=True)
    Afdeling = Column(String, nullable=False)
    AfdelingsEAN = Column(String)
    brugere = relationship('Bruger', secondary=bruger_afdeling, back_populates='afdelinger')


class Bruger(Base):
    __tablename__ = 'Bruger'
    BrugerID = Column(Integer, primary_key=True, autoincrement=True)
    PrimaryFullName = Column(String, nullable=False)
    PrimaryUser = Column(String, nullable=False)
    afdelinger = relationship('Afdeling', secondary=bruger_afdeling, back_populates='brugere')
    computere = relationship('Computer', back_populates='bruger')


class Computer(Base):
    __tablename__ = 'Computer'
    UnitName = Column(String, primary_key=True)
    Producent = Column(String)
    Model = Column(String)
    Enhedstype = Column(String)
    Serienummer = Column(String)
    SidsteLoginDato = Column(DateTime)
    SidsteRul = Column(DateTime)
    BitlockerKode = Column(String)
    BitlockerStatus = Column(String)
    BitlockerKrypteringProcent = Column(String)
    OSVersion = Column(String)
    MACAdresse = Column(String)
    LanMACAdresse = Column(String)
    DeviceLicense = Column(Boolean)
    Drift = Column(Boolean)
    Price = Column(Float)
    OrderDate = Column(DateTime)
    Warranty = Column(DateTime)
    KøbsEANnr = Column(String)
    BrugerID = Column(Integer, ForeignKey('Bruger.BrugerID'))
    bruger = relationship('Bruger', back_populates='computere')
