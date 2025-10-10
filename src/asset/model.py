from sqlalchemy import Column, Integer, String, Float, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Afdeling(Base):
    __tablename__ = 'Afdeling'
    AfdelingID = Column(Integer, primary_key=True, autoincrement=True)
    Afdeling = Column(String, nullable=False)
    AfdelingsEAN = Column(String)
    brugere = relationship('Bruger', back_populates='afdeling')


class Bruger(Base):
    __tablename__ = 'Bruger'
    BrugerID = Column(Integer, primary_key=True, autoincrement=True)
    PrimaryFullName = Column(String, nullable=False)
    PrimaryUser = Column(String, nullable=False)
    AfdelingID = Column(Integer, ForeignKey('Afdeling.AfdelingID'))
    afdeling = relationship('Afdeling', back_populates='brugere')
    computere = relationship('Computer', back_populates='bruger')


class Computer(Base):
    __tablename__ = 'Computer'
    UnitName = Column(String, primary_key=True)
    Producent = Column(String)
    Model = Column(String)
    Enhedstype = Column(String)
    Serienummer = Column(String)
    SidsteLoginDato = Column(String)
    SidsteRul = Column(String)
    BitlockerKode = Column(String)
    BitlockerStatus = Column(String)
    BitlockerKrypteringProcent = Column(String)
    OSVersion = Column(String)
    MACAdresse = Column(String)
    LanMACAdresse = Column(String)
    DeviceLicense = Column(Boolean)
    Drift = Column(Boolean)
    Price = Column(Float)
    OrderDate = Column(String)
    Warranty = Column(String)
    BrugerID = Column(Integer, ForeignKey('Bruger.BrugerID'))
    bruger = relationship('Bruger', back_populates='computere')
