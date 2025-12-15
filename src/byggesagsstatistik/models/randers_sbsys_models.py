# This file declares partial classes for "SbsysNetDrift" and "SbsysNetDrift_Byggesag" databases
from typing import Optional
import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, Identity, Index, Integer, PrimaryKeyConstraint, Unicode
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# Models for SbsysNetDrift
class Sag(Base):
    __tablename__ = 'Sag'
    __table_args__ = (
        ForeignKeyConstraint(['BeslutningsTypeID'], ['SbsysNetDrift.dbo.BeslutningsType.ID'], name='Sag_BeslutningsType'),
        ForeignKeyConstraint(['SkabelonID'], ['SbsysNetDrift.dbo.SagSkabelon.ID'], name='Sag_SagSkabelon'),
        PrimaryKeyConstraint('ID', name='PK_Sag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    BeslutningsTypeID: Mapped[Optional[int]] = mapped_column(Integer)
    LastStatusChange: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    Created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    SkabelonID: Mapped[Optional[int]] = mapped_column(Integer)
    SagsStatusID: Mapped[Optional[int]] = mapped_column(Integer)

    BeslutningsType: Mapped[Optional["BeslutningsType"]] = relationship("BeslutningsType", back_populates="Sag")
    ByggeSager: Mapped[list['ByggeSag']] = relationship('ByggeSag', back_populates='Sag')
    SagSkabelon: Mapped[Optional['SagSkabelon']] = relationship('SagSkabelon', back_populates='Sag')


class SagSkabelon(Base):
    __tablename__ = 'SagSkabelon'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_SagSkabelon'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn: Mapped[str] = mapped_column(Unicode(100, 'SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Sag: Mapped[list['Sag']] = relationship('Sag', back_populates='SagSkabelon')


class BeslutningsType(Base):
    __tablename__ = 'BeslutningsType'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_AfgoeringType'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn: Mapped[str] = mapped_column(Unicode(50, 'SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Sag: Mapped[list['Sag']] = relationship('Sag', back_populates='BeslutningsType')


# Models for SbsysNetDrift_Byggesag database
class ByggeSag(Base):
    __tablename__ = 'ByggeSag'
    __table_args__ = (
        ForeignKeyConstraint(['ByggeSagKodeID'], ['SbsysNetDrift_Byggesag.dbo.ByggeSagKode.ID'], name='FK_ByggeSag_ByggeSagKode'),
        ForeignKeyConstraint(['SagID'], ['SbsysNetDrift.dbo.Sag.ID'], name='FK_ByggeSag_Sag'),
        PrimaryKeyConstraint('ID', name='PK_ByggeSag'),
        Index('IX_ByggeSag_SagId', 'SagID'),
        {"schema": "SbsysNetDrift_Byggesag.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID: Mapped[int] = mapped_column(Integer, nullable=False)
    Modtaget: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    ByggeSagKodeID: Mapped[Optional[int]] = mapped_column(Integer)
    Registreret: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    FaerdigMeldt: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    Byggetilladelse: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)

    ByggeSagKode: Mapped[Optional['ByggeSagKode']] = relationship('ByggeSagKode', back_populates='ByggeSag')
    Sag: Mapped[Optional['Sag']] = relationship('Sag', back_populates='ByggeSager')


class ByggeSagKode(Base):
    __tablename__ = 'ByggeSagKode'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Byggekode'),
        {"schema": "SbsysNetDrift_Byggesag.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Kode: Mapped[str] = mapped_column(Unicode(100, 'SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    ByggeSag: Mapped[list['ByggeSag']] = relationship('ByggeSag', back_populates='ByggeSagKode')
