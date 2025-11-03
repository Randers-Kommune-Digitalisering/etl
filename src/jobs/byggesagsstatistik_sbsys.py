import logging
import urllib.parse

from datetime import datetime

from utils.api_requests import APIClient
from byggesagsstatistik.models.randers_sbsys_models import Sag, BeslutningsType, ByggeSag, \
    ByggeSagKode, SagSkabelon
from byggesagsstatistik.models.byggesager_db_models import Base, Byggesagskode, \
    Byggesagsgruppe, Beslutningstype, ByggesagByg, ByggesagSag
from utils.config import SBSYS_DB_HOST, SBSYS_DB_USER, SBSYS_DB_PASS, SBSYS_DB_PORT, BYGGESAGER_POSTGRES_DB_HOST, \
    BYGGESAGER_POSTGRES_DB_USER, BYGGESAGER_POSTGRES_DB_PASS, BYGGESAGER_POSTGRES_DB_DATABASE, BYGGESAGER_POSTGRES_DB_PORT, \
    CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE
from utils.database_client import DatabaseClient


START_DATE = datetime(2020, 1, 1)

logger = logging.getLogger(__name__)


def job():
    logger.info("Starting byggesagsstatistik_sbsys job")

    config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
    config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE)
    config = config_library_client.make_request(path=config_path)

    GROUPINGS = config["GROUPINGS"]
    SKABELON_IDS = config["SKABELON_IDS"]

    if not GROUPINGS or not SKABELON_IDS:
        logging.error(f"Failed to load config file: {BYGGESAGER_CONFIG_FILE}")
        return False

    db_client_sbsys = DatabaseClient(
        db_type='mssql',
        host=SBSYS_DB_HOST,
        username=SBSYS_DB_USER,
        password=SBSYS_DB_PASS,
        port=SBSYS_DB_PORT
    )

    db_client_byggesager = DatabaseClient(
        db_type='postgresql',
        host=BYGGESAGER_POSTGRES_DB_HOST,
        username=BYGGESAGER_POSTGRES_DB_USER,
        password=BYGGESAGER_POSTGRES_DB_PASS,
        database=BYGGESAGER_POSTGRES_DB_DATABASE,
        port=BYGGESAGER_POSTGRES_DB_PORT
    )

    logger.info("Initializing")
    Base.metadata.create_all(db_client_byggesager.get_engine())

    with db_client_sbsys.get_session() as sbsys_session:
        with db_client_byggesager.get_session() as kubernetes_session:
            # Create or get grouping ids
            new_groupings = {}
            for key in GROUPINGS.keys():
                dist = kubernetes_session.query(Byggesagsgruppe).filter_by(name=key).first()
                if not dist:
                    dist = Byggesagsgruppe(name=key)
                    kubernetes_session.add(dist)
                    kubernetes_session.flush()
                new_groupings[dist.id] = GROUPINGS[key]

            def get_grouping_id(id: int) -> int:
                """Helper function to get Byggesagsgruppe id"""
                return next((k for k, v in new_groupings.items() if id in v), None)

            logger.info("Syncing data from SBSYS to byggesager Postgres DB")
            for orig in sbsys_session.query(BeslutningsType).all():
                dist = Beslutningstype(id=orig.ID, name=orig.Navn)
                kubernetes_session.merge(dist)
            for orig in sbsys_session.query(ByggeSagKode).all():
                dist = Byggesagskode(id=orig.ID, byggesagsgruppe_id=get_grouping_id(orig.ID), name=orig.Kode)
                kubernetes_session.merge(dist)
            for orig in sbsys_session.query(SagSkabelon).filter(SagSkabelon.ID.in_(SKABELON_IDS)).all():
                dist = Byggesagskode(id=orig.ID, byggesagsgruppe_id=get_grouping_id(orig.ID), name=orig.Navn)
                kubernetes_session.merge(dist)
            for orig in sbsys_session.query(ByggeSag).filter(ByggeSag.Modtaget >= START_DATE).all():
                if orig.ByggeSagKodeID:
                    dist = ByggesagByg(
                        id=orig.ID,
                        byggesagskode_id=orig.ByggeSagKodeID,
                        beslutningstype_id=orig.Sag.BeslutningsTypeID if orig.Sag else None,
                        byggetilladelse_date=orig.Byggetilladelse,
                        received_date=orig.Modtaget
                    )
                    kubernetes_session.merge(dist)
            for orig in sbsys_session.query(Sag).filter(Sag.SkabelonID.in_(SKABELON_IDS), Sag.Created >= START_DATE).all():
                dist = ByggesagSag(
                    id=orig.ID,
                    byggesagskode_id=orig.SkabelonID,
                    beslutningstype_id=orig.BeslutningsTypeID,
                    byggetilladelse_date=orig.LastStatusChange,
                    received_date=orig.Created
                )
                kubernetes_session.merge(dist)
            logger.info("Committing changes to byggesager Postgres DB")
            kubernetes_session.commit()
    logger.info("byggesagsstatistik_sbsys job completed successfully")
    return True
