from jobindsats.jobindsats import get_data
from jobindsats.jobindsats_y30r21 import get_jobindats_ydelsesgrupper
import logging

logger = logging.getLogger(__name__)


# TODO: Move configuration to a better place. Maybe use https://github.com/Randers-Kommune-Digitalisering/config-library ?
jobindsats_jobs_config = [
    {
        "name": "Ydelsesmodtagere med løntimer",
        "years_back": 2,
        "dataset": "otij01",
        "period_format": "M",
        "data_to_get": {
            "_ygrp_j01": [
                "Kontanthjælp",
                "Uddannelseshjælp",
                "Selvforsørgelses- og hjemrejseydelse mv.",
                "Jobafklaringsforløb",
                "Forrevalidering og revalidering",
                "Ressourceforløb"
            ],
            "_maalgrp": [
                "Jobparate mv.",
            ]
        }
    },
    {
        "name": "Offentligt forsørgede",
        "years_back": 2,
        "dataset": "ptv_a02",
        "period_format": "M",
        "data_to_get": {
            "_ygrpa02": [
                "Førtidspension",
                "Efterløn",
                "Seniorpension",
                "Jobafklaringsforløb",
                "Tidlig pension",
            ],
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
        }
    },
    {
        "name": "Tilbud og samtaler",
        "years_back": 2,
        "dataset": "ptvc01",
        "period_format": "M",
        "data_to_get": {
            "_ygrpc02": [
                "A-dagpenge",
                "Kontanthjælp",
                "Uddannelseshjælp",
                "Selvforsørgelses- og hjemrejseydelse mv.",
                "Sygedagpenge"
            ],
        }
    },
    {
        "name": "A-Dagpenge",
        "years_back": 2,
        "dataset": "y01a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Revalidering",
        "years_back": 2,
        "dataset": "y04a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Herkomst i alt",
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Sygedagppenge",
        "years_back": 1,
        "dataset": "y07a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ],
            "_sagsart": [
                "Lønmodtagere",
                "Selvstændige erhvervsdrivende",
                "Fleksjob",
                "A-dagpengemodtagere"
            ]
        }
    },
    {
        "name": "Fleksjob",
        "years_back": 2,
        "dataset": "y08a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Ledighedsydelse",
        "years_back": 2,
        "dataset": "y09a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Tilbagetrækningsydelser",
        "years_back": 2,
        "dataset": "y10a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Ressourceforløb",
        "years_back": 2,
        "dataset": "y11a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Herkomst i alt",
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande"
            ]
        }
    },
    {
        "name": "Jobafklaringsforløb",
        "years_back": 2,
        "dataset": "y12a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Selvforsørgelses- og hjemrejseydelse samt overgangsydelse",
        "years_back": 2,
        "dataset": "y35a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ]
        }
    },
    {
        "name": "Kontanthjælp",
        "years_back": 2,
        "dataset": "y36a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ],
            "_viskat_1kth": [
                "Jobparat",
                "Aktivitetsparat",
                "Uoplyst visitationskategori"
            ]
        }
    },
    {
        "name": "Uddannelseshjælp",
        "years_back": 0,
        "dataset": "y38a02",
        "period_format": "M",
        "data_to_get": {
            "_kon": [
                "Kvinder",
                "Mænd"
            ],
            "_oprinda": [
                "Personer med dansk oprindelse",
                "Indvandrere fra vestlige lande",
                "Efterkommere fra vestlige lande",
                "Indvandrere fra ikke-vestlige lande",
                "Efterkommere fra ikke-vestlige lande"
            ],
            "_viskat_2udh": [
                "Alle uddannelsesparate",
                "Uddannelsesparat",
                "Åbenlys uddannelsesparat",
                "Aktivitetsparat",
                "Uoplyst visitationskategori"
            ]
        }
    },
    {
        "name": "Fra ydelse til job",
        "years_back": 2,
        "dataset": "y14d03",
        "period_format": "QMAT",
        "data_to_get": {
            "_ygrpmm12": [
                "A-dagpenge",
                "Kontanthjælp",
                "Sygedagpenge"
            ],
            "_tilbud_d03": [
                "Privat virksomhedspraktik",
                "Offentlig virksomhedspraktik"
            ],
            "_maalgrp_d03": [
                "Jobparate mv.",
                "Aktivitetsparate mv."
            ],
        }
    },
    {
        "name": "Ydelsesgrupper",
        "years_back": 2,
        "dataset": "y30r21",
        "period_format": "QMAT",
        "data_to_get": {
            "_ygrp_y30r21": [
                "Ydelsesgrupper i alt",
                "A-dagpenge mv.",
                "Sygedagpenge mv.",
                "Kontanthjælp mv."
            ]
        }
    },

]


def job():
    try:
        logger.info('Starting jobindsats ETL job!')
        results = []
        for job in jobindsats_jobs_config:
            results.append(get_data(job['name'], job['years_back'], job['dataset'], job['period_format'], job['data_to_get']))
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
