from collections import defaultdict
import polars as pl


def industry_to_sector(mapping: dict) -> pl.DataFrame:
    """
    converts the sector to industry mapping to industry to sector mapping
    """
    ind_to_sec = defaultdict(list)
    for sector, industries in mapping.items():
        for ind in industries:
            ind_to_sec[ind].append(sector)

    ind_to_sec = [
        (ind, sector) for ind, sectors in ind_to_sec.items() for sector in sectors
    ]

    ind_to_sec_df = pl.DataFrame(
        ind_to_sec, schema=["Basic Industry", "Sector"], orient="row"
    )

    return ind_to_sec_df
