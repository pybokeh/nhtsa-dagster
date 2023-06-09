from datetime import datetime
from dagster_snowflake_pandas import snowflake_pandas_io_manager
from dagster import AssetIn, Definitions, SourceAsset, asset
from tqdm import tqdm
from utilities import fetch_manufacturers, fetch_model_names, fetch_wmi_by_manufacturer, fetch_wmi_data
import io
import pandas as pd
import requests


# https://docs.dagster.io/concepts/assets/software-defined-assets#defining-external-asset-dependencies
# A SourceAsset is an asset that was not created or materialized by dagster.
# The "make_id_cars_trucks_motorcycles" table was loaded from a separate external process.
# This SourceAsset declaration below ensures that this table will be available to assets that depend on it.
make_id_cars_trucks_motorcycles = SourceAsset(key='make_id_cars_trucks_motorcycles')
make_id_cars_trucks_motorcycles.description = 'Table containing make IDs for cars, trucks, and motorcycles only'


@asset(group_name="nhtsa")
def manufacturers(context) -> pd.DataFrame:
    """
    Vehicle manufacturer information from NHTSA API
    """

    df_list = []
    page = 1
    while True:
        context.log.info(f"Fetching page {page}")
        json_dict = fetch_manufacturers(page)     # imported from utilities
        if json_dict['Count'] == 0:
            context.log.info("Count is equal to zero/0 - exiting loop")
            break
        else:
            # json_normalize() will drop records where the record_path contains an empty list
            # To prevent this, see this SO question:
            # https://stackoverflow.com/questions/63813378/how-to-json-normalize-a-column-in-pandas-with-empty-lists-without-losing-record
            # For loop below is checking for "emptiness" of VehicleTypes, if empty,then fill with dictionary
            for i, record in enumerate(json_dict['Results']):
                if not record['VehicleTypes']:
                    json_dict['Results'][i]['VehicleTypes'] = [{'IsPrimary': 'Null', 'Name': 'Null'}]

            context.log.info(f"    Count={json_dict['Count']}")

            df = pd.json_normalize(
                json_dict['Results'],
                record_path=['VehicleTypes'],
                meta=['Country', 'Mfr_CommonName', 'Mfr_ID', 'Mfr_Name'],
            )
            df_list.append(df)
            page = page + 1

    df_combined = pd.concat(df_list, ignore_index=True)

    today = datetime.today().strftime('%Y-%m-%d')
    df_combined = df_combined.assign(Created_Date=today)
    context.log.info(f"Number of rows in manufacturers dataframe: {df_combined.shape[0]}")

    return df_combined[['Mfr_ID', 'Mfr_Name', 'Mfr_CommonName', 'Country', 'Created_Date']].drop_duplicates()


@asset(group_name="nhtsa")
def makes() -> pd.DataFrame:
    """
    Vehicle makes from NHTSA API
    """

    df = pd.read_csv('https://vpic.nhtsa.dot.gov/api/vehicles/GetAllMakes?format=csv')
    today = datetime.today().strftime('%Y-%m-%d')
    df = df.assign(created_date=today)

    return df


# To return only make_id column, need to add this extra boilerplate
# https://docs.dagster.io/integrations/snowflake/reference#selecting-specific-columns-in-a-downstream-asset
@asset(
    group_name="nhtsa",
    ins={
        "make_id_cars_trucks_motorcycles": AssetIn(
            key="make_id_cars_trucks_motorcycles",
            metadata={"columns": ["make_id"]},
        )
    }
)
def model_names(make_id_cars_trucks_motorcycles: pd.DataFrame) -> pd.DataFrame:
    """
    Vehicle model names from NHTSA API (passenger cars and trucks only, last 15 years)

    Parameters
    ----------
    make_id_cars_trucks_motorcycles: this is a smaller set of IDs since if we're to do all make IDs, then the downstream
    process of obtaining model names would take a significant amount of time.

    Returns
    -------
    A pandas dataframe containing model name information
    """

    # A good source for vehicle makes: https://cars.usnews.com/cars-trucks/car-brands-available-in-america

    # Instead of hard-coding that we want last 15 model years, we can programmatically define the years for us
    current_year = datetime.today().year
    start_year = datetime.today().year - 14

    df_list = []

    # Initialize progress bar
    progress_bar = tqdm(total=(current_year - start_year + 1) * len(make_id_cars_trucks_motorcycles) * 3)

    for year in range(start_year, current_year + 1):
        for make_id in make_id_cars_trucks_motorcycles['make_id']:
            for vehicle_type in ['passenger', 'truck', 'motorcycle']:
                try:
                    response = fetch_model_names(make_id=make_id, model_year=year, vehicle_type=vehicle_type)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    print(f"HTTP error occurred: {e}")
                    continue
                csv_file = io.StringIO(response.content.decode('utf-8'))
                df = pd.read_csv(csv_file)
                df = df.assign(year=year)
                # Some model_names can "look" like int types and so we want to make sure they are explicitly defined as str
                # when concatenating the dataframes together.  Otherwise, will get a pyarrow error due to int/str confusion.
                # Relevant background: https://github.com/wesm/feather/issues/349
                df['model_name'] = df['model_name'].astype('str')
                df_list.append(df)
                progress_bar.update(1)  # increment progress bar

    df_concat = pd.concat(df_list, ignore_index=True)
    today = datetime.today().strftime('%Y-%m-%d')
    df_concat = df_concat.assign(Created_Date=today)

    return df_concat.drop_duplicates()


# To return only mfr_id column, need to add this extra boilerplate
# https://docs.dagster.io/integrations/snowflake/reference#selecting-specific-columns-in-a-downstream-asset
@asset(
    group_name="nhtsa",
    ins={
        "manufacturers": AssetIn(
            key="manufacturers",
            metadata={"columns": ["mfr_id"]},
        )
    }
)
def wmi_by_manufacturer_id(manufacturers: pd.DataFrame) -> pd.DataFrame:
    """
    WMI codes by manufacturer using NHTSA's API.
    It will take up to approximately 2 hours to materialize this asset.
    """

    df_list = []
    for mfr_id in tqdm(manufacturers['mfr_id']):
        df = fetch_wmi_by_manufacturer(mfr_id)      # imported from utilities
        if df is not None:
            df_list.append(df)

    df_concat = pd.concat(df_list, ignore_index=True)
    today = datetime.today().strftime('%Y-%m-%d')
    df_concat = df_concat.assign(Created_Date=today)

    return df_concat.drop_duplicates()


# To return only wmi column, need to add this extra boilerplate
# https://docs.dagster.io/integrations/snowflake/reference#selecting-specific-columns-in-a-downstream-asset
@asset(
    group_name="nhtsa",
    ins={
        "wmi_by_manufacturer_id": AssetIn(
            key="wmi_by_manufacturer_id",
            metadata={"columns": ["wmi"]},
        )
    }
)
def wmi_with_makes(wmi_by_manufacturer_id: pd.DataFrame) -> pd.DataFrame:
    """
    WMI codes with vehicle make information from NHTSA's API.

    Parameters
    ----------
    wmi_by_manufacturer_id

    Returns
    -------
    pandas dataframe
    """

    df_list = []
    for wmi in tqdm(wmi_by_manufacturer_id['wmi']):
        df = fetch_wmi_data(wmi)          # imported from utilities
        if df is not None:
            df_list.append(df)

    df_concat = pd.concat(df_list, ignore_index=True)
    today = datetime.today().strftime('%Y-%m-%d')
    df_concat = df_concat.assign(Created_Date=today)

    return df_concat.drop_duplicates()


defs = Definitions(
    assets=[
        manufacturers,
        makes,
        make_id_cars_trucks_motorcycles,
        model_names,
        wmi_by_manufacturer_id,
        wmi_with_makes,
    ],
    resources={
        "io_manager": snowflake_pandas_io_manager.configured(
            {
                "account": {"env": "SF_ACCOUNT"},
                "warehouse": {"env": "SF_WAREHOUSE"},
                "database": {"env": "SF_DATABASE"},
                "schema": "nhtsa",
                "role": {"env": "SF_ROLE"},
                "user": {"env": "SF_USERNAME"},
                "password": {"env": "SF_PASSWORD"},
                "authenticator": {"env": "SF_AUTHENTICATOR"},
            }
        )
    },
)

