import os
import argparse
import math
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np
from supabase import create_client


BASE_URL = "https://apis.data.go.kr/B551177/AviationStatsByCountry"
TABLE_NAME = "airport_country_monthly_stats"


def get_required_env(name):
    value = os.getenv(name)

    if not value:
        raise ValueError(f"환경변수 {name}이 설정되어 있지 않습니다.")

    return value


def get_previous_month_yyyymm():
    """
    GitHub Actions가 매월 10일 실행될 때,
    직전 월 데이터를 적재하기 위한 함수입니다.

    예:
    2026-04-10 실행 → 202603 적재
    """
    now_kst = datetime.now(ZoneInfo("Asia/Seoul"))

    year = now_kst.year
    month = now_kst.month - 1

    if month == 0:
        year -= 1
        month = 12

    return f"{year}{month:02d}"


def xml_to_dataframe(xml_text):
    root = ET.fromstring(xml_text)

    result_code = root.findtext(".//resultCode")
    result_msg = root.findtext(".//resultMsg")

    if result_code != "00":
        raise ValueError(f"API 응답 오류: resultCode={result_code}, resultMsg={result_msg}")

    items = []

    for item in root.findall(".//item"):
        row = {}

        for child in item:
            row[child.tag] = child.text

        items.append(row)

    return pd.DataFrame(items)


def clean_number_column(series):
    return (
        series
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )


def fetch_passenger_stats(month, public_data_key):
    url = BASE_URL + "/getTotalNumberOfPassenger"

    params = {
        "serviceKey": public_data_key,
        "from_month": month,
        "to_month": month,
        "periodicity": "0",
        "pax_cargo": "Y",
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"여객 수 API 호출 실패: status={response.status_code}, body={response.text[:500]}"
        )

    df = xml_to_dataframe(response.text)

    if df.empty:
        raise ValueError(f"{month} 여객 수 API 응답 데이터가 비어 있습니다.")

    df = df.rename(columns={
        "arrPassenger": "arr_passengers",
        "depPassenger": "dep_passengers",
        "passenger": "total_passengers",
    })

    number_cols = [
        "arr_passengers",
        "dep_passengers",
        "total_passengers",
    ]

    for col in number_cols:
        df[col] = clean_number_column(df[col])

    return df[
        [
            "region",
            "country",
            "arr_passengers",
            "dep_passengers",
            "total_passengers",
        ]
    ]


def fetch_flight_stats(month, public_data_key):
    url = BASE_URL + "/getTotalNumberOfFlight"

    params = {
        "serviceKey": public_data_key,
        "from_month": month,
        "to_month": month,
        "periodicity": "0",
        "pax_cargo": "Y",
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"운항편 수 API 호출 실패: status={response.status_code}, body={response.text[:500]}"
        )

    df = xml_to_dataframe(response.text)

    if df.empty:
        raise ValueError(f"{month} 운항편 수 API 응답 데이터가 비어 있습니다.")

    df = df.rename(columns={
        "arrFlight": "arr_flights",
        "depFlight": "dep_flights",
        "flights": "total_flights",
    })

    number_cols = [
        "arr_flights",
        "dep_flights",
        "total_flights",
    ]

    for col in number_cols:
        df[col] = clean_number_column(df[col])

    return df[
        [
            "region",
            "country",
            "arr_flights",
            "dep_flights",
            "total_flights",
        ]
    ]


def make_iiac_monthly_stats(month, public_data_key):
    df_passengers = fetch_passenger_stats(month, public_data_key)
    df_flights = fetch_flight_stats(month, public_data_key)

    df_merged = pd.merge(
        df_flights,
        df_passengers,
        on=["region", "country"],
        how="outer",
    )

    stat_month = f"{month[:4]}-{month[4:]}-01"

    df_merged["stat_month"] = stat_month
    df_merged["source_name"] = "IIAC"

    df_merged = df_merged[
        [
            "stat_month",
            "source_name",
            "region",
            "country",
            "arr_flights",
            "dep_flights",
            "total_flights",
            "arr_passengers",
            "dep_passengers",
            "total_passengers",
        ]
    ]

    return df_merged


def clean_value_for_json(value):
    if pd.isna(value):
        return None

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        if math.isnan(value) or math.isinf(value):
            return None

        if float(value).is_integer():
            return int(value)

        return float(value)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

        if value.is_integer():
            return int(value)

        return value

    return value


def dataframe_to_clean_records(df):
    records = []

    for row in df.to_dict(orient="records"):
        clean_row = {}

        for key, value in row.items():
            clean_row[key] = clean_value_for_json(value)

        records.append(clean_row)

    return records


def save_to_supabase(df, supabase_url, supabase_key):
    supabase = create_client(supabase_url, supabase_key)

    records = dataframe_to_clean_records(df)

    if not records:
        raise ValueError("저장할 데이터가 없습니다.")

    response = (
        supabase
        .table(TABLE_NAME)
        .upsert(
            records,
            on_conflict="stat_month,source_name,country"
        )
        .execute()
    )

    return response


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--month",
        type=str,
        default="",
        help="적재할 월. YYYYMM 형식. 비워두면 한국 시간 기준 직전 월을 사용합니다.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    target_month = args.month.strip() if args.month else get_previous_month_yyyymm()

    if len(target_month) != 6 or not target_month.isdigit():
        raise ValueError(f"month는 YYYYMM 형식이어야 합니다. 입력값: {target_month}")

    supabase_url = get_required_env("SUPABASE_URL")
    supabase_key = get_required_env("SUPABASE_KEY")
    public_data_key = get_required_env("PUBLIC_DATA_KEY")

    print("=" * 80)
    print(f"IIAC 데이터 적재 시작: {target_month}")

    df_month = make_iiac_monthly_stats(target_month, public_data_key)

    print("기준월:", df_month["stat_month"].iloc[0])
    print("국가 수:", len(df_month))
    print("출국 운항편 합계:", int(df_month["dep_flights"].sum()))
    print("출국 승객 합계:", int(df_month["dep_passengers"].sum()))
    print("전체 운항편 합계:", int(df_month["total_flights"].sum()))
    print("전체 승객 합계:", int(df_month["total_passengers"].sum()))

    response = save_to_supabase(df_month, supabase_url, supabase_key)

    print("Supabase 저장 완료")
    print("저장/업데이트 행 수:", len(response.data or []))
    print("=" * 80)


if __name__ == "__main__":
    main()
