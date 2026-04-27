import os
import math
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np
from supabase import create_client


IIAC_WEEKLY_DEPARTURE_URL = (
    "http://apis.data.go.kr/B551177/StatusOfPassengerFlightsDSOdp/"
    "getPassengerDeparturesDSOdp"
)

TABLE_NAME = "airport_weekly_route_schedules"


def get_required_env(name):
    value = os.getenv(name)

    if not value:
        raise ValueError(f"환경변수 {name}이 설정되어 있지 않습니다.")

    return value.strip()


def get_today_kst():
    return datetime.now(ZoneInfo("Asia/Seoul")).date()


def find_items_from_response(data):
    response_data = data.get("response", {})
    body = response_data.get("body", {})
    items = body.get("items", {})

    if isinstance(items, dict):
        item_list = items.get("item", [])
    else:
        item_list = items

    if isinstance(item_list, dict):
        item_list = [item_list]

    if item_list is None:
        item_list = []

    return item_list


def fetch_all_iiac_weekly_departures(public_data_key, page_size=1000):
    first_params = {
        "serviceKey": public_data_key,
        "type": "json",
        "numOfRows": page_size,
        "pageNo": 1,
    }

    first_response = requests.get(
        IIAC_WEEKLY_DEPARTURE_URL,
        params=first_params,
        timeout=60,
    )

    print("첫 요청 상태 코드:", first_response.status_code)

    if first_response.status_code != 200:
        raise RuntimeError(first_response.text[:1000])

    first_data = first_response.json()
    first_body = first_data.get("response", {}).get("body", {})

    total_count = int(first_body.get("totalCount", 0))
    total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

    print("totalCount:", total_count)
    print("totalPages:", total_pages)

    all_items = []

    first_items = find_items_from_response(first_data)
    all_items.extend(first_items)

    for page_no in range(2, total_pages + 1):
        params = {
            "serviceKey": public_data_key,
            "type": "json",
            "numOfRows": page_size,
            "pageNo": page_no,
        }

        api_response = requests.get(
            IIAC_WEEKLY_DEPARTURE_URL,
            params=params,
            timeout=60,
        )

        print(f"page {page_no}/{total_pages} 상태 코드:", api_response.status_code)

        if api_response.status_code != 200:
            raise RuntimeError(api_response.text[:1000])

        data = api_response.json()
        items = find_items_from_response(data)
        all_items.extend(items)

    df = pd.DataFrame(all_items)

    print("수집된 전체 원본 행 수:", len(df))

    return df


def normalize_iiac_weekly_departures(df_raw, snapshot_date):
    if df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()

    required_cols = [
        "airline",
        "flightId",
        "scheduleDateTime",
        "estimatedDateTime",
        "airport",
        "airportCode",
        "remark",
        "terminalid",
        "codeshare",
        "masterflightid",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["effective_flight_no"] = df["masterflightid"].fillna("").astype(str)

    df.loc[
        df["effective_flight_no"].str.strip() == "",
        "effective_flight_no",
    ] = df["flightId"].astype(str)

    df["dedupe_key"] = (
        "ICN|"
        + df["airportCode"].astype(str)
        + "|"
        + df["scheduleDateTime"].astype(str)
        + "|"
        + df["effective_flight_no"].astype(str)
    )

    original_count = len(df)

    df = df.drop_duplicates(subset=["dedupe_key"]).copy()

    print("코드쉐어/중복 제거 전 행 수:", original_count)
    print("코드쉐어/중복 제거 후 행 수:", len(df))
    print("제거된 행 수:", original_count - len(df))

    schedule_datetime_text = (
        df["scheduleDateTime"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    df["schedule_datetime"] = pd.to_datetime(
        schedule_datetime_text,
        format="%Y%m%d%H%M",
        errors="coerce",
    )

    service_window_start = snapshot_date
    service_window_end = snapshot_date + timedelta(days=6)

    df["snapshot_date"] = str(snapshot_date)
    df["service_window_start"] = str(service_window_start)
    df["service_window_end"] = str(service_window_end)

    df["source_name"] = "IIAC"
    df["origin_airport_code"] = "ICN"
    df["origin_airport_name"] = "인천"

    df["destination_airport_code"] = df["airportCode"]
    df["destination_airport_name"] = df["airport"]

    # 도시/국가는 airport_city_country_map과 View에서 매핑합니다.
    df["destination_city"] = None
    df["destination_country"] = None

    df["airline_name"] = df["airline"]
    df["airline_code"] = None
    df["flight_no"] = df["flightId"]
    df["master_flight_no"] = df["masterflightid"]

    df["is_codeshare"] = (
        df["codeshare"].notna()
        & (df["codeshare"].astype(str).str.strip() != "")
        & (df["codeshare"].astype(str).str.lower() != "nan")
    )

    df["departure_time"] = df["schedule_datetime"].dt.strftime("%H:%M")
    df["operation_days"] = None

    # IIAC 주간 운항현황은 향후 7일 개별 항공편 목록이므로 1행 = 1편
    df["weekly_frequency"] = 1

    df["status_remark"] = df["remark"]

    raw_records = df_raw.to_dict(orient="records")

    # drop_duplicates 후 index가 바뀌어도 대응되도록 원본 행 일부만 raw_data로 저장합니다.
    df["raw_data"] = df.apply(
        lambda row: {
            "airline": row.get("airline"),
            "flightId": row.get("flightId"),
            "scheduleDateTime": row.get("scheduleDateTime"),
            "estimatedDateTime": row.get("estimatedDateTime"),
            "airport": row.get("airport"),
            "airportCode": row.get("airportCode"),
            "remark": row.get("remark"),
            "terminalid": row.get("terminalid"),
            "codeshare": row.get("codeshare"),
            "masterflightid": row.get("masterflightid"),
        },
        axis=1,
    )

    df_to_save = df[
        [
            "snapshot_date",
            "service_window_start",
            "service_window_end",
            "source_name",
            "origin_airport_code",
            "origin_airport_name",
            "destination_airport_code",
            "destination_airport_name",
            "destination_city",
            "destination_country",
            "airline_name",
            "airline_code",
            "flight_no",
            "master_flight_no",
            "is_codeshare",
            "schedule_datetime",
            "departure_time",
            "operation_days",
            "weekly_frequency",
            "status_remark",
            "dedupe_key",
            "raw_data",
        ]
    ].copy()

    return df_to_save


def clean_value_for_json(value):
    if isinstance(value, dict):
        return {
            key: clean_value_for_json(val)
            for key, val in value.items()
        }

    if isinstance(value, list):
        return [
            clean_value_for_json(item)
            for item in value
        ]

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None

        return value.strftime("%Y-%m-%d %H:%M:%S")

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

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    return value


def dataframe_to_clean_records(df):
    records = []

    for row in df.to_dict(orient="records"):
        clean_row = {}

        for key, value in row.items():
            clean_row[key] = clean_value_for_json(value)

        records.append(clean_row)

    return records


def upsert_records_in_chunks(supabase, records, chunk_size=500):
    total_saved = 0

    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]

        response = (
            supabase
            .table(TABLE_NAME)
            .upsert(
                chunk,
                on_conflict="snapshot_date,source_name,dedupe_key"
            )
            .execute()
        )

        saved_count = len(response.data or [])
        total_saved += saved_count

        print(
            f"chunk {start // chunk_size + 1}: "
            f"{saved_count}행 저장/업데이트"
        )

    return total_saved


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--snapshot-date",
        type=str,
        default="",
        help="스냅샷 날짜. YYYY-MM-DD 형식. 비워두면 한국 시간 기준 오늘 날짜를 사용합니다.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.snapshot_date:
        snapshot_date = datetime.strptime(args.snapshot_date, "%Y-%m-%d").date()
    else:
        snapshot_date = get_today_kst()

    supabase_url = get_required_env("SUPABASE_URL")
    supabase_key = get_required_env("SUPABASE_KEY")
    public_data_key = get_required_env("PUBLIC_DATA_KEY")

    supabase = create_client(supabase_url, supabase_key)

    print("=" * 80)
    print("IIAC 주간 출발 스케줄 수집 시작")
    print("snapshot_date:", snapshot_date)
    print("service_window_start:", snapshot_date)
    print("service_window_end:", snapshot_date + timedelta(days=6))

    df_raw = fetch_all_iiac_weekly_departures(public_data_key)
    df_to_save = normalize_iiac_weekly_departures(df_raw, snapshot_date)

    if df_to_save.empty:
        raise ValueError("저장할 IIAC 주간 스케줄 데이터가 없습니다.")

    print("저장 대상 행 수:", len(df_to_save))

    records = dataframe_to_clean_records(df_to_save)

    saved_count = upsert_records_in_chunks(
        supabase=supabase,
        records=records,
        chunk_size=500,
    )

    print("Supabase 저장 완료")
    print("저장/업데이트 응답 행 수 합계:", saved_count)

    airport_summary = (
        df_to_save
        .groupby(
            [
                "destination_airport_code",
                "destination_airport_name",
            ],
            as_index=False
        )
        .agg(
            weekly_departure_flights=("weekly_frequency", "sum")
        )
        .sort_values("weekly_departure_flights", ascending=False)
    )

    print("목적지 공항 수:", len(airport_summary))
    print("상위 목적지 공항:")
    print(airport_summary.head(20).to_string(index=False))
    print("=" * 80)


if __name__ == "__main__":
    main()
