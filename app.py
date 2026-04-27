import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client


# ------------------------------------------------------------
# 기본 설정
# ------------------------------------------------------------
st.set_page_config(
    page_title="공항 국가별 항공 통계 대시보드",
    page_icon="✈️",
    layout="wide"
)

st.title("✈️ 공항 국가별 항공 통계 대시보드")
st.caption(
    "인천국제공항공사(IIAC) API 데이터와 한국공항공사(KAC) 공개 통계 데이터를 합산해 조회합니다."
)


# ------------------------------------------------------------
# Supabase 데이터 불러오기
# ------------------------------------------------------------
@st.cache_data(ttl=600)
def load_airport_stats():
    supabase = create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"]
    )

    table_name = "airport_country_monthly_stats"

    page_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + page_size - 1

        response = (
            supabase
            .table(table_name)
            .select(
                "stat_month, source_name, region, country, "
                "arr_flights, dep_flights, total_flights, "
                "arr_passengers, dep_passengers, total_passengers"
            )
            .order("stat_month")
            .range(start, end)
            .execute()
        )

        rows = response.data or []
        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        start += page_size

    df = pd.DataFrame(all_rows)

    if df.empty:
        return df

    df["stat_month"] = pd.to_datetime(df["stat_month"])

    number_cols = [
        "arr_flights",
        "dep_flights",
        "total_flights",
        "arr_passengers",
        "dep_passengers",
        "total_passengers"
    ]

    for col in number_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["source_name"] = df["source_name"].fillna("UNKNOWN")
    df["region"] = df["region"].fillna("미분류")
    df["country"] = df["country"].fillna("미상")

    return df


df = load_airport_stats()
@st.cache_data(ttl=600)
def load_weekly_route_summary():
    supabase = create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"]
    )

    page_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + page_size - 1

        response = (
            supabase
            .table("airport_weekly_city_summary")
            .select(
                "snapshot_date, service_window_start, service_window_end, "
                "source_name, origin_airport_code, origin_airport_name, "
                "destination_airport_code, destination_airport_name, "
                "destination_country, destination_city, "
                "weekly_departure_flights, schedule_row_count, flight_no_count"
            )
            .order("snapshot_date", desc=True)
            .range(start, end)
            .execute()
        )

        rows = response.data or []
        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        start += page_size

    df_weekly = pd.DataFrame(all_rows)

    if df_weekly.empty:
        return df_weekly

    date_cols = [
        "snapshot_date",
        "service_window_start",
        "service_window_end"
    ]

    for col in date_cols:
        df_weekly[col] = pd.to_datetime(df_weekly[col], errors="coerce")

    number_cols = [
        "weekly_departure_flights",
        "schedule_row_count",
        "flight_no_count"
    ]

    for col in number_cols:
        df_weekly[col] = pd.to_numeric(df_weekly[col], errors="coerce").fillna(0).astype(int)

    df_weekly["source_name"] = df_weekly["source_name"].fillna("UNKNOWN")
    df_weekly["origin_airport_name"] = df_weekly["origin_airport_name"].fillna("미상")
    df_weekly["destination_country"] = df_weekly["destination_country"].fillna("미분류")
    df_weekly["destination_city"] = df_weekly["destination_city"].fillna("미분류")
    df_weekly["destination_airport_code"] = df_weekly["destination_airport_code"].fillna("")
    df_weekly["destination_airport_name"] = df_weekly["destination_airport_name"].fillna("미분류")

    return df_weekly

# ------------------------------------------------------------
# 데이터 없음 처리
# ------------------------------------------------------------
if df.empty:
    st.warning(
        "Supabase에서 데이터를 불러오지 못했습니다. "
        "테이블명, RLS 정책, Supabase URL/KEY를 확인해주세요."
    )
    st.stop()


# ------------------------------------------------------------
# 사이드바 필터
# ------------------------------------------------------------
st.sidebar.header("필터")

min_date = df["stat_month"].min().date()
max_date = df["stat_month"].max().date()

date_range = st.sidebar.date_input(
    "조회 기간",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date


country_options = sorted(df["country"].dropna().unique().tolist())

selected_countries = st.sidebar.multiselect(
    "국가",
    options=country_options,
    default=[]
)

top_n = st.sidebar.slider(
    "국가 TOP N",
    min_value=5,
    max_value=30,
    value=15,
    step=5
)

st.sidebar.caption("모든 지표와 차트는 IIAC + KAC 합산 기준입니다.")
st.sidebar.caption("KPI와 국가 TOP 차트는 선택 기간 내 가장 최신월 기준입니다.")


# ------------------------------------------------------------
# 필터 적용
# ------------------------------------------------------------
filtered_df = df.copy()

filtered_df = filtered_df[
    (filtered_df["stat_month"].dt.date >= start_date)
    & (filtered_df["stat_month"].dt.date <= end_date)
]

if selected_countries:
    filtered_df = filtered_df[
        filtered_df["country"].isin(selected_countries)
    ]

if filtered_df.empty:
    st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    st.stop()


# ------------------------------------------------------------
# 공통 함수
# ------------------------------------------------------------
def calc_sum(dataframe, column):
    if dataframe.empty:
        return 0

    return int(dataframe[column].sum())


def calc_rate(current, previous):
    if previous == 0:
        return None

    return ((current - previous) / previous) * 100


def format_delta_for_metric(current, previous):
    rate = calc_rate(current, previous)

    if rate is None:
        return None

    return f"{rate:+.1f}%"


def format_month(dt):
    return dt.strftime("%Y년 %m월")


def format_previous_value(value, suffix, is_float=False):
    if value == 0:
        return "전년 데이터 없음"

    if is_float:
        return f"{value:,.1f}{suffix}"

    return f"{int(value):,}{suffix}"


# ------------------------------------------------------------
# 최신월, 전월, 전년 동월 데이터 생성
# ------------------------------------------------------------
latest_month = filtered_df["stat_month"].max()
previous_month = latest_month - pd.DateOffset(months=1)
previous_year_month = latest_month - pd.DateOffset(years=1)

latest_df = filtered_df[
    filtered_df["stat_month"] == latest_month
]

previous_df = filtered_df[
    filtered_df["stat_month"] == previous_month
]

previous_year_df = filtered_df[
    filtered_df["stat_month"] == previous_year_month
]


# ------------------------------------------------------------
# 최신월 KPI
# ------------------------------------------------------------
latest_dep_passengers = calc_sum(latest_df, "dep_passengers")
latest_dep_flights = calc_sum(latest_df, "dep_flights")

previous_dep_passengers = calc_sum(previous_df, "dep_passengers")
previous_dep_flights = calc_sum(previous_df, "dep_flights")

previous_year_dep_passengers = calc_sum(previous_year_df, "dep_passengers")
previous_year_dep_flights = calc_sum(previous_year_df, "dep_flights")

avg_dep_passengers_per_flight = (
    latest_dep_passengers / latest_dep_flights
    if latest_dep_flights > 0
    else 0
)

previous_avg_dep_passengers_per_flight = (
    previous_dep_passengers / previous_dep_flights
    if previous_dep_flights > 0
    else 0
)

previous_year_avg_dep_passengers_per_flight = (
    previous_year_dep_passengers / previous_year_dep_flights
    if previous_year_dep_flights > 0
    else 0
)

st.subheader(f"최신월 현황: {format_month(latest_month)}")

kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

kpi_col1.metric(
    "출국 승객 수",
    f"{latest_dep_passengers:,}명",
    delta=format_delta_for_metric(
        latest_dep_passengers,
        previous_dep_passengers
    )
)

kpi_col2.metric(
    "출국 운항편 수",
    f"{latest_dep_flights:,}편",
    delta=format_delta_for_metric(
        latest_dep_flights,
        previous_dep_flights
    )
)

kpi_col3.metric(
    "출국편당 평균 승객 수",
    f"{avg_dep_passengers_per_flight:,.1f}명",
    delta=format_delta_for_metric(
        avg_dep_passengers_per_flight,
        previous_avg_dep_passengers_per_flight
    )
)

st.caption(
    f"전월 대비 기준: {format_month(previous_month)} → {format_month(latest_month)}"
)

st.divider()


# ------------------------------------------------------------
# 전년 동월 대비 KPI
# ------------------------------------------------------------
st.subheader("전년 동월 대비")

yoy_col1, yoy_col2, yoy_col3 = st.columns(3)

with yoy_col1:
    st.metric(
        "출국 승객 수 YoY",
        f"{latest_dep_passengers:,}명",
        delta=format_delta_for_metric(
            latest_dep_passengers,
            previous_year_dep_passengers
        )
    )
    st.caption(
        f"{format_month(previous_year_month)}: "
        f"{format_previous_value(previous_year_dep_passengers, '명')}"
    )

with yoy_col2:
    st.metric(
        "출국 운항편 수 YoY",
        f"{latest_dep_flights:,}편",
        delta=format_delta_for_metric(
            latest_dep_flights,
            previous_year_dep_flights
        )
    )
    st.caption(
        f"{format_month(previous_year_month)}: "
        f"{format_previous_value(previous_year_dep_flights, '편')}"
    )

with yoy_col3:
    st.metric(
        "출국편당 평균 승객 수 YoY",
        f"{avg_dep_passengers_per_flight:,.1f}명",
        delta=format_delta_for_metric(
            avg_dep_passengers_per_flight,
            previous_year_avg_dep_passengers_per_flight
        )
    )
    st.caption(
        f"{format_month(previous_year_month)}: "
        f"{format_previous_value(previous_year_avg_dep_passengers_per_flight, '명', is_float=True)}"
    )

if previous_year_df.empty:
    st.caption(
        f"전년 동월 데이터({format_month(previous_year_month)})가 없어 증감률을 계산하지 않았습니다."
    )
else:
    st.caption(
        f"전년 동월 대비 기준: {format_month(previous_year_month)} → {format_month(latest_month)}"
    )

st.divider()


# ------------------------------------------------------------
# 월별 합산 추이 데이터
# ------------------------------------------------------------
monthly_total = (
    filtered_df
    .groupby("stat_month", as_index=False)
    .agg(
        dep_passengers=("dep_passengers", "sum"),
        dep_flights=("dep_flights", "sum"),
        total_passengers=("total_passengers", "sum"),
        total_flights=("total_flights", "sum")
    )
    .sort_values("stat_month")
)


# ------------------------------------------------------------
# 월별 출국 승객 수 추이
# ------------------------------------------------------------
st.subheader("월별 출국 승객 수 추이")

fig_dep_passengers = px.line(
    monthly_total,
    x="stat_month",
    y="dep_passengers",
    markers=True,
    labels={
        "stat_month": "월",
        "dep_passengers": "출국 승객 수"
    }
)

fig_dep_passengers.update_layout(
    hovermode="x unified"
)

st.plotly_chart(fig_dep_passengers, use_container_width=True)

st.divider()


# ------------------------------------------------------------
# 월별 출국 운항편 수 추이
# ------------------------------------------------------------
st.subheader("월별 출국 운항편 수 추이")

fig_dep_flights = px.line(
    monthly_total,
    x="stat_month",
    y="dep_flights",
    markers=True,
    labels={
        "stat_month": "월",
        "dep_flights": "출국 운항편 수"
    }
)

fig_dep_flights.update_layout(
    hovermode="x unified"
)

st.plotly_chart(fig_dep_flights, use_container_width=True)

st.divider()


# ------------------------------------------------------------
# 최신월 국가별 TOP N
# ------------------------------------------------------------
st.subheader(f"{format_month(latest_month)} 국가별 TOP {top_n}")

latest_country_summary = (
    latest_df
    .groupby("country", as_index=False)
    .agg(
        dep_passengers=("dep_passengers", "sum"),
        dep_flights=("dep_flights", "sum"),
        total_passengers=("total_passengers", "sum"),
        total_flights=("total_flights", "sum")
    )
)

tab_passengers, tab_flights = st.tabs(
    ["출국 승객 기준", "출국 운항편 기준"]
)

with tab_passengers:
    passenger_top = (
        latest_country_summary
        .sort_values("dep_passengers", ascending=False)
        .head(top_n)
    )

    fig_country_passenger_top = px.bar(
        passenger_top.sort_values("dep_passengers", ascending=True),
        x="dep_passengers",
        y="country",
        orientation="h",
        text="dep_passengers",
        labels={
            "dep_passengers": "출국 승객 수",
            "country": "국가"
        }
    )

    fig_country_passenger_top.update_traces(
        texttemplate="%{text:,}",
        textposition="outside"
    )

    fig_country_passenger_top.update_layout(
        yaxis_title=None,
        xaxis_title="출국 승객 수"
    )

    st.plotly_chart(fig_country_passenger_top, use_container_width=True)

with tab_flights:
    flight_top = (
        latest_country_summary
        .sort_values("dep_flights", ascending=False)
        .head(top_n)
    )

    fig_country_flight_top = px.bar(
        flight_top.sort_values("dep_flights", ascending=True),
        x="dep_flights",
        y="country",
        orientation="h",
        text="dep_flights",
        labels={
            "dep_flights": "출국 운항편 수",
            "country": "국가"
        }
    )

    fig_country_flight_top.update_traces(
        texttemplate="%{text:,}",
        textposition="outside"
    )

    fig_country_flight_top.update_layout(
        yaxis_title=None,
        xaxis_title="출국 운항편 수"
    )

    st.plotly_chart(fig_country_flight_top, use_container_width=True)

st.divider()


# ------------------------------------------------------------
# 최신월 국가별 상세 테이블
# ------------------------------------------------------------
st.subheader(f"{format_month(latest_month)} 국가별 상세")

latest_country_table = (
    latest_country_summary
    .sort_values("dep_passengers", ascending=False)
)

st.dataframe(
    latest_country_table,
    use_container_width=True,
    hide_index=True
)

st.divider()


# ------------------------------------------------------------
# 월별 요약 테이블
# ------------------------------------------------------------
st.subheader("월별 요약 테이블")

monthly_display = monthly_total.copy()
monthly_display["stat_month"] = monthly_display["stat_month"].dt.strftime("%Y-%m")

st.dataframe(
    monthly_display,
    use_container_width=True,
    hide_index=True
)

st.divider()

# ------------------------------------------------------------
# 주간 국제선 출발 스케줄
# ------------------------------------------------------------
st.subheader("주간 국제선 출발 스케줄")

df_weekly = load_weekly_route_summary()

if df_weekly.empty:
    st.info("아직 주간 국제선 출발 스케줄 데이터가 없습니다.")
else:
    latest_snapshot_date = df_weekly["snapshot_date"].max()

    weekly_latest_df = df_weekly[
        df_weekly["snapshot_date"] == latest_snapshot_date
    ].copy()

    service_window_start = weekly_latest_df["service_window_start"].min()
    service_window_end = weekly_latest_df["service_window_end"].max()

    st.caption(
        f"수집일: {latest_snapshot_date.strftime('%Y-%m-%d')} / "
        f"대상 기간: {service_window_start.strftime('%Y-%m-%d')} ~ {service_window_end.strftime('%Y-%m-%d')}"
    )

    weekly_origin_options = sorted(
        weekly_latest_df["origin_airport_name"].dropna().unique().tolist()
    )

    weekly_country_options = sorted(
        weekly_latest_df["destination_country"].dropna().unique().tolist()
    )

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        selected_weekly_origins = st.multiselect(
            "출발공항",
            options=weekly_origin_options,
            default=weekly_origin_options,
            key="weekly_origin_filter"
        )

    with filter_col2:
        selected_weekly_countries = st.multiselect(
            "도착국가",
            options=weekly_country_options,
            default=[],
            key="weekly_country_filter"
        )

    with filter_col3:
        weekly_top_n = st.slider(
            "주간 스케줄 TOP N",
            min_value=5,
            max_value=30,
            value=15,
            step=5,
            key="weekly_top_n"
        )

    weekly_filtered_df = weekly_latest_df.copy()

    if selected_weekly_origins:
        weekly_filtered_df = weekly_filtered_df[
            weekly_filtered_df["origin_airport_name"].isin(selected_weekly_origins)
        ]

    if selected_weekly_countries:
        weekly_filtered_df = weekly_filtered_df[
            weekly_filtered_df["destination_country"].isin(selected_weekly_countries)
        ]

    if weekly_filtered_df.empty:
        st.warning("선택한 조건에 해당하는 주간 스케줄 데이터가 없습니다.")
    else:
        # ------------------------------------------------------------
        # 국가별 집계
        # ------------------------------------------------------------
        weekly_country_summary = (
            weekly_filtered_df
            .groupby("destination_country", as_index=False)
            .agg(
                weekly_departure_flights=("weekly_departure_flights", "sum")
            )
            .sort_values("weekly_departure_flights", ascending=False)
        )

        # ------------------------------------------------------------
        # 도시별 집계
        # ------------------------------------------------------------
        weekly_city_summary = (
            weekly_filtered_df
            .groupby(["destination_country", "destination_city"], as_index=False)
            .agg(
                weekly_departure_flights=("weekly_departure_flights", "sum")
            )
            .sort_values("weekly_departure_flights", ascending=False)
        )

        # ------------------------------------------------------------
        # 목적지 공항별 집계
        # ------------------------------------------------------------
        weekly_airport_summary = (
            weekly_filtered_df
            .groupby(
                [
                    "destination_country",
                    "destination_city",
                    "destination_airport_code",
                    "destination_airport_name"
                ],
                as_index=False
            )
            .agg(
                weekly_departure_flights=("weekly_departure_flights", "sum")
            )
            .sort_values("weekly_departure_flights", ascending=False)
        )

        top_country_row = weekly_country_summary.iloc[0]
        top_city_row = weekly_city_summary.iloc[0]

        destination_city_count = (
            weekly_city_summary[
                weekly_city_summary["destination_city"] != "미분류"
            ]["destination_city"]
            .nunique()
        )

        weekly_kpi_col1, weekly_kpi_col2, weekly_kpi_col3 = st.columns(3)

        weekly_kpi_col1.metric(
            "최다 출발 국가",
            str(top_country_row["destination_country"]),
            f"{int(top_country_row['weekly_departure_flights']):,}편"
        )

        weekly_kpi_col2.metric(
            "최다 출발 도시",
            str(top_city_row["destination_city"]),
            f"{int(top_city_row['weekly_departure_flights']):,}편"
        )

        weekly_kpi_col3.metric(
            "운항 목적지 도시 수",
            f"{destination_city_count:,}개"
        )

        st.divider()

        # ------------------------------------------------------------
        # 국가별 TOP 차트
        # ------------------------------------------------------------
        st.subheader(f"국가별 주간 출발편 수 TOP {weekly_top_n}")

        country_top = weekly_country_summary.head(weekly_top_n)

        fig_weekly_country = px.bar(
            country_top.sort_values("weekly_departure_flights", ascending=True),
            x="weekly_departure_flights",
            y="destination_country",
            orientation="h",
            text="weekly_departure_flights",
            labels={
                "weekly_departure_flights": "주간 출발편 수",
                "destination_country": "도착국가"
            }
        )

        fig_weekly_country.update_traces(
            texttemplate="%{text:,}",
            textposition="outside"
        )

        fig_weekly_country.update_layout(
            yaxis_title=None,
            xaxis_title="주간 출발편 수"
        )

        st.plotly_chart(fig_weekly_country, use_container_width=True)

        st.divider()

        # ------------------------------------------------------------
        # 도시별 TOP 차트
        # ------------------------------------------------------------
        st.subheader(f"도시별 주간 출발편 수 TOP {weekly_top_n}")

        city_top = weekly_city_summary.head(weekly_top_n).copy()
        city_top["city_label"] = (
            city_top["destination_city"]
            + " · "
            + city_top["destination_country"]
        )

        fig_weekly_city = px.bar(
            city_top.sort_values("weekly_departure_flights", ascending=True),
            x="weekly_departure_flights",
            y="city_label",
            orientation="h",
            text="weekly_departure_flights",
            labels={
                "weekly_departure_flights": "주간 출발편 수",
                "city_label": "도착도시"
            }
        )

        fig_weekly_city.update_traces(
            texttemplate="%{text:,}",
            textposition="outside"
        )

        fig_weekly_city.update_layout(
            yaxis_title=None,
            xaxis_title="주간 출발편 수"
        )

        st.plotly_chart(fig_weekly_city, use_container_width=True)

        st.divider()

        # ------------------------------------------------------------
        # 목적지 공항별 TOP 차트
        # ------------------------------------------------------------
        st.subheader(f"목적지 공항별 주간 출발편 수 TOP {weekly_top_n}")

        airport_top = weekly_airport_summary.head(weekly_top_n).copy()
        airport_top["airport_label"] = (
            airport_top["destination_airport_code"].astype(str)
            + " · "
            + airport_top["destination_airport_name"].astype(str)
        )

        fig_weekly_airport = px.bar(
            airport_top.sort_values("weekly_departure_flights", ascending=True),
            x="weekly_departure_flights",
            y="airport_label",
            orientation="h",
            text="weekly_departure_flights",
            labels={
                "weekly_departure_flights": "주간 출발편 수",
                "airport_label": "목적지 공항"
            }
        )

        fig_weekly_airport.update_traces(
            texttemplate="%{text:,}",
            textposition="outside"
        )

        fig_weekly_airport.update_layout(
            yaxis_title=None,
            xaxis_title="주간 출발편 수"
        )

        st.plotly_chart(fig_weekly_airport, use_container_width=True)

        st.divider()

        # ------------------------------------------------------------
        # 상세 테이블
        # ------------------------------------------------------------
        st.subheader("주간 출발 스케줄 상세")

        weekly_detail_table = (
            weekly_airport_summary
            .rename(columns={
                "destination_country": "도착국가",
                "destination_city": "도착도시",
                "destination_airport_code": "도착공항코드",
                "destination_airport_name": "도착공항명",
                "weekly_departure_flights": "주간출발편수"
            })
        )

        st.dataframe(
            weekly_detail_table,
            use_container_width=True,
            hide_index=True
        )
# ------------------------------------------------------------
# 원본 데이터 테이블
# ------------------------------------------------------------
with st.expander("원본 데이터 보기"):
    display_df = filtered_df.copy()
    display_df["stat_month"] = display_df["stat_month"].dt.strftime("%Y-%m")

    display_df = display_df.sort_values(
        ["stat_month", "country", "dep_passengers"],
        ascending=[True, True, False]
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

    csv = display_df.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        label="CSV 다운로드",
        data=csv,
        file_name="airport_country_monthly_stats.csv",
        mime="text/csv"
    )
