import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import streamlit as st


# -----------------------------
# Config
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("PGHOST", "80.253.246.90"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "tribalwars"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "21532375sH+*"),
}

st.set_page_config(
    page_title="TribalWars Klan Degisim Analizi",
    layout="wide",
)


# -----------------------------
# DB helpers
# -----------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=60)
def get_runs() -> pd.DataFrame:
    query = """
        SELECT id, queried_at, tribe_count, total_member_count
        FROM query_runs
        ORDER BY queried_at DESC
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_tribes() -> pd.DataFrame:
    query = """
        SELECT id, clan_id, clan_name, source_url, created_at
        FROM tribes
        ORDER BY clan_name
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_parent_map() -> pd.DataFrame:
    query = """
        SELECT
            child.id AS tribe_id,
            child.id AS child_tribe_id,
            child.clan_id AS child_clan_id,
            child.clan_name AS child_clan_name,
            parent.id AS parent_tribe_id,
            parent.clan_id AS parent_clan_id,
            parent.clan_name AS parent_clan_name,
            r.relation_type,
            r.valid_from,
            r.valid_to
        FROM tribe_relations r
        JOIN tribes parent ON parent.id = r.parent_tribe_id
        JOIN tribes child ON child.id = r.child_tribe_id
        WHERE r.valid_to IS NULL
        ORDER BY parent.clan_name, child.clan_name
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_snapshots() -> pd.DataFrame:
    query = """
        SELECT
            ts.id AS tribe_snapshot_id,
            ts.query_run_id,
            ts.tribe_id,
            t.clan_id,
            t.clan_name,
            ts.member_count,
            qr.queried_at
        FROM tribe_snapshots ts
        JOIN query_runs qr ON qr.id = ts.query_run_id
        JOIN tribes t ON t.id = ts.tribe_id
        ORDER BY qr.queried_at DESC, t.clan_name
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_members_for_snapshot(snapshot_id: int) -> pd.DataFrame:
    query = """
        SELECT
            m.id,
            m.tribe_snapshot_id,
            m.tribe_id,
            m.tribe_rank,
            m.name,
            m.player_id,
            m.player_url,
            m.points,
            m.villages,
            m.total_change_text,
            m.total_change,
            m.total_absolute,
            m.daily_changes,
            m.created_at
        FROM members m
        WHERE m.tribe_snapshot_id = %s
        ORDER BY m.points DESC NULLS LAST, m.name ASC
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=(snapshot_id,))
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_members_for_run(run_id: int, tribe_ids: Optional[List[int]] = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        if tribe_ids:
            query = """
                SELECT
                    qr.id AS query_run_id,
                    qr.queried_at,
                    ts.id AS tribe_snapshot_id,
                    ts.tribe_id,
                    t.clan_id,
                    t.clan_name,
                    m.name,
                    m.player_id,
                    m.points,
                    m.villages,
                    m.player_url,
                    m.tribe_rank,
                    m.total_change,
                    m.total_absolute,
                    m.daily_changes
                FROM tribe_snapshots ts
                JOIN query_runs qr ON qr.id = ts.query_run_id
                JOIN tribes t ON t.id = ts.tribe_id
                JOIN members m ON m.tribe_snapshot_id = ts.id
                WHERE qr.id = %s
                  AND ts.tribe_id = ANY(%s)
                ORDER BY t.clan_name, m.points DESC NULLS LAST, m.name ASC
            """
            return pd.read_sql(query, conn, params=(run_id, tribe_ids))

        query = """
            SELECT
                qr.id AS query_run_id,
                qr.queried_at,
                ts.id AS tribe_snapshot_id,
                ts.tribe_id,
                t.clan_id,
                t.clan_name,
                m.name,
                m.player_id,
                m.points,
                m.villages,
                m.player_url,
                m.tribe_rank,
                m.total_change,
                m.total_absolute,
                m.daily_changes
            FROM tribe_snapshots ts
            JOIN query_runs qr ON qr.id = ts.query_run_id
            JOIN tribes t ON t.id = ts.tribe_id
            JOIN members m ON m.tribe_snapshot_id = ts.id
            WHERE qr.id = %s
            ORDER BY t.clan_name, m.points DESC NULLS LAST, m.name ASC
        """
        return pd.read_sql(query, conn, params=(run_id,))
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_member_movements(curr_run_id: int, tribe_ids: Optional[List[int]] = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        if tribe_ids:
            query = """
                SELECT
                    mm.id,
                    mm.movement_type,
                    mm.player_id,
                    mm.player_name,
                    mm.prev_query_run_id,
                    mm.curr_query_run_id,
                    mm.prev_snapshot_at,
                    mm.curr_snapshot_at,
                    mm.prev_points,
                    mm.curr_points,
                    mm.prev_villages,
                    mm.curr_villages,
                    ft.clan_name AS from_clan_name,
                    tt.clan_name AS to_clan_name,
                    mm.from_tribe_id,
                    mm.to_tribe_id
                FROM member_movements mm
                LEFT JOIN tribes ft ON ft.id = mm.from_tribe_id
                LEFT JOIN tribes tt ON tt.id = mm.to_tribe_id
                WHERE mm.curr_query_run_id = %s
                  AND (
                        mm.from_tribe_id = ANY(%s)
                     OR mm.to_tribe_id = ANY(%s)
                  )
                ORDER BY mm.curr_snapshot_at DESC, mm.movement_type, mm.player_name
            """
            return pd.read_sql(query, conn, params=(curr_run_id, tribe_ids, tribe_ids))

        query = """
            SELECT
                mm.id,
                mm.movement_type,
                mm.player_id,
                mm.player_name,
                mm.prev_query_run_id,
                mm.curr_query_run_id,
                mm.prev_snapshot_at,
                mm.curr_snapshot_at,
                mm.prev_points,
                mm.curr_points,
                mm.prev_villages,
                mm.curr_villages,
                ft.clan_name AS from_clan_name,
                tt.clan_name AS to_clan_name,
                mm.from_tribe_id,
                mm.to_tribe_id
            FROM member_movements mm
            LEFT JOIN tribes ft ON ft.id = mm.from_tribe_id
            LEFT JOIN tribes tt ON tt.id = mm.to_tribe_id
            WHERE mm.curr_query_run_id = %s
            ORDER BY mm.curr_snapshot_at DESC, mm.movement_type, mm.player_name
        """
        return pd.read_sql(query, conn, params=(curr_run_id,))
    finally:
        conn.close()

# -----------------------------
# Analysis helpers
# -----------------------------
def normalize_player_key(df: pd.DataFrame) -> pd.Series:
    if "player_id" in df.columns:
        keys = df["player_id"].astype("Int64").astype(str)
        names = df["name"].fillna("").astype(str).str.strip().str.lower()
        return keys.where(keys != "<NA>", "name:" + names)
    return "name:" + df["name"].fillna("").astype(str).str.strip().str.lower()


def compare_runs(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    prev = prev_df.copy()
    curr = curr_df.copy()

    prev["player_key"] = normalize_player_key(prev)
    curr["player_key"] = normalize_player_key(curr)

    prev_keys = set(prev["player_key"])
    curr_keys = set(curr["player_key"])

    joined_keys = curr_keys - prev_keys
    left_keys = prev_keys - curr_keys
    stayed_keys = curr_keys & prev_keys

    joined = curr[curr["player_key"].isin(joined_keys)].copy()
    left = prev[prev["player_key"].isin(left_keys)].copy()

    prev_stayed = prev[prev["player_key"].isin(stayed_keys)].copy()
    curr_stayed = curr[curr["player_key"].isin(stayed_keys)].copy()

    compare_cols = [
        "player_key",
        "name",
        "player_id",
        "clan_name",
        "points",
        "villages",
        "tribe_rank",
        "player_url",
    ]

    prev_stayed = prev_stayed[compare_cols].rename(
        columns={
            "clan_name": "prev_clan_name",
            "points": "prev_points",
            "villages": "prev_villages",
            "tribe_rank": "prev_tribe_rank",
            "player_url": "prev_player_url",
        }
    )

    curr_stayed = curr_stayed[compare_cols].rename(
        columns={
            "clan_name": "curr_clan_name",
            "points": "curr_points",
            "villages": "curr_villages",
            "tribe_rank": "curr_tribe_rank",
            "player_url": "curr_player_url",
        }
    )

    stayed = curr_stayed.merge(prev_stayed, on=["player_key", "name", "player_id"], how="inner")
    stayed["point_diff"] = stayed["curr_points"].fillna(0) - stayed["prev_points"].fillna(0)
    stayed["village_diff"] = stayed["curr_villages"].fillna(0) - stayed["prev_villages"].fillna(0)
    stayed["moved_between_clans"] = stayed["curr_clan_name"] != stayed["prev_clan_name"]

    joined = joined.sort_values(["clan_name", "points", "name"], ascending=[True, False, True])
    left = left.sort_values(["clan_name", "points", "name"], ascending=[True, False, True])
    stayed = stayed.sort_values(["moved_between_clans", "point_diff", "curr_points"], ascending=[False, False, False])

    return joined, left, stayed


def summarize_changes(joined: pd.DataFrame, left: pd.DataFrame, stayed: pd.DataFrame) -> Dict[str, int]:
    moved = int(stayed["moved_between_clans"].sum()) if not stayed.empty else 0
    return {
        "katilan": len(joined),
        "ayrilan": len(left),
        "klani_degisen": moved,
        "kalan": len(stayed),
    }


def build_family_options(tribes_df: pd.DataFrame, relations_df: pd.DataFrame) -> Dict[str, List[int]]:
    families: Dict[str, List[int]] = {}

    if relations_df.empty:
        for _, row in tribes_df.sort_values("clan_name").iterrows():
            families[row["clan_name"]] = [int(row["id"])]
        return families

    child_ids = set(relations_df["child_tribe_id"].dropna().astype(int).tolist())
    parent_ids = set(relations_df["parent_tribe_id"].dropna().astype(int).tolist())

    root_tribes = tribes_df[
        tribes_df["id"].isin(parent_ids) | (~tribes_df["id"].isin(child_ids))
    ]

    for _, row in root_tribes.sort_values("clan_name").iterrows():
        tribe_id = int(row["id"])
        tribe_name = row["clan_name"]

        children = relations_df.loc[
            relations_df["parent_tribe_id"] == tribe_id,
            "child_tribe_id",
        ].dropna().astype(int).tolist()

        family_ids = [tribe_id] + [x for x in children if x != tribe_id]
        families[tribe_name] = family_ids

    return families


# -----------------------------
# UI
# -----------------------------
st.title("TribalWars Klan Uye Degisim Analizi")
st.caption("PostgreSQL veritabanindaki snapshot ve movement verilerinden katilan, ayrilan ve klan degistiren oyunculari analiz eder.")

runs_df = get_runs()
tribes_df = get_tribes()
relations_df = get_parent_map()
snapshots_df = get_snapshots()

if runs_df.empty:
    st.error("query_runs tablosunda veri bulunamadi.")
    st.stop()

if tribes_df.empty:
    st.error("tribes tablosunda veri bulunamadi.")
    st.stop()

family_options = build_family_options(tribes_df, relations_df)
mode = st.sidebar.radio(
    "Analiz modu",
    ["Tum klanlar", "Tek klan", "Klan ailesi"],
)

selected_tribe_ids: Optional[List[int]] = None
selected_label = "Tum klanlar"

if mode == "Tek klan":
    tribe_choice = st.sidebar.selectbox(
        "Klan sec",
        options=tribes_df["clan_name"].tolist(),
    )
    tribe_row = tribes_df[tribes_df["clan_name"] == tribe_choice].iloc[0]
    selected_tribe_ids = [int(tribe_row["id"])]
    selected_label = tribe_choice

elif mode == "Klan ailesi":
    if not family_options:
        st.sidebar.warning("Aktif alt klan iliskisi bulunamadi. Tek klan modunu kullan.")
    else:
        family_choice = st.sidebar.selectbox(
            "Aile sec",
            options=list(family_options.keys()),
        )
        selected_tribe_ids = family_options[family_choice]
        selected_label = family_choice

run_options = runs_df.copy()
run_options["label"] = run_options.apply(
    lambda r: f"#{int(r['id'])} - {pd.to_datetime(r['queried_at']).strftime('%Y-%m-%d %H:%M:%S')}",
    axis=1,
)

curr_run_label = st.sidebar.selectbox("Mevcut snapshot", options=run_options["label"].tolist(), index=0)
curr_run_id = int(run_options[run_options["label"] == curr_run_label].iloc[0]["id"])

older_runs = run_options[run_options["id"] != curr_run_id].copy()
if older_runs.empty:
    st.error("Karsilastirma yapabilmek icin en az iki query_run olmali.")
    st.stop()

prev_run_label = st.sidebar.selectbox("Onceki snapshot", options=older_runs["label"].tolist(), index=0)
prev_run_id = int(older_runs[older_runs["label"] == prev_run_label].iloc[0]["id"])

if prev_run_id == curr_run_id:
    st.error("Mevcut ve onceki snapshot ayni olamaz.")
    st.stop()

curr_members = get_members_for_run(curr_run_id, selected_tribe_ids)
prev_members = get_members_for_run(prev_run_id, selected_tribe_ids)

if curr_members.empty:
    st.warning("Secilen filtre icin mevcut snapshotta uye verisi yok.")
    st.stop()
if prev_members.empty:
    st.warning("Secilen filtre icin onceki snapshotta uye verisi yok.")
    st.stop()

joined_df, left_df, stayed_df = compare_runs(prev_members, curr_members)
summary = summarize_changes(joined_df, left_df, stayed_df)

# movement verisi
try:
    movements_df = get_member_movements(curr_run_id, selected_tribe_ids)
except Exception:
    movements_df = pd.DataFrame()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Katilan", summary["katilan"])
col2.metric("Ayrilan", summary["ayrilan"])
col3.metric("Klan Degistiren", summary["klani_degisen"])
col4.metric("Ayni Kalan", summary["kalan"])

st.markdown(f"### Kapsam: {selected_label}")
st.write(f"Karsilastirma: **{prev_run_label}** -> **{curr_run_label}**")

with st.expander("Aktif alt klan iliskileri", expanded=False):
    if relations_df.empty:
        st.info("Aktif alt klan iliskisi bulunmuyor.")
    else:
        st.dataframe(relations_df, use_container_width=True)

st.markdown("### Ozet")
summary_rows = []
for clan_name in sorted(set(curr_members["clan_name"].dropna().tolist()) | set(prev_members["clan_name"].dropna().tolist())):
    clan_joined = int((joined_df["clan_name"] == clan_name).sum()) if not joined_df.empty else 0
    clan_left = int((left_df["clan_name"] == clan_name).sum()) if not left_df.empty else 0
    clan_moved_in = int(
        (stayed_df["curr_clan_name"] == clan_name).sum() - (stayed_df["prev_clan_name"] == clan_name).sum()
    ) if not stayed_df.empty else 0
    summary_rows.append(
        {
            "klan": clan_name,
            "katilan": clan_joined,
            "ayrilan": clan_left,
            "net_hareket": clan_joined - clan_left,
            "ic_transfer_etkisi": clan_moved_in,
        }
    )

summary_df = pd.DataFrame(summary_rows).sort_values(["net_hareket", "katilan"], ascending=[False, False])
st.dataframe(summary_df, use_container_width=True)

st.markdown("### Katilan Oyuncular")
joined_movements = (
    movements_df[movements_df["movement_type"] == "joined"].copy()
    if not movements_df.empty and "movement_type" in movements_df.columns
    else pd.DataFrame()
)

if not joined_movements.empty:
    st.dataframe(
        joined_movements[
            [
                "player_name",
                "player_id",
                "to_clan_name",
                "prev_query_run_id",
                "curr_query_run_id",
                "prev_snapshot_at",
                "curr_snapshot_at",
                "curr_points",
                "curr_villages",
            ]
        ].rename(
            columns={
                "player_name": "oyuncu",
                "to_clan_name": "katildigi_klan",
                "prev_query_run_id": "onceki_query_id",
                "curr_query_run_id": "simdiki_query_id",
                "prev_snapshot_at": "onceki_tarih",
                "curr_snapshot_at": "simdiki_tarih",
                "curr_points": "puan",
                "curr_villages": "koy",
            }
        ),
        use_container_width=True,
    )
elif joined_df.empty:
    st.info("Yeni katilan oyuncu yok.")
else:
    st.dataframe(
        joined_df[
            ["clan_name", "name", "player_id", "points", "villages", "tribe_rank", "player_url"]
        ].rename(
            columns={
                "clan_name": "katildigi_klan",
                "name": "oyuncu",
                "points": "puan",
                "villages": "koy",
            }
        ),
        use_container_width=True,
    )

st.markdown("### Ayrilan Oyuncular")
left_movements = (
    movements_df[movements_df["movement_type"] == "left"].copy()
    if not movements_df.empty and "movement_type" in movements_df.columns
    else pd.DataFrame()
)

if not left_movements.empty:
    st.dataframe(
        left_movements[
            [
                "player_name",
                "player_id",
                "from_clan_name",
                "prev_query_run_id",
                "curr_query_run_id",
                "prev_snapshot_at",
                "curr_snapshot_at",
                "prev_points",
                "prev_villages",
            ]
        ].rename(
            columns={
                "player_name": "oyuncu",
                "from_clan_name": "ayrildigi_klan",
                "prev_query_run_id": "onceki_query_id",
                "curr_query_run_id": "simdiki_query_id",
                "prev_snapshot_at": "onceki_tarih",
                "curr_snapshot_at": "simdiki_tarih",
                "prev_points": "puan",
                "prev_villages": "koy",
            }
        ),
        use_container_width=True,
    )
elif left_df.empty:
    st.info("Ayrilan oyuncu yok.")
else:
    st.dataframe(
        left_df[
            ["clan_name", "name", "player_id", "points", "villages", "tribe_rank", "player_url"]
        ].rename(
            columns={
                "clan_name": "ayrildigi_klan",
                "name": "oyuncu",
                "points": "puan",
                "villages": "koy",
            }
        ),
        use_container_width=True,
    )

st.markdown("### Klan Degistiren Oyuncular")
moved_movements = (
    movements_df[movements_df["movement_type"] == "moved"].copy()
    if not movements_df.empty and "movement_type" in movements_df.columns
    else pd.DataFrame()
)

if not moved_movements.empty:
    st.dataframe(
        moved_movements[
            [
                "player_name",
                "player_id",
                "from_clan_name",
                "to_clan_name",
                "prev_query_run_id",
                "curr_query_run_id",
                "prev_snapshot_at",
                "curr_snapshot_at",
                "prev_points",
                "curr_points",
                "prev_villages",
                "curr_villages",
            ]
        ].rename(
            columns={
                "player_name": "oyuncu",
                "from_clan_name": "onceki_klan",
                "to_clan_name": "yeni_klan",
                "prev_query_run_id": "onceki_query_id",
                "curr_query_run_id": "simdiki_query_id",
                "prev_snapshot_at": "onceki_tarih",
                "curr_snapshot_at": "simdiki_tarih",
                "prev_points": "onceki_puan",
                "curr_points": "simdiki_puan",
                "prev_villages": "onceki_koy",
                "curr_villages": "simdiki_koy",
            }
        ),
        use_container_width=True,
    )
else:
    moved_df = stayed_df[stayed_df["moved_between_clans"]].copy() if not stayed_df.empty else pd.DataFrame()
    if moved_df.empty:
        st.info("Secilen aralikta klan degistiren oyuncu yok.")
    else:
        st.dataframe(
            moved_df[
                [
                    "name",
                    "player_id",
                    "prev_clan_name",
                    "curr_clan_name",
                    "prev_points",
                    "curr_points",
                    "point_diff",
                    "prev_villages",
                    "curr_villages",
                    "village_diff",
                ]
            ].rename(
                columns={
                    "name": "oyuncu",
                    "prev_clan_name": "onceki_klan",
                    "curr_clan_name": "yeni_klan",
                    "prev_points": "onceki_puan",
                    "curr_points": "simdiki_puan",
                    "prev_villages": "onceki_koy",
                    "curr_villages": "simdiki_koy",
                }
            ),
            use_container_width=True,
        )

st.markdown("### Kalan Oyuncular (puan degisimi)")
if stayed_df.empty:
    st.info("Ortak oyuncu yok.")
else:
    st.dataframe(
        stayed_df[
            [
                "name",
                "player_id",
                "prev_clan_name",
                "curr_clan_name",
                "prev_points",
                "curr_points",
                "point_diff",
                "prev_villages",
                "curr_villages",
                "village_diff",
                "moved_between_clans",
            ]
        ].rename(
            columns={
                "name": "oyuncu",
                "prev_clan_name": "onceki_klan",
                "curr_clan_name": "simdiki_klan",
                "prev_points": "onceki_puan",
                "curr_points": "simdiki_puan",
                "prev_villages": "onceki_koy",
                "curr_villages": "simdiki_koy",
            }
        ),
        use_container_width=True,
    )

st.markdown("### Movement Kayitlari")
if movements_df.empty:
    st.info("member_movements tablosunda bu query icin movement kaydi bulunmuyor.")
else:
    st.dataframe(movements_df, use_container_width=True)

st.markdown("### Ham veri kontrolleri")
col_a, col_b = st.columns(2)
with col_a:
    st.caption("Mevcut snapshot uye sayisi")
    st.dataframe(
        curr_members.groupby("clan_name", as_index=False).size().rename(columns={"size": "uye_sayisi"}),
        use_container_width=True,
    )
with col_b:
    st.caption("Onceki snapshot uye sayisi")
    st.dataframe(
        prev_members.groupby("clan_name", as_index=False).size().rename(columns={"size": "uye_sayisi"}),
        use_container_width=True,
    )

st.sidebar.markdown("---")
st.sidebar.caption("Calistirma")
st.sidebar.code("python -m streamlit run web.py")
