import pandas as pd
import numpy as np

# ============================
# 1. LOAD FILE
# ============================
file = "Ops Analyst Takehome Test.xlsx"

# ganti "Problem A" jika ingin sheet lain
df = pd.read_excel(file, sheet_name="Problem A")

# Normalisasi kolom nama
df.columns = df.columns.str.strip().str.lower()

# Rename jika beda
df = df.rename(columns={
    "agent name": "agent",
    "driver id": "driver_id",
    "timestamp": "timestamp"
})

# Pastikan timestamp dalam format datetime
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
# Convert timezone-aware to timezone-naive for Excel compatibility
if df["timestamp"].dt.tz is not None:
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)


# ============================
# 2. CARI ANOMALI — INPUT TERLALU CEPAT
# ============================
# Sort berdasarkan timestamp
df = df.sort_values("timestamp")

# Hitung selisih antar input (per agent)
df["time_diff"] = df.groupby("agent")["timestamp"].diff().dt.total_seconds()

# Anomali jika selisih < 1 detik
anomaly_fast = df[df["time_diff"] < 1]


# ============================
# 3. ANOMALI — MULTIPLE INPUT SAME SECOND
# ============================
df["timestamp_second"] = df["timestamp"].dt.floor("s")

count_per_second = df.groupby(["agent", "timestamp_second"]).size().reset_index(name="count")

anomaly_same_second = count_per_second[count_per_second["count"] > 1]


# ============================
# 4. ANOMALI — SPIKE VOLUME PER AGENT
# ============================
# Hitung jumlah input per hari
df["date"] = df["timestamp"].dt.date

count_per_day = df.groupby(["agent", "date"]).size().reset_index(name="daily_count")

# Ambil threshold: lebih dari 3 * rata-rata per agent
threshold = count_per_day.groupby("agent")["daily_count"].mean() * 3
threshold = threshold.reset_index()
threshold.columns = ["agent", "threshold"]

anomaly_spike = count_per_day.merge(threshold, on="agent")
anomaly_spike = anomaly_spike[anomaly_spike["daily_count"] > anomaly_spike["threshold"]]


# ============================
# 5. ANOMALI — TIMESTAMP DUPLICATE EXACT
# ============================
duplicate_timestamp = df[df.duplicated(subset=["agent", "timestamp"], keep=False)]


# ============================
# 6. SIMPAN HASIL
# ============================
with pd.ExcelWriter("anomaly_results.xlsx") as writer:
    anomaly_fast.to_excel(writer, sheet_name="Fast Input (<1s)", index=False)
    anomaly_same_second.to_excel(writer, sheet_name="Multiple Same Second", index=False)
    anomaly_spike.to_excel(writer, sheet_name="Spike Volume", index=False)
    duplicate_timestamp.to_excel(writer, sheet_name="Exact Duplicate Ts", index=False)
    df.to_excel(writer, sheet_name="Cleaned Data", index=False)

print("DONE! File anomaly_results.xlsx berhasil dibuat.")
