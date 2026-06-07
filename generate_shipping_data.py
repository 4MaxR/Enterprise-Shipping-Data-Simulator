#!/usr/bin/env python3
"""
Synthetic ocean freight data generator.

The generator creates realistic, relational shipping and logistics datasets for
data lake, warehouse, ETL, ML, and analytics testing workloads.

Record count means shipment count. Container and tracking-event counts are
derived from shipments because real shipments commonly contain multiple
containers and many lifecycle events.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import math
import os
import random
import string
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from faker import Faker
except ImportError:  # Allows CSV smoke tests in minimal runtimes.
    Faker = None  # type: ignore[assignment]


LOGGER = logging.getLogger("shipping-data-generator")

CONTAINER_EVENTS = [
    "Booking Confirmed",
    "Container Released",
    "Gate In",
    "Customs Export Clearance",
    "Loaded on Vessel",
    "Vessel Departure",
    "Transshipment Arrival",
    "Transshipment Departure",
    "Vessel Arrival",
    "Discharged",
    "Customs Import Clearance",
    "Gate Out",
    "Delivered",
]

CUSTOMS_STATUSES = ["Cleared", "Pending", "Inspection Required", "Held", "Rejected"]
EVENT_STATUSES = ["Completed", "Estimated", "Delayed", "Exception"]
FREIGHT_TERMS = ["Prepaid", "Collect"]
CUSTOMER_TYPES = ["Shipper", "Consignee", "Freight Forwarder", "NVOCC"]
CURRENCIES = ["USD", "EUR", "CNY", "SGD", "AED", "INR", "BRL", "ZAR"]
PACKAGE_TYPES = ["Cartons", "Pallets", "Crates", "Drums", "Bales", "Bags", "Cases"]
SHIPMENT_STATUSES = [
    "Booked",
    "In Transit",
    "Delayed",
    "Arrived",
    "Customs Hold",
    "Delivered",
    "Cancelled",
]
CONTAINER_STATUSES = ["Empty Released", "Gate In", "Loaded", "In Transit", "Discharged", "Gate Out", "Delivered"]

COMMODITIES = [
    ("Electronics", "854239", "Integrated circuits and electronic components"),
    ("Furniture", "940360", "Wooden furniture for commercial and household use"),
    ("Textiles", "520839", "Woven cotton fabrics and finished textile goods"),
    ("Automotive Parts", "870899", "Parts and accessories for motor vehicles"),
    ("Machinery", "847989", "Industrial machinery and mechanical appliances"),
    ("Pharmaceuticals", "300490", "Packaged pharmaceutical products"),
    ("Frozen Seafood", "030617", "Frozen shrimp and seafood products"),
    ("Coffee", "090111", "Green coffee beans"),
    ("Cocoa", "180100", "Cocoa beans and cocoa preparations"),
    ("Copper Cathodes", "740311", "Refined copper cathodes and sections"),
    ("Apparel", "620342", "Men's cotton garments and apparel"),
    ("Solar Panels", "854143", "Photovoltaic cells assembled in modules"),
    ("Chemicals", "382499", "Prepared chemical products for industrial use"),
    ("Wine", "220421", "Bottled wine and beverages"),
    ("Rubber", "400122", "Technically specified natural rubber"),
]

CONTAINER_SPECS = {
    "20GP": {"size": 20, "tare": (2100, 2400), "cargo": (7000, 21800)},
    "40GP": {"size": 40, "tare": (3600, 3900), "cargo": (9000, 26500)},
    "40HC": {"size": 40, "tare": (3800, 4200), "cargo": (9000, 26500)},
    "Reefer": {"size": 40, "tare": (4200, 4800), "cargo": (7000, 24000)},
    "Open Top": {"size": 40, "tare": (3800, 4400), "cargo": (8000, 26000)},
    "Flat Rack": {"size": 40, "tare": (4800, 5600), "cargo": (10000, 32000)},
}

PORTS = [
    ("CNSHA", "Shanghai", "China", "East Asia", 31.2304, 121.4737),
    ("CNNGB", "Ningbo-Zhoushan", "China", "East Asia", 29.8683, 121.5440),
    ("CNSZX", "Shenzhen", "China", "East Asia", 22.5431, 114.0579),
    ("CNQIN", "Qingdao", "China", "East Asia", 36.0671, 120.3826),
    ("SGSIN", "Singapore", "Singapore", "Southeast Asia", 1.3521, 103.8198),
    ("AEJEA", "Jebel Ali", "United Arab Emirates", "Middle East", 25.0118, 55.0612),
    ("AEDXB", "Dubai", "United Arab Emirates", "Middle East", 25.2048, 55.2708),
    ("INNSA", "Nhava Sheva", "India", "South Asia", 18.9490, 72.9512),
    ("INMUN", "Mundra", "India", "South Asia", 22.8395, 69.7213),
    ("NLRTM", "Rotterdam", "Netherlands", "Europe", 51.9244, 4.4777),
    ("DEHAM", "Hamburg", "Germany", "Europe", 53.5511, 9.9937),
    ("BEANR", "Antwerp-Bruges", "Belgium", "Europe", 51.2194, 4.4025),
    ("ESALG", "Algeciras", "Spain", "Europe", 36.1408, -5.4562),
    ("USLAX", "Los Angeles", "United States", "North America", 33.7405, -118.2775),
    ("USLGB", "Long Beach", "United States", "North America", 33.7701, -118.1937),
    ("USNYC", "New York/New Jersey", "United States", "North America", 40.7128, -74.0060),
    ("USSAV", "Savannah", "United States", "North America", 32.0809, -81.0912),
    ("USCHS", "Charleston", "United States", "North America", 32.7765, -79.9311),
    ("BRSSZ", "Santos", "Brazil", "Latin America", -23.9608, -46.3336),
    ("MXZLO", "Manzanillo", "Mexico", "Latin America", 19.1138, -104.3385),
    ("CLSAI", "San Antonio", "Chile", "Latin America", -33.5922, -71.6217),
    ("COCTG", "Cartagena", "Colombia", "Latin America", 10.3910, -75.4794),
    ("ZADUR", "Durban", "South Africa", "Africa", -29.8587, 31.0218),
    ("EGPSD", "Port Said", "Egypt", "Africa", 31.2653, 32.3019),
    ("MACAS", "Casablanca", "Morocco", "Africa", 33.5731, -7.5898),
    ("NGLOS", "Lagos", "Nigeria", "Africa", 6.5244, 3.3792),
]

TRADE_LANES = [
    ("CNSHA", "SGSIN", 6, 0.10, ["SGSIN"]),
    ("CNSHA", "NLRTM", 32, 0.11, ["SGSIN", "EGPSD"]),
    ("CNNGB", "USLAX", 17, 0.11, []),
    ("CNSZX", "DEHAM", 34, 0.09, ["SGSIN", "EGPSD"]),
    ("SGSIN", "AEJEA", 11, 0.07, []),
    ("INNSA", "NLRTM", 25, 0.08, ["AEJEA", "EGPSD"]),
    ("INMUN", "AEJEA", 5, 0.04, []),
    ("AEJEA", "AEDXB", 1, 0.02, []),
    ("USNYC", "DEHAM", 12, 0.06, []),
    ("USSAV", "NLRTM", 14, 0.05, []),
    ("BRSSZ", "NLRTM", 18, 0.06, []),
    ("CLSAI", "USLAX", 19, 0.04, ["MXZLO"]),
    ("MXZLO", "USLAX", 5, 0.04, []),
    ("ZADUR", "SGSIN", 18, 0.05, []),
    ("EGPSD", "NLRTM", 8, 0.04, []),
    ("MACAS", "ESALG", 3, 0.03, []),
    ("NGLOS", "AEJEA", 18, 0.04, ["EGPSD"]),
]

CARRIERS = [
    "Maersk",
    "MSC",
    "CMA CGM",
    "COSCO Shipping",
    "Hapag-Lloyd",
    "Ocean Network Express",
    "Evergreen",
    "HMM",
    "Yang Ming",
    "ZIM",
]

INDUSTRIES = [
    "Retail",
    "Automotive",
    "Electronics",
    "Pharmaceuticals",
    "Agriculture",
    "Energy",
    "Chemicals",
    "Manufacturing",
    "Food and Beverage",
    "Construction",
]

VESSEL_PREFIXES = [
    "Ocean",
    "Pacific",
    "Atlantic",
    "Ever",
    "Global",
    "Cosco",
    "Maersk",
    "CMA",
    "Hansa",
    "Nile",
]
VESSEL_SUFFIXES = ["Star", "Bridge", "Voyager", "Trader", "Horizon", "Express", "Majesty", "Pioneer"]


@dataclass(frozen=True)
class GeneratorConfig:
    output_dir: Path
    record_count: int
    chunk_size: int
    workers: int
    seed: int
    output_format: str
    compression: str
    customer_count: int
    vessel_count: int
    start_date: date
    end_date: date
    anomaly_rate: float


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(
        description="Generate synthetic ocean freight, container, B/L, shipment, and tracking-event data."
    )
    parser.add_argument("--output-dir", default="output", type=Path)
    parser.add_argument("--records", type=int, default=100_000, help="Number of shipment records to generate.")
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Shipments per chunk/part file.")
    parser.add_argument("--workers", type=int, default=max(os.cpu_count() or 1, 1))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--compression", default="snappy", help="Parquet compression codec.")
    parser.add_argument("--customers", type=int, default=50_000)
    parser.add_argument("--vessels", type=int, default=700)
    parser.add_argument("--start-date", default="2022-01-01")
    parser.add_argument("--end-date", default="2026-12-31")
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    args = parser.parse_args()

    if args.records < 1:
        raise SystemExit("--records must be at least 1")
    if args.chunk_size < 1:
        raise SystemExit("--chunk-size must be at least 1")
    if not 0 <= args.anomaly_rate <= 0.25:
        raise SystemExit("--anomaly-rate must be between 0 and 0.25")

    return GeneratorConfig(
        output_dir=args.output_dir,
        record_count=args.records,
        chunk_size=args.chunk_size,
        workers=max(args.workers, 1),
        seed=args.seed,
        output_format=args.format,
        compression=args.compression,
        customer_count=args.customers,
        vessel_count=args.vessels,
        start_date=datetime.strptime(args.start_date, "%Y-%m-%d").date(),
        end_date=datetime.strptime(args.end_date, "%Y-%m-%d").date(),
        anomaly_rate=args.anomaly_rate,
    )


def make_ports() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "port_id": f"PORT-{locode}",
                "port_name": name,
                "un_locode": locode,
                "country": country,
                "region": region,
                "latitude": lat,
                "longitude": lon,
            }
            for locode, name, country, region, lat, lon in PORTS
        ]
    )


def make_vessels(count: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 10)
    rows = []
    used_imo: set[str] = set()
    for i in range(count):
        imo = _unique_number("9", 7, used_imo, rng)
        teu = int(rng.choice([1800, 2800, 5100, 8500, 12000, 14500, 18000, 23000], p=[0.10, 0.12, 0.15, 0.20, 0.18, 0.12, 0.08, 0.05]))
        vessel_type = "Ultra Large Container Vessel" if teu >= 18000 else "Container Ship"
        rows.append(
            {
                "vessel_id": f"VES-{i + 1:06d}",
                "vessel_name": f"{rng.choice(VESSEL_PREFIXES)} {rng.choice(VESSEL_SUFFIXES)} {i + 1:03d}",
                "imo_number": f"IMO{imo}",
                "carrier": rng.choice(CARRIERS),
                "vessel_type": vessel_type,
                "teu_capacity": teu,
                "build_year": int(rng.integers(1998, 2025)),
                "flag_country": rng.choice(
                    ["Panama", "Liberia", "Marshall Islands", "Singapore", "Hong Kong", "Malta", "Denmark"]
                ),
            }
        )
    return pd.DataFrame(rows)


def make_customers(count: int, seed: int) -> pd.DataFrame:
    fake = Faker() if Faker is not None else None
    if Faker is not None:
        Faker.seed(seed + 20)
    rng = np.random.default_rng(seed + 21)
    country_weights = [
        ("China", 0.16),
        ("United States", 0.14),
        ("Germany", 0.08),
        ("Singapore", 0.07),
        ("India", 0.07),
        ("United Arab Emirates", 0.06),
        ("Netherlands", 0.06),
        ("Brazil", 0.05),
        ("South Africa", 0.04),
        ("Mexico", 0.04),
        ("Chile", 0.03),
        ("Nigeria", 0.03),
        ("Egypt", 0.03),
        ("Morocco", 0.02),
        ("Belgium", 0.02),
        ("Spain", 0.02),
        ("United Kingdom", 0.04),
        ("France", 0.04),
    ]
    countries, weights = zip(*country_weights)
    weights = np.array(weights) / np.sum(weights)

    rows = []
    for i in range(count):
        customer_type = rng.choice(CUSTOMER_TYPES, p=[0.40, 0.38, 0.14, 0.08])
        rows.append(
            {
                "customer_id": f"CUS-{i + 1:08d}",
                "company_name": fake.company() if fake is not None else fallback_company_name(i, rng),
                "customer_type": customer_type,
                "country": rng.choice(countries, p=weights),
                "industry": rng.choice(INDUSTRIES),
            }
        )
    return pd.DataFrame(rows)


def fallback_company_name(index: int, rng: np.random.Generator) -> str:
    prefixes = ["Bluewater", "Global", "Harbor", "Summit", "Pioneer", "Atlas", "Meridian", "Noble"]
    cores = ["Trading", "Exports", "Logistics", "Supply", "Industrial", "Foods", "Chemicals", "Retail"]
    suffixes = ["Ltd", "Inc", "GmbH", "Pte Ltd", "LLC", "SA", "BV"]
    return f"{rng.choice(prefixes)} {rng.choice(cores)} {index + 1:06d} {rng.choice(suffixes)}"


def _unique_number(prefix: str, digits: int, used: set[str], rng: np.random.Generator) -> str:
    while True:
        value = prefix + "".join(str(x) for x in rng.integers(0, 10, size=digits - len(prefix)))
        if value not in used:
            used.add(value)
            return value


def write_dimension_tables(config: GeneratorConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    ports = make_ports()
    vessels = make_vessels(config.vessel_count, config.seed)
    customers = make_customers(config.customer_count, config.seed)

    write_table(ports, config.output_dir / f"ports.{config.output_format}", config)
    write_table(vessels, config.output_dir / f"vessels.{config.output_format}", config)
    write_table(customers, config.output_dir / f"customers.{config.output_format}", config)
    return ports, vessels, customers


def write_table(df: pd.DataFrame, path: Path, config: GeneratorConfig) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if config.output_format == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError(
                "Parquet output requires pyarrow. Install dependencies with "
                "`py -m pip install -r requirements.txt` or use `--format csv`."
            ) from exc
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path, compression=config.compression)
    else:
        df.to_csv(path, index=False)


def month_demand_multiplier(day: date) -> float:
    peak_months = {8: 1.18, 9: 1.30, 10: 1.25, 11: 1.16, 12: 1.12}
    holiday_months = {1: 0.94, 2: 0.88, 7: 1.08}
    return peak_months.get(day.month, holiday_months.get(day.month, 1.0))


def weighted_departure_date(rng: np.random.Generator, start: date, end: date) -> date:
    span = (end - start).days
    if span <= 0:
        return start
    # Draw several candidates and keep one weighted by seasonal demand.
    candidates = [start + timedelta(days=int(rng.integers(0, span + 1))) for _ in range(4)]
    weights = np.array([month_demand_multiplier(candidate) for candidate in candidates])
    weights = weights / weights.sum()
    return candidates[int(rng.choice(np.arange(len(candidates)), p=weights))]


def route_delay_days(rng: np.random.Generator, base_days: int, congestion: float, anomalous: bool) -> int:
    weather_delay = int(rng.choice([0, 0, 0, 1, 2, 3, 5], p=[0.54, 0.16, 0.10, 0.08, 0.06, 0.04, 0.02]))
    congestion_delay = int(rng.poisson(max(congestion * 12, 0.1)))
    schedule_change = int(rng.choice([0, 0, 1, 2, 4, 7], p=[0.70, 0.10, 0.08, 0.05, 0.04, 0.03]))
    anomaly_delay = int(rng.integers(5, 25)) if anomalous and rng.random() < 0.35 else 0
    return weather_delay + congestion_delay + schedule_change + anomaly_delay


def check_digit_for_container(prefix_and_serial: str) -> str:
    alphabet = "0123456789A?BCDEFGHIJK?LMNOPQRSTU?VWXYZ"
    values = {char: index for index, char in enumerate(alphabet) if char != "?"}
    total = 0
    for position, char in enumerate(prefix_and_serial):
        total += values[char] * (2**position)
    check = total % 11
    check = 0 if check == 10 else check
    return str(check)


def make_container_number(rng: np.random.Generator, index: int) -> str:
    owner = "".join(rng.choice(list(string.ascii_uppercase), size=3))
    category = "U"
    serial_seed = hashlib.sha1(f"{index}-{rng.integers(0, 10_000_000)}".encode()).hexdigest()
    serial = f"{int(serial_seed[:8], 16) % 1_000_000:06d}"
    base = f"{owner}{category}{serial}"
    return f"{base}{check_digit_for_container(base)}"


def bl_number(carrier: str, index: int, rng: np.random.Generator, duplicate_pool: list[str], anomalous: bool) -> str:
    carrier_code = "".join(word[0] for word in carrier.split())[:3].upper().ljust(3, "X")
    if anomalous and duplicate_pool and rng.random() < 0.12:
        return rng.choice(duplicate_pool)
    number = f"{carrier_code}{index:011d}"
    duplicate_pool.append(number)
    if len(duplicate_pool) > 10_000:
        duplicate_pool.pop(0)
    return number


def seal_number(rng: np.random.Generator, anomalous: bool) -> str | None:
    if anomalous and rng.random() < 0.35:
        return None
    return "SL" + "".join(rng.choice(list(string.ascii_uppercase + string.digits), size=10))


def booking_number(rng: np.random.Generator, index: int) -> str:
    return f"BKG{index:010d}{rng.choice(list(string.ascii_uppercase))}"


def voyage_number(rng: np.random.Generator, departure: date) -> str:
    return f"{departure.strftime('%y%W')}{rng.choice(list(string.ascii_uppercase))}{int(rng.integers(1, 99)):02d}"


def make_part_file_name(part_index: int, output_format: str) -> str:
    return f"part-{part_index:04d}.{output_format}"


def generate_chunk(
    part_index: int,
    start_index: int,
    count: int,
    config: GeneratorConfig,
    ports_records: list[dict[str, Any]],
    vessels_records: list[dict[str, Any]],
    customers_records: list[dict[str, Any]],
) -> dict[str, int]:
    rng = np.random.default_rng(config.seed + (part_index * 10_003))
    random.seed(config.seed + part_index)
    duplicate_bl_pool: list[str] = []

    port_by_locode = {row["un_locode"]: row for row in ports_records}
    vessel_ids = np.array([row["vessel_id"] for row in vessels_records])
    vessel_carriers = {row["vessel_id"]: row["carrier"] for row in vessels_records}
    customer_ids = np.array([row["customer_id"] for row in customers_records])

    lane_weights = np.array([lane[3] for lane in TRADE_LANES], dtype=float)
    lane_weights = lane_weights / lane_weights.sum()

    containers: list[dict[str, Any]] = []
    bills: list[dict[str, Any]] = []
    shipments: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    for local_offset in range(count):
        sequence = start_index + local_offset + 1
        anomalous = rng.random() < config.anomaly_rate
        lane = TRADE_LANES[int(rng.choice(np.arange(len(TRADE_LANES)), p=lane_weights))]
        origin, destination, base_transit, congestion_rate, transshipment_options = lane
        departure = weighted_departure_date(rng, config.start_date, config.end_date)
        vessel_id = str(rng.choice(vessel_ids))
        carrier = vessel_carriers[vessel_id]
        delay = route_delay_days(rng, base_transit, congestion_rate, anomalous)
        planned_transit = int(max(1, rng.normal(base_transit, max(base_transit * 0.12, 1))))
        eta = departure + timedelta(days=planned_transit)
        actual_arrival = eta + timedelta(days=delay)

        if anomalous and rng.random() < 0.16:
            eta_value: date | None = None
        else:
            eta_value = eta

        transshipment = None
        if transshipment_options and rng.random() < 0.72:
            transshipment = str(rng.choice(transshipment_options))

        shipper_id = str(rng.choice(customer_ids))
        consignee_id = str(rng.choice(customer_ids))
        while consignee_id == shipper_id:
            consignee_id = str(rng.choice(customer_ids))

        bl = bl_number(carrier, sequence, rng, duplicate_bl_pool, anomalous)
        master_bl = bl if rng.random() < 0.70 else f"MBL{sequence:011d}"
        house_bl = None if master_bl == bl else f"HBL{sequence:011d}"
        commodity, hs_code, cargo_description = COMMODITIES[int(rng.integers(0, len(COMMODITIES)))]
        package_count = int(max(1, rng.lognormal(mean=5.4, sigma=0.75)))
        declared_value = round(float(max(500, rng.lognormal(mean=10.2, sigma=1.0))), 2)
        container_count = int(rng.choice([1, 1, 1, 2, 2, 3, 4, 5], p=[0.28, 0.20, 0.12, 0.16, 0.10, 0.07, 0.05, 0.02]))
        shipment_status = status_from_dates(departure, actual_arrival, delay, anomalous, rng)
        customs_risk_score = risk_score(rng, commodity, delay, anomalous)

        bills.append(
            {
                "bl_number": bl,
                "master_bl_number": master_bl,
                "house_bl_number": house_bl,
                "booking_number": booking_number(rng, sequence),
                "issue_date": departure - timedelta(days=int(rng.integers(2, 25))),
                "carrier": carrier,
                "shipper_id": shipper_id,
                "consignee_id": consignee_id,
                "freight_terms": rng.choice(FREIGHT_TERMS, p=[0.64, 0.36]),
            }
        )

        shipments.append(
            {
                "shipment_id": f"SHP-{sequence:012d}",
                "bl_number": bl,
                "vessel_id": vessel_id,
                "voyage_number": voyage_number(rng, departure),
                "origin_port": origin,
                "destination_port": destination,
                "transshipment_port": transshipment,
                "departure_date": departure,
                "eta": eta_value,
                "actual_arrival_date": actual_arrival,
                "shipment_status": shipment_status,
                "commodity": commodity,
                "hs_code": hs_code,
                "cargo_description": cargo_description,
                "package_count": package_count,
                "package_type": rng.choice(PACKAGE_TYPES),
                "declared_value": declared_value,
                "currency": rng.choice(CURRENCIES, p=[0.56, 0.19, 0.08, 0.04, 0.04, 0.03, 0.03, 0.03]),
                "planned_transit_days": planned_transit,
                "actual_transit_days": (actual_arrival - departure).days,
                "delay_days": delay,
                "port_congestion_score": round(min(1.0, congestion_rate * 3.5 + rng.random() * 0.25), 3),
                "weather_delay_days": max(0, min(delay, int(rng.poisson(1.2)))),
                "customs_risk_score": customs_risk_score,
                "peak_season_indicator": month_demand_multiplier(departure) > 1.1,
                "anomaly_flag": anomalous,
            }
        )

        container_numbers = []
        for container_offset in range(container_count):
            container_index = sequence * 10 + container_offset
            container_number = make_container_number(rng, container_index)
            container_numbers.append(container_number)
            container_type = str(
                rng.choice(
                    list(CONTAINER_SPECS.keys()),
                    p=[0.26, 0.26, 0.26, 0.12, 0.05, 0.05],
                )
            )
            spec = CONTAINER_SPECS[container_type]
            tare = int(rng.integers(*spec["tare"]))
            cargo_weight = int(rng.integers(*spec["cargo"]))
            gross_weight = tare + cargo_weight
            if anomalous and rng.random() < 0.15:
                gross_weight = int(rng.choice([-1, tare - 100, cargo_weight]))

            containers.append(
                {
                    "container_number": container_number,
                    "shipment_id": f"SHP-{sequence:012d}",
                    "bl_number": bl,
                    "container_type": container_type,
                    "container_size": spec["size"],
                    "tare_weight_kg": tare,
                    "cargo_weight_kg": cargo_weight,
                    "gross_weight_kg": gross_weight,
                    "seal_number": seal_number(rng, anomalous),
                    "hazardous_indicator": bool(rng.random() < hazardous_probability(commodity)),
                    "container_status": container_status_from_shipment(shipment_status),
                    "utilization_ratio": round(min(cargo_weight / max_container_payload(container_type), 1.4), 3),
                }
            )

        events.extend(
            make_tracking_events(
                sequence=sequence,
                container_numbers=container_numbers,
                origin=origin,
                destination=destination,
                transshipment=transshipment,
                departure=departure,
                eta=eta,
                actual_arrival=actual_arrival,
                anomalous=anomalous,
                rng=rng,
                port_by_locode=port_by_locode,
            )
        )

    write_table(pd.DataFrame(containers), config.output_dir / "containers" / make_part_file_name(part_index, config.output_format), config)
    write_table(pd.DataFrame(bills), config.output_dir / "bills_of_lading" / make_part_file_name(part_index, config.output_format), config)
    write_table(pd.DataFrame(shipments), config.output_dir / "shipments" / make_part_file_name(part_index, config.output_format), config)
    write_table(pd.DataFrame(events), config.output_dir / "tracking_events" / make_part_file_name(part_index, config.output_format), config)

    return {
        "part": part_index,
        "shipments": len(shipments),
        "bills_of_lading": len(bills),
        "containers": len(containers),
        "tracking_events": len(events),
    }


def hazardous_probability(commodity: str) -> float:
    if commodity in {"Chemicals", "Pharmaceuticals"}:
        return 0.18
    if commodity in {"Machinery", "Automotive Parts"}:
        return 0.05
    return 0.015


def max_container_payload(container_type: str) -> int:
    return {
        "20GP": 21800,
        "40GP": 26500,
        "40HC": 26500,
        "Reefer": 24000,
        "Open Top": 26000,
        "Flat Rack": 32000,
    }[container_type]


def container_status_from_shipment(shipment_status: str) -> str:
    return {
        "Booked": "Empty Released",
        "In Transit": "In Transit",
        "Delayed": "In Transit",
        "Arrived": "Discharged",
        "Customs Hold": "Discharged",
        "Delivered": "Delivered",
        "Cancelled": "Empty Released",
    }[shipment_status]


def status_from_dates(departure: date, actual_arrival: date, delay: int, anomalous: bool, rng: np.random.Generator) -> str:
    today = date.today()
    if anomalous and rng.random() < 0.04:
        return "Cancelled"
    if actual_arrival < today - timedelta(days=5):
        return "Delivered" if rng.random() < 0.78 else "Arrived"
    if departure <= today <= actual_arrival:
        return "Delayed" if delay >= 4 else "In Transit"
    if actual_arrival <= today:
        return "Customs Hold" if delay >= 8 and rng.random() < 0.25 else "Arrived"
    return "Booked"


def risk_score(rng: np.random.Generator, commodity: str, delay: int, anomalous: bool) -> float:
    base = 0.18 + (0.20 if commodity in {"Chemicals", "Pharmaceuticals", "Electronics"} else 0.0)
    base += min(delay * 0.015, 0.20)
    base += 0.30 if anomalous else 0.0
    return round(float(min(max(rng.normal(base, 0.09), 0.01), 0.99)), 3)


def make_tracking_events(
    sequence: int,
    container_numbers: list[str],
    origin: str,
    destination: str,
    transshipment: str | None,
    departure: date,
    eta: date,
    actual_arrival: date,
    anomalous: bool,
    rng: np.random.Generator,
    port_by_locode: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    timeline = event_timeline(origin, destination, departure, eta, actual_arrival, transshipment, rng)
    if anomalous and rng.random() < 0.28:
        timeline = drop_random_events(timeline, rng)
    if anomalous and rng.random() < 0.12 and len(timeline) > 4:
        first = int(rng.integers(1, len(timeline) - 2))
        timeline[first], timeline[first + 1] = timeline[first + 1], timeline[first]

    rows = []
    for container_number in container_numbers:
        for event_position, (event_type, timestamp, location) in enumerate(timeline, start=1):
            rows.append(
                {
                    "event_id": f"EVT-{sequence:012d}-{container_number}-{event_position:02d}",
                    "shipment_id": f"SHP-{sequence:012d}",
                    "container_number": container_number,
                    "event_type": event_type,
                    "event_timestamp": timestamp,
                    "event_location": location,
                    "event_location_name": port_by_locode[location]["port_name"],
                    "event_status": event_status(event_type, timestamp, actual_arrival, anomalous, rng),
                }
            )
    return rows


def event_timeline(
    origin: str,
    destination: str,
    departure: date,
    eta: date,
    actual_arrival: date,
    transshipment: str | None,
    rng: np.random.Generator,
) -> list[tuple[str, datetime, str]]:
    booking_time = datetime.combine(departure - timedelta(days=int(rng.integers(18, 45))), datetime.min.time())
    released_time = datetime.combine(departure - timedelta(days=int(rng.integers(10, 18))), datetime.min.time())
    gate_in_time = datetime.combine(departure - timedelta(days=int(rng.integers(2, 7))), datetime.min.time())
    customs_export_time = gate_in_time + timedelta(hours=int(rng.integers(8, 36)))
    loaded_time = datetime.combine(departure - timedelta(days=1), datetime.min.time()) + timedelta(hours=int(rng.integers(4, 20)))
    departure_time = datetime.combine(departure, datetime.min.time()) + timedelta(hours=int(rng.integers(1, 23)))
    arrival_time = datetime.combine(actual_arrival, datetime.min.time()) + timedelta(hours=int(rng.integers(1, 23)))
    discharged_time = arrival_time + timedelta(hours=int(rng.integers(6, 48)))
    import_clearance_time = discharged_time + timedelta(hours=int(rng.integers(8, 96)))
    gate_out_time = import_clearance_time + timedelta(hours=int(rng.integers(6, 72)))
    delivered_time = gate_out_time + timedelta(hours=int(rng.integers(4, 96)))

    timeline = [
        ("Booking Confirmed", booking_time, origin),
        ("Container Released", released_time, origin),
        ("Gate In", gate_in_time, origin),
        ("Customs Export Clearance", customs_export_time, origin),
        ("Loaded on Vessel", loaded_time, origin),
        ("Vessel Departure", departure_time, origin),
    ]

    if transshipment:
        total_seconds = max((arrival_time - departure_time).total_seconds(), 1)
        mid_time = departure_time + timedelta(seconds=total_seconds * float(rng.uniform(0.38, 0.68)))
        timeline.extend(
            [
                ("Transshipment Arrival", mid_time, transshipment),
                ("Transshipment Departure", mid_time + timedelta(hours=int(rng.integers(12, 96))), transshipment),
            ]
        )

    timeline.extend(
        [
            ("Vessel Arrival", arrival_time, destination),
            ("Discharged", discharged_time, destination),
            ("Customs Import Clearance", import_clearance_time, destination),
            ("Gate Out", gate_out_time, destination),
            ("Delivered", delivered_time, destination),
        ]
    )

    return timeline


def drop_random_events(
    timeline: list[tuple[str, datetime, str]], rng: np.random.Generator
) -> list[tuple[str, datetime, str]]:
    removable = [i for i, row in enumerate(timeline) if row[0] not in {"Booking Confirmed", "Vessel Departure", "Delivered"}]
    if not removable:
        return timeline
    remove_count = int(rng.integers(1, min(3, len(removable)) + 1))
    remove_indexes = set(rng.choice(removable, size=remove_count, replace=False))
    return [row for i, row in enumerate(timeline) if i not in remove_indexes]


def event_status(
    event_type: str, timestamp: datetime, actual_arrival: date, anomalous: bool, rng: np.random.Generator
) -> str:
    if event_type in {"Customs Export Clearance", "Customs Import Clearance"}:
        if anomalous and rng.random() < 0.10:
            return "Invalid Customs Status"
        return str(rng.choice(CUSTOMS_STATUSES, p=[0.80, 0.08, 0.07, 0.04, 0.01]))
    if timestamp.date() > date.today():
        return "Estimated"
    if anomalous and rng.random() < 0.08:
        return "Exception"
    if timestamp.date() > actual_arrival + timedelta(days=3):
        return "Delayed"
    return "Completed"


def generate_chunk_wrapper(args: tuple[int, int, int, GeneratorConfig, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]) -> dict[str, int]:
    return generate_chunk(*args)


def chunk_plan(record_count: int, chunk_size: int) -> Iterable[tuple[int, int, int]]:
    parts = math.ceil(record_count / chunk_size)
    for part_index in range(1, parts + 1):
        start_index = (part_index - 1) * chunk_size
        count = min(chunk_size, record_count - start_index)
        yield part_index, start_index, count


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    configure_logging()
    config = parse_args()
    LOGGER.info("writing dimension tables to %s", config.output_dir)
    ports, vessels, customers = write_dimension_tables(config)

    ports_records = ports.to_dict("records")
    vessels_records = vessels.to_dict("records")
    customers_records = customers.to_dict("records")
    tasks = [
        (part, start, count, config, ports_records, vessels_records, customers_records)
        for part, start, count in chunk_plan(config.record_count, config.chunk_size)
    ]
    LOGGER.info(
        "generating %s shipments across %d part(s) with %d worker(s)",
        f"{config.record_count:,}",
        len(tasks),
        config.workers,
    )

    totals = {"shipments": 0, "bills_of_lading": 0, "containers": 0, "tracking_events": 0}
    if config.workers == 1 or len(tasks) == 1:
        for task in tasks:
            result = generate_chunk_wrapper(task)
            log_part(result)
            for key in totals:
                totals[key] += result[key]
    else:
        with ProcessPoolExecutor(max_workers=config.workers) as executor:
            futures = [executor.submit(generate_chunk_wrapper, task) for task in tasks]
            for future in as_completed(futures):
                result = future.result()
                log_part(result)
                for key in totals:
                    totals[key] += result[key]

    LOGGER.info("done: %s", ", ".join(f"{key}={value:,}" for key, value in totals.items()))


def log_part(result: dict[str, int]) -> None:
    LOGGER.info(
        "part-%04d: shipments=%s bills=%s containers=%s events=%s",
        result["part"],
        f"{result['shipments']:,}",
        f"{result['bills_of_lading']:,}",
        f"{result['containers']:,}",
        f"{result['tracking_events']:,}",
    )


if __name__ == "__main__":
    main()
