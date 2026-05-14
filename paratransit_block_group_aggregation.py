# ============================================================
# Paratransit Client Home Block Group Aggregation + AGOL Publish
# ArcGIS Pro / ArcPy
#
# A privacy-first geospatial pipeline that aggregates sensitive
# paratransit client data into publishable block-group-level
# map layers and pushes them to ArcGIS Online.
#
# Workflow:
# 1. Download Census TIGER/Line block groups for the target state
# 2. Filter to the target county
# 3. Pull paratransit client home records from SQL Server
# 4. Pull completed trip summary by client from SQL Server
# 5. Create temporary home points locally
# 6. Assign each home point to a paratransit service type
# 7. Spatially join home points to block groups
# 8. Aggregate metrics to block group level
# 9. Create a service-type summary table
# 10. Create an outside-service active client block group layer
# 11. Delete all non-publish/sensitive fields
# 12. Publish privacy-safe aggregated layers to AGOL
#
# Published layers DO NOT include:
#   ClientId, DOB, DateofBirth, exact age, longitude, latitude,
#   or raw home address.
# ============================================================

import os
import zipfile
import urllib.request
import datetime as dt

import pandas as pd
import pyodbc
import arcpy


# ============================================================
# SECTION 1 — USER CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# Project / output folders
# Replace with the path to your working directory.
# ------------------------------------------------------------

PROJECT_ROOT = r"C:\path\to\project\ParaTransitUser"
OUTPUT_FOLDER = os.path.join(PROJECT_ROOT, "Outputs")


# ------------------------------------------------------------
# Internal authoritative paratransit service area layer
# This is a polygon feature class describing where the agency
# operates, with a field indicating service tier (e.g. 5/6/7 Day).
# ------------------------------------------------------------

SERVICE_AREA_FC = r"C:\path\to\project\CurrentServiceArea\ServiceArea.shp"


# ------------------------------------------------------------
# Service area type settings
# Update SERVICE_TYPE_SOURCE_FIELD to match the field in your
# service area layer that holds tier values like "5 Days", "6 Days", etc.
# ------------------------------------------------------------

SERVICE_TYPE_SOURCE_FIELD = "Abbr"
SERVICE_TYPE_FIELD = "ServiceType"

# Priority is used if a home point intersects multiple service polygons.
# Higher service level wins.
SERVICE_TYPE_PRIORITY = {
    "7 Day": 4,
    "6 Day": 3,
    "5 Day": 2,
    "2 Day": 1,
    "Outside Service Area": 0,
    "Unknown": -1,
}

SERVICE_TYPE_DISPLAY_ORDER = {
    "2 Day": 1,
    "5 Day": 2,
    "6 Day": 3,
    "7 Day": 4,
    "Outside Service Area": 5,
    "Unknown": 99,
}


# ------------------------------------------------------------
# Census TIGER/Line block group settings
# ------------------------------------------------------------

DOWNLOAD_TIGER_BLOCK_GROUPS = True

TIGER_YEAR = 2025
STATE_FIPS = "48"     # Example: Texas
COUNTY_FIPS = "029"   # Example: Bexar County
COUNTY_GEOID = STATE_FIPS + COUNTY_FIPS

TIGER_BG_URL = (
    f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/BG/"
    f"tl_{TIGER_YEAR}_{STATE_FIPS}_bg.zip"
)

# TIGER block group shapefiles use GEOID.
BG_ID_FIELD = "GEOID"


# ------------------------------------------------------------
# SQL Server connection
# Replace with your environment values, or pull from environment
# variables / a secrets manager.
# ------------------------------------------------------------

SQL_SERVER = r"YOUR_SERVER\YOUR_INSTANCE"
SQL_DATABASE = "your_database_name"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"


# ------------------------------------------------------------
# Trip activity analysis window
# ------------------------------------------------------------

START_DATE = "2024-01-01"
END_DATE = "2026-05-06"


# ------------------------------------------------------------
# Active client definition
# Active client = client has at least this many completed trips
# during the full START_DATE / END_DATE window.
# ------------------------------------------------------------

ACTIVE_CLIENT_MIN_TRIPS = 20


# ------------------------------------------------------------
# Scheduled / frequent rider definition
# Scheduled rider = average completed trips per week >= threshold.
# ------------------------------------------------------------

SCHEDULED_RIDER_WEEKLY_TRIP_THRESHOLD = 2.0


# ------------------------------------------------------------
# Privacy suppression
# Block groups with fewer than this many total clients will not
# be included in the main hosted block group layer.
# ------------------------------------------------------------

MIN_CLIENT_COUNT_TO_PUBLISH = 5

# For outside-service active client block group layer:
# only show block groups with at least this many outside-service
# active clients.
MIN_OUTSIDE_ACTIVE_COUNT_TO_PUBLISH = 2


# ------------------------------------------------------------
# Coordinate QA bounds — broad bounds around the target region.
# ------------------------------------------------------------

MIN_LON = -101.0
MAX_LON = -96.0
MIN_LAT = 27.0
MAX_LAT = 31.0


# ------------------------------------------------------------
# AGOL publishing settings
# If you are already signed into ArcGIS Pro, leave credentials as None.
# ------------------------------------------------------------

PUBLISH_TO_AGOL = True

PORTAL_URL = "https://www.arcgis.com"

AGOL_USERNAME = None
AGOL_PASSWORD = None

OVERWRITE_EXISTING_SERVICE = True

BLOCK_GROUP_SERVICE_NAME = "Paratransit_Client_Block_Groups"
SERVICE_AREA_SERVICE_NAME = "Current_Paratransit_Service_Area"
OUTSIDE_ACTIVE_BG_SERVICE_NAME = "Outside_Service_Active_Client_Block_Groups"


# ------------------------------------------------------------
# Cleanup settings
# ------------------------------------------------------------

DELETE_TEMP_PRIVACY_OUTPUTS = True


# ------------------------------------------------------------
# ArcPy environment
# ------------------------------------------------------------

arcpy.env.overwriteOutput = True


# ============================================================
# SECTION 2 — OUTPUT SETUP
# ============================================================

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
gdb_name = f"ParatransitClientBG_{timestamp}.gdb"
GDB_PATH = os.path.join(OUTPUT_FOLDER, gdb_name)

if not arcpy.Exists(GDB_PATH):
    arcpy.management.CreateFileGDB(OUTPUT_FOLDER, gdb_name)

print("============================================================")
print("Starting Paratransit Client Block Group Aggregation")
print("============================================================")
print(f"Output folder: {OUTPUT_FOLDER}")
print(f"Output GDB: {GDB_PATH}")


# ============================================================
# SECTION 3 — HELPER FUNCTIONS
# ============================================================

def get_sql_connection():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def safe_delete(path):
    try:
        if not path:
            return

        if arcpy.Exists(path):
            arcpy.management.Delete(path)
            print(f"Deleted temporary ArcGIS item: {path}")

        elif os.path.exists(path):
            os.remove(path)
            print(f"Deleted temporary file: {path}")

    except Exception as ex:
        print(f"Warning: could not delete {path}: {ex}")


def add_field_if_missing(feature_class, field_name, field_type, field_length=None):
    existing = {f.name.lower() for f in arcpy.ListFields(feature_class)}

    if field_name.lower() in existing:
        return

    if field_length:
        arcpy.management.AddField(
            feature_class,
            field_name,
            field_type,
            field_length=field_length,
        )
    else:
        arcpy.management.AddField(
            feature_class,
            field_name,
            field_type,
        )


def get_count(feature_class):
    return int(arcpy.management.GetCount(feature_class)[0])


def normalize_service_type(value):
    """
    Normalizes service area labels into:
    2 Day, 5 Day, 6 Day, 7 Day, Unknown
    """

    raw = str(value).strip().lower() if value is not None else ""

    if raw == "":
        return "Unknown"

    # Handles values like "5 Days", "VITrans 5 Day Service Area", etc.
    if "7" in raw:
        return "7 Day"
    if "6" in raw:
        return "6 Day"
    if "5" in raw:
        return "5 Day"
    if "2" in raw:
        return "2 Day"

    return "Unknown"


def download_and_prepare_county_block_groups(
    output_folder,
    output_gdb,
    tiger_year,
    tiger_bg_url,
    state_fips,
    county_fips,
):
    tiger_folder = os.path.join(output_folder, "TIGER_BlockGroups")
    os.makedirs(tiger_folder, exist_ok=True)

    extract_folder = os.path.join(
        tiger_folder,
        f"tl_{tiger_year}_{state_fips}_bg",
    )
    os.makedirs(extract_folder, exist_ok=True)

    zip_path = os.path.join(
        tiger_folder,
        f"tl_{tiger_year}_{state_fips}_bg.zip",
    )

    if not os.path.exists(zip_path):
        print("Downloading Census TIGER/Line block groups...")
        print(f"Source: {tiger_bg_url}")
        urllib.request.urlretrieve(tiger_bg_url, zip_path)
        print(f"Downloaded: {zip_path}")
    else:
        print(f"Using existing TIGER/Line zip: {zip_path}")

    shp_name = f"tl_{tiger_year}_{state_fips}_bg.shp"
    shp_path = os.path.join(extract_folder, shp_name)

    if not os.path.exists(shp_path):
        print(f"Extracting TIGER/Line zip to: {extract_folder}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_folder)

        print("Extraction complete.")
    else:
        print(f"Using existing extracted shapefile: {shp_path}")

    if not arcpy.Exists(shp_path):
        raise RuntimeError(
            f"Expected TIGER/Line shapefile not found: {shp_path}"
        )

    county_bg_fc = os.path.join(
        output_gdb,
        f"County_BlockGroups_TIGER{tiger_year}",
    )

    where_clause = f"COUNTYFP = '{county_fips}'"

    print(f"Selecting county block groups using: {where_clause}")

    arcpy.analysis.Select(
        in_features=shp_path,
        out_feature_class=county_bg_fc,
        where_clause=where_clause,
    )

    count = get_count(county_bg_fc)

    if count == 0:
        raise RuntimeError(
            "No block groups were selected from the TIGER/Line file. "
            f"Check COUNTYFP = '{county_fips}' and source file: {shp_path}"
        )

    print(f"County block groups created: {county_bg_fc}")
    print(f"County block group count: {count:,}")

    return county_bg_fc


def prepare_service_area_by_type(
    service_area_fc,
    output_gdb,
    source_field,
    output_field,
):
    """
    Copies the service area layer, creates a normalized ServiceType field,
    and dissolves polygons into one multipart feature per service type.

    This handles service area shapefiles with many small polygons/corridors.
    """

    if not arcpy.Exists(service_area_fc):
        raise RuntimeError(f"Service area feature class does not exist: {service_area_fc}")

    source_fields = [f.name for f in arcpy.ListFields(service_area_fc)]

    if source_field not in source_fields:
        raise RuntimeError(
            f"Service type source field '{source_field}' not found in service area layer. "
            f"Available fields: {source_fields}"
        )

    service_area_local = os.path.join(
        output_gdb,
        "Current_Paratransit_Service_Area",
    )

    service_area_typed = os.path.join(
        output_gdb,
        "Current_Paratransit_Service_Area_Typed",
    )

    service_area_dissolved = os.path.join(
        output_gdb,
        "Current_Paratransit_Service_Area_ByType",
    )

    arcpy.management.CopyFeatures(service_area_fc, service_area_local)
    arcpy.management.RepairGeometry(service_area_local)

    arcpy.management.CopyFeatures(service_area_local, service_area_typed)

    add_field_if_missing(
        service_area_typed,
        output_field,
        "TEXT",
        field_length=50,
    )

    with arcpy.da.UpdateCursor(
        service_area_typed,
        [source_field, output_field],
    ) as cursor:
        for source_value, service_type in cursor:
            normalized = normalize_service_type(source_value)
            cursor.updateRow([source_value, normalized])

    arcpy.management.Dissolve(
        in_features=service_area_typed,
        out_feature_class=service_area_dissolved,
        dissolve_field=output_field,
        multi_part="MULTI_PART",
    )

    print(f"Prepared dissolved service area by type: {service_area_dissolved}")
    print(f"Dissolved service type feature count: {get_count(service_area_dissolved):,}")

    return service_area_local, service_area_dissolved


def create_summary_table_from_dataframe(df, out_gdb, table_name, field_type_overrides=None):
    """
    Creates a geodatabase table from a pandas dataframe.
    Allows text fields through field_type_overrides.
    """

    field_type_overrides = field_type_overrides or {}

    out_table = os.path.join(out_gdb, table_name)

    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)

    arcpy.management.CreateTable(out_gdb, table_name)

    for col in df.columns:
        if col in field_type_overrides:
            field_def = field_type_overrides[col]
            field_type = field_def.get("type", "TEXT")
            field_length = field_def.get("length", 255)

            if field_type.upper() == "TEXT":
                arcpy.management.AddField(out_table, col, "TEXT", field_length=field_length)
            else:
                arcpy.management.AddField(out_table, col, field_type)

        elif (
            col.endswith("_Count")
            or col.endswith("_Cnt")
            or col in {
                "Client_Count",
                "Active_Client_Count",
                "Scheduled_Rider_Count",
                "Trip_Count",
                "Sort_Order",
            }
        ):
            arcpy.management.AddField(out_table, col, "LONG")

        else:
            arcpy.management.AddField(out_table, col, "DOUBLE")

    insert_fields = list(df.columns)

    with arcpy.da.InsertCursor(out_table, insert_fields) as cursor:
        for _, row in df.iterrows():
            values = []

            for field in insert_fields:
                value = row[field]

                if pd.isna(value):
                    values.append(None)

                elif field in field_type_overrides and field_type_overrides[field].get("type", "TEXT").upper() == "TEXT":
                    values.append(str(value))

                elif (
                    field.endswith("_Count")
                    or field.endswith("_Cnt")
                    or field in {
                        "Client_Count",
                        "Active_Client_Count",
                        "Scheduled_Rider_Count",
                        "Trip_Count",
                        "Sort_Order",
                    }
                ):
                    values.append(int(value))

                else:
                    values.append(float(value))

            cursor.insertRow(values)

    return out_table


def publish_feature_layer_from_fc(
    feature_class,
    service_name,
    portal_url,
    username=None,
    password=None,
    overwrite=True,
):
    """
    Publishes a single feature class as a hosted feature layer from ArcGIS Pro.
    """

    print("------------------------------------------------------------")
    print(f"Preparing to publish: {service_name}")
    print("------------------------------------------------------------")

    if username and password:
        print("Signing into portal using provided credentials...")
        arcpy.SignInToPortal(portal_url, username, password)
    else:
        print("Using current ArcGIS Pro portal sign-in.")

    if not arcpy.Exists(feature_class):
        raise RuntimeError(f"Feature class does not exist: {feature_class}")

    feature_count = get_count(feature_class)

    print(f"Feature class: {feature_class}")
    print(f"Feature count: {feature_count:,}")

    if feature_count == 0:
        raise RuntimeError(f"Cannot publish empty feature class: {feature_class}")

    aprx = arcpy.mp.ArcGISProject("CURRENT")

    publish_map_name = f"Publish_{service_name}"

    for m in aprx.listMaps():
        if m.name == publish_map_name:
            aprx.deleteItem(m)

    publish_map = aprx.createMap(publish_map_name)
    publish_map.addDataFromPath(feature_class)

    candidate_layers = [
        lyr for lyr in publish_map.listLayers()
        if lyr.supports("DATASOURCE")
        and lyr.isFeatureLayer
        and os.path.normcase(lyr.dataSource) == os.path.normcase(feature_class)
    ]

    if not candidate_layers:
        print("Layers currently in publishing map:")
        for lyr in publish_map.listLayers():
            print(
                f"  Layer: {lyr.name}, "
                f"isFeatureLayer={getattr(lyr, 'isFeatureLayer', None)}, "
                f"supports DATASOURCE={lyr.supports('DATASOURCE')}"
            )
            if lyr.supports("DATASOURCE"):
                print(f"    dataSource={lyr.dataSource}")

        raise RuntimeError(
            "No valid feature layer was found in the publishing map. "
            "ArcGIS cannot stage this as a hosted feature layer."
        )

    layer = candidate_layers[0]

    sddraft_path = os.path.join(OUTPUT_FOLDER, f"{service_name}.sddraft")
    sd_path = os.path.join(OUTPUT_FOLDER, f"{service_name}.sd")

    if os.path.exists(sddraft_path):
        os.remove(sddraft_path)

    if os.path.exists(sd_path):
        os.remove(sd_path)

    sharing_draft = publish_map.getWebLayerSharingDraft(
        "HOSTING_SERVER",
        "FEATURE",
        service_name,
        [layer],
    )

    sharing_draft.summary = service_name
    sharing_draft.tags = (
        "paratransit, service planning, census block groups, "
        "aggregated analysis"
    )
    sharing_draft.description = (
        "Aggregated paratransit planning layer. "
        "No client-level identifiers, dates of birth, exact ages, "
        "coordinates, or raw addresses are published."
    )
    sharing_draft.credits = (
        "Generated from internal paratransit client and trip data, "
        "internal service area geography, and Census TIGER/Line block groups."
    )
    sharing_draft.useLimitations = (
        "For planning and analysis use. Data is aggregated to census "
        "block groups. Small-count block groups may be suppressed."
    )
    sharing_draft.overwriteExistingService = overwrite

    print(f"Exporting service definition draft: {sddraft_path}")
    sharing_draft.exportToSDDraft(sddraft_path)

    print(f"Staging service definition: {sd_path}")
    arcpy.server.StageService(sddraft_path, sd_path)

    print(f"Uploading service definition to portal: {service_name}")
    arcpy.server.UploadServiceDefinition(sd_path, "HOSTING_SERVER")

    print(f"Published hosted feature layer: {service_name}")


# ============================================================
# SECTION 4 — DOWNLOAD / PREPARE CENSUS BLOCK GROUPS
# ============================================================

if DOWNLOAD_TIGER_BLOCK_GROUPS:
    BLOCK_GROUPS_FC = download_and_prepare_county_block_groups(
        output_folder=OUTPUT_FOLDER,
        output_gdb=GDB_PATH,
        tiger_year=TIGER_YEAR,
        tiger_bg_url=TIGER_BG_URL,
        state_fips=STATE_FIPS,
        county_fips=COUNTY_FIPS,
    )
else:
    BLOCK_GROUPS_FC = r"C:\path\to\Census\County_BlockGroups.shp"

if not arcpy.Exists(BLOCK_GROUPS_FC):
    raise RuntimeError(f"Block group feature class does not exist: {BLOCK_GROUPS_FC}")

if BG_ID_FIELD not in [f.name for f in arcpy.ListFields(BLOCK_GROUPS_FC)]:
    raise RuntimeError(
        f"BG_ID_FIELD '{BG_ID_FIELD}' not found in block group layer."
    )


# ============================================================
# SECTION 5 — PREPARE SERVICE AREA BY SERVICE TYPE
# ============================================================

service_area_local, service_area_by_type = prepare_service_area_by_type(
    service_area_fc=SERVICE_AREA_FC,
    output_gdb=GDB_PATH,
    source_field=SERVICE_TYPE_SOURCE_FIELD,
    output_field=SERVICE_TYPE_FIELD,
)


# ============================================================
# SECTION 6 — CLIENT HOME SQL QUERY
# Adapt schema/table/column names to your scheduling system.
# ============================================================

client_sql = """
SELECT
    a.[ClientId],
    a.[Gender],
    a.[DateofBirth],

    d.DOB,

    age_calc.CurrentAge,

    CASE
        WHEN age_calc.CurrentAge IS NULL THEN NULL
        WHEN age_calc.CurrentAge >= 1  AND age_calc.CurrentAge < 10 THEN '1 to 10'
        WHEN age_calc.CurrentAge >= 10 AND age_calc.CurrentAge < 18 THEN '10 to 18'
        WHEN age_calc.CurrentAge >= 18 AND age_calc.CurrentAge < 25 THEN '18 to 25'
        WHEN age_calc.CurrentAge >= 25 AND age_calc.CurrentAge < 35 THEN '25 to 35'
        WHEN age_calc.CurrentAge >= 35 AND age_calc.CurrentAge < 45 THEN '35 to 45'
        WHEN age_calc.CurrentAge >= 45 AND age_calc.CurrentAge < 55 THEN '45 to 55'
        WHEN age_calc.CurrentAge >= 55 AND age_calc.CurrentAge < 65 THEN '55 to 65'
        WHEN age_calc.CurrentAge >= 65 AND age_calc.CurrentAge < 75 THEN '65 to 75'
        WHEN age_calc.CurrentAge >= 75 AND age_calc.CurrentAge < 85 THEN '75 to 85'
        WHEN age_calc.CurrentAge >= 85 THEN '85+'
        ELSE NULL
    END AS AgeCategory,

    b.[AddrType],

    TRY_CAST(b.[lon] AS decimal(18,6)) / 1000000.0 AS Longitude,
    TRY_CAST(b.[lat] AS decimal(18,6)) / 1000000.0 AS Latitude

FROM dbo.[Clients] a

INNER JOIN dbo.[Address] b
    ON b.[AddrId] = a.[ClientId]
    AND b.[AddrType] = 'CH'

CROSS APPLY (
    SELECT TRY_CONVERT(date, CONVERT(char(8), a.[DateofBirth]), 112) AS DOB
) d

CROSS APPLY (
    SELECT
        CASE
            WHEN d.DOB IS NULL THEN NULL
            ELSE
                DATEDIFF(YEAR, d.DOB, CAST(GETDATE() AS date))
                - CASE
                    WHEN DATEADD(YEAR, DATEDIFF(YEAR, d.DOB, CAST(GETDATE() AS date)), d.DOB) > CAST(GETDATE() AS date)
                    THEN 1
                    ELSE 0
                  END
        END AS CurrentAge
) age_calc

WHERE
    a.[DateofBirth] IS NOT NULL
    AND a.[DateofBirth] <> 0
    AND d.DOB IS NOT NULL
    AND b.[lon] IS NOT NULL
    AND b.[lat] IS NOT NULL;
"""


# ============================================================
# SECTION 7 — COMPLETED TRIP SUMMARY SQL QUERY
# ============================================================

trip_summary_sql = """
WITH

puLegs AS(
    SELECT *
    FROM dbo.BookingLegs
    WHERE LegNum = 0
),

puPassengers AS(
    SELECT
        LegID,
        SUM(NumSpacesPu) AS [Passenger Count]
    FROM dbo.BookingActivity
    WHERE LegID IN (SELECT DISTINCT LegID FROM puLegs)
    GROUP BY LegID
),

bookingPassengers AS(
    SELECT DISTINCT
        puLegs.BookingID,
        puPassengers.[Passenger Count]
    FROM puLegs
    LEFT JOIN puPassengers
        ON puLegs.LegID = puPassengers.LegId
),

pickup_event AS(
    SELECT
        e.BookingId,
        e.schid,
        e.evstrid,
        e.PassOn,
        e.SpaceOn AS [Seat Type/Mobility Aids],
        CASE
            WHEN e.SchedStatus = 1 THEN 'Scheduled'
            WHEN e.SchedStatus = 2 THEN 'Arrived'
            WHEN e.SchedStatus = 3 THEN 'Complete (Performed)'
            WHEN e.SchedStatus = 4 THEN 'Missed Trip but Transported'
            WHEN e.SchedStatus = 0 THEN 'Unscheduled'
            WHEN e.SchedStatus = 21 THEN 'No Show - Missed Trip'
            WHEN e.SchedStatus = 20 THEN 'No Show'
            WHEN e.SchedStatus = 40 THEN 'Cancel In Advance'
            WHEN e.SchedStatus = 41 THEN 'Cancel Late'
            WHEN e.SchedStatus = 42 THEN 'Cancel At Door'
            WHEN e.SchedStatus = 43 THEN 'Cancel Sameday'
            WHEN e.SchedStatus = 44 THEN 'Site Closure Cancel'
            WHEN e.SchedStatus = 45 THEN 'Cancel User Error'
            WHEN e.SchedStatus = 50 THEN 'Cancel Overnight'
            WHEN e.SchedStatus = 51 THEN 'Cancel No Penalty'
            ELSE 'Other'
        END AS [Actual Ride Status],
        e.SchedStatus AS SchedStatusRaw
    FROM dbo.events e
    WHERE Activity = 0
),

bookedTrips AS(
    SELECT
        b.BookingID AS [Trip ID],
        CONVERT(DATE, CONVERT(CHAR(8), b.Ldate)) AS [Ride Date],
        COALESCE(
            pu_event.[Actual Ride Status],
            CASE
                WHEN b.SchedStatus = 43 THEN 'Cancel Sameday'
                WHEN b.SchedStatus = 40 THEN 'Cancel In Advance'
                WHEN b.SchedStatus = 45 THEN 'Cancel User Error'
                WHEN b.SchedStatus = 0 THEN 'Unscheduled'
                WHEN b.SchedStatus = 50 THEN 'Cancel Overnight'
                WHEN b.SchedStatus = 41 THEN 'Cancel Late'
                WHEN b.SchedStatus = 51 THEN 'Cancel No Penalty'
                WHEN b.SchedStatus = 44 THEN 'Site Closure Cancel'
                WHEN b.SchedStatus = 3 THEN 'Complete (Performed)'
                ELSE NULL
            END
        ) AS [Ride Status],
        b.SubtypeAbbr AS [Booking Type],
        c.ClientID AS [Client ID],
        c.ClientCode AS [Client Code],
        bookpass.[Passenger Count]
    FROM dbo.Booking b
    LEFT JOIN dbo.Clients c
        ON b.ClientID = c.ClientID
    LEFT JOIN pickup_event AS pu_event
        ON b.BookingId = pu_event.BookingId
    LEFT JOIN bookingPassengers AS bookPass
        ON b.BookingId = bookPass.BookingId
)

SELECT
    [Client ID] AS ClientId,
    COUNT(*) AS CompletedTripCount,
    COUNT(DISTINCT [Ride Date]) AS ActiveRideDays,
    SUM(ISNULL([Passenger Count], 0)) AS PassengerSpaceCount,
    MIN([Ride Date]) AS FirstRideDate,
    MAX([Ride Date]) AS LastRideDate
FROM bookedTrips
WHERE [Ride Date] BETWEEN ? AND ?
  AND [Ride Status] = 'Complete (Performed)'
  AND [Client ID] IS NOT NULL
GROUP BY [Client ID];
"""


# ============================================================
# SECTION 8 — PULL SQL DATA
# ============================================================

print("Connecting to SQL Server...")
conn = get_sql_connection()

print("Pulling client home records...")
clients_df = pd.read_sql(client_sql, conn)
print(f"Raw client home records returned: {len(clients_df):,}")

print("Pulling completed trip summary by client...")
trips_df = pd.read_sql(
    trip_summary_sql,
    conn,
    params=[START_DATE, END_DATE],
)
print(f"Trip summary client records returned: {len(trips_df):,}")

conn.close()


# ============================================================
# SECTION 9 — CLIENT DATA QA
# ============================================================

required_client_fields = [
    "ClientId",
    "AgeCategory",
    "CurrentAge",
    "Longitude",
    "Latitude",
]

missing = [f for f in required_client_fields if f not in clients_df.columns]

if missing:
    raise RuntimeError(f"Client query missing required fields: {missing}")

clients_df = clients_df.dropna(
    subset=[
        "ClientId",
        "Longitude",
        "Latitude",
        "AgeCategory",
    ]
).copy()

clients_df = clients_df[
    clients_df["Longitude"].between(MIN_LON, MAX_LON)
    & clients_df["Latitude"].between(MIN_LAT, MAX_LAT)
].copy()

clients_df["ClientId"] = clients_df["ClientId"].astype(int)

print(f"Client records after coordinate/null QA: {len(clients_df):,}")

if len(clients_df) == 0:
    raise RuntimeError(
        "No valid client home records available after QA. Stopping."
    )


# ============================================================
# SECTION 10 — MERGE CLIENTS WITH TRIP ACTIVITY
# ============================================================

if len(trips_df) > 0:
    trips_df["ClientId"] = trips_df["ClientId"].astype(int)

df = clients_df.merge(
    trips_df,
    on="ClientId",
    how="left",
)

for col in ["CompletedTripCount", "ActiveRideDays", "PassengerSpaceCount"]:
    if col not in df.columns:
        df[col] = 0

    df[col] = df[col].fillna(0).astype(int)

start_dt = pd.to_datetime(START_DATE)
end_dt = pd.to_datetime(END_DATE)

analysis_days = (end_dt - start_dt).days + 1
analysis_weeks = analysis_days / 7.0

if analysis_weeks <= 0:
    raise RuntimeError(
        "Invalid analysis window. END_DATE must be on or after START_DATE."
    )

df["AvgWeeklyTrips"] = df["CompletedTripCount"] / analysis_weeks

# Active client:
# Client has at least ACTIVE_CLIENT_MIN_TRIPS completed trips
# during the full analysis window.
df["IsActiveClient"] = (
    df["CompletedTripCount"] >= ACTIVE_CLIENT_MIN_TRIPS
).astype(int)

# Scheduled/frequent rider:
# Client averages at least the threshold completed trips per week.
df["IsScheduledRider"] = (
    df["AvgWeeklyTrips"] >= SCHEDULED_RIDER_WEEKLY_TRIP_THRESHOLD
).astype(int)

print(f"Analysis window: {START_DATE} to {END_DATE}")
print(f"Analysis days: {analysis_days}")
print(f"Analysis weeks: {analysis_weeks:.2f}")
print(f"Active client trip threshold: {ACTIVE_CLIENT_MIN_TRIPS:,}")
print(f"Active clients: {int(df['IsActiveClient'].sum()):,}")
print(f"Scheduled/frequent riders: {int(df['IsScheduledRider'].sum()):,}")


# ============================================================
# SECTION 11 — CREATE TEMPORARY CLIENT HOME POINTS
# ============================================================
# This CSV and feature class contain sensitive temporary processing
# fields. They are not published.

client_csv = os.path.join(
    OUTPUT_FOLDER,
    f"_TEMP_client_home_points_{timestamp}.csv",
)

df.to_csv(client_csv, index=False)

client_points = os.path.join(
    GDB_PATH,
    "_TEMP_Client_Home_Points",
)

arcpy.management.XYTableToPoint(
    in_table=client_csv,
    out_feature_class=client_points,
    x_field="Longitude",
    y_field="Latitude",
    coordinate_system=arcpy.SpatialReference(4326),
)

print(f"Created temporary client home points: {client_points}")
print(f"Temporary point count: {get_count(client_points):,}")


# ============================================================
# SECTION 12 — ASSIGN CLIENT POINTS TO SERVICE TYPE
# ============================================================
# This uses JOIN_ONE_TO_MANY so we can handle overlapping service
# areas. If a client point intersects multiple service types, the
# highest-priority service type wins:
# 7 Day > 6 Day > 5 Day > 2 Day.

client_service_join_raw = os.path.join(
    GDB_PATH,
    "_TEMP_Client_Service_Join_Raw",
)

arcpy.analysis.SpatialJoin(
    target_features=client_points,
    join_features=service_area_by_type,
    out_feature_class=client_service_join_raw,
    join_operation="JOIN_ONE_TO_MANY",
    join_type="KEEP_ALL",
    match_option="INTERSECT",
)

print(f"Raw client-to-service spatial join: {client_service_join_raw}")
print(f"Raw service join records: {get_count(client_service_join_raw):,}")

service_join_fields = [
    "ClientId",
    SERVICE_TYPE_FIELD,
    "IsActiveClient",
    "IsScheduledRider",
    "CompletedTripCount",
]

existing_service_join_fields = {f.name for f in arcpy.ListFields(client_service_join_raw)}

missing_service_join_fields = [
    f for f in service_join_fields
    if f not in existing_service_join_fields
]

if missing_service_join_fields:
    raise RuntimeError(
        "Missing fields in service join output. "
        f"Missing: {missing_service_join_fields}"
    )

service_records = []

with arcpy.da.SearchCursor(client_service_join_raw, service_join_fields) as cursor:
    for row in cursor:
        rec = dict(zip(service_join_fields, row))

        service_type = rec.get(SERVICE_TYPE_FIELD)

        if service_type is None or str(service_type).strip() == "":
            service_type = "Outside Service Area"

        service_type = normalize_service_type(service_type)

        if service_type == "Unknown":
            service_type = "Outside Service Area"

        rec[SERVICE_TYPE_FIELD] = service_type
        rec["ServiceTypePriority"] = SERVICE_TYPE_PRIORITY.get(service_type, -1)

        service_records.append(rec)

service_join_df = pd.DataFrame(service_records)

if len(service_join_df) == 0:
    raise RuntimeError("Service type assignment produced zero records.")

# Pick highest-priority service type per client.
service_assignment_df = (
    service_join_df
    .sort_values(["ClientId", "ServiceTypePriority"], ascending=[True, False])
    .drop_duplicates(subset=["ClientId"], keep="first")
    [["ClientId", SERVICE_TYPE_FIELD, "ServiceTypePriority"]]
    .copy()
)

print("Client service type assignment counts:")
print(service_assignment_df[SERVICE_TYPE_FIELD].value_counts(dropna=False))

# Merge service type assignment back into main processing dataframe.
df = df.merge(
    service_assignment_df,
    on="ClientId",
    how="left",
)

df[SERVICE_TYPE_FIELD] = df[SERVICE_TYPE_FIELD].fillna("Outside Service Area")
df["ServiceTypePriority"] = df["ServiceTypePriority"].fillna(0).astype(int)

# Export again with ServiceType included.
client_csv_with_service = os.path.join(
    OUTPUT_FOLDER,
    f"_TEMP_client_home_points_with_service_{timestamp}.csv",
)

df.to_csv(client_csv_with_service, index=False)

client_points_with_service = os.path.join(
    GDB_PATH,
    "_TEMP_Client_Home_Points_With_ServiceType",
)

arcpy.management.XYTableToPoint(
    in_table=client_csv_with_service,
    out_feature_class=client_points_with_service,
    x_field="Longitude",
    y_field="Latitude",
    coordinate_system=arcpy.SpatialReference(4326),
)

print(f"Created temporary client home points with service type: {client_points_with_service}")
print(f"Temporary service-type point count: {get_count(client_points_with_service):,}")


# ============================================================
# SECTION 13 — SPATIAL JOIN CLIENT POINTS TO BLOCK GROUPS
# ============================================================

points_with_bg = os.path.join(
    GDB_PATH,
    "_TEMP_Client_Home_Points_With_BG",
)

arcpy.analysis.SpatialJoin(
    target_features=client_points_with_service,
    join_features=BLOCK_GROUPS_FC,
    out_feature_class=points_with_bg,
    join_operation="JOIN_ONE_TO_ONE",
    join_type="KEEP_COMMON",
    match_option="INTERSECT",
)

print(f"Spatial joined client points to block groups: {points_with_bg}")
print(f"Joined point count: {get_count(points_with_bg):,}")


# ============================================================
# SECTION 14 — READ JOINED POINTS BACK TO PANDAS
# ============================================================

joined_fields = [
    "ClientId",
    "AgeCategory",
    "CurrentAge",
    "CompletedTripCount",
    "ActiveRideDays",
    "AvgWeeklyTrips",
    "IsActiveClient",
    "IsScheduledRider",
    SERVICE_TYPE_FIELD,
    BG_ID_FIELD,
]

existing_join_fields = {f.name for f in arcpy.ListFields(points_with_bg)}

missing_join_fields = [
    f for f in joined_fields
    if f not in existing_join_fields
]

if missing_join_fields:
    raise RuntimeError(
        "Missing fields after block group spatial join. "
        f"Check BG_ID_FIELD and field naming. Missing: {missing_join_fields}"
    )

records = []

with arcpy.da.SearchCursor(points_with_bg, joined_fields) as cursor:
    for row in cursor:
        rec = dict(zip(joined_fields, row))
        records.append(rec)

joined_df = pd.DataFrame(records)

if len(joined_df) == 0:
    raise RuntimeError(
        "No client points intersected county block groups. Stopping."
    )

joined_df[SERVICE_TYPE_FIELD] = joined_df[SERVICE_TYPE_FIELD].fillna("Outside Service Area")
joined_df["IsActiveClient"] = joined_df["IsActiveClient"].fillna(0).astype(int)
joined_df["IsScheduledRider"] = joined_df["IsScheduledRider"].fillna(0).astype(int)
joined_df["CompletedTripCount"] = joined_df["CompletedTripCount"].fillna(0).astype(float)

print(f"Joined point records available for aggregation: {len(joined_df):,}")


# ============================================================
# SECTION 15 — SERVICE TYPE SUMMARY TABLE
# ============================================================

service_summary_rows = []

# Ensure all five categories appear, even if count is zero.
all_service_types = [
    "2 Day",
    "5 Day",
    "6 Day",
    "7 Day",
    "Outside Service Area",
]

for service_type in all_service_types:
    group = joined_df[joined_df[SERVICE_TYPE_FIELD] == service_type]

    client_count = group["ClientId"].nunique()

    active_client_count = int(
        group["IsActiveClient"].fillna(0).astype(int).sum()
    ) if client_count else 0

    scheduled_rider_count = int(
        group["IsScheduledRider"].fillna(0).astype(int).sum()
    ) if client_count else 0

    trip_count = int(
        group["CompletedTripCount"].fillna(0).astype(float).sum()
    ) if client_count else 0

    active_pct = round(
        (active_client_count / client_count) * 100,
        2,
    ) if client_count else 0

    scheduled_pct = round(
        (scheduled_rider_count / client_count) * 100,
        2,
    ) if client_count else 0

    trips_per_client = round(
        trip_count / client_count,
        2,
    ) if client_count else 0

    trips_per_active_client = round(
        trip_count / active_client_count,
        2,
    ) if active_client_count else 0

    service_summary_rows.append({
        "ServiceType": service_type,
        "Sort_Order": SERVICE_TYPE_DISPLAY_ORDER.get(service_type, 99),
        "Client_Count": int(client_count),
        "Active_Client_Count": int(active_client_count),
        "Active_Client_Pct": active_pct,
        "Scheduled_Rider_Count": int(scheduled_rider_count),
        "Scheduled_Rider_Pct": scheduled_pct,
        "Trip_Count": int(trip_count),
        "Trips_Per_Client": trips_per_client,
        "Trips_Per_Active_Client": trips_per_active_client,
    })

service_summary_df = pd.DataFrame(service_summary_rows)
service_summary_df = service_summary_df.sort_values("Sort_Order")

service_summary_csv = os.path.join(
    OUTPUT_FOLDER,
    f"Service_Type_Client_Summary_{timestamp}.csv",
)

service_summary_df.to_csv(service_summary_csv, index=False)

service_summary_table = create_summary_table_from_dataframe(
    df=service_summary_df,
    out_gdb=GDB_PATH,
    table_name="Service_Type_Client_Summary",
    field_type_overrides={
        "ServiceType": {"type": "TEXT", "length": 50},
    },
)

print(f"Created service type summary CSV: {service_summary_csv}")
print(f"Created service type summary table: {service_summary_table}")


# ============================================================
# SECTION 16 — AGGREGATE BY BLOCK GROUP
# ============================================================

age_categories = [
    "1 to 10",
    "10 to 18",
    "18 to 25",
    "25 to 35",
    "35 to 45",
    "45 to 55",
    "55 to 65",
    "65 to 75",
    "75 to 85",
    "85+",
]

age_field_map = {
    "1 to 10": "Age_1_10",
    "10 to 18": "Age_10_18",
    "18 to 25": "Age_18_25",
    "25 to 35": "Age_25_35",
    "35 to 45": "Age_35_45",
    "45 to 55": "Age_45_55",
    "55 to 65": "Age_55_65",
    "65 to 75": "Age_65_75",
    "75 to 85": "Age_75_85",
    "85+": "Age_85Plus",
}

summary_rows = []

total_clients_all_bgs = joined_df["ClientId"].nunique()

for bg_id, group in joined_df.groupby(BG_ID_FIELD):
    client_count = group["ClientId"].nunique()

    active_client_count = int(
        group["IsActiveClient"].fillna(0).astype(int).sum()
    )

    scheduled_rider_count = int(
        group["IsScheduledRider"].fillna(0).astype(int).sum()
    )

    trip_count = int(
        group["CompletedTripCount"].fillna(0).astype(float).sum()
    )

    outside_service_mask = group[SERVICE_TYPE_FIELD] == "Outside Service Area"

    outside_service_client_count = group.loc[
        outside_service_mask,
        "ClientId",
    ].nunique()

    outside_service_active_count = int(
        group.loc[
            outside_service_mask,
            "IsActiveClient",
        ].fillna(0).astype(int).sum()
    )

    row = {
        BG_ID_FIELD: str(bg_id),

        "Client_Count": int(client_count),

        "Pct_Total_Clients": round(
            (client_count / total_clients_all_bgs) * 100,
            2,
        ) if total_clients_all_bgs else None,

        "Active_Client_Count": int(active_client_count),

        "Active_Client_Pct": round(
            (active_client_count / client_count) * 100,
            2,
        ) if client_count else None,

        "Scheduled_Rider_Count": int(scheduled_rider_count),

        "Scheduled_Rider_Pct": round(
            (scheduled_rider_count / client_count) * 100,
            2,
        ) if client_count else None,

        "Trip_Count": int(trip_count),

        "Trips_Per_Client": round(
            trip_count / client_count,
            2,
        ) if client_count else None,

        "Trips_Per_Active_Client": round(
            trip_count / active_client_count,
            2,
        ) if active_client_count else None,

        "Outside_Service_Client_Count": int(outside_service_client_count),

        "Outside_Service_Active_Count": int(outside_service_active_count),

        "Outside_Service_Active_Pct": round(
            (outside_service_active_count / client_count) * 100,
            2,
        ) if client_count else None,
    }

    for cat in age_categories:
        base = age_field_map[cat]

        count_field = f"{base}_Count"
        pct_field = f"{base}_Pct"

        cat_count = int((group["AgeCategory"] == cat).sum())

        row[count_field] = cat_count

        row[pct_field] = round(
            (cat_count / client_count) * 100,
            2,
        ) if client_count else None

    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)

print(f"Block groups with at least one client: {len(summary_df):,}")

if len(summary_df) == 0:
    raise RuntimeError("Aggregation produced zero block groups. Stopping.")


# ============================================================
# SECTION 17 — CREATE CLEAN BLOCK GROUP SUMMARY TABLE
# ============================================================

summary_table = create_summary_table_from_dataframe(
    df=summary_df,
    out_gdb=GDB_PATH,
    table_name="Block_Group_Client_Summary",
    field_type_overrides={
        BG_ID_FIELD: {"type": "TEXT", "length": 20},
    },
)

print(f"Created block group summary table: {summary_table}")


# ============================================================
# SECTION 18 — JOIN SUMMARY TO BLOCK GROUP POLYGONS
# ============================================================

bg_all_output = os.path.join(
    GDB_PATH,
    "Paratransit_Client_BlockGroups_All",
)

arcpy.management.CopyFeatures(
    BLOCK_GROUPS_FC,
    bg_all_output,
)

arcpy.management.JoinField(
    in_data=bg_all_output,
    in_field=BG_ID_FIELD,
    join_table=summary_table,
    join_field=BG_ID_FIELD,
)

print(f"Joined summary metrics to block groups: {bg_all_output}")


# ============================================================
# SECTION 19 — FILTER TO BLOCK GROUPS WITH CLIENTS
# ============================================================

bg_with_clients = os.path.join(
    GDB_PATH,
    "Paratransit_Client_BlockGroups_WithClients",
)

arcpy.analysis.Select(
    in_features=bg_all_output,
    out_feature_class=bg_with_clients,
    where_clause="Client_Count IS NOT NULL AND Client_Count > 0",
)

print(f"Created block groups with clients: {bg_with_clients}")
print(f"Block groups with clients count: {get_count(bg_with_clients):,}")


# ============================================================
# SECTION 20 — ADD SERVICE AREA INTERSECTION FLAG
# ============================================================

bg_service_join = os.path.join(
    GDB_PATH,
    "Paratransit_Client_BlockGroups_ServiceJoin",
)

arcpy.analysis.SpatialJoin(
    target_features=bg_with_clients,
    join_features=service_area_local,
    out_feature_class=bg_service_join,
    join_operation="JOIN_ONE_TO_ONE",
    join_type="KEEP_ALL",
    match_option="INTERSECT",
)

add_field_if_missing(
    bg_service_join,
    "In_Service_Area",
    "TEXT",
    field_length=3,
)

with arcpy.da.UpdateCursor(
    bg_service_join,
    ["Join_Count", "In_Service_Area"],
) as cursor:
    for join_count, in_service_area in cursor:
        flag = "Yes" if join_count and join_count > 0 else "No"
        cursor.updateRow([join_count, flag])

print(f"Added service area flag: {bg_service_join}")


# ============================================================
# SECTION 21 — APPLY SMALL COUNT SUPPRESSION TO MAIN BG LAYER
# ============================================================

bg_publish = os.path.join(
    GDB_PATH,
    "Paratransit_Client_BlockGroups_Publish",
)

arcpy.analysis.Select(
    in_features=bg_service_join,
    out_feature_class=bg_publish,
    where_clause=f"Client_Count >= {MIN_CLIENT_COUNT_TO_PUBLISH}",
)

publish_count = get_count(bg_publish)

print(f"Block groups meeting main publish threshold: {publish_count:,}")

if publish_count == 0:
    raise RuntimeError(
        "No block groups meet the minimum client count threshold. "
        "Publishing stopped."
    )


# ============================================================
# SECTION 22 — DELETE NON-PUBLISH FIELDS FROM MAIN BG LAYER
# ============================================================

safe_fields = {
    "OBJECTID",
    "FID",
    "Shape",
    "Shape_Length",
    "Shape_Area",

    BG_ID_FIELD,

    "Client_Count",
    "Pct_Total_Clients",

    "Active_Client_Count",
    "Active_Client_Pct",

    "Scheduled_Rider_Count",
    "Scheduled_Rider_Pct",

    "Trip_Count",
    "Trips_Per_Client",
    "Trips_Per_Active_Client",

    "Outside_Service_Client_Count",
    "Outside_Service_Active_Count",
    "Outside_Service_Active_Pct",

    "Age_1_10_Count",
    "Age_1_10_Pct",
    "Age_10_18_Count",
    "Age_10_18_Pct",
    "Age_18_25_Count",
    "Age_18_25_Pct",
    "Age_25_35_Count",
    "Age_25_35_Pct",
    "Age_35_45_Count",
    "Age_35_45_Pct",
    "Age_45_55_Count",
    "Age_45_55_Pct",
    "Age_55_65_Count",
    "Age_55_65_Pct",
    "Age_65_75_Count",
    "Age_65_75_Pct",
    "Age_75_85_Count",
    "Age_75_85_Pct",
    "Age_85Plus_Count",
    "Age_85Plus_Pct",

    "In_Service_Area",
}

fields_to_delete = []

for field in arcpy.ListFields(bg_publish):
    if field.required:
        continue

    if field.name not in safe_fields:
        fields_to_delete.append(field.name)

if fields_to_delete:
    arcpy.management.DeleteField(bg_publish, fields_to_delete)

    print("Deleted non-publish fields from main block group layer:")
    for f in fields_to_delete:
        print(f"  - {f}")
else:
    print("No non-publish fields found in main block group layer.")


# ============================================================
# SECTION 23 — CREATE OUTSIDE-SERVICE ACTIVE CLIENT BG LAYER
# ============================================================
# This is the privacy-safe "masked" geospatial layer.
# It shows block groups, not individual home points.

outside_active_bg = os.path.join(
    GDB_PATH,
    "Outside_Service_Active_Client_BlockGroups",
)

arcpy.analysis.Select(
    in_features=bg_publish,
    out_feature_class=outside_active_bg,
    where_clause=(
        "Outside_Service_Active_Count IS NOT NULL "
        f"AND Outside_Service_Active_Count >= {MIN_OUTSIDE_ACTIVE_COUNT_TO_PUBLISH}"
    ),
)

outside_active_bg_count = get_count(outside_active_bg)

print(f"Outside-service active client block groups: {outside_active_bg_count:,}")
print(f"Outside-service active BG layer: {outside_active_bg}")


# ============================================================
# SECTION 24 — FINAL PRIVACY VALIDATION
# ============================================================

blocked_field_names = {
    "ClientId",
    "ClientID",
    "Client_Id",
    "Client_ID",
    "DOB",
    "DateofBirth",
    "BirthDate",
    "CurrentAge",
    "ExactAge",
    "Longitude",
    "Latitude",
    "lon",
    "lat",
    "LON",
    "LAT",
    "AddrName",
    "Address",
    "RawAddress",
    "Raw_Address",
    "Pickup Address",
    "Dropoff Address",
    "Pickup_Address",
    "Dropoff_Address",
}

for fc_to_check in [bg_publish, outside_active_bg]:
    final_fields = {f.name for f in arcpy.ListFields(fc_to_check)}
    blocked_found = final_fields.intersection(blocked_field_names)

    if blocked_found:
        raise RuntimeError(
            f"Privacy validation failed for {fc_to_check}. "
            f"Blocked fields found: {sorted(blocked_found)}"
        )

print("Privacy validation passed.")
print("No client-level identifiers, DOBs, exact ages, coordinates, or addresses are in publish layers.")


# ============================================================
# SECTION 25 — OPTIONAL CLEANUP OF TEMPORARY SENSITIVE OUTPUTS
# ============================================================

if DELETE_TEMP_PRIVACY_OUTPUTS:
    print("Deleting temporary sensitive processing outputs...")

    safe_delete(client_points)
    safe_delete(client_service_join_raw)
    safe_delete(client_points_with_service)
    safe_delete(points_with_bg)
    safe_delete(client_csv)
    safe_delete(client_csv_with_service)

else:
    print("Temporary sensitive processing outputs were retained locally.")


# ============================================================
# SECTION 26 — ADD FINAL OUTPUTS TO CURRENT MAP
# ============================================================

try:
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    active_map = aprx.activeMap

    if active_map is None:
        active_map = aprx.createMap("Paratransit Client BG Analysis")

    for lyr in list(active_map.listLayers()):
        if not lyr.isBasemapLayer:
            active_map.removeLayer(lyr)

    active_map.addDataFromPath(bg_publish)
    active_map.addDataFromPath(outside_active_bg)
    active_map.addDataFromPath(service_area_local)
    active_map.addDataFromPath(service_summary_table)

    print("Added final layers/tables to the current ArcGIS Pro map.")

except Exception as ex:
    print(f"Warning: could not add final outputs to current map: {ex}")


# ============================================================
# SECTION 27 — PUBLISH TO AGOL
# ============================================================

if PUBLISH_TO_AGOL:
    print("Publishing final privacy-safe layers to AGOL...")

    publish_feature_layer_from_fc(
        feature_class=bg_publish,
        service_name=BLOCK_GROUP_SERVICE_NAME,
        portal_url=PORTAL_URL,
        username=AGOL_USERNAME,
        password=AGOL_PASSWORD,
        overwrite=OVERWRITE_EXISTING_SERVICE,
    )

    publish_feature_layer_from_fc(
        feature_class=outside_active_bg,
        service_name=OUTSIDE_ACTIVE_BG_SERVICE_NAME,
        portal_url=PORTAL_URL,
        username=AGOL_USERNAME,
        password=AGOL_PASSWORD,
        overwrite=OVERWRITE_EXISTING_SERVICE,
    )

    publish_feature_layer_from_fc(
        feature_class=service_area_local,
        service_name=SERVICE_AREA_SERVICE_NAME,
        portal_url=PORTAL_URL,
        username=AGOL_USERNAME,
        password=AGOL_PASSWORD,
        overwrite=OVERWRITE_EXISTING_SERVICE,
    )

else:
    print("PUBLISH_TO_AGOL is False. Skipping AGOL publishing.")


# ============================================================
# SECTION 28 — COMPLETE
# ============================================================

print("============================================================")
print("Paratransit Client Block Group Aggregation Complete")
print("============================================================")
print(f"Final main block group layer: {bg_publish}")
print(f"Final outside-service active BG layer: {outside_active_bg}")
print(f"Final service area layer: {service_area_local}")
print(f"Service type summary table: {service_summary_table}")
print(f"Service type summary CSV: {service_summary_csv}")
print(f"Minimum client count for main BG layer: {MIN_CLIENT_COUNT_TO_PUBLISH}")
print(f"Minimum outside-service active count layer: {MIN_OUTSIDE_ACTIVE_COUNT_TO_PUBLISH}")
print(f"Active client trip threshold: {ACTIVE_CLIENT_MIN_TRIPS}")
print("Published layers contain aggregated metrics only.")
print("============================================================")
