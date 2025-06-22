import os
import requests
import pandas as pd
import mysql.connector
from openpyxl.styles import PatternFill, Alignment, Border, Side
from dotenv import load_dotenv

load_dotenv()

AQUIFER_API_KEY = os.environ.get("AQUIFER_API_KEY")
AQUIFER_HEADERS = {"api-key": AQUIFER_API_KEY}
AQUIFER_BASE_URL = os.environ.get("AQUIFER_BASE_URL")

all_sli_categories = set()

# Define output directory and ensure it exists
OUTPUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EXCEL_FILE_PATH = os.path.join(OUTPUT_DIR, "kr1_report.xlsx")

db_config = {
    'host': os.environ.get("DB_HOST"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'database': os.environ.get("DATABASE"),
    'ssl_ca': os.environ.get("SSL_CERT_LOCATION"),
}

GET_STRATEGIC_RESOURCES_QUERY = """
    SELECT 
        CONCAT(`language`, ' - [', bcp47, ']') AS `Strategic Language`, 
        bcp47 as language_code_2,
        iso_629_2 as language_code_3,
        resource_level AS `Resource Level`
    FROM sli_language_data sld
    WHERE resource_level IS NOT NULL AND resource_level > 0
    ORDER BY resource_level desc, `language` asc;
"""

GET_STRATEGIC_RESOURCE_TYPES_QUERY = """
    select r.resource_name as resource_code, srt.resource_type 
    from resources r 
    inner join resource_type_mapping rtm on r.resource_id = rtm.resource_id 
    inner join sli_resource_type srt on srt.resource_type_id = rtm.resource_type_id 
"""

# Define a color map for headers
header_color_map = {
    "Foundational BT Training Videos": ("93c47d", "d9ead3"),  # Light blue header, very light blue cells
    "Foundational Bible Stories": ("93c47d", "d9ead3"),  # Light green header, very light green cells
    "Bible Translation Source Text (Audio Preferred)": ("ffe599", "fff2cc"),
    "Key Biblical Concepts Resource": ("ffe599", "fff2cc"),
    "Exegetical Notes": ("6d9eeb", "c9daf8"),
    "Translation Guide": ("6d9eeb", "c9daf8"),
    "Bible Dictionary": ("6d9eeb", "c9daf8"),
    "Comprehension Testing": ("6d9eeb", "c9daf8"),
    "Requisite Dataset - L3": ("6d9eeb", "c9daf8"),
    "Bible Aligned to Greek": ("cc4125", "f4cccc"),
    "Bible Aligned to Hebrew": ("cc4125", "f4cccc"),
    "Exegetical Commentary": ("cc4125", "f4cccc"),
    "Bible Translation Manual": ("cc4125", "f4cccc"),
    "Greek Semantic Lexicons": ("cc4125", "f4cccc"),
    "Hebrew Semantic Lexicons": ("cc4125", "f4cccc"),
    "Greek Grammars": ("cc4125", "f4cccc"),
    "Hebrew Grammars": ("cc4125", "f4cccc"),
    "Requisite Dataset - L4": ("cc4125", "f4cccc")
}

dcs_resource_type_map = {
    "Translation Academy": "Bible Translation Manual",
    "TSV Translation Notes": "",
    "": ""
}


def fetch_aquifer_api_data(endpoint):
    """Fetch JSON data from Aquifer API"""
    url = f"{AQUIFER_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=AQUIFER_HEADERS)
    if response.status_code != 200:
        raise Exception(f"API request to {endpoint} failed with status {response.status_code}")
    return response.json()


def get_aquifer_resources():
    """Fetch and process resource types"""
    aquifer_resource_data = fetch_aquifer_api_data("resources/types")
    df = pd.json_normalize(
        aquifer_resource_data,
        record_path=["collections"],
        meta=["type"],
        errors="ignore"
    ).rename(columns={
        "licenseInformation.title": "licenseInformation$title",
        "licenseInformation.copyright.dates": "licenseInformation$copyright$dates",
        "licenseInformation.copyright.holder.name": "licenseInformation$copyright$holder$name",
        "licenseInformation.copyright.holder.url": "licenseInformation$copyright$holder$url",
        "licenseInformation.licenses": "licenseInformation$licenses",
        "licenseInformation.showAdaptationNoticeForEnglish": "licenseInformation$showAdaptationNoticeForEnglish",
        "licenseInformation.showAdaptationNoticeForNonEnglish": "licenseInformation$showAdaptationNoticeForNonEnglish",
    })
    return df


def get_resource_collections(resource_codes):
    """Fetch collections for each resource and extract relevant language details."""
    collections = []

    for collection_code in resource_codes:
        collection_data = fetch_aquifer_api_data(f"resources/collections/{collection_code}")
        sli_category = collection_data.get("sliCategory", None)
        if sli_category:
            all_sli_categories.add(sli_category)
        else:
            raise AttributeError("missing sli category!!!")

        # Extract language details (normalize availableLanguages list)
        languages_data = pd.json_normalize(
            collection_data.get("availableLanguages", []),
            errors="ignore"
        )

        if not languages_data.empty:
            languages_data["code"] = collection_data.get("code")
            languages_data["display_name"] = collection_data.get("displayName")
            languages_data["resource_type"] = sli_category
            languages_data["resource_owner"] = collection_data.get("licenseInfo", {}).get("copyright", {}).get("holder", {}).get("name")
        else:
            languages_data = pd.DataFrame([{
                "languageId": None,
                "languageCode": None,
                "displayName": None,
                "resourceItemCount": None,
                "code": collection_data.get("code"),
                "display_name": collection_data.get("displayName"),
                "resource_type": sli_category,
                "resource_owner": collection_data.get("licenseInfo", {}).get("copyright", {}).get("holder", {}).get("name")
            }])

        collections.append(languages_data)

    combined_df = pd.concat(collections, ignore_index=True) if collections else pd.DataFrame()
    print("sli categories\n______________")
    print(", ".join(sorted(all_sli_categories)))
    return combined_df


def get_languages():
    """Fetch language data from Aquifer"""
    return pd.DataFrame(fetch_aquifer_api_data("languages"))


def get_status(pct):
    if pct >= 90:
        return "Satisfied"
    elif pct > 0:
        return "In Progress"
    return ""


def get_bibles(aq_langs):
    """Fetch Bible resources and merge with languages"""
    aq_bibles = pd.DataFrame(fetch_aquifer_api_data("bibles"))
    aq_bibles["resource_status"] = "Satisfied"

    # Merge with language data
    bibles = aq_bibles.merge(aq_langs, left_on="languageId", right_on="id", how="inner")
    return bibles[["englishDisplay", "name", "resource_status", "code"]].rename(
        columns={"name": "resource_code", "code": "languageCode"}
    )


def generate_aquifer_resource_data():
    """Master function to generate total gaps resource data"""
    aq_resources = get_aquifer_resources()
    collections_df = get_resource_collections(aq_resources["code"])
    aq_langs = get_languages()
    bibles_df = get_bibles(aq_langs)

    tight_collection = collections_df[["languageId", "languageCode", "code", "resource_type", "resourceItemCount"]].copy()
    base_resource_count = tight_collection[tight_collection["languageId"] == 1][["code", "resourceItemCount"]].rename(
        columns={"resourceItemCount": "base_count"}
    )

    lang_factor = aq_langs["id"].tolist()
    resource_factor = aq_resources["code"].tolist()

    tight_collection["languageId"] = pd.Categorical(tight_collection["languageId"], categories=lang_factor)
    tight_collection["code"] = pd.Categorical(tight_collection["code"], categories=resource_factor)

    full_index = pd.MultiIndex.from_product([lang_factor, resource_factor], names=["languageId", "code"])
    aquifer_gaps = tight_collection.set_index(["languageId", "code"]).reindex(full_index).reset_index()

    aquifer_gaps["languageId"] = pd.to_numeric(aquifer_gaps["languageId"], errors="coerce")
    aquifer_gaps["code"] = aquifer_gaps["code"].astype(str)

    total_gaps = aquifer_gaps.merge(aq_langs, left_on="languageId", right_on="id", how="inner")
    total_gaps = total_gaps.merge(aq_resources, left_on="code_x", right_on="code", how="inner")
    total_gaps["resource_owner"] = total_gaps["licenseInformation$copyright$holder$name"]

    total_gaps = total_gaps[["languageId", "languageCode", "code", "englishDisplay", "type", "resource_type",
                             "resource_owner", "resourceItemCount"]].rename(
        columns={"code": "resource_code"}
    )
    total_gaps = total_gaps.merge(base_resource_count, left_on="resource_code", right_on="code", how="left")
    total_gaps["resourceItemCount"] = total_gaps["resourceItemCount"].fillna(0).astype(int)
    total_gaps["base_count"] = total_gaps["base_count"].fillna(1)
    total_gaps["completion_pct"] = round((total_gaps["resourceItemCount"] / total_gaps["base_count"]) * 100, 3)
    total_gaps["resource_status"] = total_gaps["completion_pct"].apply(get_status)
    total_gaps = total_gaps[["englishDisplay", "languageCode", "resource_code", "resource_status", "resource_type"]]
    total_gaps = pd.concat([total_gaps, bibles_df], ignore_index=True)

    return total_gaps


def fetch_slr_data():
    connection = mysql.connector.connect(**db_config)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(GET_STRATEGIC_RESOURCES_QUERY)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        return df
    finally:
        connection.close()


def fetch_resource_data():
    """Fetch resource data from MySQL and return as a DataFrame."""
    connection = mysql.connector.connect(**db_config)

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(GET_STRATEGIC_RESOURCE_TYPES_QUERY)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        return df

    finally:
        connection.close()  # Ensure connection is closed even if an error occurs


def fetch_dcs_data():
    url = "https://git.door43.org/api/v1/repos/search?topic=tc-ready"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    rows = []
    if "data" in data:
        for obj in data["data"]:
            row = [
                obj.get("language", "N/A"),
                obj.get("language", "N/A").split("-", 1)[0],
                obj.get("subject", "N/A"),
                "Satisfied"
            ]
            rows.append(row)
    df_columns = ["englishDisplay", "languageCode", "resource_code", "resource_status"]
    df = pd.DataFrame(rows, columns=df_columns)
    return df


def calculate_status_from_resources(resources_df):
    final_status = ""
    for _, row in resources_df.iterrows():
        status = row["resource_status"]
        if status is not None and ((status == 'Satisfied') or (status == 'In Progress' and final_status == "")):
            final_status = status

    return final_status


def save_to_excel(sl_resource_data, crd, headers, file_path=EXCEL_FILE_PATH):
    """Expand 'data' by adding columns from 'pdf_headers' based on 'resource_status' in 'combined_resource_data'."""
    expanded_rows = []

    # Loop through each row in 'sl_resource_data' and dynamically add columns
    for _, row in sl_resource_data.iterrows():
        language_code_2 = row["language_code_2"]
        language_code_3 = row["language_code_3"]

        new_row = list(row.values)

        for resource_type in headers:
            # Find the corresponding resource_status where resource_type matches pdf_header
            matching_resources = crd[
                (crd["resource_type"] == resource_type) &
                ((crd["languageCode"] == language_code_2) | (crd["languageCode"] == language_code_3))
                ]

            final_status = calculate_status_from_resources(matching_resources)
            resource_status = final_status

            new_row.append(resource_status)

        expanded_rows.append(new_row)

    expanded_df = pd.DataFrame(expanded_rows,
                               columns=["Strategic Language", "language_code_2", "language_code_3",
                                        "resource_level"] + headers)
    expanded_df.rename(columns={"resource_level": "Resource Level"}, inplace=True)
    expanded_df.drop(columns=["language_code_2", "language_code_3"], inplace=True)

    # Save to Excel
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        expanded_df.to_excel(writer, sheet_name="Strategic Resources", index=False)

        # Apply styling
        worksheet = writer.sheets["Strategic Resources"]

        # ✅ Remove the weird empty first column (shift column alignment)
        for col_idx, col_name in enumerate(expanded_df.columns, start=1):  # Start at 1 (Excel is 1-based)
            # Set text alignment to left
            for cell in worksheet[1]:
                cell.alignment = Alignment(horizontal="left")

            # Apply color fill if it's in our color map
            if col_name in header_color_map:
                header_color, cell_color = header_color_map[col_name]

                # Apply header color
                worksheet.cell(row=1, column=col_idx).fill = PatternFill(
                    start_color=header_color, end_color=header_color, fill_type="solid"
                )

                # Apply cell color for all rows in that column
                for row_idx in range(2, worksheet.max_row + 1):  # Start from second row
                    worksheet.cell(row=row_idx, column=col_idx).fill = PatternFill(
                        start_color=cell_color, end_color=cell_color, fill_type="solid"
                    )

        # Apply borders around each cell
        thin_border = Border(
            left=Side(border_style="thin", color="000000"),
            right=Side(border_style="thin", color="000000"),
            top=Side(border_style="thin", color="000000"),
            bottom=Side(border_style="thin", color="000000"),
        )

        for row in worksheet.iter_rows():
            for cell in row:
                cell.border = thin_border

        # Auto-adjust column widths
        for col in worksheet.columns:
            max_length = max((len(str(cell.value)) for cell in col if cell.value), default=10)
            worksheet.column_dimensions[col[0].column_letter].width = max_length + 2  # Add padding

    print(f"Excel file saved at {file_path}")


slr_data = fetch_slr_data()
aquifer_data = generate_aquifer_resource_data()
dcs_data = fetch_dcs_data()
combined_data = pd.concat([aquifer_data, dcs_data], ignore_index=True)

# combine data pulled from FRED DB and aquifer
resource_data = fetch_resource_data()
combined_resource_data = resource_data.merge(combined_data, on="resource_code", how="inner")

save_to_excel(slr_data, combined_resource_data, sorted(all_sli_categories), EXCEL_FILE_PATH)
print("KR1 delta report successfully generated!")
