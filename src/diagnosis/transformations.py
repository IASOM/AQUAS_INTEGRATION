# ========================
# TRANSFORMACIO DEL CHUNK
# ========================
import pandas as pd 
import re 
from typing import Optional


def normalize_icd10_code(code) -> str | None:
    if pd.isna(code):
        return None
    
    code = str(code).upper().strip()
    code = re.sub(r"[^A-Z0-9]", "", code)

    if len(code) < 3:
        return None
    
    if not code[0].isalpha() or not code[1:3].isdigit():
        return None

    return code

def normalize_icd10_3(code) -> str | None:
    if pd.isna(code):
        return None
    
    code = str(code).upper().strip()
    code = re.sub(r"[^A-Z0-9]", "", code)

    match = re.search(r"[A-Z][0-9]{2}", code)
    if match is None:
        return None
    return match.group(0)

def icd10_to_number(code) -> int | None:
    code = normalize_icd10_3(code)
    if code is None:
        return None

    letter = code[0]
    number = int(code[1:3])

    return (ord(letter) - ord("A")) * 100 + number




def to_icd10_3(code: object) -> Optional[str]:
    code = normalize_icd10_code(code)
    
    if code is None:
        return None
    
    return code[:3]

def _icd10_to_ordinal(code3: str) -> int:
    return (ord(code3[0]) - ord("A")) * 100 + int(code3[1:3])

def _match_range(code3:str, ranges) -> str:
    value = _icd10_to_ordinal(code3)
    
    for name,start, end in ranges:
        if _icd10_to_ordinal(start) <= value <= _icd10_to_ordinal(end):
            return name 

        return "UNKNOWN"


def match_icd10_range(code, ranges) -> str:
    value = _icd10_to_ordinal(code)

    if value is None:
        return "UNKNOWN"

    for name, start, end in ranges:
        start_value = icd10_to_number(start)
        end_value = icd10_to_number(end)

        if start_value is None or end_value is None:
            continue
        
        if start_value <= value <= end_value:
            return name
    
    return "UNKNOWN"

def chapter_from_icd10_3(code:str) -> str:
    return match_icd10_range(code, get_icd10_chapter_ranges())

def subchapter_from_icd10_3(code:str) -> str:
    return match_icd10_range(code, get_icd10_subchapter_ranges())


def prepare_diagnosis_chunk(
    df:pd.DataFrame, 
    up_rs: pd.DataFrame,
    date_column: str = "DATA_VISITA",
    up_column: str ="UP",
    diag_code_column: str = "DIAG_CODE",
) -> pd.DataFrame:
    
    out = df.copy()
    rename_map = {}

    if date_column in out.columns and date_column != "DATA_VISITA":
        rename_map[date_column] = "DATA_VISITA"
    if up_column in out.columns and up_column != "UP":
        rename_map[up_column] = "UP"
    if diag_code_column in out.columns and diag_code_column != "DIAG_CODE":
        rename_map[diag_code_column] = "DIAG_CODE"
    if rename_map:
        out = out.rename(columns=rename_map)

    required = {"DATA_VISITA", "UP", "DIAG_CODE"}
    missing = required.difference(out.columns)
    
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["DATA_VISITA"] = pd.to_datetime(out["DATA_VISITA"], errors="coerce").dt.floor("D")
    out["UP"] = out["UP"].astype(str).str.zfill(5).str.strip()
    out["ICD10_CODE"] = out["DIAG_CODE"].map(normalize_icd10_code)
    out["ICD10_3"] = out["ICD10_CODE"].map(normalize_icd10_3)
    out = out.dropna(subset=["DATA_VISITA", "UP", "ICD10_3"]).copy()
    out["n"] = 1

    reduced = out.groupby(
        ["DATA_VISITA", "UP", "ICD10_3"],
        as_index=False
    ).sum().sort_values(["DATA_VISITA", "UP", "ICD10_3"])

    # Lookup UP -> RS
    lookup = up_rs.copy()
    lookup.columns = [str(c).strip() for c in lookup.columns] 

    if "Codi UP" not in lookup.columns or "RS" not in lookup.columns:
        raise ValueError("up_rs must contain columns 'Codi UP' and 'RS'")

    lookup["Codi UP"] = lookup["Codi UP"].astype(str).str.zfill(5).str.strip()
    lookup["RS"] = lookup["RS"].fillna("RS:SENSEESPECIFICAR_SE").astype(str).str.strip()
    lookup = lookup[["Codi UP", "RS"]].drop_duplicates()



    reduced = reduced.merge(
        lookup,
        left_on="UP",
        right_on="Codi UP",
        how="left",
    ).drop(columns=["Codi UP"])

    reduced["RS"] = reduced["RS"]. fillna("RS:SENSEESPECIFICAR_SE")
    reduced["ICD10_CHAPTER"] = reduced["ICD10_3"].map(chapter_from_icd10_3)
    reduced["ICD10_SUBCHAPTER"] = reduced["ICD10_3"].map(subchapter_from_icd10_3)

    unknown_counts = reduced.loc[
        reduced["ICD10_CHAPTER"].eq("UNKNOWN"),
        ["DIAG_CODE", "ICD10_CODE", "ICD10_3"]
    ].drop_duplicates()

    if not unknown_counts.empty:
        print(f"[Unknown ICD10 chapter - original codes:")
        print(unknown_counts.head(100).to_string())

    return reduced


def get_icd10_chapter_ranges():
    return [
        ("chapter_01", "A00", "B99"), 
        ("chapter_02", "C00", "D48"), 
        ("chapter_03", "D50", "D89"), 
        ("chapter_04", "D50", "D89"), 
        ("chapter_05", "E00", "E90"), 
        ("chapter_06", "F00", "F99"), 
        ("chapter_07", "G00", "G99"), 
        ("chapter_08", "H00", "H59"), 
        ("chapter_09", "H60", "I95"), 
        ("chapter_10", "I00", "I99"), 
        ("chapter_11", "J00", "J99"), 
        ("chapter_12", "L00", "L99"), 
        ("chapter_13", "M00", "M99"), 
        ("chapter_14", "N00", "N99"), 
        ("chapter_15", "O00", "O99"), 
        ("chapter_16", "P00", "P96"), 
        ("chapter_17", "Q00", "Q99"), 
        ("chapter_18", "R00", "R99"), 
        ("chapter_19", "S00", "T98"), 
        ("chapter_20", "V01", "Y98"), 
        ("chapter_21", "Z00", "Z99"), 
        ("chapter_22", "U00", "U99"), 
    ]

def get_icd10_subchapter_ranges():
    return [
        ("SUBchapter_01_01", "A00", "A09"),     
        ("SUBchapter_01_02", "A15", "A19"),    
        ("SUBchapter_01_03", "A20", "A28"),    
        ("SUBchapter_01_04", "A30", "A49"),    
        ("SUBchapter_01_05", "A50", "A64"),    
        ("SUBchapter_01_06", "A65", "A69"),    
        ("SUBchapter_01_07", "A70", "A74"),    
        ("SUBchapter_01_08", "A75", "A79"),    
        ("SUBchapter_01_09", "A80", "A89"),    
        ("SUBchapter_01_10", "A92", "A99"),  
    ]

"""("SUBchapter_01_11", "B00", "B09"), 
        ("SUBchapter_01_12", "B15", "B19"),    
        ("SUBchapter_01_13", "B20", "B24"),    
        ("SUBchapter_01_14", "B25", "B34"),    
        ("SUBchapter_01_15", "B35", "B49"),    
        ("SUBchapter_01_16", "B50", "B64"),    
        ("SUBchapter_01_17", "B65", "B83"),    
        ("SUBchapter_01_18", "B85", "B89"),    
        ("SUBchapter_01_19", "B90", "B94"),    
        ("SUBchapter_01_20", "B95", "B98"),    
        ("SUBchapter_01_21", "B99", "B99"),    
        ("SUBchapter_02_01", "C00", "C97"),    
        ("SUBchapter_02_02", "D00", "D09"),    
        ("SUBchapter_02_03", "D10", "D36"),    
        ("SUBchapter_02_04", "D37", "D48"),    
        ("SUBchapter_03_01", "D50", "D53"),     
        ("SUBchapter_03_02", "D55", "D59"),    
        ("SUBchapter_03_03", "D60", "D64"),    
        ("SUBchapter_03_04", "D65", "D69"),    
        ("SUBchapter_03_05", "D70", "D77"),    
        ("SUBchapter_03_06", "D80", "D89"), 
        ("SUBchapter_04_01", "E00", "E07"),     
        ("SUBchapter_04_02", "E10", "E14"),    
        ("SUBchapter_04_03", "E15", "E16"),    
        ("SUBchapter_04_04", "E20", "E35"),    
        ("SUBchapter_04_05", "E40", "E46"),    
        ("SUBchapter_04_06", "E50", "E64"),    
        ("SUBchapter_04_07", "E65", "E68"),    
        ("SUBchapter_04_08", "E70", "E90"), 
        ("SUBchapter_05_01", "F00", "F09"),     
        ("SUBchapter_05_02", "F10", "F19"),    
        ("SUBchapter_05_03", "F20", "F29"),    
        ("SUBchapter_05_04", "F30", "F39"),    
        ("SUBchapter_05_05", "F40", "F48"),    
        ("SUBchapter_05_06", "F50", "F59"),    
        ("SUBchapter_05_07", "F60", "F69"),    
        ("SUBchapter_05_08", "F70", "F79"),    
        ("SUBchapter_05_09", "F80", "F89"),    
        ("SUBchapter_05_10", "F90", "F98"),  
        ("SUBchapter_05_11", "F99", "F99"), 
        ("SUBchapter_06_01", "G00", "G09"),     
        ("SUBchapter_06_02", "G10", "G14"),    
        ("SUBchapter_06_03", "G20", "G26"),    
        ("SUBchapter_06_04", "G30", "G32"),    
        ("SUBchapter_06_05", "G30", "G37"),    
        ("SUBchapter_06_06", "G40", "G47"),    
        ("SUBchapter_06_07", "G50", "G59"),    
        ("SUBchapter_06_08", "G60", "G64"),    
        ("SUBchapter_06_09", "G70", "G73"),    
        ("SUBchapter_06_10", "G80", "G83"),  
        ("SUBchapter_06_11", "G90", "G99"), 
        ("SUBchapter_07_01", "H00", "H06"),     
        ("SUBchapter_07_02", "H10", "H13"),    
        ("SUBchapter_07_03", "H15", "H22"),    
        ("SUBchapter_07_04", "H25", "H28"),    
        ("SUBchapter_07_05", "H30", "H36"),    
        ("SUBchapter_07_06", "H40", "H42"),    
        ("SUBchapter_07_07", "H43", "H45"),    
        ("SUBchapter_07_08", "H46", "H48"),    
        ("SUBchapter_07_09", "H49", "H52"),    
        ("SUBchapter_07_10", "H53", "H54"),  
        ("SUBchapter_07_11", "H55", "H59"), 
        ("SUBchapter_08_01", "H60", "H62"),     
        ("SUBchapter_08_02", "H65", "H75"),    
        ("SUBchapter_08_03", "H80", "H83"),    
        ("SUBchapter_08_04", "H90", "H95"),  
        ("SUBchapter_09_01", "I00", "I02"),     
        ("SUBchapter_09_02", "I05", "I09"),    
        ("SUBchapter_09_03", "I10", "I15"),    
        ("SUBchapter_09_04", "I20", "I25"),    
        ("SUBchapter_09_05", "I26", "I28"),    
        ("SUBchapter_09_06", "I30", "I52"),    
        ("SUBchapter_09_07", "I60", "I69"),    
        ("SUBchapter_09_08", "I70", "I79"),    
        ("SUBchapter_09_09", "I80", "I89"),    
        ("SUBchapter_09_10", "I95", "I99"), 
        ("SUBchapter_10_01", "J00", "J06"),     
        ("SUBchapter_10_02", "J09", "J18"),    
        ("SUBchapter_10_03", "J20", "J22"),    
        ("SUBchapter_10_04", "J30", "J39"),    
        ("SUBchapter_10_05", "J40", "J47"),    
        ("SUBchapter_10_06", "J60", "J70"),    
        ("SUBchapter_10_07", "J80", "J84"),    
        ("SUBchapter_10_08", "J85", "J86"),    
        ("SUBchapter_10_09", "J90", "J94"),    
        ("SUBchapter_10_10", "J95", "J99"), 
        ("SUBchapter_11_01", "K00", "K14"),     
        ("SUBchapter_11_02", "K20", "K31"),    
        ("SUBchapter_11_03", "K35", "K38"),    
        ("SUBchapter_11_04", "K40", "K46"),    
        ("SUBchapter_11_05", "K50", "K52"),    
        ("SUBchapter_11_06", "K55", "K64"),    
        ("SUBchapter_11_07", "K65", "K67"),    
        ("SUBchapter_11_08", "K70", "K77"),    
        ("SUBchapter_11_09", "K80", "K87"),    
        ("SUBchapter_11_10", "K90", "K93"), 
        ("SUBchapter_12_01", "L00", "L08"),     
        ("SUBchapter_12_02", "L10", "L14"),    
        ("SUBchapter_12_03", "L20", "L30"),    
        ("SUBchapter_12_04", "L40", "L45"),    
        ("SUBchapter_12_05", "L50", "L54"),    
        ("SUBchapter_12_06", "L55", "L59"),    
        ("SUBchapter_12_07", "L60", "L75"),    
        ("SUBchapter_12_08", "L80", "L99"), 
        ("SUBchapter_13_01", "M00", "25"),     
        ("SUBchapter_13_02", "M30", "M36"),    
        ("SUBchapter_13_03", "M40", "M54"),    
        ("SUBchapter_13_04", "M60", "M79"),    
        ("SUBchapter_13_05", "M80", "M94"),    
        ("SUBchapter_13_06", "M95", "M99"),
        ("SUBchapter_14_01", "N00", "N08"),     
        ("SUBchapter_14_02", "N10", "N16"),    
        ("SUBchapter_14_03", "N17", "N19"),    
        ("SUBchapter_14_04", "N20", "N23"),    
        ("SUBchapter_14_05", "N25", "N29"),    
        ("SUBchapter_14_06", "N30", "N39"),    
        ("SUBchapter_14_07", "N40", "N51"),    
        ("SUBchapter_14_08", "N60", "N64"),    
        ("SUBchapter_14_09", "N70", "N77"),    
        ("SUBchapter_14_10", "N80", "N98"),  
        ("SUBchapter_14_11", "N99", "N99"),
        ("SUBchapter_15_01", "O00", "O08"),     
        ("SUBchapter_15_02", "O10", "O19"),    
        ("SUBchapter_15_03", "O20", "O29"),    
        ("SUBchapter_15_04", "O30", "O48"),    
        ("SUBchapter_15_05", "O60", "O75"),    
        ("SUBchapter_15_06", "O80", "O84"),    
        ("SUBchapter_15_07", "O85", "O92"),    
        ("SUBchapter_15_08", "O94", "O99"), 
        ("SUBchapter_16_01", "P00", "P04"),     
        ("SUBchapter_16_02", "P05", "P08"),    
        ("SUBchapter_16_03", "P10", "P15"),    
        ("SUBchapter_16_04", "P20", "P29"),    
        ("SUBchapter_16_05", "P35", "P39"),    
        ("SUBchapter_16_06", "P50", "P61"),    
        ("SUBchapter_16_07", "P70", "P74"),    
        ("SUBchapter_16_08", "P75", "P78"),    
        ("SUBchapter_16_09", "P80", "P83"),    
        ("SUBchapter_16_10", "P90", "P96"),  
        ("SUBchapter_17_01", "Q00", "Q07"),     
        ("SUBchapter_17_02", "Q10", "Q18"),    
        ("SUBchapter_17_03", "Q20", "Q28"),    
        ("SUBchapter_17_04", "Q30", "Q34"),    
        ("SUBchapter_17_05", "Q35", "Q37"),    
        ("SUBchapter_17_06", "Q38", "Q45"),    
        ("SUBchapter_17_07", "Q50", "Q56"),    
        ("SUBchapter_17_08", "Q60", "Q64"),    
        ("SUBchapter_17_09", "Q65", "Q79"),    
        ("SUBchapter_17_10", "Q80", "Q89"),  
        ("SUBchapter_17_11", "Q90", "Q99"),
        ("SUBchapter_18_01", "R00", "R09"),     
        ("SUBchapter_18_02", "R10", "R19"),    
        ("SUBchapter_18_03", "R20", "R23"),    
        ("SUBchapter_18_04", "R25", "R29"),    
        ("SUBchapter_18_05", "R30", "R39"),    
        ("SUBchapter_18_06", "R40", "R46"),    
        ("SUBchapter_18_07", "R47", "R49"),    
        ("SUBchapter_18_08", "R50", "R69"),    
        ("SUBchapter_18_09", "R70", "R79"),    
        ("SUBchapter_18_10", "R80", "R82"),  
        ("SUBchapter_18_11", "R83", "R89"), 
        ("SUBchapter_18_12", "R90", "R94"),    
        ("SUBchapter_18_13", "R95", "R99"), 
        ("SUBchapter_19_01", "S00", "S09"),     
        ("SUBchapter_19_02", "S10", "S19"),    
        ("SUBchapter_19_03", "S20", "S29"),    
        ("SUBchapter_19_04", "S30", "S39"),    
        ("SUBchapter_19_05", "S40", "S49"),    
        ("SUBchapter_19_06", "S50", "S59"),    
        ("SUBchapter_19_07", "S60", "S69"),    
        ("SUBchapter_19_08", "S70", "S79"),    
        ("SUBchapter_19_09", "S80", "S89"),    
        ("SUBchapter_19_10", "S90", "S99"), 
        ("SUBchapter_19_11", "T00", "T07"), 
        ("SUBchapter_19_12", "T08", "T14"),    
        ("SUBchapter_19_13", "T15", "T19"),    
        ("SUBchapter_19_14", "T20", "T32"),    
        ("SUBchapter_19_15", "T33", "T35"),    
        ("SUBchapter_19_16", "T36", "T50"),    
        ("SUBchapter_19_17", "T51", "T65"),    
        ("SUBchapter_19_18", "T66", "T78"),    
        ("SUBchapter_19_19", "T79", "T79"),    
        ("SUBchapter_19_20", "T80", "T88"),    
        ("SUBchapter_19_21", "T90", "T98"),   
        ("SUBchapter_20_01", "V01", "X59"),     
        ("SUBchapter_20_02", "X60", "X84"),    
        ("SUBchapter_20_03", "X85", "Y09"),    
        ("SUBchapter_20_04", "Y10", "Y34"),    
        ("SUBchapter_20_05", "Y35", "Y36"),    
        ("SUBchapter_20_06", "Y40", "Y84"),    
        ("SUBchapter_20_07", "Y85", "Y89"),    
        ("SUBchapter_20_08", "Y90", "Y98"), 
        ("SUBchapter_21_01", "Z00", "Z13"),     
        ("SUBchapter_21_02", "Z20", "Z29"),    
        ("SUBchapter_21_03", "Z30", "Z39"),    
        ("SUBchapter_21_04", "Z40", "Z54"),    
        ("SUBchapter_21_05", "Z55", "Z65"),    
        ("SUBchapter_21_06", "Z70", "Z76"),    
        ("SUBchapter_21_07", "Z80", "Z99"), 
        ("SUBchapter_22_01", "U00", "ZU49"),     
        ("SUBchapter_22_02", "U82", "U85"), 
    ]
"""
