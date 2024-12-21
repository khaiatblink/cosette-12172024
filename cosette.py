"""
Script to create CSV needed to generate RX images for ORTF prescriptions.

Input:
ORTF CSV File - CSV version of the ORTF file. CSV can be generated using ortf_parser in prescription service repo.
ORTF Output File - File generated as an output by GRx when ORTF is imported.
Note: CSV File and Output File should be of the same ORTF File.

Output:
CSV File containing all columns of ORTF CSV File with an additional 'SCRIPT NUMBER' column(1st) from output file.
CSV would be placed in the same location as ORTF CSV file with a suffix "-rximage.csv"

The output file can then be imported into Django UI to generate Rx Images for each of the script numbers mentioned
in the CSV. Image would be generated only if one does not exist in the system.

Sample Command:
python ortf_image_csv.py --ortf_csv CSV_FILE_PATH --ortf_output TEXT_INPUT_FILE_PATH

Example(files present in current directory):
python generate_ortf_csv_for_rx_image.py --ortf_csv "./avrio_20210714-smaller-001.csv" --ortf_output "./avrio_20210714-smaller-001.out"
"""

import argparse
import copy
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
import typing

date_format = (
    "%Y%m%d"  # for both RX records most_recent_date_filled and command-line parameters
)

REPLACE_RULES = {
    # these rules were added to map old NDCs to new NDCs and update related fields
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061019"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061014"),
        ("PRESCRIBED DRUG DESCRIPTION", "VASCULERA TABLETS 30"),
        ("PRODUCT DOSAGE FORM", "TABLET"),
        ("PRODUCT STRENGTH", "630 mg"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061118"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061116"),
        ("PRESCRIBED DRUG DESCRIPTION", "FOSTEUM PLUS CAPSULE 60"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "500 mg-70 mg-27"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075019"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075043"),
        ("PRESCRIBED DRUG DESCRIPTION", "RHEUMATE CAPSULE"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "1 mg-1 mg-500 m"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075260"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075240"),
        ("PRESCRIBED DRUG DESCRIPTION", "EPICERAM EMUL 225GM"),
        ("PRODUCT DOSAGE FORM", "EMULSION EXTENDED RELEAS"),
        ("PRODUCT STRENGTH", ""),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075280"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075240"),
        ("PRESCRIBED DRUG DESCRIPTION", "EPICERAM EMUL 225GM"),
        ("PRODUCT DOSAGE FORM", "EMULSION EXTENDED RELEAS"),
        ("PRODUCT STRENGTH", ""),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075018"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075043"),
        ("PRESCRIBED DRUG DESCRIPTION", "RHEUMATE CAPSULE"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "1 mg-1 mg-500 m"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040060318"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040060316"),
        ("PRESCRIBED DRUG DESCRIPTION", "FOSTEUM CAPSULE 60"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "27 mg-20 mg-200"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061016"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061014"),
        ("PRESCRIBED DRUG DESCRIPTION", "VASCULERA TABLETS 30"),
        ("PRODUCT DOSAGE FORM", "TABLET"),
        ("PRODUCT STRENGTH", "630 mg"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075016"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075043"),
        ("PRESCRIBED DRUG DESCRIPTION", "RHEUMATE CAPSULE"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "1 mg-1 mg-500 m"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "69482080099"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040071428"),
        ("PRESCRIBED DRUG DESCRIPTION", "SERNIVO 0.05% SPRAY"),
        ("PRODUCT DOSAGE FORM", "SPRAY WITH PUMP"),
        ("PRODUCT STRENGTH", ""),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061112"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040061116"),
        ("PRESCRIBED DRUG DESCRIPTION", "FOSTEUM PLUS CAPSULE 60"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "500 mg-70 mg-27"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040060312"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040060316"),
        ("PRESCRIBED DRUG DESCRIPTION", "FOSTEUM CAPSULE 60"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "27 mg-20 mg-200"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "51013080036"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075240"),
        ("PRESCRIBED DRUG DESCRIPTION", "EPICERAM EMUL 225GM"),
        ("PRODUCT DOSAGE FORM", "EMULSION EXTENDED RELEAS"),
        ("PRODUCT STRENGTH", ""),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075014"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075043"),
        ("PRESCRIBED DRUG DESCRIPTION", "RHEUMATE CAPSULE"),
        ("PRODUCT DOSAGE FORM", "CAPSULE"),
        ("PRODUCT STRENGTH", "1 mg-1 mg-500 m"),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "51013080090"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075217"),
        ("PRESCRIBED DRUG DESCRIPTION", "EPICERAM EMUL 90GM"),
        ("PRODUCT DOSAGE FORM", "EMULSION EXTENDED RELEAS"),
        ("PRODUCT STRENGTH", ""),
    ],
    ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "67857080090"): [
        ("ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE", "68040075217"),
        ("PRESCRIBED DRUG DESCRIPTION", "EPICERAM EMUL 90GM"),
        ("PRODUCT DOSAGE FORM", "EMULSION EXTENDED RELEAS"),
        ("PRODUCT STRENGTH", ""),
    ],
}

RX_FORMATS = {
    # this info is copied from the spec, which uses 1-based strings and inclusive delimiters
    # e.g. "RECORD TYPE" has length 2, starts at the 1st char (inclusive), and ends at the 2nd char (inclusive)
    # the RX records are stored in Python strings, which are 0-based and when sliced use an inclusive initial delimiter
    # and an exclusive final delimiter.  translating from spec delimiters to Python delimiters is done in the
    # RXPrescriptionRecord.read_fields() function
    "20": {
        "RECORD TYPE": ("601-04", "M", "A/N", 2, 1, 2),
        "CARDHOLDER ID": ("302-C2", "S", "A/N", 20, 3, 22),
        "ALTERNATE ID NUMBER": ("724-ST", "S", "A/N", 20, 23, 42),
        "CARDHOLDER LAST NAME": ("313-CD", "S", "A/N", 35, 43, 77),
        "CARDHOLDER FIRST NAME": ("312-CC", "S", "A/N", 35, 78, 112),
        "CARDHOLDER MIDDLE INITIAL": ("718-SZ", "S", "A/N", 1, 113, 113),
        "PATIENT LAST NAME": ("311-CB", "M", "A/N", 35, 114, 148),
        "PATIENT FIRST NAME": ("310-CA", "M", "A/N", 35, 149, 183),
        "PATIENT MIDDLE INITIAL": ("718-SZ", "S", "A/N", 1, 184, 184),
        "PATIENT RESIDENCE": ("384-4X", "S", "N", 2, 185, 186),
        "PATIENT ADDRESS LINE 1": ("726-SR", "M", "A/N", 30, 187, 216),
        "PATIENT ADDRESS LINE 2": ("727-SS", "S", "A/N", 30, 217, 246),
        "PATIENT CITY": ("728-SU", "M", "A/N", 20, 247, 266),
        "PATIENT STATE": ("729-TA", "M", "A/N", 2, 267, 268),
        "PATIENT ZIP/POSTAL CODE": ("730-TC", "M", "A/N", 15, 269, 283),
        "PATIENT TELEPHONE NUMBER QUALIFIER": ("629-SH", "S", "A/N", 2, 284, 285),
        "PATIENT TELEPHONE NUMBER": ("732-TB", "S", "N", 10, 286, 295),
        "PATIENT E-MAIL ADDRESS": ("350-HN", "S", "A/N", 80, 296, 375),
        "DATE OF BIRTH": ("304-C4", "M", "N", 8, 376, 383),
        "PATIENT GENDER CODE": ("305-C5", "M", "N", 1, 384, 384),
        "PREGNANCY INDICATOR": ("335-2C", "S", "A/N", 1, 385, 385),
        "SMOKER/NON-SMOKER CODE": ("334-1C", "S", "A/N", 1, 386, 386),
        "EASY OPEN CAP INDICATOR": ("608-NF", "S", "A/N", 1, 387, 387),
        "PRESCRIPTION/SERVICE REFERENCE NUMBER": ("402-D2", "M", "N", 12, 388, 399),
        "DATE PRESCRIPTION WRITTEN": ("414-DE", "M", "N", 8, 400, 407),
        "ORIGINALLY PRESCRIBED PRODUCT/SERVICE ID QUALIFIER": (
            "453-EJ",
            "M",
            "A/N",
            2,
            408,
            409,
        ),
        "ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE": (
            "445-EA",
            "M",
            "A/N",
            19,
            410,
            428,
        ),
        "COMPOUND CODE": ("406-D6", "M", "N", 1, 429, 429),
        "PRESCRIBED DRUG DESCRIPTION": ("619-RW", "M", "A/N", 60, 430, 489),
        "PRODUCT DOSAGE FORM": ("601-21", "M", "A/N", 30, 490, 519),
        "PRODUCT STRENGTH": ("601-24", "M", "A/N", 15, 520, 534),
        "DISPENSE AS WRITTEN (DAW)/PRODUCT SELECTION CODE": (
            "408-D8",
            "M",
            "A/N",
            1,
            535,
            535,
        ),
        "QUANTITY PRESCRIBED": ("460-ET", "M", "N", 10, 536, 545),
        "NUMBER OF REFILLS AUTHORIZED": ("415-DF", "M", "N", 2, 546, 547),
        "DAYS SUPPLY": ("405-D5", "M", "N", 3, 548, 550),
        "PRODUCT/SERVICE ID QUALIFIER": ("426-E1", "M", "A/N", 2, 551, 552),
        "PRODUCT/SERVICE ID": ("407-D7", "M", "A/N", 19, 553, 571),
        "DRUG DESCRIPTION": ("516-FG", "M", "A/N", 60, 572, 631),
        "LABEL DIRECTIONS": ("613-NM", "M", "A/N", 200, 632, 831),
        "ORIGINAL DISPENSED DATE": ("617-RQ", "M", "N", 8, 832, 839),
        "ORIGINAL DISPENSED QUANTITY": ("A44-ZL", "M", "N", 10, 840, 849),
        "MOST RECENT DATE FILLED": ("614-NW", "M", "N", 8, 850, 857),
        "QUANTITY DISPENSED TO DATE": ("623-SA", "M", "N", 10, 858, 867),
        "REMAINING QUANTITY": ("625-SC", "M", "N", 10, 868, 877),
        "NUMBER OF FILLS TO DATE": ("615-NY", "S", "N", 2, 878, 879),
        "NUMBER OF FILLS REMAINING": ("616-PU", "M", "N", 2, 880, 881),
        "DISCONTINUE DATE": ("607-ND", "S", "N", 8, 884, 891),
        "INACTIVE PRESCRIPTION INDICATOR": ("612-NK", "M", "A/N", 1, 892, 892),
        "TRANSFER FLAG": ("631-SK", "S", "A/N", 1, 893, 893),
        "PRESCRIBER LAST NAME": ("716-SY", "M", "A/N", 25, 894, 918),
        "PRESCRIBER FIRST NAME": ("717-SX", "M", "A/N", 15, 919, 933),
        "PRESCRIBER ADDRESS LINE 1": ("726-SR", "M", "A/N", 30, 934, 963),
        "PRESCRIBER ADDRESS LINE 2": ("727-SS", "S", "A/N", 30, 964, 993),
        "PRESCRIBER CITY": ("728-SU", "M", "A/N", 20, 994, 1013),
        "PRESCRIBER STATE": ("729-TA", "M", "A/N", 2, 1014, 1015),
        "PRESCRIBER ZIP/POSTAL CODE": ("730-TC", "M", "A/N", 15, 1016, 1030),
        "PRESCRIBER TELEPHONE NUMBER QUALIFIER": ("629-SH", "M", "A/N", 2, 1031, 1032),
        "PRESCRIBER TELEPHONE NUMBER": ("732-TB", "M", "N", 10, 1033, 1042),
        "PRESCRIBER ID (DEA)": ("411-DB", "S", "A/N", 15, 1043, 1057),
        "PRESCRIBER ID QUALIFIER": ("466-EZ", "M", "A/N", 2, 1058, 1059),
        "PRESCRIBER ID": ("411-DB", "M", "A/N", 15, 1060, 1074),
        "ADDITIONAL MESSAGE INFORMATION": ("526-FQ", "S", "A/N", 200, 1075, 1274),
        "PAYER ID QUALIFIER": ("568-J7", "S", "A/N", 2, 1275, 1276),
        "PAYER ID": ("569-J8", "S", "A/N", 10, 1277, 1286),
        "PROCESSOR CONTROL NUMBER": ("104-A4", "S", "A/N", 10, 1287, 1296),
        "GROUP ID": ("301-C1", "S", "A/N", 15, 1297, 1311),
        "PERSON CODE": ("303-C3", "S", "A/N", 3, 1312, 1314),
        "PATIENT RELATIONSHIP CODE": ("306-C6", "S", "N", 1, 1315, 1315),
        "FILLER": ("", "M", "A/N", 285, 1316, 1600),
    },
    "33": {
        "RECORD TYPE": ("601-04", "M", "A/N", 2, 1, 2),
        "CARDHOLDER ID": ("302-C2", "S", "A/N", 20, 3, 22),
        "ALTERNATE ID NUMBER": ("724-ST", "S", "A/N", 20, 23, 42),
        "CARDHOLDER LAST NAME": ("313-CD", "S", "A/N", 35, 43, 77),
        "CARDHOLDER FIRST NAME": ("312-CC", "S", "A/N", 35, 78, 112),
        "CARDHOLDER MIDDLE INITIAL": ("718-SZ", "S", "A/N", 1, 113, 113),
        "PATIENT LAST NAME": ("311-CB", "M", "A/N", 35, 114, 148),
        "PATIENT FIRST NAME": ("310-CA", "M", "A/N", 35, 149, 183),
        "PATIENT MIDDLE INITIAL": ("718-SZ", "S", "A/N", 1, 184, 184),
        "PATIENT RESIDENCE": ("384-4X", "S", "N", 2, 185, 186),
        "PATIENT ADDRESS LINE 1": ("726-SR", "M", "A/N", 40, 187, 226),
        "PATIENT ADDRESS LINE 2": ("727-SS", "S", "A/N", 40, 227, 266),
        "PATIENT CITY": ("728-SU", "M", "A/N", 20, 267, 286),
        "PATIENT STATE": ("729-TA", "M", "A/N", 2, 287, 288),
        "PATIENT ZIP/POSTAL CODE": ("730-TC", "M", "A/N", 15, 289, 303),
        "PATIENT ENTITY COUNTRY CODE": ("B36-1W", "S", "A/N", 2, 304, 305),
        "PATIENT TELEPHONE NUMBER QUALIFIER": ("629-SH", "S", "A/N", 2, 306, 307),
        "PATIENT TELEPHONE NUMBER": ("732-TB", "S", "N", 10, 308, 317),
        "PATIENT TELEPHONE NUMBER Extn": ("B10-8A", "S", "N", 8, 318, 325),
        "PATIENT E-MAIL ADDRESS": ("350-HN", "S", "A/N", 80, 326, 405),
        "DATE OF BIRTH": ("304-C4", "M", "N", 8, 406, 413),
        "PATIENT GENDER CODE": ("305-C5", "M", "N", 1, 414, 414),
        "PREGNANCY INDICATOR": ("335-2C", "S", "A/N", 1, 415, 415),
        "SMOKER/NON-SMOKER CODE": ("334-1C", "S", "A/N", 1, 416, 416),
        "EASY OPEN CAP INDICATOR": ("608-NF", "S", "A/N", 1, 417, 417),
        "PRESCRIPTION/SERVICE REFERENCE NUMBER": ("402-D2", "M", "N", 12, 418, 429),
        "DATE PRESCRIPTION WRITTEN": ("414-DE", "M", "N", 8, 430, 437),
        "ORIGINALLY PRESCRIBED PRODUCT/SERVICE ID QUALIFIER": (
            "453-EJ",
            "M",
            "A/N",
            2,
            438,
            439,
        ),
        "ORIGINALLY PRESCRIBED PRODUCT/SERVICE CODE": (
            "445-EA",
            "M",
            "A/N",
            19,
            440,
            458,
        ),
        "COMPOUND CODE": ("406-D6", "M", "N", 1, 459, 459),
        "PRESCRIBED DRUG DESCRIPTION": ("619-RW", "M", "A/N", 60, 460, 519),
        "PRODUCT DOSAGE FORM": ("601-21", "M", "A/N", 30, 520, 549),
        "PRODUCT STRENGTH": ("601-24", "M", "A/N", 15, 550, 564),
        "DISPENSE AS WRITTEN (DAW)/PRODUCT SELECTION CODE": (
            "408-D8",
            "M",
            "A/N",
            1,
            565,
            565,
        ),
        "QUANTITY PRESCRIBED": ("460-ET", "M", "N", 10, 566, 575),
        "NUMBER OF REFILLS AUTHORIZED": ("415-DF", "M", "N", 2, 576, 577),
        "DAYS SUPPLY": ("405-D5", "M", "N", 3, 578, 580),
        "PRODUCT/SERVICE ID QUALIFIER": ("426-E1", "M", "A/N", 2, 581, 582),
        "PRODUCT/SERVICE ID": ("407-D7", "M", "A/N", 19, 583, 601),
        "DRUG DESCRIPTION": ("516-FG", "M", "A/N", 60, 602, 661),
        "LABEL DIRECTIONS": ("613-NM", "M", "A/N", 200, 662, 861),
        "ORIGINAL DISPENSED DATE": ("617-RQ", "M", "N", 8, 862, 869),
        "ORIGINAL DISPENSED QUANTITY": ("A44-ZL", "M", "N", 10, 870, 879),
        "MOST RECENT DATE FILLED": ("614-NW", "M", "N", 8, 880, 887),
        "QUANTITY DISPENSED TO DATE": ("623-SA", "M", "N", 10, 888, 897),
        "REMAINING QUANTITY": ("625-SC", "M", "N", 10, 898, 907),
        "NUMBER OF FILLS TO DATE": ("615-NY", "S", "N", 2, 908, 909),
        "NUMBER OF FILLS REMAINING": ("616-PU", "M", "N", 2, 910, 911),
        "FILL NUMBER": ("403-D3", "S", "N", 2, 912, 913),
        "DISCONTINUE DATE": ("607-ND", "S", "N", 8, 914, 921),
        "INACTIVE PRESCRIPTION INDICATOR": ("612-NK", "M", "A/N", 1, 922, 922),
        "TRANSFER FLAG": ("631-SK", "S", "A/N", 1, 923, 923),
        "PRESCRIBER LAST NAME": ("716-SY", "M", "A/N", 35, 924, 958),
        "PRESCRIBER FIRST NAME": ("717-SX", "M", "A/N", 35, 959, 993),
        "PRESCRIBER ADDRESS LINE 1": ("726-SR", "M", "A/N", 40, 994, 1033),
        "PRESCRIBER ADDRESS LINE 2": ("727-SS", "S", "A/N", 40, 1034, 1073),
        "PRESCRIBER CITY": ("728-SU", "M", "A/N", 20, 1074, 1093),
        "PRESCRIBER STATE": ("729-TA", "M", "A/N", 2, 1094, 1095),
        "PRESCRIBER ZIP/POSTAL CODE": ("730-TC", "M", "A/N", 15, 1096, 1110),
        "PRESCRIBER ENTITY COUNTRY CODE": ("B36-1W", "S", "A/N", 2, 1111, 1112),
        "PRESCRIBER TELEPHONE NUMBER QUALIFIER": ("629-SH", "M", "A/N", 2, 1113, 1114),
        "PRESCRIBER TELEPHONE NUMBER": ("732-TB", "M", "N", 10, 1115, 1124),
        "PRESCRIBER TELEPHONE NUMBER Extn": ("B10-8A", "S", "N", 8, 1125, 1132),
        "PRESCRIBER ID (DEA)": ("411-DB", "S", "A/N", 15, 1133, 1147),
        "PRESCRIBER ID QUALIFIER": ("466-EZ", "M", "A/N", 2, 1148, 1149),
        "PRESCRIBER ID": ("411-DB", "M", "A/N", 15, 1150, 1164),
        "ADDITIONAL MESSAGE INFORMATION": ("526-FQ", "S", "A/N", 200, 1165, 1364),
        "PAYER ID QUALIFIER": ("568-J7", "S", "A/N", 2, 1365, 1366),
        "PAYER ID": ("569-J8", "S", "A/N", 10, 1367, 1376),
        "PROCESSOR CONTROL NUMBER": ("104-A4", "S", "A/N", 10, 1377, 1386),
        "GROUP ID": ("301-C1", "S", "A/N", 15, 1387, 1401),
        "PERSON CODE": ("303-C3", "S", "A/N", 3, 1402, 1404),
        "PATIENT RELATIONSHIP CODE": ("306-C6", "S", "N", 1, 1405, 1405),
        "FILLER": ("", "M", "A/N", 195, 1406, 1600),
    },
}


class LineRecord:
    def __init__(self, expected_prefix: str, raw_record: str):
        assert raw_record.startswith(expected_prefix)
        self.raw_record = raw_record
        self.record = raw_record

    def __str__(self):
        return self.record


class RAPrescriptionTransferHeaderRecord(LineRecord):
    def __init__(self, raw_record: str):
        super().__init__("RA", raw_record)

        self.version_release_number = self.record[2:4]
        assert self.version_release_number in [
            "20",
            "33",
        ]  # we only know about versions 2.0 and 3.3


class SRSendingReceivingPharmacyRecord(LineRecord):
    def __init__(self, raw_record: str):
        super().__init__("SR", raw_record)


class RXPrescriptionRecord(LineRecord):
    def __init__(self, raw_record: str, version_release_number: str):
        super().__init__("RX", raw_record)
        self.version_release_number = version_release_number
        self.fields = self.read_fields()

        try:
            most_recent_date_filled = datetime.strptime(
                str(self.fields["MOST RECENT DATE FILLED"]), date_format
            ).date()
        except ValueError:
            most_recent_date_filled = date(2050, 1, 1)

        try:
            days_supply = int(self.fields["DAYS SUPPLY"])
        except ValueError:
            days_supply = 30

        self.needs_by_date = most_recent_date_filled + timedelta(
            days=days_supply - 7 - (days_supply % 7)
        )

    def read_fields(self):
        result = {}
        i = 0

        for field_name, (_, _, field_type, num_chars, start, end) in RX_FORMATS[
            self.version_release_number
        ].items():
            field = self.record[start - 1 : end].strip()
            result[field_name] = (
                int(field) if field_type == "N" and field.isdecimal() else field
            )
            i += num_chars

        return result

    def set_field(self, field_name: str, value: str):
        (_, _, field_type, num_chars, start, end) = RX_FORMATS[
            self.version_release_number
        ][field_name]

        assert (
            "A/N" == field_type
        )  # only allowing changes to alphanumeric fields for now

        field = value.strip()
        assert num_chars >= len(field)

        self.fields[field_name] = field
        padded_value = field.ljust(num_chars)
        self.record = self.record[: start - 1] + padded_value + self.record[end:]
        assert len(self.record) == 1600


class STSendingReceivingPharmacyTotalRecord(LineRecord):
    def __init__(self, raw_record: str):
        super().__init__("ST", raw_record)

    def update_subtotal(self, num):
        self.record = (
            self.record[:71] + f"{num:08d}" + self.record[79:]
        )  # same indices for both versions 2.0 and 3.3


class XTPrescriptionTransferTrailerRecord(LineRecord):
    def __init__(self, raw_record: str):
        super().__init__("XT", raw_record)

    def update_total(self, num):
        self.record = (
            self.record[:9] + f"{num:010d}" + self.record[19:]
        )  # same indices for both versions 2.0 and 3.3


class ORTF:
    records_prefixes_pattern = re.compile("RASR(RX)+STXT")

    def __init__(self):
        self.ra: RAPrescriptionTransferHeaderRecord | None = None
        self.sr: SRSendingReceivingPharmacyRecord | None = None
        self.rxs = []
        self.st: STSendingReceivingPharmacyTotalRecord | None = None
        self.xt: XTPrescriptionTransferTrailerRecord | None = None

    def from_records(self, lines):
        all_records_prefixes_str = "".join([line[0:2] for line in lines])
        match = self.records_prefixes_pattern.fullmatch(all_records_prefixes_str)
        assert match is not None

        self.ra = RAPrescriptionTransferHeaderRecord(lines[0])
        self.sr = SRSendingReceivingPharmacyRecord(lines[1])

        self.rxs = []
        print(len(lines))
        for i in range(2, len(lines) - 2):
            print(lines[i])
            self.rxs.append(
                RXPrescriptionRecord(lines[i], self.ra.version_release_number)
            )

        self.st = STSendingReceivingPharmacyTotalRecord(lines[-2])
        self.xt = XTPrescriptionTransferTrailerRecord(lines[-1])

    def deep_copy_no_rxs(self):
        result = ORTFRecord()
        result.ra = copy.deepcopy(self.ra)
        result.sr = copy.deepcopy(self.sr)
        result.rxs = []
        result.st = copy.deepcopy(self.st)
        result.xt = copy.deepcopy(self.xt)
        return result

    def set_rxs(self, rxs):
        self.rxs = rxs
        self.st.update_subtotal(len(rxs) + 2)
        self.xt.update_total(len(rxs) + 4)

    def __str__(self):
        records = [self.ra, self.sr]
        records.extend(self.rxs)
        records.extend([self.st, self.xt])
        return "\r\n".join(str(record) for record in records)


def main(ortf_csv, ortf_map, ortf_grx, options: dict) -> typing.List[typing.Dict]:
    data = []
    fieldnames = []
    with (
        open(ortf_csv) as ortf_csv_file,
        open(ortf_map) as ortf_map_file,
        open(ortf_grx) as ortf_grx_file,
    ):
        # get mapping from external reference id to grx_rx_id
        ref_no_to_grx_rx_id = {}
        for line in ortf_grx_file.readlines():
            grx_rx_id = line.split(":")[0]
            ortf_rx_line = line.replace(f"{grx_rx_id}:", "")
            ortf_rx = RXPrescriptionRecord(ortf_rx_line, "20")
            reference_id = ortf_rx.fields["PRESCRIPTION/SERVICE REFERENCE NUMBER"]
            ref_no_to_grx_rx_id[str(reference_id)] = int(grx_rx_id)

        # get mapping from grx_rx_id to script_no
        grx_rx_id_to_script_no = {}
        for line in ortf_map_file.readlines()[1:]:
            grx_rx_id, script_no = line.split(",")
            grx_rx_id_to_script_no[int(grx_rx_id)] = str(script_no.strip())

        csv_reader = csv.DictReader(ortf_csv_file)
        fieldnames = ["SCRIPT NUMBER"] + csv_reader.fieldnames
        for _, val in enumerate(csv_reader):
            try:
                grx_rx_id = ref_no_to_grx_rx_id.get(
                    val["PRESCRIPTION/SERVICE REFERENCE NUMBER"]
                )
                script_no = grx_rx_id_to_script_no.get(grx_rx_id)
                csv_row = {"SCRIPT NUMBER": script_no}
                csv_row.update(val)

                # remove most recent date filled if quantity dispensed to date is zero
                if int(csv_row["QUANTITY DISPENSED TO DATE"]) == 0:
                    csv_row["MOST RECENT DATE FILLED"] = ""
                    print(
                        f"QUANTITY DISPENSED TO DATE is zero - Updated Date: '{csv_row['MOST RECENT DATE FILLED']}'"
                    )

                # calculate number of fills remaining from qty remaining, rounded down to nearest whole number
                if options.get("fix_fills"):
                    total_fills = Decimal(csv_row["REMAINING QUANTITY"]) / Decimal(
                        csv_row["QUANTITY PRESCRIBED"]
                    )
                    csv_row["NUMBER OF FILLS REMAINING"] = int(total_fills)

                data.append(csv_row)
            except IndexError:
                break

    output_filename = ortf_csv.replace(".csv", "")
    output_filename += (
        "-rximage.csv"
        if options.get("fix_fills")
        else "-rximage-wrong-fills.csv"
    )
    with open(output_filename, "w") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        print(f"Output: {output_filename}")
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Arguments")
    parser.add_argument(
        "--csv",
        required=True,
        type=str,
        help="ORTF CSV",
    )
    parser.add_argument(
        "--map",
        required=True,
        type=str,
        help="grx_rx_id to script_no",
    )
    parser.add_argument(
        "--grx",
        required=True,
        type=str,
        help="ORTF GRX",
    )
    args = parser.parse_args()

    output_filename = args.csv.replace(".csv", "")
    main(
        args.csv,
        args.map,
        args.grx,
        options={"fix_fills": True},
    )
