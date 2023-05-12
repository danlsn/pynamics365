import base64
import json
import urllib
import xml.etree.ElementTree as etree
import zipfile
from collections import OrderedDict
from pathlib import Path
from zipfile import Path as ZipPath
import lxml
import magic
import xlsxwriter
import xmltodict
from bs4 import BeautifulSoup
from tqdm import tqdm


def load_template_sheets(templates_dir: Path):
    files = []
    for file in templates_dir.iterdir():
        if file.suffix == ".xlsx":
            try:
                filepath = str(file)
                mime_type = magic.from_file(str(file), mime=True)
                if mime_type in [
                    "application/zip",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ]:
                    files.append(file)
                else:
                    print(f"File {filepath} is not a zip file")
            except PermissionError:
                print(f"File {filepath} is in use")
    return files


def parse_xml_to_soup(zf: zipfile.FastLookup, xml_file: str):
    xml = zf.read(xml_file).decode("utf-8")
    soup = BeautifulSoup(xml, "xml")
    return soup


def parse_xml_to_dict(zf: zipfile.FastLookup, xml_file: str):
    xml = zf.read(xml_file).decode("utf-8")
    xml_dict = xmltodict.parse(xml)
    return xml_dict


def parse_crm_template_map(sheet_xml: BeautifulSoup):
    sheet_data = sheet_xml.find("sheetData")
    crm_template_mapping_str = urllib.parse.unquote(sheet_data.find("row").find("c").find("v").text)
    crm_template_mapping = crm_template_mapping_str.split(":")
    field_map_dict = OrderedDict()
    for field_map in crm_template_mapping[2].split("&"):
        field_map = field_map.split("=")
        field_map_dict[field_map[0]] = field_map[1]

    crm_template_mapping = {
        "entity": crm_template_mapping[0],
        "raw": crm_template_mapping_str,
        "base64": crm_template_mapping[1],
        "columns": field_map_dict,
    }

    return crm_template_mapping


def parse_table_columns(table_xml: BeautifulSoup):
    table_columns = OrderedDict()
    for column in table_xml.find("tableColumns").find_all("tableColumn"):
        table_columns[column.attrs["id"]] = column.attrs["name"]
    return table_columns


def parse_workbook_sheets(workbook_xml: BeautifulSoup):
    sheets = OrderedDict()
    for sheet in workbook_xml.find("sheets").find_all("sheet"):
        sheets[sheet.attrs["r:id"]] = {"sheetId": sheet.attrs["sheetId"], "name": sheet.attrs["name"]}
        if "state" in sheet.attrs:
            sheets[sheet.attrs["r:id"]]["state"] = sheet.attrs["state"]
    return sheets


def parse_data_validations(sheet2_xml):
    data_validations = OrderedDict()
    for data_validation in sheet2_xml.find("dataValidations").find_all("dataValidation"):
        column = data_validation.attrs["sqref"]

        data_validation_rule = {
            "sqref": data_validation.attrs["sqref"],
            "type": data_validation.attrs["type"],
            "allowBlank": data_validation.attrs["allowBlank"],
            "showInputMessage": data_validation.attrs["showInputMessage"],
            "showErrorMessage": data_validation.attrs["showErrorMessage"],
            "errorTitle": data_validation.attrs["errorTitle"],
            "error": data_validation.attrs["error"],
            "promptTitle": data_validation.attrs["promptTitle"],
            "prompt": data_validation.attrs["prompt"],
        }

        if data_validation.find("operator"):
            data_validation_rule["operator"] = data_validation.find("operator").text
        if data_validation.find("formula1"):
            data_validation_rule["formula1"] = data_validation.find("formula1").text
        if data_validation.find("formula2"):
            data_validation_rule["formula2"] = data_validation.find("formula2").text

        data_validations[column] = data_validation_rule
    return data_validations


def parse_column_headers(sheet2_xml):
    column_headers = OrderedDict()
    first_row = sheet2_xml.find("sheetData").find("row")
    for cell in first_row.find_all("c"):
        r = cell.attrs["r"]
        name = cell.find("t").text
        column_headers[r] = name
    return column_headers


def parse_lookup_values(sheet_xml):
    lookup_values = OrderedDict()
    rows = sheet_xml.find("sheetData").find_all("row")
    sheet_pr = sheet_xml.find("sheetPr").attrs["codeName"]
    # Remove first row
    rows.pop(0)
    for row in rows:
        cells = row.find_all("c")
        try:
            first_cell_r = cells[0].attrs["r"]
            last_cell_r = cells[-1].attrs["r"]
        except IndexError:
            print(f"Row {row} is empty")
            continue
        row_range = f"{first_cell_r}:{last_cell_r}"
        lookup_values[row_range] = {"sheet_pr": sheet_pr, "values": []}
        for cell in row.find_all("c"):
            cell_coord = cell.attrs["r"]
            cell_type = cell.attrs["t"]
            value = cell.find("v").text
            lookup_values[row_range]["values"].append(
                {"cell_coord": cell_coord, "cell_type": cell_type, "value": value}
            )

    return lookup_values


def parse_xlsx_template(template: Path):
    output_path = template.parent / template.stem
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(template, "r") as z:
            zp = ZipPath(z)
            sheet_xml = parse_xml_to_soup(z, "xl/worksheets/sheet1.xml")
            sheet2_xml = parse_xml_to_soup(z, "xl/worksheets/sheet2.xml")
            table_xml = parse_xml_to_soup(z, "xl/tables/table1.xml")
            workbook_xml = parse_xml_to_soup(z, "xl/workbook.xml")

            field_mapping = parse_crm_template_map(sheet_xml)
            workbook_sheets = parse_workbook_sheets(workbook_xml)
            validations = parse_data_validations(sheet2_xml)
            columns = parse_column_headers(sheet2_xml)
            lookup_values = parse_lookup_values(sheet_xml)
            template = {
                "entity_name": field_mapping["entity"],
                "template_name": template.stem,
                "worksheet_name": workbook_sheets["dataSheet"]["name"],
                "field_mapping": field_mapping,
                "workbook_sheets": workbook_sheets,
                "lookup_values": lookup_values,
                "validations": validations,
                "columns": columns,
            }
            return template
    except zipfile.BadZipFile:
        print(f"File {template} is not a zip file")
        return None


def extract_xlsx_to_dir(template: Path, output_dir: Path):
    """Extract the contents of a given xlsx file to the given directory.

    Args:
        template (Path): The xlsx file to extract.
        output_dir (Path): The directory to extract the xlsx file to.
    """
    mime_type = magic.from_file(str(template), mime=True)
    if template.is_dir():
        return
    elif magic.from_file(str(template), mime=True) not in ( "application/zip", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ):
        return
    else:
        with zipfile.ZipFile(template, "r") as z:
            z.extractall(output_dir)


def load_templates(template_dir: Path):
    """Load all templates from the given directory.

    Args:
        template_dir (Path): The directory containing the templates.

    Returns:
        list: A list of all templates.
    """
    templates = []
    for template in template_dir.iterdir():
        if template.suffix == ".xlsx":
            templates.append(template)
    return templates


# def main():
#     templates = load_template_sheets(
#         Path(r"C:\Users\DanielLawson\IdeaProjects\MIP-CRM-Migration\docs\MIP - Data Import Template")
#     )
#     output = []
#     pbar = tqdm(templates, total=len(templates))
#     for template in pbar:
#         pbar.set_description(f"Processing {template.name}")
#         # output.append(parse_xlsx_template(template))
#         extract_xlsx_to_dir(template, template.parent / template.stem)
#         pbar.update(1)
#
#     with open(
#         r"C:\Users\DanielLawson\IdeaProjects\MIP-CRM-Migration\docs\MIP - Data Import Template\import_templates.json",
#         "w",
#     ) as f:
#         json.dump(output, f, indent=4)


def get_template_xml_files(template_dir: Path):
    xml_files = []
    # Get all files recursively
    for file in template_dir.rglob("*"):
        if file.suffix == ".xml":
            xml_files.append(file)
    return xml_files


def parse_updated_template_to_soup(template_dict: dict):
    """Parse an updated template.

    Args:
        template_dict (dict): The updated template.

    Returns:
        dict: The updated template.
    """
    template = template_dict["template_name"]
    xml_files = template_dict["xml_content"]

    for xml_file in xml_files:
        xml_file["soup"] = BeautifulSoup(xml_file["xml"], "lxml")
        ...
    return {"template_name": template, "xml_content": xml_files}


def parse_updated_templates(template_dir: Path):
    """Parse the updated templates.

    Args:
        template_dir (Path): The directory containing the updated templates.

    Returns:
        list: A list of all updated templates.
    """
    templates = set()
    for template in template_dir.iterdir():
        if template.is_dir():
            templates.add(template)
        elif template.suffix == ".xlsx":
            extract_xlsx_to_dir(template, template.parent / template.stem)
            templates.add(template.parent / template.stem)

    template_xml_files = {}
    pbar = tqdm(templates, total=len(templates))
    for template_dir in pbar:
        pbar.set_description(f"Processing {template_dir.name}")
        template_xml_files[template_dir.name] = get_template_xml_files(template_dir)
        pbar.update(1)

    templates = []
    for template_name, xml_files in template_xml_files.items():
        template = {"template_name": template_name, "xml_content": []}
        for xml_file in xml_files:
            try:
                with open(xml_file, "r") as f:
                    xml = f.read()
                template["xml_content"].append({"name": xml_file.name, "xml": xml})
            except UnicodeDecodeError:
                print(f"File {xml_file} is not a text file")
        templates.append(template)

    parsed_templates = [parse_updated_template_to_soup(template) for template in templates]


if __name__ == "__main__":
    parse_updated_templates(
        Path(r"../data/import_templates")
    )
    parse_updated_templates(
        Path(r"../data/templates_formatted_name")
    )
    parse_updated_templates(
        Path(r"../data/templates_logical_name")
    )
    parse_updated_templates(
        Path(r"../data/templates")
    )
