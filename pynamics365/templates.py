import copy
import json
import shutil
import tempfile
import urllib
import zipfile
from collections import OrderedDict
from pathlib import Path
import re
from urllib.parse import unquote
from xml.sax.saxutils import unescape
import logging
import requests
from bs4 import BeautifulSoup

from pynamics365.config import TEMPLATE_ENTITIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

dfh = logging.FileHandler("../logs/pynamics365_templates.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)

TEMPLATES_ROOT = Path(
    r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\import_templates"
)


cookies = {
    'visid_incap_2029367': 'ZDdF/BiBQyqDk+QZo9sQVNHp7mMAAAAAQUIPAAAAAABwpYpyuT48mAHwIGBL5X7m',
    'ReqClientId': '9e4c0b96-7a84-4620-934a-58b6e95d49d3',
    'orgId': 'bcd4f0de-97c7-ed11-a10d-0022481536c6',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation': 'HideMessage',
    'CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20': 'HideMessage',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm': 'HideMessage',
    'persistentSearchTypeCookieEx': '0',
    'persistentSearchTypeCookie': '1',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience': 'HideMessage',
    'lastUsedApp': 'appId=4be68029-35c5-ed11-b597-002248144a90&userId=2bbb35de-0cc9-ed11-b595-00224818a651',
    '_clck': '1cujtye|1|faq|1',
    'CrmOwinAuth': 'MAAAAIKrY4TGdhHtoQsAIkjjQekrDzsfV-ZGV1wnPH52Qsf0T_rnINsFUTdrmnexWhrSfR-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakX9LcHm_RhllYCNXxi_AdtfEBSFKczLFBjsH5XfGNw9vdkZraJkbCUlKEEVdercqluXc-_v7u5-mFYo6aTpHkG5LII3d2_u_tQ03zul90S-FIG41C8VeH16LW_ufg6KIsOf2m1c4FYD5qUVbiWwaEPfZymSEghA-T7BApokHNehm1fKZSSGdF2Kbr-5u0cYX4txPQen-I7iRYqSKEmUHkbL6eRqzMT1b44J6-wzJifeCPPTZfxlOAhg7OBWjECe4tQvWiCN2yByUIzbTuMbSQyLIPVwDn2YwwRAfJ9V3vWLfw2BHDv_XwKxfwMC-r8lUOE2TZJim-TbyINJgYrTr0ldWoBToDRBCS6c5NWRaVoib-Igb4EdODj4WPl9gTeExYTmqngALMGFgKyut_7nr6yv4winTtZK891n27l2c31tOy7zxInh27FT4TS5nsbfvo3GDh1h8kyk6yQIRtcTeYsyx_PyH2iSadGk1NxCi6Wvx324sPz-heXjrVbt68370nW_XqzUDSEoXlp9BPMngaE5T6REguN4imAhDwiHZnmCkyjgA0Z0HEq8geVZibw_UiTJNLtI0z250xOYzvW4v8sDnmzJhir36i4p2LYyVE-N-ao7XXUEdQfk11L98qQosqq2rp_7DxjtmhNvN0e-gO-9I7DjZHeDaPwF7rsLLoizG8M-7GOMrsfsftuJvRyDf7ngn8NctWwrKDFtkemq2Tu2v0VcQnaHHl-yW8VcH6q8jjT2tLueMvdfHZcCJk08R97Tf4J-PblG-nnP0aEVPUeHvzc_F88pnIZsmkYXhi2nvH4a6du2rcyS_wm9-7JAHwPTXLAjqWMb_e4AeJRZrmxZvgH4Eeb3VOsGsvmhQh5-cgXJd33WIxjoSwTLixIhUixDCLxLSSwUOY6Urp_rfR1jO84AektzUrM_N5EV7y6gDeYDw98I8ZlmhVHxQIk3gvyYNoNaIL-4VhEU6b7xyVMGfyNrenP3oZneOyV2Xvzjw7f9FeSdrMiy2rx3CaI7lhlzKsxzz9uNdoIFj7m86zYGGrI0s7OxkGUGnC6pGrMblx8OQOlbY7uraNPxqPmjjvhefmaCTB6FwiJYmIfFajFbx15GxOYqH_ZOsE4UlTE20Xpz0uZJhzmj3g72Az9Mt6q4Z5TK9Dh6Xs3ILkEb8gxqHgxh2QTwiWAF3DmJyrN7xEwV8hqn8axsLsgxBWj_7IbjWo9AZjPbpcEW1aLorHtHfbDeT-DkTE14Xg_COBsqUUr5ipOLXqivpGLvH2VU4qlwkg99b8DrTEIO-2a6Uuw8QYsdQJKRR9KZPc6Lxfw0leZRZmzoE-Os1wJaqmAfTM5oHqcj0i_VJZmfzp0zz6_Tw34-BvLhSAb46GfGacdMXDwL7E5vbY6ne3NdkvvTuFkA15gyomVr2DPNvawq3fNBNayBPZz30aYbSZU_nG6V0bQuSt1T6YKqXHFsZVyaaYk73C5X3bFpdghVsyxBldByRo1OO0VzDVIx6PO09EAx8SbxWapETVx7ZyOoBrGaNbQsjpx39ivl4JHulhQ5391ZYXWu8YEaZdMIkkVYiJOxFbpUbOHCqrfSjB51ujPbGDAdqi9520CdGrQRmruZP61tmNX6strsKdrs97iVriyo1NYIzmBzx40KQUB8tZkMtT0w43K7O0jH4QFPtTXvekN10JmnC9OwtrREm_EiRb3cIBFbulNkHte9zsoNA8ZSCMsYyPpMCrpWdtz35yCahbleSIODjRGxdL2o5_JkSkoSvY1nId4qlt9dq3FBMvKCTfvE2V_LiepYNOa3-8PKXPnzmPA7hS1a6cGr1VO5MAQ2NioNGFpkdipz6yX9zQZYc4Hj6o0li7kOFMB0M0Ag7A0of6KkWloHOmvHUKrdcZJ3LFvPDkOJPZdOyCR42tckMw2r4WSLstKpNoO6D-qYI0aWFO20gXbE-1ICoXYaEP0wGgJUzjoHYbJSJnDkWsKWlkXeG1pI16uuPNYnqCZEwZicF9oWBL4hjuIDCktlfjSMZOUybqiCSN-OHIXTI1MMunNXXM5J5O27tRoeIhmrSaEDF2e62BePu-2OTNjeuDc_UKtR0i1P9gCuVtF0HaXBMDsM5sa8NzTnIWo6pwNHXJFJajls2IiVnhms1AoNTH_L1PjU0X0UaIklVWnFmZ47zu2tVx5KzZjT8SxlmE193viMfnqJX999jl-7_CWFFCSOJvkv-6Lf6LuUS_22uT-0cuihvBH5j6-RN27SmfISQvlWg-A0igc_i53vWxhi3KSty4skfGIBBRzosYQjcCLBeh5sMgEHEIB2_KaF9X0a_thqJAPYL1_GDXt5o-T--jpPlO6aUP2LoEqTCCXwJYNP4zhN2uklhNPtZ4jPU_--lcEcI9wIreLufQvWWcMd_6SnyV8eKepRzvLHJgtjHknuE0d9YvjHvr5610IYl9D7aVXCX31Ff_nVPwGYL0_cXBMAAA',
    'ARRAffinity': 'af4796fea18c49732fb504beb0cabf6cb8e778a93ded7034dcdc447b7baef2e580edd05839c87ae17bc323a228a36e7a533327001b2849f23e7cf0835621dc5508DB3FD0CC95FF45262598790',
}

headers = {
    'authority': 'mipau.crm6.dynamics.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'visid_incap_2029367=ZDdF/BiBQyqDk+QZo9sQVNHp7mMAAAAAQUIPAAAAAABwpYpyuT48mAHwIGBL5X7m; ReqClientId=9e4c0b96-7a84-4620-934a-58b6e95d49d3; orgId=bcd4f0de-97c7-ed11-a10d-0022481536c6; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation=HideMessage; CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20=HideMessage; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm=HideMessage; persistentSearchTypeCookieEx=0; persistentSearchTypeCookie=1; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience=HideMessage; lastUsedApp=appId=4be68029-35c5-ed11-b597-002248144a90&userId=2bbb35de-0cc9-ed11-b595-00224818a651; _clck=1cujtye|1|faq|1; CrmOwinAuth=MAAAAIKrY4TGdhHtoQsAIkjjQekrDzsfV-ZGV1wnPH52Qsf0T_rnINsFUTdrmnexWhrSfR-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakX9LcHm_RhllYCNXxi_AdtfEBSFKczLFBjsH5XfGNw9vdkZraJkbCUlKEEVdercqluXc-_v7u5-mFYo6aTpHkG5LII3d2_u_tQ03zul90S-FIG41C8VeH16LW_ufg6KIsOf2m1c4FYD5qUVbiWwaEPfZymSEghA-T7BApokHNehm1fKZSSGdF2Kbr-5u0cYX4txPQen-I7iRYqSKEmUHkbL6eRqzMT1b44J6-wzJifeCPPTZfxlOAhg7OBWjECe4tQvWiCN2yByUIzbTuMbSQyLIPVwDn2YwwRAfJ9V3vWLfw2BHDv_XwKxfwMC-r8lUOE2TZJim-TbyINJgYrTr0ldWoBToDRBCS6c5NWRaVoib-Igb4EdODj4WPl9gTeExYTmqngALMGFgKyut_7nr6yv4winTtZK891n27l2c31tOy7zxInh27FT4TS5nsbfvo3GDh1h8kyk6yQIRtcTeYsyx_PyH2iSadGk1NxCi6Wvx324sPz-heXjrVbt68370nW_XqzUDSEoXlp9BPMngaE5T6REguN4imAhDwiHZnmCkyjgA0Z0HEq8geVZibw_UiTJNLtI0z250xOYzvW4v8sDnmzJhir36i4p2LYyVE-N-ao7XXUEdQfk11L98qQosqq2rp_7DxjtmhNvN0e-gO-9I7DjZHeDaPwF7rsLLoizG8M-7GOMrsfsftuJvRyDf7ngn8NctWwrKDFtkemq2Tu2v0VcQnaHHl-yW8VcH6q8jjT2tLueMvdfHZcCJk08R97Tf4J-PblG-nnP0aEVPUeHvzc_F88pnIZsmkYXhi2nvH4a6du2rcyS_wm9-7JAHwPTXLAjqWMb_e4AeJRZrmxZvgH4Eeb3VOsGsvmhQh5-cgXJd33WIxjoSwTLixIhUixDCLxLSSwUOY6Urp_rfR1jO84AektzUrM_N5EV7y6gDeYDw98I8ZlmhVHxQIk3gvyYNoNaIL-4VhEU6b7xyVMGfyNrenP3oZneOyV2Xvzjw7f9FeSdrMiy2rx3CaI7lhlzKsxzz9uNdoIFj7m86zYGGrI0s7OxkGUGnC6pGrMblx8OQOlbY7uraNPxqPmjjvhefmaCTB6FwiJYmIfFajFbx15GxOYqH_ZOsE4UlTE20Xpz0uZJhzmj3g72Az9Mt6q4Z5TK9Dh6Xs3ILkEb8gxqHgxh2QTwiWAF3DmJyrN7xEwV8hqn8axsLsgxBWj_7IbjWo9AZjPbpcEW1aLorHtHfbDeT-DkTE14Xg_COBsqUUr5ipOLXqivpGLvH2VU4qlwkg99b8DrTEIO-2a6Uuw8QYsdQJKRR9KZPc6Lxfw0leZRZmzoE-Os1wJaqmAfTM5oHqcj0i_VJZmfzp0zz6_Tw34-BvLhSAb46GfGacdMXDwL7E5vbY6ne3NdkvvTuFkA15gyomVr2DPNvawq3fNBNayBPZz30aYbSZU_nG6V0bQuSt1T6YKqXHFsZVyaaYk73C5X3bFpdghVsyxBldByRo1OO0VzDVIx6PO09EAx8SbxWapETVx7ZyOoBrGaNbQsjpx39ivl4JHulhQ5391ZYXWu8YEaZdMIkkVYiJOxFbpUbOHCqrfSjB51ujPbGDAdqi9520CdGrQRmruZP61tmNX6strsKdrs97iVriyo1NYIzmBzx40KQUB8tZkMtT0w43K7O0jH4QFPtTXvekN10JmnC9OwtrREm_EiRb3cIBFbulNkHte9zsoNA8ZSCMsYyPpMCrpWdtz35yCahbleSIODjRGxdL2o5_JkSkoSvY1nId4qlt9dq3FBMvKCTfvE2V_LiepYNOa3-8PKXPnzmPA7hS1a6cGr1VO5MAQ2NioNGFpkdipz6yX9zQZYc4Hj6o0li7kOFMB0M0Ag7A0of6KkWloHOmvHUKrdcZJ3LFvPDkOJPZdOyCR42tckMw2r4WSLstKpNoO6D-qYI0aWFO20gXbE-1ICoXYaEP0wGgJUzjoHYbJSJnDkWsKWlkXeG1pI16uuPNYnqCZEwZicF9oWBL4hjuIDCktlfjSMZOUybqiCSN-OHIXTI1MMunNXXM5J5O27tRoeIhmrSaEDF2e62BePu-2OTNjeuDc_UKtR0i1P9gCuVtF0HaXBMDsM5sa8NzTnIWo6pwNHXJFJajls2IiVnhms1AoNTH_L1PjU0X0UaIklVWnFmZ47zu2tVx5KzZjT8SxlmE193viMfnqJX999jl-7_CWFFCSOJvkv-6Lf6LuUS_22uT-0cuihvBH5j6-RN27SmfISQvlWg-A0igc_i53vWxhi3KSty4skfGIBBRzosYQjcCLBeh5sMgEHEIB2_KaF9X0a_thqJAPYL1_GDXt5o-T--jpPlO6aUP2LoEqTCCXwJYNP4zhN2uklhNPtZ4jPU_--lcEcI9wIreLufQvWWcMd_6SnyV8eKepRzvLHJgtjHknuE0d9YvjHvr5610IYl9D7aVXCX31Ff_nVPwGYL0_cXBMAAA; ARRAffinity=af4796fea18c49732fb504beb0cabf6cb8e778a93ded7034dcdc447b7baef2e580edd05839c87ae17bc323a228a36e7a533327001b2849f23e7cf0835621dc5508DB3FD0CC95FF45262598790',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48',
}


def get_two_letter_cols():
    all_columns = []
    ascii_uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for letter in ascii_uppercase:
        all_columns.append(letter)
        for letter2 in ascii_uppercase:
            all_columns.append(letter + letter2)
    # Sort the columns by length, then alphabetically
    all_columns.sort(key=lambda x: (len(x), x))
    return all_columns


def get_two_letter_col_range(start, end):
    all_columns = get_two_letter_cols()
    start_index = all_columns.index(start)
    end_index = all_columns.index(end)
    return all_columns[start_index: end_index + 1]


def load_html(file):
    with open(file, "r") as f:
        html = f.read()
    return html


def parse_html_options(html):
    soup = BeautifulSoup(html, "html.parser")
    options = soup.find_all("option")
    templates = []
    for option in options:
        templates.append(option.attrs)
    return templates


def get_template(title, entity_name, headers, cookies, output_path="../data/templates"):
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    # Replace invalid characters in title
    title = title.replace("/", "_")
    filename = output_path / f"{title}.xlsx"
    # if filename.exists():
    #     print(f"File {filename} already exists")
    #     return
    url = f"https://mipau.crm6.dynamics.com/tools/importwizard/createtemplate.aspx?entityName={entity_name}"
    response = requests.get(url, headers=headers, cookies=cookies)
    res_headers = response.headers
    content_type = res_headers["Content-Type"]
    if content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        print(f"Unexpected content type: {content_type}")
        return
    with open(filename, "wb") as f:
        f.write(response.content)
        print(f"Saved {filename}")


def load_template_sheets(templates_dir: Path):
    files = []
    for file in templates_dir.iterdir():
        if file.suffix == ".xlsx":
            files.append(file)
    return files


def unzip_xlsx_template(template: Path):
    output_path = template.parent / template.stem
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(template, "r") as z:
            print(f"Unzipping {template} to {output_path}")
            z.extractall(output_path)
        return output_path
    except zipfile.BadZipFile:
        print(f"Could not unzip {template}")
        return None


def get_import_templates(entity_list=TEMPLATE_ENTITIES, templates_html=Path("../docs/Templates for Data Import.html"), **kwargs):
    html = load_html(templates_html)
    templates = parse_html_options(html)
    for template in templates:
        print(f"Getting {template['title']}")
        if template["value"] in TEMPLATE_ENTITIES:
            get_template(template["title"], template["value"], output_path=Path("../data/import_templates"), headers=kwargs.get("headers"), cookies=kwargs.get("cookies"))
            try:
                unzip_xlsx_template(Path("../data/import_templates") / f"{template['title']}.xlsx")
            except FileNotFoundError:
                print(f"Could not unzip {template['title']}")
        # get_template(template["value"], template["value"], output_path=Path("../data/templates_logical_name"))
        # get_template(template["title"], template["value"], output_path=Path("../data/templates_formatted_name"))
    print("Done")


def letter_range(start, end):
    all_columns = []
    ascii_uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for letter in ascii_uppercase:
        all_columns.append(letter)
        for letter2 in ascii_uppercase:
            all_columns.append(letter + letter2)
    # Sort the columns by length, then alphabetically
    all_columns.sort(key=lambda x: (len(x), x))
    start_index = all_columns.index(start)
    end_index = all_columns.index(end)
    return all_columns[start_index: end_index + 1]


class ExcelTemplate:
    xml_files = []
    entity_name = None
    checksum = None
    field_mapping = None

    def __init__(self, template_path: Path):
        self.worksheets = None
        self.shared_strings = {}
        self.xml_soup = {}
        self.data_validations = {}
        self.single_range_data_validations = []
        self.lookups = {}
        self.option_sets = {}
        self.column_types = {}
        self.sheet_data = {}
        self.table_columns = {}
        self.option_set_validations = {}

        self.template_path = template_path
        self.template_name = template_path.name
        self.get_xml_files()

        self._xml_to_soup()
        self.get_table_columns()
        self.get_worksheets()
        self.get_shared_strings()
        for worksheet in self.worksheets:
            code_name, sheet_data, data_validations, option_set_validations = self.parse_worksheet(worksheet)
            if code_name:
                for row in sheet_data:
                    for cell in row.find_all("c"):
                        try:
                            if cell.attrs["t"] == "s":
                                cell.attrs["t"] = "str"
                                v = cell.find("v")
                                v.string = self.shared_strings[int(v.contents[0])]
                        except KeyError:
                            pass
                self.sheet_data[code_name] = sheet_data
                single_range_data_validations = []
                all_validations = data_validations
                for option_set_validation in option_set_validations or []:
                    all_validations.append(option_set_validation)
                for validation in all_validations or []:
                    if "sqref" not in validation.attrs:
                        sqref = validation.find("xm:sqref").contents[0]
                        ...
                    else:
                        sqref = validation.attrs["sqref"]
                    if len(sqref.split(" ")) == 1:
                        self.single_range_data_validations.append(validation)
                    else:
                        formula_range_list = sqref.split(" ")
                        for formula_range in formula_range_list:
                            if ":" not in formula_range:
                                range_start = formula_range
                                range_end = formula_range
                            else:
                                range_start, range_end = formula_range.split(":")
                            range_start_col = "".join([c for c in range_start if not c.isdigit()])
                            range_start_row = "".join([c for c in range_start if c.isdigit()])
                            range_end_col = "".join([c for c in range_end if not c.isdigit()])
                            range_end_row = "".join([c for c in range_end if c.isdigit()])
                            if range_start_col != range_end_col:
                                col_range_list = list(letter_range(range_start_col, range_end_col))
                                for col in col_range_list:
                                    single_range_validation = copy.copy(validation)
                                    single_range_validation.attrs[
                                        "sqref"
                                    ] = f"{col}{range_start_row}:{col}{range_end_row}"
                                    self.single_range_data_validations.append(single_range_validation)
                            else:
                                single_range_validation = copy.copy(validation)
                                single_range_validation.attrs["sqref"] = formula_range
                                self.single_range_data_validations.append(single_range_validation)
                self.data_validations[code_name] = data_validations
                self.option_set_validations[code_name] = option_set_validations
        self.parse_mapping_str()
        self.get_lookups()
        self.get_option_sets()
        self.get_column_types_from_validations

        ...

    def parse_mapping_str(self):
        # Mapping str is the longest in shared_strings
        mapping_str = ""
        for ss in self.shared_strings:
            if "==:" in ss:
                mapping_str = str(ss)
                break
        try:
            entity, checksum, mapping = mapping_str.split(":")
        except ValueError:
            return
        mapping_dict = {}
        for mapping in mapping.split("&"):
            field, column = mapping.split("=")
            # HTML unescape column name
            column = urllib.parse.unquote(column)
            mapping_dict[column] = field
        self.field_mapping = mapping_dict
        self.entity_name = entity
        self.checksum = checksum
        return entity, checksum, mapping_dict

    def get_xml_files(self):
        self.xml_files = []
        for file in self.template_path.glob("**/*.xml"):
            self.xml_files.append(file)

    def get_lookups(self):
        lookups = {}
        # Data Validation Rules with promptTitle beginning with "Lookup"
        for code_name, data_validations in self.data_validations.items():
            for data_validation in data_validations or []:
                if data_validation.attrs["promptTitle"].startswith("Lookup"):
                    sqref = data_validation.attrs["sqref"]
                    prompt_title = data_validation.attrs["promptTitle"]
                    is_mandatory = prompt_title.endswith(" (required)")
                    prompt = data_validation.attrs["prompt"]
                    column_ref = sqref.split(":")[0]
                    column_name = self.get_column_name(column_ref)
                    lookups[column_name] = {
                        "column_name": column_name,
                        "lookup_entity": None,
                        "prompt_title": prompt_title,
                        "is_mandatory": is_mandatory,
                        "prompt": prompt,
                        "sqref": sqref,
                    }
        # Sort Lookups by sqref
        lookups = OrderedDict(sorted(lookups.items(), key=lambda x: x[1]["sqref"]))
        self.lookups = lookups

    def to_json(self):
        template_dict = {
            "entity_name": self.entity_name,
            "checksum": self.checksum,
            "template_name": self.template_name,
            "template_path": str(self.template_path),
            "shared_strings": self.shared_strings,
            "column_types": self.column_types,
            "lookups": self.lookups,
            "option_sets": self.option_sets,
            "field_mapping": self.field_mapping,
        }

        return json.dumps(template_dict, indent=4)

    @property
    def get_column_types_from_validations(self):
        all_validations = self.single_range_data_validations
        for code_name, option_set_validations in self.option_set_validations.items():
            for option_set_validation in option_set_validations or []:
                all_validations.append(option_set_validation)

        table_columns = self.table_columns
        column_names = [col.attrs["name"] for col in table_columns if col]
        column_types = {}
        for validation in all_validations:
            if "sqref" not in validation.attrs:
                sqref = validation.find("xm:sqref").contents[0]
                ...
            else:
                sqref = validation.attrs["sqref"]
            column_ref = sqref.split(":")[0]
            column_name = self.get_column_name(column_ref)
            column_type = validation.attrs["type"] if "type" in validation.attrs else None
            formula_1 = validation.find("formula1").contents[0] if validation.find("formula1") else None
            formula_2 = validation.find("formula2").contents[0] if validation.find("formula2") else None
            option_set = self.option_sets[column_name] if column_name in self.option_sets else None
            option_set_range_values = option_set["option_set_range_values"] if option_set else None
            option_set_items = []
            if option_set_range_values:
                for key, option_set_range_value in option_set_range_values.items():
                    option_set_items.append(option_set_range_value)
            validation_dict = {
                "sqref": sqref,
                "column_ref": "".join([i for i in column_ref if not i.isdigit()]),
                "column_name": column_name,
                "crm_name": self.field_mapping[column_name] if column_name in self.field_mapping else None,
                "column_type": column_type,
                "formula_1": str(formula_1),
                "formula_2": str(formula_2),
            }
            for attr in validation.attrs:
                validation_dict[attr] = str(validation.attrs[attr])
            try:
                sql_comment = [f"Column: [{validation_dict['column_ref']}] {column_name}"]
                sql_comment.append(f"CRM Field: {validation_dict['crm_name']}") if validation_dict["crm_name"] else None
                if "promptTitle" in validation_dict.keys():
                    sql_comment.append(f"Column Type: {validation_dict['promptTitle']}")
                if option_set:
                    sql_comment.append(f"Option Set: {str(tuple(option_set_items))}")
                if "error_message" in validation_dict:
                    sql_comment.append(f"Constraint: {validation_dict['error_message']}")
                else:
                    sql_comment.append(f"Constraint: {validation_dict['error']}")
                sql_comment_str = "\n".join(sql_comment)
                validation_dict["sql_comment"] = sql_comment_str
            except TypeError:
                pass

            column_types[column_name] = validation_dict
        # Sort column_types by sqref, AA after Z

        column_types = OrderedDict(
            sorted(column_types.items(), key=lambda x: (len(x[1]["column_ref"]), x[1]["column_ref"]))
        )
        self.column_types = column_types
        return column_types

    def get_option_set_range_values(self, option_set_range):
        option_set_range_values = OrderedDict()
        sheet_name, range = option_set_range.split("!")
        if sheet_name == "hiddenSheet":
            sheet_name = "hiddenDataSheet"
        sheet_rows = self.sheet_data[sheet_name]
        range_start = range.split(":")[0]
        # Get integers from range_start as row number
        row_number = int("".join([c for c in range_start if c.isdigit()]))
        # Fine row from sheet_rows where r = row_number
        row = [r for r in sheet_rows if r.attrs["r"] == str(row_number)][0]
        # Get cells from row
        cells = row.find_all("c")
        for cell in cells:
            if cell.attrs["t"] == "str":
                cell_value = cell.find("v").contents[0]
                option_set_range_values[cell.attrs["r"]] = cell_value
        return option_set_range_values

    def get_option_sets(self):
        option_sets = {}
        # Option Set Validations with promptTitle beginning with "Option Set"
        for code_name, option_set_validations in self.option_set_validations.items():
            for option_set_validation in option_set_validations or []:
                if option_set_validation.attrs["promptTitle"].startswith("Option set"):
                    option_set_range = option_set_validation.find("xm:f").contents[0]
                    error_message = (
                        f'{option_set_validation.attrs["errorTitle"]}: {option_set_validation.attrs["error"]}'
                    )
                    sqref = option_set_validation.find("xm:sqref").contents[0]
                    column_ref = sqref.split(":")[0]
                    column_name = self.get_column_name(column_ref)
                    option_set_range_values = self.get_option_set_range_values(option_set_range)
                    option_sets[column_name] = {
                        "column_name": column_name,
                        "option_set_range": option_set_range,
                        "option_set_range_values": option_set_range_values,
                        "error_message": error_message,
                        "sqref": sqref,
                    }
        # Sort Option Sets by sqref
        option_sets = OrderedDict(sorted(option_sets.items(), key=lambda x: x[1]["sqref"]))
        self.option_sets = option_sets
        return option_sets

    def get_column_name(self, column_ref):
        # Get letters from column_ref
        column_letters = "".join([c for c in column_ref if c.isalpha()])
        data_sheet = self.sheet_data["dataSheet"]
        header_row = data_sheet[0]
        for cell in header_row.find_all("c"):
            if cell.attrs["r"].startswith(column_letters):
                cell_value = cell.find("v").contents[0]
                return str(cell_value)

    def _xml_to_soup(self):
        for xml_file in self.xml_files:
            try:
                self.xml_soup[xml_file.stem] = BeautifulSoup(xml_file.read_text(), "xml")
            except UnicodeDecodeError:
                pass

    def get_shared_strings(self):
        self.shared_strings = []
        for name, xml_soup in self.xml_soup.items():
            if xml_soup.find("sst"):
                sis = xml_soup.find("sst").find_all("si")
                for si in sis:
                    self.shared_strings.append(si.find("t").contents[0])
        return self.shared_strings

    def get_worksheets(self):
        self.worksheets = [ws for ws in self.xml_soup.keys() if "sheet" in ws]
        return self.worksheets

    def _get_table_columns(self):
        ...

    def get_table_columns(self, n=1):
        table_soup = self.xml_soup[f"table{n}"]
        table_columns = table_soup.find("tableColumns").find_all("tableColumn")
        self.table_columns = [col for col in table_columns]
        return self.table_columns

    def parse_worksheet(self, sheet):
        sheet_soup = self.xml_soup[sheet]
        code_name = sheet_soup.find("sheetPr")
        sheet_data = sheet_soup.find("sheetData")
        data_validations = sheet_soup.find("dataValidations")
        ext_list = sheet_soup.find("extLst")
        if code_name:
            code_name = code_name.attrs["codeName"]
        if sheet_data:
            sheet_data = sheet_data.find_all("row")
        if data_validations and code_name:
            data_validations = data_validations.find_all("dataValidation")
        if ext_list:
            option_set_validations = ext_list.find_all("x14:dataValidation")
        else:
            option_set_validations = None

        return code_name, sheet_data, data_validations, option_set_validations


class BaseXML:
    name: str
    xml_file: Path
    xml_soup: BeautifulSoup

    def __init__(self, xml_file: Path):
        self.xml_file = xml_file
        self.name = xml_file.stem
        self.xml_to_soup()

    def xml_to_soup(self):
        with open(self.xml_file, "rb") as f:
            self.xml_soup = BeautifulSoup(f, "xml")


class TableXML(BaseXML):
    id: str
    table_name: str
    display_name: str
    ref: str
    table_columns: dict
    num_columns: int

    def __init__(self, xml_file: Path):
        super().__init__(xml_file)
        self.get_table_info()
        self.get_table_columns()

    def get_table_info(self):
        table = self.xml_soup.find("table")
        self.id = table.attrs["id"]
        self.table_name = table.attrs["name"]
        self.display_name = table.attrs["displayName"]
        self.ref = table.attrs["ref"]
        return self.id, self.name, self.display_name, self.ref

    def get_table_columns(self):
        table_columns = self.xml_soup.find("tableColumns").find_all("tableColumn")
        self.table_columns = {col.attrs["id"]: col.attrs for col in table_columns}
        self.num_columns = len(self.table_columns)
        return self.table_columns


class WorksheetXML(BaseXML):
    code_name: str
    dimension: str
    columns: dict
    sheet_data: dict
    data_validations: dict
    option_set_validations: dict
    mapping_str: str
    logical_name: str
    checksum: str
    column_name_map: dict

    def __init__(self, xml_file: Path):
        super().__init__(xml_file)
        self.get_worksheet_info()
        self.get_sheet_data()
        self.get_mapping_str()
        self.get_option_sets()
        self.get_data_validations()
        self.get_columns()

    def to_json(self):
        # Output dict if type is str, dict, or list
        def _to_json(obj):
            if isinstance(obj, (str, dict, list)):
                return obj
            # Output dict if type is BaseXML
            elif isinstance(obj, BaseXML):
                return obj.__dict__
            # Output list if type is list of BaseXML
            elif isinstance(obj, list):
                return [_to_json(o) for o in obj]
            # Output dict if type is dict of BaseXML
            elif isinstance(obj, dict):
                return {k: _to_json(v) for k, v in obj.items()}
            # Output None if type is None
            elif obj is None:
                return None
            # Output str if type is unknown
            else:
                return str(obj)

    def get_worksheet_info(self):
        sheet = self.xml_soup.find("x:worksheet")
        if not sheet:
            return None, None
        sheet_pr = sheet.find("x:sheetPr")
        self.code_name = sheet_pr.attrs.get("codeName", None)
        dimension = sheet.find("x:dimension")
        self.dimension = dimension.attrs["ref"] if dimension else None
        return self.code_name, self.dimension

    def get_data_validations(self):
        dvs = {}
        data_validations = self.xml_soup.find("x:dataValidations")
        for dv in data_validations.find_all("x:dataValidation") if data_validations else []:
            formula_1 = dv.find_all("x:formula1")
            formula_2 = dv.find_all("x:formula2")
            dvs[dv.attrs["sqref"]] = dv.attrs
            if formula_1:
                dvs[dv.attrs["sqref"]]["formula_1"] = formula_1[0].getText()
            if formula_2:
                dvs[dv.attrs["sqref"]]["formula_2"] = formula_2[0].getText()
        self.data_validations = dvs
        return self.data_validations

    def get_sheet_data(self):
        self.sheet_data = self.xml_soup.find("x:sheetData")
        return self.sheet_data

    def get_mapping_str(self):
        if self.code_name == "hiddenDataSheet":
            mapping_str = self.sheet_data.find("x:c", {"r": "A1"}).find("x:v").getText()
            # Trim off leading/trailing spaces and remove any newlines
            self.mapping_str = mapping_str.strip().replace("\r", "").replace("\n", "")
            self.logical_name, self.checksum, self.column_name_map = parse_mapping_string(self.mapping_str)
        elif self.code_name == "dataSheet":
            self.mapping_str = None

    def get_option_sets(self):
        if self.code_name == "hiddenDataSheet":
            rows = self.sheet_data.find_all("x:row")[1:]
            option_sets = {}
            for row in rows:
                cells = row.find_all("x:c")
                for cell in cells:
                    r = cell.attrs["r"]
                    parse_r_pat = re.compile(r"(?P<col>[A-Z]+)(?P<row>[0-9]+)")
                    match = parse_r_pat.match(r)
                    if match:
                        if match.group("row") not in option_sets:
                            option_sets[match.group("row")] = []
                        option_sets[match.group("row")].append({"value": cell.find("x:v").text, "attrs": cell.attrs})
            self.option_sets = option_sets
        elif self.code_name == "dataSheet":
            self.option_sets = None
        return self.option_sets

    def get_columns(self):
        sheet_data = self.xml_soup.find("x:sheetData")
        if self.code_name == "hiddenDataSheet":
            ...
        elif self.code_name == "dataSheet":
            header_row = sheet_data.find("x:row", {"r": "1"})
            columns = header_row.find_all("c")
            self.columns = {col.attrs["r"]: {"attrs": col.attrs, "name": col.find("t").text} for col in columns}
            ...


class WorkbookXML(BaseXML):
    worksheets: dict

    def __init__(self, xml_file: Path):
        super().__init__(xml_file)
        self.worksheets = {}
        self.get_worksheets()

    def get_worksheets(self):
        worksheets = self.xml_soup.find_all("x:sheet")
        for ws in worksheets:
            self.worksheets[ws.attrs["r:id"]] = ws.attrs
        return self.worksheets


class MigrationTemplate:
    template_name: str
    template_path: Path
    crm_display_name: str
    crm_logical_name: str
    xml_files: list[Path, ...]
    xml_soup: dict[str, BeautifulSoup]
    tables: dict[TableXML, ...]
    worksheets: dict[WorksheetXML, ...]
    workbook: WorkbookXML
    column_name_map: dict[str, str]

    def __init__(self, template_path: Path):
        self.template_path = template_path
        self.template_name = template_path.name
        self.load_xml_files()
        self.load_workbook()
        self.load_worksheets()
        self.load_tables()
        self.set_logical_name()
        self.set_display_name()
        self.get_columns()
        self.get_column_mapping()
        self.get_data_validations()
        self.get_option_sets()
        self.get_columns_by_display_name()
        self.get_columns_by_logical_name()
        ...

    def get_column_mapping(self):
        hidden_data_sheet = self.worksheets.get("hiddenDataSheet", None)
        if hidden_data_sheet:
            self.column_name_map = hidden_data_sheet.column_name_map
        else:
            self.column_name_map = {}

    def get_columns(self):
        data_sheet = self.worksheets.get("dataSheet", None)
        cellref_pat = re.compile(r"([A-Z]+)([0-9]+)")
        self.get_column_mapping()
        column_display_names = {v: k for k, v in self.column_name_map.items()}
        if data_sheet:
            cols = data_sheet.columns
            self.columns = {}
            for col, colref in cols.items():
                col_letter = cellref_pat.match(col).group(1)
                col_name = colref["name"]
                self.columns[col_letter] = {"display_name": col_name,
                                            "logical_name": column_display_names.get(col_name, None)}
        else:
            self.columns = {}

    def get_data_validations(self):
        data_sheet = self.worksheets.get("dataSheet", None)
        if data_sheet:
            self.data_validations = data_sheet.data_validations
            for dv in self.data_validations.values():
                sqref_pat = re.compile(r"(?P<start_col>[A-Z]+)(?P<start_row>[0-9]+):(?P<end_col>[A-Z]+)(?P<end_row>[0-9]+)")
                match = sqref_pat.match(dv["sqref"])
                if not match:
                    continue
                start_col = match.group("start_col")
                start_row = match.group("start_row")
                end_col = match.group("end_col")
                end_row = match.group("end_row")
                col_range = get_two_letter_col_range(start_col, end_col)
                for col in col_range:
                    column = self.columns.get(col, {})
                    if column == {}:
                        self.columns[col] = {}
                    column["type"] = dv["promptTitle"]
                    column["constraint"] = dv["error"]
                    column["prompt"] = dv["prompt"]
                    if dv["allowBlank"] == "0":
                        column["required"] = True
                    else:
                        column["required"] = False
                    if "Minimum Value:" in dv["prompt"] and "Maximum Value:" in dv["prompt"]:
                        if dv["formula_1"] and dv["formula_2"]:
                            column["min_value"] = dv["formula_1"]
                            column["max_value"] = dv["formula_2"]
                    if dv["type"] == "textLength":
                        column["max_length"] = dv["formula_1"]





    def get_option_sets(self):
        if not self.columns:
            self.get_columns()
        formula_pat = re.compile(r"hiddenSheet!\$([A-Z]+)\$([0-9]+):\$([A-Z]+)\$([0-9]+)")
        hidden_data_sheet = self.worksheets.get("hiddenDataSheet", None)
        if not hidden_data_sheet:
            self.option_sets = {}
        if not self.data_validations:
            self.get_data_validations()
        list_validations = [dv for dv in self.data_validations.values() if dv["type"] == "list"]
        option_sets = hidden_data_sheet.option_sets
        template_columns = {}
        for dv in list_validations:
            option_list = []
            sqref_pat = re.compile(r"(?P<start_col>[A-Z]+)(?P<start_row>[0-9]+):(?P<end_col>[A-Z]+)(?P<end_row>[0-9]+)")
            sqref_start_col, sqref_start_row, sqref_end_col, sqref_end_row = sqref_pat.match(dv["sqref"]).groups()
            sqref_cols = get_two_letter_col_range(sqref_start_col, sqref_end_col)
            option_set_formula = dv.get("formula_1", None)
            if not option_set_formula:
                continue
            match = formula_pat.match(option_set_formula)
            if match:
                option_set_row = match.group(2)
                option_set_start_col = match.group(1)
                option_set_end_col = match.group(3)
                option_set_col_range = get_two_letter_col_range(option_set_start_col, option_set_end_col)
                option_set = option_sets.get(option_set_row, None)
                if not option_set:
                    continue
                for o_s in option_set:
                    option_list.append(o_s["value"])
            for col in sqref_cols:
                self.columns[col]["option_set"] = option_list

            ...

        self.option_sets = option_sets




    def set_logical_name(self, logical_name=None):
        hidden_data_sheet = self.worksheets.get("hiddenDataSheet", None)
        if hidden_data_sheet:
            self.crm_logical_name = hidden_data_sheet.logical_name

    def set_display_name(self):
        datasheet_name = self.workbook.worksheets.get("dataSheet", None).get("name", None)
        if datasheet_name:
            self.crm_display_name = datasheet_name

    def load_xml_files(self):
        self.xml_files = [xml_file.relative_to(self.template_path) for xml_file in self.template_path.rglob("*.xml")]
        ...

    def load_workbook(self):
        if self.xml_files is None:
            self.load_xml_files()
        if Path("xl/workbook.xml") in self.xml_files:
            self.workbook = WorkbookXML(self.template_path / "xl/workbook.xml")
        else:
            raise KeyError("Workbook XML file not found.")

    def load_worksheets(self):
        if self.xml_files is None:
            self.load_xml_files()
        self.worksheets = {}
        for worksheet_xml in self.template_path.rglob("xl/worksheets/sheet*.xml"):
            ws = WorksheetXML(worksheet_xml)
            self.worksheets[ws.code_name] = ws

        if self.worksheets == {}:
            raise KeyError("Worksheet XML files not found.")

    def load_tables(self):
        if self.xml_files is None:
            self.load_xml_files()

        self.tables = {}
        for table_xml in self.template_path.rglob("xl/tables/table*.xml"):
            self.tables[table_xml.stem] = TableXML(table_xml)
        if self.tables == {}:
            raise KeyError("Table XML files not found.")

    def to_json(self):
        return {self.crm_logical_name: {
            "template_name": self.template_name,
            "template_path": str(self.template_path),
            "crm_display_name": self.crm_display_name,
            "crm_logical_name": self.crm_logical_name,
            "column_name_map": self.column_name_map,
            "data_validations": self.data_validations,
            "option_sets": self.option_sets,
            "columns": self.columns,
            "columns_by_display_name": self.columns_by_display_name,
            "columns_by_logical_name": self.columns_by_logical_name,

        }}

    def get_columns_by_display_name(self):
        self.columns_by_display_name = {}
        for k, v in self.columns.items():
            self.columns_by_display_name[v["display_name"]] = v
            self.columns_by_display_name[v["display_name"]]["column"] = k

    def get_columns_by_logical_name(self):
        self.columns_by_logical_name = {}
        for k, v in self.columns.items():
            self.columns_by_logical_name[v["logical_name"]] = v
            self.columns_by_logical_name[v["logical_name"]]["column"] = k


def parse_mapping_string(mapping_str: str):
    entity_logical_name, checksum, col_name_map_str = mapping_str.split(":")
    # URL Unquote the column name map, and XML unescape the string
    col_name_map_str = unquote(col_name_map_str)
    col_name_map_str = unescape(col_name_map_str)
    col_name_map_pairs = col_name_map_str.split("&")
    col_name_map = {}
    for pair in col_name_map_pairs:
        col_logical_name, col_display_name = pair.split("=")
        col_name_map[col_logical_name] = col_display_name
    return entity_logical_name, checksum, col_name_map


def template_dirs_to_json(templates_path: Path):
    templates = []
    for template_path in templates_path.iterdir():
        if template_path.is_dir():
            try:
                templates.append(ExcelTemplate(template_path))
            except KeyError:
                continue
    for template in templates:
        print(template.template_name)
        try:
            table_columns = template.get_table_columns()
        except KeyError:
            continue
        # Save to JSON
        output_path = templates_path / "../templates/json"
        output_path.mkdir(exist_ok=True)
        with open(output_path / f"{template.template_name}_Template.json", "w") as f:
            f.write(template.to_json())


def import_templates_to_json(template_parent_path: Path):
    templates = []
    for template_path in template_parent_path.iterdir():
        if template_path.is_dir():
            try:
                logger.info(f"Processing {template_path}")
                template = MigrationTemplate(template_path)
                templates.append(template.to_json())
            except KeyError as e:
                print(f"KeyError: {template_path}. {e}")
                continue
    for template in templates:
        logical_name = list(template.keys())[0]
        template_json = template[logical_name]
        output_path = template_parent_path / "../json_templates" / template_parent_path.name
        output_path.mkdir(exist_ok=True)
        with open(output_path / f"{logical_name}.json", "w") as f:
            f.write(json.dumps(template_json, indent=2))
    ...


def extract_xlsx_to_template_dir(template_path: Path):
    template_name = template_path.stem
    # Extract the template to a temp directory
    temp_dir = Path(tempfile.mkdtemp())
    with zipfile.ZipFile(template_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
    # Rename the temp_dir to the template name and move it to the templates directory
    output_path = Path(template_path.parent) / template_name
    if output_path.exists():
        shutil.rmtree(output_path)
    temp_dir.rename(Path(template_path.parent) / template_name)
    return temp_dir


if __name__ == "__main__":
    import_templates_to_json(template_parent_path=TEMPLATES_ROOT)
    # extract_xlsx_to_template_dir(Path(r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\import_templates\Opportunity Close.xlsx"))
    # test_migration_template_class()
    # main()
    ...
    # template_paths = [
    #     Path(r"../data/templates"),
    #     Path(r"../data/import_templates"),
    #     Path(r"../data/templates_logical_name"),
    #     Path(r"../data/templates_formatted_name")
    #     ]
    # for template_path in template_paths:
    #     parse_updated_templates(template_path)
    #     template_dirs_to_json(template_path)

    # templates = load_template_sheets(Path("../../data/mipcrm-extract/_Data Model/templates"))
    # for template in templates:
    #     unzip_xlsx_template(template)
