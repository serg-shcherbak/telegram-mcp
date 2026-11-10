import io
import wave
import zipfile
from xml.etree import ElementTree

from PIL import Image
from pypdf import PdfReader

from dot_original_fixtures import fixtures


def test_generated_originals_have_real_known_answers():
    records = fixtures()
    files = {record["name"]: record["data"] for record in records}
    assert len(records) == 7
    assert len({record["message_id"] for record in records}) == 7
    assert sum(map(len, files.values())) < 100_000
    assert all(record["task"] and record["expected"] for record in records)
    image = Image.open(io.BytesIO(files["colors.png"]))
    assert [image.getpixel((x, 30)) for x in (50, 150, 250)] == [
        (255, 0, 0),
        (0, 128, 0),
        (0, 0, 255),
    ]
    text_pdf = PdfReader(io.BytesIO(files["contract.pdf"]))
    assert len(text_pdf.pages) == 2
    assert "PDF-482" in text_pdf.pages[0].extract_text()
    assert "731" in text_pdf.pages[1].extract_text()
    scan = PdfReader(io.BytesIO(files["scan.pdf"]))
    assert not scan.pages[0].extract_text().strip()
    assert len(scan.pages[0].images) == 1
    with zipfile.ZipFile(io.BytesIO(files["parts.docx"])) as archive:
        document = ElementTree.fromstring(archive.read("word/document.xml"))
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    cells = document.findall(".//w:tbl/w:tr/w:tc", ns)
    assert ["".join(cell.itertext()) for cell in cells] == [
        "Item",
        "Quantity",
        "Synthetic bolts",
        "731",
    ]
    with zipfile.ZipFile(io.BytesIO(files["orders.xlsx"])) as archive:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        assert [sheet.attrib["name"] for sheet in workbook.findall("s:sheets/s:sheet", ns)] == [
            "Orders",
            "Summary",
        ]
        summary = ElementTree.fromstring(archive.read("xl/worksheets/sheet2.xml"))
        formula = summary.find(".//s:c[@r='B2']", ns)
        assert formula.find("s:f", ns).text == "Orders!B2+Orders!B3"
        assert formula.find("s:v", ns).text == "731"
    with wave.open(io.BytesIO(files["tone.wav"]), "rb") as audio:
        assert (audio.getnchannels(), audio.getsampwidth(), audio.getframerate()) == (1, 2, 8000)
        assert audio.getnframes() == 8000
    assert files["rgb.mp4"][4:8] == b"ftyp"
    album = [record["message_id"] for record in records if record["grouped_id"] == 901]
    assert album == [1, 3]
