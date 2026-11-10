"""Small generated originals and local-only known answers for host acceptance."""

import io
import math
import struct
import wave
import zipfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject


def _zip(parts):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, text in parts.items():
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, text)
    return output.getvalue()


def fixtures():
    image = Image.new("RGB", (300, 100), "white")
    draw = ImageDraw.Draw(image)
    for index, color in enumerate(("red", "green", "blue")):
        draw.rectangle((index * 100, 0, index * 100 + 99, 59), fill=color)
    draw.text((12, 70), "IMAGE-482", fill="black", font=ImageFont.load_default(size=20))
    output = io.BytesIO()
    image.save(output, format="PNG")
    png = output.getvalue()

    writer = PdfWriter()
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    for line in ("Synthetic contract code PDF-482", "Final total: 731"):
        page = writer.add_blank_page(width=400, height=200)
        page[NameObject("/Resources")] = DictionaryObject(
            {NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})}
        )
        stream = DecodedStreamObject()
        stream.set_data(f"BT /F1 18 Tf 30 120 Td ({line}) Tj ET".encode("ascii"))
        page[NameObject("/Contents")] = writer._add_object(stream)
    output = io.BytesIO()
    writer.write(output)
    pdf = output.getvalue()

    scan = Image.new("RGB", (480, 160), "white")
    draw = ImageDraw.Draw(scan)
    draw.text((20, 20), "SCAN-482", fill="black", font=ImageFont.load_default(size=40))
    draw.text((20, 90), "Total 731", fill="black", font=ImageFont.load_default(size=30))
    output = io.BytesIO()
    scan.save(
        output,
        format="PDF",
        resolution=72,
        creationDate="D:20200101000000Z",
        modDate="D:20200101000000Z",
    )
    scan_pdf = output.getvalue()

    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
    )
    relationship_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    office_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    word_ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    docx = _zip(
        {
            "[Content_Types].xml": content_types
            + '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>',
            "_rels/.rels": f'<Relationships xmlns="{relationship_ns}"><Relationship Id="rId1" Type="{office_ns}/officeDocument" Target="word/document.xml"/></Relationships>',
            "word/document.xml": f'<w:document xmlns:w="{word_ns}"><w:body>'
            "<w:p><w:r><w:t>DOCX-482</w:t></w:r></w:p><w:tbl>"
            "<w:tr><w:tc><w:p><w:r><w:t>Item</w:t></w:r></w:p></w:tc>"
            "<w:tc><w:p><w:r><w:t>Quantity</w:t></w:r></w:p></w:tc></w:tr>"
            "<w:tr><w:tc><w:p><w:r><w:t>Synthetic bolts</w:t></w:r></w:p></w:tc>"
            "<w:tc><w:p><w:r><w:t>731</w:t></w:r></w:p></w:tc></w:tr>"
            "</w:tbl><w:sectPr/></w:body></w:document>",
        }
    )
    spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    xlsx = _zip(
        {
            "[Content_Types].xml": content_types
            + '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
            "_rels/.rels": f'<Relationships xmlns="{relationship_ns}"><Relationship Id="rId1" Type="{office_ns}/officeDocument" Target="xl/workbook.xml"/></Relationships>',
            "xl/workbook.xml": f'<workbook xmlns="{spreadsheet_ns}" xmlns:r="{office_ns}">'
            '<sheets><sheet name="Orders" sheetId="1" r:id="rId1"/>'
            '<sheet name="Summary" sheetId="2" r:id="rId2"/></sheets></workbook>',
            "xl/_rels/workbook.xml.rels": f'<Relationships xmlns="{relationship_ns}">'
            f'<Relationship Id="rId1" Type="{office_ns}/worksheet" Target="worksheets/sheet1.xml"/>'
            f'<Relationship Id="rId2" Type="{office_ns}/worksheet" Target="worksheets/sheet2.xml"/></Relationships>',
            "xl/worksheets/sheet1.xml": f'<worksheet xmlns="{spreadsheet_ns}"><sheetData>'
            '<row r="1"><c r="A1" t="inlineStr"><is><t>XLSX-482</t></is></c></row>'
            '<row r="2"><c r="A2" t="inlineStr"><is><t>Bolts</t></is></c><c r="B2"><v>700</v></c></row>'
            '<row r="3"><c r="A3" t="inlineStr"><is><t>Washers</t></is></c><c r="B3"><v>31</v></c></row>'
            "</sheetData></worksheet>",
            "xl/worksheets/sheet2.xml": f'<worksheet xmlns="{spreadsheet_ns}"><sheetData>'
            '<row r="2"><c r="A2" t="inlineStr"><is><t>Total</t></is></c>'
            '<c r="B2"><f>Orders!B2+Orders!B3</f><v>731</v></c></row>'
            "</sheetData></worksheet>",
        }
    )
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setparams((1, 2, 8000, 8000, "NONE", "not compressed"))
        audio.writeframes(
            b"".join(
                struct.pack("<h", round(8000 * math.sin(2 * math.pi * 440 * index / 8000)))
                for index in range(8000)
            )
        )
    specs = [
        (
            "colors.png",
            "image/png",
            png,
            "Name the left-to-right colors and visible code.",
            "Red, green, blue; IMAGE-482.",
            901,
        ),
        (
            "contract.pdf",
            "application/pdf",
            pdf,
            "Give the code, page count and final-page total.",
            "PDF-482; two pages; final total 731.",
            None,
        ),
        (
            "scan.pdf",
            "application/pdf",
            scan_pdf,
            "Read the code and total in this scanned page.",
            "SCAN-482; total 731; one scanned page.",
            901,
        ),
        (
            "parts.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            docx,
            "Give the document code and the item and quantity in its table.",
            "DOCX-482; Synthetic bolts; 731.",
            None,
        ),
        (
            "orders.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            xlsx,
            "Name both sheets, the formula in Summary!B2 and its cached value.",
            "Orders and Summary; Orders!B2+Orders!B3; cached 731. No recalculation is claimed.",
            None,
        ),
        (
            "tone.wav",
            "audio/wav",
            output.getvalue(),
            "Describe duration and audible content.",
            "One second of a 440 Hz tone; no speech. This does not prove ASR.",
            None,
        ),
        (
            "rgb.mp4",
            "video/mp4",
            (Path(__file__).parent / "fixtures/dot-originals/rgb.mp4").read_bytes(),
            "Describe colors at 0.5, 1.5 and 2.5 seconds and the full covered range.",
            "Red, green, blue; three seconds; no audio.",
            None,
        ),
    ]
    return [
        dict(
            message_id=index,
            name=name,
            mime=mime,
            data=data,
            task=task,
            expected=expected,
            grouped_id=grouped_id,
        )
        for index, (name, mime, data, task, expected, grouped_id) in enumerate(specs, start=1)
    ]
