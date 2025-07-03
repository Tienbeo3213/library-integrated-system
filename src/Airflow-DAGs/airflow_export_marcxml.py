from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pyodbc
from lxml import etree
from pymarc import Record, Field, Subfield
import re
import os
from pytz import timezone

# Hàm xử lý logic xuất dữ liệu
def export_marcxml_new_records():
    # Kết nối đến SQL Server
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.150.6;'
        'DATABASE=libol;'
        'UID=itc;'
        'PWD=spkt@2025;'
    )
    cursor = connection.cursor()

    today = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
    SELECT Tai_lieu_ID
    FROM Tai_lieu
    WHERE CONVERT(DATE, Ngay_giao_dich) = '{today}'
    """
    cursor.execute(query)
    tai_lieu_ids = [row[0] for row in cursor.fetchall()]

    if not tai_lieu_ids:
        print("Không có tài liệu mới trong ngày.")
        return

    # Hàm tạo record từ logic của bạn
    def is_three_digit_tag(tag):
        return bool(re.fullmatch(r'\d{3}', tag)) and int(tag) <= 900

    def create_valid_leader(leader, record_length):
        leader = leader.ljust(24, '0')
        leader = f"{record_length:05d}{leader[5:]}"
        if leader[5] not in {'a', 'c', 'd', 'n', 'p'}:
            leader = leader[:5] + 'n' + leader[6:]
        if leader[6] not in {'a', 'c', 'd', 'e', 'f', 'g', 'i', 'j', 'k', 'm', 'o', 'p', 'r', 't'}:
            leader = leader[:6] + 'a' + leader[7:]
        if leader[7] not in {'a', 'b', 'c', 'd', 'i', 'm', 's'}:
            leader = leader[:7] + 'm' + leader[8:]
        if leader[8] not in {'#', 'a'}:
            leader = leader[:8] + ' ' + leader[9:]
        if leader[9] not in {'#', 'a'}:
            leader = leader[:9] + 'a' + leader[10:]
        leader = leader[:10] + '2' + '2' + leader[12:]
        leader = leader[:12] + '00000' + leader[17:]
        if leader[17] not in {'#', '1', '2', '3', '4', '5', '7', '8', 'u', 'z'}:
            leader = leader[:17] + ' ' + leader[18:]
        if leader[18] not in {'#', 'a', 'c', 'i', 'n', 'u'}:
            leader = leader[:18] + 'a' + leader[19:]
        if leader[19] not in {'#', 'a', 'b', 'c'}:
            leader = leader[:19] + ' ' + leader[20:]
        leader = leader[:20] + '4500'
        return leader

    def normalize_indicator(indicator):
        if not indicator or len(indicator) != 2:
            return ' ', ' '
        return indicator[0], indicator[1]

    def unicode_to_html_entities(text):
        if not isinstance(text, str):
            text = str(text)
        return text

    def process_field(nhan_truong, indicators, gia_tri):
        indicators = normalize_indicator(indicators)
        if '$' not in gia_tri:
            return Field(
                tag=nhan_truong,
                indicators=[' ', ' '],
                subfields=[],
                data=unicode_to_html_entities(gia_tri)
            )
        else:
            subfields = process_subfields(gia_tri)
            return Field(
                tag=nhan_truong,
                indicators=list(indicators),
                subfields=subfields,
                data=None
            )

    def process_subfields(gia_tri):
        subfields = []
        if not isinstance(gia_tri, str):
            gia_tri = str(gia_tri)
        matches = re.finditer(r'\$([a-zA-Z0-9])([^$]*)', gia_tri)
        for match in matches:
            code = match.group(1)
            value = match.group(2).strip()
            if value:
                subfields.append(Subfield(code=code, value=unicode_to_html_entities(value)))
        return subfields

    def get_leader(tai_lieu_id):
        query = f"SELECT Leader FROM Tai_lieu WHERE Tai_lieu_ID = {tai_lieu_id};"
        cursor.execute(query)
        leader_row = cursor.fetchone()
        return leader_row[0] if leader_row else None

    def generate_record_for_tai_lieu(tai_lieu_id):
        try:
            query = f"""
            DECLARE @tai_lieu_id INT;
            SET @tai_lieu_id = {tai_lieu_id};
            SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) as GT FROM Field000s, Ten_truong_USMARC  WHERE Ten_truong_USMARC.Truong_ID = Field000s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field100s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field100s.Truong_ID AND Tai_lieu_ID = @tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field200s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field200s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field250s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field250s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field300s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field300s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field400s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field400s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field500s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field500s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field600s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field600s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field700s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field700s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field800s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field800s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            UNION ALL SELECT ID, Ten_truong_USMARC.Truong_ID,  Nhan_truong, Indicators,dbo.DecodeUTF8String(REPLACE(REPLACE(REPLACE(REPLACE(Gia_tri, '\$a', ''), '\$b', ''), '\$c', ''), '\$d', '')) FROM Field900s, Ten_truong_USMARC WHERE Ten_truong_USMARC.Truong_ID = Field900s.Truong_ID AND Tai_lieu_ID =@tai_lieu_id 
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            record = Record()
            record.add_field(Field(tag="001", data=str(tai_lieu_id)))

            for row in rows:
                truong_id, nhan_truong, indicators, gia_tri = row[1:]
                if not gia_tri or gia_tri.strip() == "":
                    continue
                if is_three_digit_tag(nhan_truong):
                    record.add_field(process_field(nhan_truong, indicators, gia_tri))

            # Loại bỏ các trường trùng lặp
            record = remove_duplicate_tags(record)

            # Tạo phần tử XML từ record
            record_element = create_record_element(record)

            leader = get_leader(tai_lieu_id)
            if leader:
                record_length = len(etree.tostring(record_element, encoding='utf-8'))
                valid_leader = create_valid_leader(leader, record_length)
                leader_element = etree.Element('leader')
                leader_element.text = valid_leader
                record_element.insert(0, leader_element)
            else:
                raise ValueError(f"Tài liệu {tai_lieu_id} không có thẻ Leader hợp lệ")

            return record_element
        except Exception as e:
            print(f"Lỗi khi xử lý tài liệu ID {tai_lieu_id}: {e}")
            return None

    def remove_duplicate_tags(record):
        unique_fields = {}
        fields_to_remove = []

        for field in record.get_fields():
            if field.tag.isdigit() and int(field.tag) >= 10:
                gt_values = "".join([subfield.value for subfield in field.subfields])
                key = (field.tag, gt_values)
                if key in unique_fields:
                    fields_to_remove.append(field)
                else:
                    unique_fields[key] = field

        for field in fields_to_remove:
            record.remove_field(field)

        return record

    def create_record_element(record):
        nsmap = {
            None: "http://www.loc.gov/MARC21/slim",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
        }
        record_element = etree.Element('record', nsmap=nsmap)
        record_element.set('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation',
                           'http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd')

        for field in record.fields:
            if field.data is not None:
                controlfield_element = etree.SubElement(record_element, 'controlfield', tag=field.tag)
                controlfield_element.text = field.data
            else:
                datafield_element = etree.SubElement(record_element, 'datafield', tag=field.tag)
                ind1, ind2 = field.indicators if len(field.indicators) == 2 else (' ', ' ')
                datafield_element.set('ind1', ind1)
                datafield_element.set('ind2', ind2)
                for subfield in field.subfields:
                    subfield_element = etree.SubElement(datafield_element, 'subfield', code=subfield.code)
                    subfield_element.text = subfield.value

        return record_element

    output_dir = "/home/lib/airflow/Data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file = os.path.join(output_dir, f"marcxml_{today}.marcxml")
    root_element = etree.Element('collection', nsmap={
        None: "http://www.loc.gov/MARC21/slim",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    })

    for tai_lieu_id in tai_lieu_ids:
        record_element = generate_record_for_tai_lieu(tai_lieu_id)
        if record_element is not None:
            root_element.append(record_element)

    tree = etree.ElementTree(root_element)
    tree.write(output_file, encoding="utf-8", xml_declaration=True, pretty_print=True)
    print(f"Dữ liệu đã được xuất ra file: {output_file}")

default_args = {
    'owner': 'lib',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='export_marcxml_daily',
    default_args=default_args,
    description='Xuất dữ liệu MARCXML hàng ngày vào lúc 12h đêm',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 11, 15),
    catchup=False,
    tags=['export', 'marcxml'],
) as dag:
    export_task = PythonOperator(
        task_id='export_marcxml',
        python_callable=export_marcxml_new_records,

    )
