import asyncio
import io
import os
import zipfile
# noinspection PyPep8Naming
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv

from api.ecp_api import AsyncECP
from file_utils import (
    get_successful_attachments,
    get_failed_attachments,
    filter_new_attachments,
    build_output_zip,
    save_files, find_missing_patients,
)
from models import PatientRecord

load_dotenv()


def get_current_month_range():
    today = datetime.now()
    first = datetime(today.year, today.month, 1)
    return first.strftime('%d.%m.%Y'), today.strftime('%d.%m.%Y')


def get_current_year_range():
    today = datetime.now()
    first = datetime(today.year, 1, 1)
    return first.strftime('%d.%m.%Y'), today.strftime('%d.%m.%Y')


async def main():
    # --- Конфигурация ---
    code_mo = os.getenv('CODE_MO_TFOMS')
    rpn_in = os.getenv('RPN_IN')
    rpn_out = os.getenv('RPN_OUT')
    archive_dir = os.getenv('ARCHIVE_DIR', 'archive')
    server = os.getenv('SERVER_ECP')
    login = os.getenv('LOGIN_ECP')
    password = os.getenv('PASSWORD_ECP')
    lpu_id = os.getenv('ATTACH_LPU_ID')

    required = [code_mo, rpn_in, rpn_out, server, login, password, lpu_id]
    if not all(required):
        exit('Не все переменные окружения заданы')

    for d in (rpn_in, rpn_out, archive_dir):
        os.makedirs(d, exist_ok=True)

    date_from, date_to = get_current_month_range()
    date_range_str = f'{date_from} - {date_to}'

    # date_from, date_to = get_current_year_range()
    # date_range_str = f'01.01.2026 - {date_to}'

    date_range = (date_from, date_to)

    print(f'Период: {date_range_str}')

    # --- 1. Сбор уже обработанных ---
    print('\n[1/4] Сбор успешных прикреплений...')
    successful = get_successful_attachments(rpn_in, code_mo, date_range)
    print(f'Успешных: {len(successful)}')

    print('\n[2/4] Сбор ошибок из FRPNM и RPNF...')
    failed_frpnm, failed_rpnf = get_failed_attachments(rpn_in, archive_dir, code_mo, date_range)
    print(f'Количество FRPNM: {len(failed_frpnm)}')
    print(f'Количество RPNF: {len(failed_rpnf)}')
    print(f'Список RPNF по которым были ошибки:')
    for row in failed_rpnf:
        print(row)

    # --- 2. Скачивание новых данных ---
    print(f'\n[3/4] Запрос данных с сервера ({date_range_str})...')

    async with AsyncECP(server, login, password) as ecp:
        result = await ecp.service_attachment(lpu_id, date_range_str)
        if not result.get('success'):
            error_msg = ' '.join(result.get('Error_Msg', '').split())
            exit(f'Ошибка формирования файла на сервере: {error_msg}')

        link = result['Link'].replace('//', '/')
        original_filename = link.split('/')[-1]
        content = await ecp.download(link)

        # Получаем данные из журнала РПН: Прикрепление
        ter = await ecp.get_person_card_grid(lpu_id, date_range_str)
        ped = await ecp.get_person_card_grid(lpu_id, date_range_str, 2)
        ter_data = ter.get('data') or []
        ped_data = ped.get('data') or []
        total_data = ter_data + ped_data

    # --- 3. Парсинг и фильтрация ---
    zip_buffer = io.BytesIO(content)

    # Парсим XML один раз
    with zipfile.ZipFile(zip_buffer, 'r') as zf:
        xml_name = next(n for n in zf.namelist() if n.endswith('.xml'))
        with zf.open(xml_name) as f:
            source_root = ET.parse(f).getroot()

    # Собираем пациентов
    new_patients = [
        PatientRecord.from_xml(zap)
        for zap in source_root.findall('ZAP')
        if PatientRecord.from_xml(zap).is_valid and zap.find('EP') is None  # + исключаем откреплённых
    ]
    print(f'Всего в выгрузке: {len(new_patients)}')

    # Фильтруем
    filtered = filter_new_attachments(new_patients, successful, failed_frpnm)
    print(f'После фильтрации: {len(filtered)}')

    if not filtered:
        print('\nНет пациентов для отправки.')
        return

    # --- 4. Формирование и сохранение ---
    print('\n[4/4] Сохранение...')

    zip_name, zip_buf = build_output_zip(source_root, filtered, original_filename)
    save_files(zip_buf, zip_name, rpn_out, archive_dir)

    print(f'\n✅ Готово! Отправлено: {len(filtered)}, файл: {zip_name}')


if __name__ == '__main__':
    asyncio.run(main())
