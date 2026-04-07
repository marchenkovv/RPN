import io
import os
import shutil
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Tuple, Set, List, Optional

from models import PatientRecord


# --------------- Поиск файлов ---------------

def _list_rpn_files(
        dir_path: str,
        prefix: str,
        required_length: int,
        date_offset: int,
        date_range: Optional[Tuple[str, str]] = None,
        suffix_filter: Optional[str] = None,
        suffix_exclude: Optional[str] = None,
) -> List[str]:
    """
    Универсальный поиск файлов по шаблону имени.

    Args:
        dir_path: Директория поиска
        prefix: Ожидаемый префикс (например 'RPNF83000426')
        required_length: Точная длина имени файла
        date_offset: Позиция начала даты YYMMDD в имени файла
        date_range: Фильтр по дате ('dd.mm.yyyy', 'dd.mm.yyyy')
        suffix_filter: Если задан - файл должен заканчиваться на это
        suffix_exclude: Если задан - файл НЕ должен заканчиваться на это
    """
    try:
        files = os.listdir(dir_path)
    except FileNotFoundError:
        return []

    start_date = end_date = None
    if date_range:
        start_date = datetime.strptime(date_range[0], '%d.%m.%Y')
        end_date = datetime.strptime(date_range[1], '%d.%m.%Y')

    result = []
    for name in files:
        if len(name) != required_length or not name.startswith(prefix):
            continue

        full_path = os.path.join(dir_path, name)
        if not os.path.isfile(full_path):
            continue

        # Фильтр по суффиксу
        if suffix_filter and not name.endswith(suffix_filter):
            continue
        if suffix_exclude and name.endswith(suffix_exclude):
            continue

        # Фильтр по дате
        if start_date and end_date:
            try:
                file_date = datetime.strptime(name[date_offset:date_offset + 6], '%y%m%d')
                if not (start_date <= file_date <= end_date):
                    continue
            except (ValueError, IndexError):
                continue

        result.append(full_path)

    return result


def rpnf_list(dir_path: str, code_mo: str, detach: bool = False,
              date_range: Optional[Tuple[str, str]] = None) -> List[str]:
    prefix = f'RPNF{code_mo}'
    return _list_rpn_files(
        dir_path, prefix,
        required_length=21,
        date_offset=10,
        date_range=date_range,
        suffix_filter='1.zip' if detach else None,
        suffix_exclude=None if detach else '1.zip',
    )


def frpn_list(dir_path: str, code_mo: str,
              date_range: Optional[Tuple[str, str]] = None) -> List[str]:
    prefix = f'FRPNM{code_mo}'
    return _list_rpn_files(
        dir_path, prefix,
        required_length=22,
        date_offset=11,
        date_range=date_range,
    )


# --------------- Парсинг XML из ZIP ---------------

def iter_zap_from_zip(zip_path: str):
    """Итератор по элементам ZAP из XML внутри ZIP-файла."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if name.endswith('.xml'):
                with zf.open(name) as f:
                    root = ET.parse(f).getroot()
                    yield from root.findall('ZAP')


def parse_zip_xml(zip_path: str) -> ET.Element:
    """Возвращает корневой элемент первого XML в ZIP."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if name.endswith('.xml'):
                with zf.open(name) as f:
                    return ET.parse(f).getroot()
    raise FileNotFoundError(f'XML не найден в {zip_path}')


# --------------- Сбор данных ---------------

def get_successful_attachments(
        rpn_in_dir: str, code_mo: str, date_range: Tuple[str, str]
) -> Set[Tuple]:
    """Множество full_key успешных прикреплений (STATUS=1) из RPNF."""
    result = set()
    for path in rpnf_list(rpn_in_dir, code_mo, detach=False, date_range=date_range):
        try:
            for zap in iter_zap_from_zip(path):
                if zap.findtext('STATUS') != '1':
                    continue
                p = PatientRecord.from_xml(zap)
                if p.is_valid:
                    result.add(p.full_key)
        except Exception as e:
            print(f'Ошибка при обработке {path}: {e}')
    return result


def get_failed_attachments(
        rpn_in_dir: str, archive_dir: str, code_mo: str, date_range: Tuple[str, str]
) -> Set[Tuple[str, str]]:
    """Множество short_key (ENP, BP) записей с ошибками из FRPNM."""
    result = set()
    result_2 = set()

    # Ошибки RPNF
    for path in rpnf_list(rpn_in_dir, code_mo, detach=False, date_range=date_range):
        try:
            for zap in iter_zap_from_zip(path):
                if zap.findtext('STATUS') != '0':
                    continue
                p = PatientRecord.from_xml(zap)
                if p.is_valid:
                    result_2.add(p.full_key)
        except Exception as e:
            print(f'Ошибка при обработке {path}: {e}')

    # Ошибки FRPNM
    for frpn_path in frpn_list(rpn_in_dir, code_mo, date_range=date_range):
        try:
            root = parse_zip_xml(frpn_path)
        except Exception as e:
            print(f'Ошибка FRPNM {frpn_path}: {e}')
            continue

        fname_i = root.findtext('FNAME_I', '')
        if not fname_i:
            continue

        # Собираем все UID с ошибками
        error_uids = {pr.findtext('UID', '') for pr in root.findall('PR')}
        error_uids.discard('')

        if not error_uids:
            continue

        rpnm_path = os.path.join(archive_dir, f'{fname_i}.zip')
        if not os.path.exists(rpnm_path):
            print(f'Файл не найден: {rpnm_path}')
            continue

        try:
            for zap in iter_zap_from_zip(rpnm_path):
                if zap.findtext('UID', '') in error_uids:
                    p = PatientRecord.from_xml(zap)
                    if p.is_valid:
                        result.add(p.short_key)
                        print(f'❌ Найдена ошибка: ENP={p.enp} BP={p.bp}')
        except Exception as e:
            print(f'Ошибка при чтении {rpnm_path}: {e}')

    return result, result_2


# --------------- Фильтрация ---------------

def filter_new_attachments(
        patients: List[PatientRecord],
        successful: Set[Tuple],
        failed: Tuple[str, str],
) -> List[PatientRecord]:
    filtered = []
    for p in sorted(patients, key=lambda p: p.bp):
        if p.full_key in successful:
            print(f'✅ Пропуск (уже прикреплён): ENP={p.enp} BP={p.bp}')
        elif p.short_key in failed:
            print(f'❌ Пропуск (ошибка ранее): ENP={p.enp} BP={p.bp}')
        else:
            filtered.append(p)
    return filtered


# --------------- Формирование файла ---------------

def get_next_file_number(archive_dir: str, code_mo: str, date: datetime) -> int:
    """Следующий номер файла (начиная с 2, т.к. 1 - открепление)."""
    try:
        files = os.listdir(archive_dir)
    except FileNotFoundError:
        return 2

    prefix = f'RPNM{code_mo}{date.strftime('%y%m%d')}'
    max_num = 1

    for name in files:
        if name.startswith(prefix) and name.endswith('.zip'):
            num_str = name[len(prefix):-4]
            if num_str.isdigit():
                max_num = max(max_num, int(num_str))

    return max_num + 1


def build_output_zip(
        source_root: ET.Element,
        filtered_patients: List[PatientRecord],
        base_filename: str,
        file_number: int,
) -> Tuple[str, io.BytesIO]:
    """
    Создаёт ZIP с отфильтрованным XML.

    Returns:
        (имя_zip_файла, буфер_с_содержимым)
    """
    xml_name = f'{base_filename}{file_number}.xml'
    zip_name = f'{base_filename}{file_number}.zip'

    # Множество для быстрого поиска
    allowed_keys = {p.full_key for p in filtered_patients}

    # Новый корень
    new_root = ET.Element(source_root.tag)

    # Копируем заголовок
    zglv = source_root.find('ZGLV')
    if zglv is not None:
        # Обновляем FILENAME
        fn_elem = zglv.find('FILENAME')
        if fn_elem is not None:
            fn_elem.text = f'{base_filename}{file_number}'
        new_root.append(zglv)

    # Добавляем только отфильтрованные ZAP
    count = 0
    for zap in source_root.findall('ZAP'):
        p = PatientRecord.from_xml(zap)
        if p.full_key not in allowed_keys:
            continue

        # Исправляем REASON 4 на 1 (ошибка разработчиков)
        reason = zap.find('REASON')
        if reason is not None and reason.text == '4':
            reason.text = '1'

        # Удаляем STATUS (ошибка разработчиков)
        status = zap.find('STATUS')
        if status is not None:
            zap.remove(status)

        new_root.append(zap)
        count += 1

    print(f'Записей в итоговом XML: {count}')

    # Сериализация
    xml_buf = io.BytesIO()
    ET.ElementTree(new_root).write(xml_buf, encoding='Windows-1251', xml_declaration=True)

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(xml_name, xml_buf.getvalue())

    return zip_name, zip_buf


def save_files(zip_buffer: io.BytesIO, filename: str, out_dir: str, archive_dir: str):
    out_path = os.path.join(out_dir, filename)
    with open(out_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    shutil.copy2(out_path, os.path.join(archive_dir, filename))
    print(f'Сохранено: {out_path}')
