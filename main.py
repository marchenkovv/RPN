import asyncio
import io
import os
import shutil
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple, Set

from api.ecp_api import AsyncECP

load_dotenv()


class PatientRecord:
    """Класс для хранения записи пациента для сверки"""

    def __init__(self, enp: str, bp: str, fam: str, im: str, ot: str, dr: str):
        self.enp = enp
        self.bp = bp  # Дата прикрепления
        self.fam = fam
        self.im = im
        self.ot = ot
        self.dr = dr  # Дата рождения

    def matches(self, other: 'PatientRecord') -> bool:
        """Проверка полного совпадения по всем ключевым полям"""
        return (self.enp == other.enp and
                self.bp == other.bp and
                self.fam == other.fam and
                self.im == other.im and
                self.ot == other.ot and
                self.dr == other.dr)

    @classmethod
    def from_xml_element(cls, zap_element: ET.Element) -> 'PatientRecord':
        """Создание объекта из XML элемента ZAP"""
        # Извлекаем данные из элемента ZAP
        enp = zap_element.findtext('ENP', '')

        # Извлекаем BP (дата прикрепления)
        bp = zap_element.findtext('BP', '')

        # Извлекаем данные из элемента PACIENT
        pacient = zap_element.find('PACIENT')
        if pacient is not None:
            fam = pacient.findtext('FAM', '')
            im = pacient.findtext('IM', '')
            ot = pacient.findtext('OT', '')
            dr = pacient.findtext('DR', '')
        else:
            fam = im = ot = dr = ''

        return cls(enp, bp, fam, im, ot, dr)


def get_successful_attachments(rpn_in_dir: str, code_mo_tfoms: str) -> Set[Tuple]:
    """
    Собирает все успешные прикрепления из ответных файлов RPNF.

    Args:
        rpn_in_dir: Путь к папке IN с ответами от ТФОМС
        code_mo_tfoms: Код МО

    Returns:
        Множество кортежей (ENP, BP, FAM, IM, OT, DR) успешных прикреплений
    """
    successful_attachments = set()

    # Получаем все RPNF файлы (кроме файлов открепления с номером 1)
    rpnf_files = rpnf_list(rpn_in_dir, code_mo_tfoms, detach=False)

    for file_path in rpnf_files:
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith('.xml'):
                        with zip_file.open(file_name) as xml_file:
                            tree = ET.parse(xml_file)
                            root = tree.getroot()

                            for zap in root.findall('ZAP'):
                                # Проверяем статус - только успешные (STATUS=1)
                                status = zap.findtext('STATUS', '')
                                if status != '1':
                                    continue

                                # Создаем запись пациента
                                patient = PatientRecord.from_xml_element(zap)

                                # Добавляем в множество, если все поля не пустые
                                if patient.enp and patient.bp:
                                    successful_attachments.add((
                                        patient.enp, patient.bp,
                                        patient.fam, patient.im, patient.ot, patient.dr
                                    ))
        except Exception as e:
            print(f"Ошибка при обработке файла {file_path}: {e}")
            continue

    return successful_attachments


def get_failed_attachments(rpn_in_dir: str, archive_dir: str, code_mo_tfoms: str) -> Set[Tuple]:
    """
    Собирает записи с ошибками из FRPNM файлов.

    Args:
        rpn_in_dir: Путь к папке IN с ответами от ТФОМС
        archive_dir: Путь к папке archive
        code_mo_tfoms: Код МО

    Returns:
        Множество кортежей (ENP, BP) записей с ошибками
    """
    failed_attachments = set()

    # Получаем все FRPNM файлы
    frpn_files = frpn_list(rpn_in_dir, code_mo_tfoms)
    print(f"Найдено FRPNM файлов: {len(frpn_files)}")

    for frpn_file in frpn_files:
        print(f"Обработка FRPNM файла: {os.path.basename(frpn_file)}")
        try:
            with zipfile.ZipFile(frpn_file, 'r') as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith('.xml'):
                        with zip_file.open(file_name) as xml_file:
                            tree = ET.parse(xml_file)
                            root = tree.getroot()

                            # Извлекаем имя исходного RPNM файла
                            fname_i = root.findtext('FNAME_I', '')
                            print(f"  FNAME_I: {fname_i}")

                            # Ищем все записи с ошибками
                            for pr in root.findall('PR'):
                                uid = pr.findtext('UID', '')
                                comment = pr.findtext('COMMENT', '')
                                print(f"    Найден UID с ошибкой: {uid}")
                                print(f"    Комментарий: {comment}")

                                if uid and fname_i:
                                    # Формируем имя RPNM файла (добавляем .zip)
                                    rpnm_filename = f"{fname_i}.zip"
                                    print(f"    Ищем в файле: {rpnm_filename}")

                                    # Ищем файл в archive
                                    rpnm_path = os.path.join(archive_dir, rpnm_filename)
                                    if os.path.exists(rpnm_path):
                                        print(f"    Файл найден: {rpnm_path}")
                                        with zipfile.ZipFile(rpnm_path, 'r') as rpnm_zip:
                                            for rpnm_file in rpnm_zip.namelist():
                                                if rpnm_file.endswith('.xml'):
                                                    with rpnm_zip.open(rpnm_file) as rpnm_xml:
                                                        rpnm_tree = ET.parse(rpnm_xml)
                                                        rpnm_root = rpnm_tree.getroot()

                                                        # Ищем ZAP с таким UID
                                                        found = False
                                                        for zap in rpnm_root.findall('ZAP'):
                                                            zap_uid = zap.findtext('UID', '')
                                                            if zap_uid == uid:
                                                                enp = zap.findtext('ENP', '')
                                                                bp = zap.findtext('BP', '')
                                                                if enp and bp:
                                                                    print(f"    ✅ НАЙДЕНА ОШИБКА: ENP={enp}, BP={bp}")
                                                                    failed_attachments.add((enp, bp))
                                                                found = True
                                                                break
                                                        if not found:
                                                            print(f"    ❌ UID {uid} не найден в файле {rpnm_filename}")
                                    else:
                                        print(f"    ❌ Файл не найден: {rpnm_path}")
        except Exception as e:
            print(f"Ошибка при обработке FRPNM файла {frpn_file}: {e}")
            continue

    return failed_attachments


def filter_new_attachments(
        new_patients: List[PatientRecord],
        successful_attachments: Set[Tuple],
        failed_attachments: Set[Tuple]
) -> List[PatientRecord]:
    """
    Фильтрует новых пациентов, исключая уже успешно прикрепленных и с ошибками.

    Args:
        new_patients: Список новых пациентов для отправки
        successful_attachments: Множество успешных прикреплений
        failed_attachments: Множество записей с ошибками

    Returns:
        Отфильтрованный список пациентов для отправки
    """
    filtered_patients = []

    for patient in new_patients:
        patient_full_tuple = (patient.enp, patient.bp, patient.fam, patient.im, patient.ot, patient.dr)
        patient_key_tuple = (patient.enp, patient.bp)

        # Включаем пациента, если:
        # 1. Его нет в успешных прикреплениях (полное совпадение всех полей)
        # 2. Его нет в записях с ошибками (по ENP+BP)
        if patient_full_tuple in successful_attachments:
            print(f"Исключен (уже успешно прикреплен): ENP={patient.enp}, BP={patient.bp}")
        elif patient_key_tuple in failed_attachments:
            print(f"Исключен (была ошибка в предыдущей отправке): ENP={patient.enp}, BP={patient.bp}")
        else:
            filtered_patients.append(patient)

    return filtered_patients


def rpnf_list(dir_path: str,
              code_mo_tfoms: str,
              detach: bool = False) -> list[str]:
    """
    Возвращает список РПН файлов.

    Формат файла: RPNF{код_МО}{дата}{номер_файла}.zip (21 символ)
    - detach=True: возвращает файлы с номером '1.zip' (открепление)
    - detach=False: возвращает файлы без '1.zip' (прикрепление)

    Args:
        dir_path: Путь к директории
        code_mo_tfoms: Код МО
        detach: Флаг для выбора типа файлов

    Returns:
        Список отфильтрованных файлов
    """
    try:
        files = os.listdir(dir_path)
    except FileNotFoundError:
        return []

    prefix = f'RPNF{code_mo_tfoms}'
    required_length = 21
    result = []

    for file in files:
        if len(file) != required_length:
            continue
        if not file.startswith(prefix):
            continue

        full_path = os.path.join(dir_path, file)
        if not os.path.isfile(full_path):
            continue

        if detach:
            if file.endswith('1.zip'):
                result.append(full_path)
        else:
            if not file.endswith('1.zip'):
                result.append(full_path)

    return result


def frpn_list(dir_path: str,
              code_mo_tfoms: str) -> list[str]:
    """
    Возвращает список ФРПН файлов.

    Формат файла: FRPNM{код_МО}{дата}{номер_файла}.zip (22 символа)

    Args:
        dir_path: Путь к директории
        code_mo_tfoms: Код МО

    Returns:
        Список отфильтрованных файлов
    """
    try:
        files = os.listdir(dir_path)
    except FileNotFoundError:
        return []

    prefix = f'FRPNM{code_mo_tfoms}'
    required_length = 22
    result = []

    for file in files:
        if len(file) != required_length:
            continue
        if not file.startswith(prefix):
            continue

        full_path = os.path.join(dir_path, file)
        if not os.path.isfile(full_path):
            continue

        result.append(full_path)

    return result


def get_next_file_number(out_dir: str, code_mo: str) -> int:
    """
    Определяет следующий номер файла для отправки.
    Номера начинаются с 2, чтобы не конфликтовать с файлами открепления (1.zip)
    """
    try:
        files = os.listdir(out_dir)
    except FileNotFoundError:
        return 2

    max_num = 1  # Начинаем с 1, так как файлы открепления имеют номер 1
    prefix = f'RPNM{code_mo}'

    for file in files:
        if file.startswith(prefix) and file.endswith('.zip'):
            # Извлекаем номер из имени файла
            # Формат: RPNM{код_МО}{дата}{номер}.zip
            try:
                # Имя файла: RPNM8300042603172.zip
                # prefix: RPNM830004
                # остаток: 2603172.zip
                # номер: 2
                num_part = file[len(prefix):-4]  # Отрезаем префикс и .zip
                # Убираем дату (6 цифр) из начала
                if len(num_part) > 6:
                    num_part = num_part[6:]  # Оставляем только номер
                if num_part.isdigit():
                    num = int(num_part)
                    if num > max_num:
                        max_num = num
            except:
                continue

    return max_num + 1


def get_current_month_date_range() -> Tuple[str, str]:
    """Возвращает период с 1-го числа текущего месяца по текущую дату"""
    today = datetime.now()
    first_day = datetime(today.year, today.month, 1)
    return first_day.strftime('%d.%m.%Y'), today.strftime('%d.%m.%Y')


def save_files(zip_buffer: io.BytesIO, filename: str, out_dir: str, archive_dir: str):
    """
    Сохраняет файл в папки OUT и archive
    """
    # Создаем директории, если их нет
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Пути для сохранения
    out_path = os.path.join(out_dir, filename)
    archive_path = os.path.join(archive_dir, filename)

    # Сохраняем в OUT
    with open(out_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    # Копируем в archive
    shutil.copy2(out_path, archive_path)

    print(f"Файл сохранен:\n  OUT: {out_path}\n  archive: {archive_path}")


async def main():
    # Загрузка переменных окружения
    code_mo = os.getenv('CODE_MO_TFOMS')  # Код МО
    rpn_in = os.getenv('RPN_IN')  # Входящее прикрепление / открепление
    rpn_out = os.getenv('RPN_OUT')  # Исходящее прикрепление / открепление
    archive_dir = os.getenv('ARCHIVE_DIR', 'archive')  # Архивная папка

    # ЕЦП
    server = os.getenv('SERVER_ECP')
    login = os.getenv('LOGIN_ECP')
    password = os.getenv('PASSWORD_ECP')
    lpu_id = os.getenv('ATTACH_LPU_ID')

    # Создаем директории, если их нет
    os.makedirs(rpn_in, exist_ok=True)
    os.makedirs(rpn_out, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Проверка переменных окружения
    if not all([code_mo, rpn_in, rpn_out, archive_dir, server, login, password, lpu_id]):
        exit("Не все переменные окружения заданы")

    print("=" * 50)
    print("Начало обработки прикреплений")
    print(f"Код МО: {code_mo}")
    print(f"Папка IN: {rpn_in}")
    print(f"Папка OUT: {rpn_out}")
    print(f"Папка archive: {archive_dir}")
    print("=" * 50)

    # Шаг 1: Сбор данных об уже прикрепленных пациентах из ответных файлов
    print("\n[1/4] Сбор успешных прикреплений из ответных файлов...")
    successful_attachments = get_successful_attachments(rpn_in, code_mo)
    print(f"Найдено {len(successful_attachments)} успешных прикреплений")

    # Шаг 1b: Сбор записей с ошибками из FRPNM файлов
    print("\n[1b/4] Сбор записей с ошибками из FRPNM файлов...")
    failed_attachments = get_failed_attachments(rpn_in, archive_dir, code_mo)
    print(f"Найдено {len(failed_attachments)} записей с ошибками")

    # Шаг 2: Получение новых данных от сервера
    print("\n[2/4] Запрос новых данных о прикреплениях...")

    # Определяем период с 1-го числа по текущую дату
    date_from, date_to = get_current_month_date_range()
    date_range = f"{date_from} - {date_to}"
    # date_range = f"29.12.2026 - {date_to}"
    print(f"Период выгрузки: {date_range}")

    async with AsyncECP(server, login, password) as ecp:
        if not ecp.sess_id:
            exit("Ошибка авторизации на сервере")

        # Запрос на формирование файла с прикрепленным населением
        result = await ecp.service_attachment(lpu_id, date_range)

        if not result.get('success'):
            exit("Ошибка при формировании файла на сервере")

        # Получаем ссылку на файл
        zip_link = server + '/' + result.get('Link').replace('//', '/')
        filename_zip = zip_link.split('/')[-1]

        print(f"Получен файл: {filename_zip}")

        # Скачиваем файл
        response = await ecp.client.request('GET', zip_link, headers=ecp.headers)
        response.raise_for_status()

    # Шаг 3: Обработка и фильтрация данных
    print("\n[3/4] Обработка и фильтрация данных...")

    zip_buffer = io.BytesIO(response.content)
    files_content = {}
    new_patients = []

    # Определяем следующий номер файла
    file_number = get_next_file_number(rpn_out, code_mo)

    # Формируем базовое имя для файлов - ИСПРАВЛЕНО
    date_str = datetime.now().strftime('%y%m%d')
    base_filename = f"RPNM{code_mo}{date_str}"
    zip_filename = f"{base_filename}{file_number}.zip"
    xml_filename = f"{base_filename}{file_number}.xml"  # Внутри архива должно быть это имя

    print(f"Новый файл будет: {zip_filename}")

    with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
        for file_name in zip_file.namelist():
            if file_name.endswith('.xml'):
                with zip_file.open(file_name) as xml_file:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()

                    # Собираем всех пациентов из файла
                    for zap in root.findall('ZAP'):
                        patient = PatientRecord.from_xml_element(zap)
                        if patient.enp and patient.bp:  # Проверяем наличие ключевых полей
                            new_patients.append(patient)

                    # Сохраняем измененный XML
                    xml_buffer = io.BytesIO()
                    tree.write(xml_buffer, encoding='windows-1251', xml_declaration=True)
                    files_content[xml_filename] = xml_buffer.getvalue()

    print(f"Всего пациентов в новой выгрузке: {len(new_patients)}")

    # Фильтруем пациентов с учетом успешных и ошибочных
    filtered_patients = filter_new_attachments(new_patients, successful_attachments, failed_attachments)
    print(f"Пациентов после фильтрации: {len(filtered_patients)}")

    # Проверка на пустоту
    if not filtered_patients:
        print("\n❌ Нет пациентов для отправки после фильтрации. Процесс завершен.")
        return

    # Шаг 4: Сохранение файлов
    print("\n[4/4] Сохранение файлов...")

    # Создаем новый XML только с отфильтрованными пациентами
    # Берём, структуру из первого обработанного XML
    with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
        for file_name in zip_file.namelist():
            if file_name.endswith('.xml'):
                with zip_file.open(file_name) as xml_file:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()

                    # Создаем новый корневой элемент
                    new_root = ET.Element(root.tag)

                    # Копируем ZGLV (заголовок) без изменений
                    zglv = root.find('ZGLV')
                    if zglv is not None:
                        new_root.append(zglv)

                    # Добавляем только отфильтрованных пациентов
                    patients_added = 0
                    for zap in root.findall('ZAP'):
                        patient = PatientRecord.from_xml_element(zap)

                        # Проверяем, есть ли пациент в отфильтрованном списке
                        for filtered_patient in filtered_patients:
                            if (filtered_patient.enp == patient.enp and
                                    filtered_patient.bp == patient.bp and
                                    filtered_patient.fam == patient.fam and
                                    filtered_patient.im == patient.im and
                                    filtered_patient.ot == patient.ot and
                                    filtered_patient.dr == patient.dr):
                                # Копируем элемент ZAP
                                new_root.append(zap)
                                patients_added += 1
                                break

                    print(f"Добавлено пациентов в новый XML: {patients_added}")

                    # Обновляем имя файла в ZGLV
                    new_root.find('ZGLV/FILENAME').text = base_filename + str(file_number)

                    # Собираем всех пациентов из файла
                    for zap in root.findall('ZAP'):
                        patient = PatientRecord.from_xml_element(zap)
                        if patient.enp and patient.bp:  # Проверяем наличие ключевых полей
                            new_patients.append(patient)

                        # Исправляем REASON 4 на 1 (ошибка разработчиков)
                        reason_elem = zap.find('REASON')
                        if reason_elem is not None and reason_elem.text == '4':
                            reason_elem.text = '1'

                        # Удаляем STATUS, если есть (выгружается по ошибке)
                        status_elem = zap.find('STATUS')
                        if status_elem is not None:
                            zap.remove(status_elem)

                    # Сохраняем новый XML
                    xml_buffer = io.BytesIO()
                    new_tree = ET.ElementTree(new_root)
                    new_tree.write(xml_buffer, encoding='windows-1251', xml_declaration=True)

                    # Создаем новый ZIP с отфильтрованным XML
                    new_zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(new_zip_buffer, 'w') as new_zip:
                        new_zip.writestr(xml_filename, xml_buffer.getvalue())

                    # Сохраняем файл
                    save_files(new_zip_buffer, zip_filename, rpn_out, archive_dir)
                    break

    print("\n✅ Обработка завершена успешно!")
    print(f"Отправлено пациентов: {len(filtered_patients)}")
    print(f"Файл для отправки: {zip_filename}")


if __name__ == "__main__":
    asyncio.run(main())
