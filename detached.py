import asyncio
import csv
import os
import time
from typing import Set, Tuple, List

from dotenv import load_dotenv

from api.ecp_api import AsyncECP
from api.tfoms_api import TFOMS
from file_utils import rpnf_list, iter_zap_from_zip
from main import get_current_year_range  # type: ignore
from models import PatientRecord

load_dotenv()

CSV_DEAD = 'tfoms_dead.csv'
CSV_REJECTED = 'tfoms_rejected.csv'
DEAD_FIELDS = [
    'surname', 'name', 'patronymic', 'birthDate', 'sex',
    'enp', 'removeDate', 'Person_id', 'Person_IsDead', 'checked',
]
REJECTED_FIELDS = ['enp', 'reason', 'checked_at']

COMMENT_DETACH = (
    'Данные о прикреплении изменились.'
    'Откреплён в связи с аннулированием всех страховок'
)

TFOMS_CONCURRENCY = int(os.getenv('TFOMS_CONCURRENCY', '10'))


def log(msg: str) -> None:
    print(msg, flush=True)


def read_csv(path: str) -> List[dict]:
    if not (os.path.isfile(path) and os.path.getsize(path) > 0):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def write_all(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, '') for k in fieldnames})


def load_rejected() -> dict:
    return {r['enp']: r for r in read_csv(CSV_REJECTED) if r.get('enp')}


def save_rejected(rejected: dict) -> None:
    write_all(CSV_REJECTED,
              [rejected[k] for k in sorted(rejected)],
              REJECTED_FIELDS)


async def collect_detached() -> None:
    code_mo = os.getenv('CODE_MO_TFOMS')
    rpn_in = os.getenv('RPN_IN')
    date_range = get_current_year_range()

    # 1. Парсинг архивов
    detached: Set[Tuple] = set()
    for path in rpnf_list(rpn_in, code_mo, detach=True, date_range=date_range):
        try:
            for zap in iter_zap_from_zip(path):
                if zap.findtext('COMMENT') != COMMENT_DETACH:
                    continue
                p = PatientRecord.from_xml(zap)
                if p.is_valid:
                    detached.add(p.get_detached)
        except Exception:
            pass

    # 2. Дедупликация
    existing_rows = read_csv(CSV_DEAD)
    existing_enps = {r['enp'] for r in existing_rows if r.get('enp')}
    rejected = load_rejected()

    new_entries = [
        e for e in sorted(detached, key=lambda x: (x[0], x[1], x[2]))
        if e[4] not in existing_enps and e[4] not in rejected
    ]

    stats = {'total': len(detached), 'new': len(new_entries),
             'added': 0, 'rejected_new': 0, 'errors': 0, 'dups': 0}
    new_rows: List[dict] = []

    # 3. Запросы к ТФОМС
    if new_entries:
        sem = asyncio.Semaphore(TFOMS_CONCURRENCY)
        async with TFOMS.Parser(  # type: ignore
                base_url=os.getenv('SERVER_TFOMS'),
                login=os.getenv('LOGIN_TFOMS'),
                password=os.getenv('PASSWORD_TFOMS'),
        ) as parser:

            async def _fetch(entry):
                async with sem:
                    try:
                        return entry, await parser.enp(entry[4])
                    except Exception as e:
                        return entry, e

            tasks = [asyncio.create_task(_fetch(e)) for e in new_entries]
            pairs = [await fut for fut in asyncio.as_completed(tasks)]

        now = time.strftime('%Y-%m-%d %H:%M:%S')
        for entry, response in pairs:
            if isinstance(response, Exception) or not response:
                stats['errors'] += 1
                continue

            valid_ins = next(
                (ins for ins in response.get('insurances', [])
                 if ins.get('removeRsn') == 101),
                None,
            )
            erz = response.get('erz', {})
            enp = erz.get('enp') or entry[4]

            if valid_ins is None or not erz.get('enp'):
                rejected[enp] = {
                    'enp': enp,
                    'reason': ('no_remove_101' if valid_ins is None
                               else 'empty_erz_enp'),
                    'checked_at': now,
                }
                stats['rejected_new'] += 1
                continue

            if enp in existing_enps:
                stats['dups'] += 1
                continue
            existing_enps.add(enp)

            new_rows.append({
                'surname': erz.get('surname', ''),
                'name': erz.get('name', ''),
                'patronymic': erz.get('patronymic', ''),
                'birthDate': erz.get('birthDate', ''),
                'sex': erz.get('sex', ''),
                'enp': enp,
                'removeDate': valid_ins.get('removeDate', ''),
                'Person_id': '',
                'Person_IsDead': '',
                'checked': 'false',
            })
            stats['added'] += 1

    if new_rows:
        write_all(CSV_DEAD, existing_rows + new_rows, DEAD_FIELDS)
    if stats['rejected_new']:
        save_rejected(rejected)

    log(f"📊 [ТФОМС] Всего откреплённых: {stats['total']} | "
        f"новых: {stats['new']} | добавлено: {stats['added']} | "
        f"отклонено: {stats['rejected_new']} | "
        f"ошибок: {stats['errors']} | дубликатов: {stats['dups']}")


async def check_ecp() -> None:
    rows = read_csv(CSV_DEAD)
    if not rows:
        log("[ЕЦП] dead.csv пуст — нечего проверять.")
        return

    to_check = [r for r in rows if (r.get('checked') or '').lower() != 'true']
    stats = {'total': len(to_check), 'alive': 0, 'dead': 0,
             'not_found': 0, 'errors': 0}

    if to_check:
        async with AsyncECP(
                os.getenv('SERVER_ECP'),
                os.getenv('LOGIN_ECP'),
                os.getenv('PASSWORD_ECP'),
        ) as ecp:
            for row in to_check:
                try:
                    result = await ecp.get_person_search_grid(
                        row.get('surname'), row.get('name'),
                        row.get('patronymic'), row.get('birthDate'),
                    )
                    data = (result or {}).get('data') or []
                    if not data:
                        stats['not_found'] += 1
                        row['checked'] = 'true'
                        continue

                    person = data[0]
                    row['Person_id'] = person.get('Person_id') or ''
                    row['Person_IsDead'] = person.get('Person_IsDead') or ''
                    row['checked'] = 'true'
                    if row['Person_IsDead'] == 'false':
                        stats['alive'] += 1
                    else:
                        stats['dead'] += 1
                except Exception:
                    stats['errors'] += 1

        write_all(CSV_DEAD, rows, DEAD_FIELDS)

    log(f"📊 [ЕЦП] К проверке: {stats['total']} | "
        f"живых: {stats['alive']} | мёртвых: {stats['dead']} | "
        f"не найдено: {stats['not_found']} | ошибок: {stats['errors']}")


async def main():
    t0 = time.perf_counter()
    await collect_detached()
    await check_ecp()
    log(f"Общее время: {time.perf_counter() - t0:.1f} с")


if __name__ == '__main__':
    asyncio.run(main())
