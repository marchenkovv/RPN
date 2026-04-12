import re

import httpx


class AsyncECP:
    def __init__(self, server: str, login: str, password: str, timeout: float = 30.0):
        self.url = server
        self.login = login
        self.password = password
        self.timeout = timeout
        self.sess_id = None
        self.client = None

        self.headers = {
            'user-agent': (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/124.0.0.0 YaBrowser/24.6.0.0 Safari/537.36'
            ),
        }

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=self.timeout,
            verify=True,
            follow_redirects=True,
        )
        self.sess_id = await self._login()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _login(self) -> str:
        data = await self._api_get('/api/user/login', {
            'login': self.login,
            'password': self.password,
        })
        return data['sess_id']

    async def _logout(self):
        if self.sess_id:
            await self._api_get('/api/user/logout', {'sess_id': self.sess_id})

    # noinspection PyBroadException
    async def close(self):
        try:
            await self._logout()
        except Exception:
            pass
        finally:
            if self.client:
                await self.client.aclose()
                self.client = None
            self.sess_id = None

    async def _api_get(self, path: str, params: dict) -> dict:
        """GET-запрос к API с проверкой error_code."""
        response = await self.client.get(f'{self.url}{path}', params=params)
        response.raise_for_status()
        body = response.json()
        if body.get('error_code') != 0:
            raise RuntimeError(f'Ошибка API: {body.get("error_msg", "unknown")}')
        return body.get('data', {})

    async def _request(self, method: str, url: str, **kwargs) -> dict:
        """Универсальный запрос с raise_for_status."""
        if not self.client:
            raise RuntimeError('Сессия не началась. Используй async with')
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    async def service_attachment(self, lpu_id: int, date_range: str) -> dict:
        """
        Запрос файла прикреплений. Если получен файл с номером 1,
        автоматически делает повторный запрос для получения файла с номером 2.

        Returns:
            (content, filename): содержимое файла и имя файла
        """
        url = f'{self.url}/?c=ServiceAttachment&m=runExport'

        for attempt in range(2):  # максимум 2 попытки
            result = await self._request('POST', url, data={
                'sess_id': self.sess_id,
                'Lpu_id': lpu_id,
                'ExportDateRange': date_range,
                'ExportType': 'attached',
            })

            # тестируем перевыгрузку
            # if attempt == 0:
            #     result = {'success': True, 'Link': 'export/attached_list//RPNM8300042604121.zip'}

            if not result.get('success'):
                return result

            link = result['Link'].replace('//', '/')
            filename = link.split('/')[-1]

            # Если это первая попытка И файл с номером 1 — продолжаем цикл
            if attempt == 0:
                match = re.search(r'RPNM\d{12}(\d+)\.zip$', filename)
                if match and match.group(1) == '1':
                    print(f'Обнаружен файл с номером 1: {filename}. Повторное формирование...')
                    continue  # делаем вторую попытку

            # Иначе возвращаем результат
            print(f'Использую файл: {filename}')
            return result
        raise RuntimeError('Не удалось получить файл после 2 попыток')

    async def download(self, path: str) -> bytes:
        """Скачивание файла по относительному пути."""
        url = f'{self.url}/{path.lstrip('/')}'
        response = await self.client.get(url)
        response.raise_for_status()
        return response.content

    async def get_person_card_grid(self, lpu_id: int, date_range: str, type_id: int = 1):
        """Получаем список прикреплённых по журналу РПН:Прикрепление"""
        url = f'{self.url}/?c=Person&m=getPersonCardGrid'
        return await self._request('POST', url, data={
            'start': 0,
            'limit': 100,
            'AttachLpu_id': lpu_id,
            'PersonCard_begDate': date_range,
            'LpuRegionType_id': type_id,  # 1, 2
            'PersonCard_IsAttachCondit': 1,
            'PersonCardStateType_id': 1,
            'dontShowUnknowns': 1,
        })

    async def get_gerson_card_history_list(self, person_id: str):
        """Получаем список прикреплений пациента по person_id"""
        url = f'{self.url}/?c=Person&m=getPersonCardHistoryList'
        return await self._request('POST', url, data={
            'Person_id': person_id,
            'AttachType': 'common_region',
        })

    async def get_person_card(self, person_card_id):
        """Загружаем данные прикрепления"""
        url = f'{self.url}/?c=PersonCard&m=getPersonCard'
        return await self._request('POST', url, data={
            'PersonCard_id': person_card_id,
            'attrObjects': [{'object': 'PersonCardEditWindow', 'identField': 'PersonCard_id'}],
        })

    async def save_person_card(self, person_card_id):
        """Загружаем данные прикрепления"""
        url = f'{self.url}/?c=PersonCard&m=savePersonCard'
        return await self._request('POST', url, data={
            'PersonCard_id': person_card_id,
            'attrObjects': [{'object': 'PersonCardEditWindow', 'identField': 'PersonCard_id'}],
        })
