import asyncio
import time

from dotenv import load_dotenv
import httpx
from playwright.async_api import async_playwright

load_dotenv()


class TFOMS:
    """
    Главный класс для работы с веб-сервисом идентификации застрахованных лиц.

    Содержит два подкласса для разных способов идентификации:
    - API: работа через официальное API TFOMS (авторизация через API-ключ)
    - Parser: работа через парсинг веб-интерфейса (авторизация через Playwright)

    Пример использования:
        # Для API
        async with TFOMS.API(url, login, password) as api:
            result = await api.find_by_enp('1234567890123456')

        # Для парсера
        async with TFOMS.Parser(url, login, password) as parser:
            result = await parser.snils('123-456-789 00')
    Ссылка на документацию: https://www.webfoms.ru/help/50
    """

    class API:
        """
        Класс для работы с официальным API TFOMS.

        Использует прямую HTTP-авторизацию.
        """

        def __init__(self, base_url, login, password):
            """
            Инициализирует API клиент.

            Args:
                base_url (str): Базовый URL для API.
                login (str): Логин для аутентификации.
                password (str): Пароль для аутентификации.
            """
            self.base_url = base_url
            self.login = login
            self.password = password
            self.auth_token = None
            self.client = None
            self.last_request_time = 0
            self.request_interval = 2  # Интервал между запросами в секундах
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 YaBrowser/25.12.0.0 Safari/537.36'
            }

        async def __aenter__(self):
            """Поддержка асинхронного контекстного менеджера."""
            await self.authenticate()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Автоматическое закрытие клиента при выходе из контекста."""
            await self.close()

        async def close(self):
            """Закрывает HTTP-клиент и освобождает ресурсы."""
            if self.client:
                await self.client.aclose()

        async def _wait_if_needed(self):
            """Ожидает, если необходимо, чтобы соблюсти ограничение на частоту запросов."""
            current_time = time.time()
            elapsed_time = current_time - self.last_request_time
            if elapsed_time < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed_time)
            self.last_request_time = time.time()

        async def authenticate(self):
            """
            Получает токен аутентификации через API, прямой запрос к эндпоинту /auth/login или использование Basic Auth.

            Returns:
                str: Токен аутентификации.

            Raises:
                Exception: Если не удалось получить токен.
            """
            # используем Basic Auth
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                headers=self.headers,
                timeout=30.0
            )
            return self.client

        async def _make_request(self, endpoint, params=None):
            """
            Выполняет GET-запрос к указанному эндпоинту официального API.

            Args:
                endpoint (str): Эндпоинт для запроса.
                params (dict, optional): Параметры для запроса.

            Returns:
                dict: Ответ сервера в формате JSON.

            Raises:
                httpx.HTTPStatusError: Если запрос завершился ошибкой.
                RuntimeError: Если клиент не инициализирован.
            """
            if not self.client:
                raise RuntimeError('Клиент не инициализирован. Сначала вызовите authenticate()')

            await self._wait_if_needed()
            url = f'{self.base_url}/insurance/{endpoint}'
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()

        async def find_by_policy(self, policy_number):
            """
            Возвращает информацию о застрахованном лице по номеру полиса.

            Args:
                policy_number (str): Номер полиса.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {'polis_num': policy_number}
            return await self._make_request('findbypolis', params)

        async def find_by_fio_and_birth(self, first_name, second_name, middle_name='', date_of_birth=None):
            """
            Возвращает информацию о застрахованном лице по ФИО и дате рождения.

            Args:
                first_name (str): Имя застрахованного.
                second_name (str): Фамилия застрахованного.
                middle_name (str, optional): Отчество застрахованного.
                date_of_birth (str, optional): Дата рождения в формате дд.мм.гггг.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {
                'firstname': first_name,
                'secondname': second_name,
                'middlename': middle_name,
                'date_of_birth': date_of_birth
            }
            return await self._make_request('findbyfiodr', params)

        async def find_by_enp(self, enp_number):
            """
            Возвращает информацию о застрахованном лице по единому номеру полиса.

            Args:
                enp_number (str): Единый номер полиса.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {'enp': enp_number}
            return await self._make_request('findbyenp', params)

    class Parser:
        """
        Класс для работы через парсинг веб-интерфейса TFOMS.

        Использует Playwright для авторизации через браузер и получения токена.
        """

        def __init__(self, base_url, login, password):
            """
            Инициализирует парсер для работы с веб-интерфейсом.

            Args:
                base_url (str): Базовый URL веб-сервиса.
                login (str): Логин для аутентификации.
                password (str): Пароль для аутентификации.
            """
            self.base_url = base_url
            self.login = login
            self.password = password
            self.auth_token = None
            self.client = None
            self.last_request_time = 0
            self.request_interval = 2  # Интервал между запросами в секундах
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 YaBrowser/25.12.0.0 Safari/537.36'
            }
            # Для парсера используем другой базовый URL для API запросов
            self.api_base_url = 'http://192.168.110.66:88/api/insurance/search'

        async def __aenter__(self):
            """Поддержка асинхронного контекстного менеджера."""
            await self.authenticate()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Автоматическое закрытие клиента при выходе из контекста."""
            await self.close()

        async def close(self):
            """Закрывает HTTP-клиент и освобождает ресурсы."""
            if self.client:
                await self.client.aclose()

        async def _wait_if_needed(self):
            """Ожидает, если необходимо, чтобы соблюсти ограничение на частоту запросов."""
            current_time = time.time()
            elapsed_time = current_time - self.last_request_time
            if elapsed_time < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed_time)
            self.last_request_time = time.time()

        async def authenticate(self):
            """
            Получает токен аутентификации через Playwright (браузерную автоматизацию).

            Returns:
                str: Токен аутентификации.

            Raises:
                Exception: Если не удалось получить токен.
            """
            login_page = self.base_url + '/auth/login'

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()

                auth_token = None
                token_event = asyncio.Event()

                async def handle_response(response):
                    nonlocal auth_token
                    auth_header = response.request.headers.get('authorization', '')
                    if auth_header and 'Bearer' in auth_header:
                        auth_token = auth_header.replace('Bearer ', '')
                        token_event.set()

                page.on('response', handle_response)

                await page.goto(login_page)
                await page.fill('#username', self.login)
                await page.fill('#password', self.password)
                await page.click('button[type="submit"]')

                await page.wait_for_selector('header > div > p')
                await page.click('header > div > p')

                try:
                    await asyncio.wait_for(token_event.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    raise Exception('Не удалось получить токен аутентификации за отведенное время')

                self.auth_token = auth_token
                await browser.close()

            # Создаем HTTP-клиент с полученным токеном
            self.client = httpx.AsyncClient(
                headers={
                    **self.headers,
                    'Authorization': f'Bearer {self.auth_token}'
                },
                timeout=30.0
            )

            return self.auth_token

        async def _make_request(self, params):
            """
            Выполняет GET-запрос к API парсинга.

            Args:
                params (dict): Параметры для запроса.

            Returns:
                dict: Ответ сервера в формате JSON.

            Raises:
                httpx.HTTPStatusError: Если запрос завершился ошибкой.
                RuntimeError: Если клиент не инициализирован.
            """
            if not self.client:
                raise RuntimeError('Клиент не инициализирован. Сначала вызовите authenticate()')

            await self._wait_if_needed()
            response = await self.client.get(self.api_base_url, params=params)
            response.raise_for_status()
            return response.json()

        async def fiodr(self, surname, name, patronymic, birth_date):
            """
            Поиск по ФИО и дате рождения.

            Args:
                surname (str): Фамилия.
                name (str): Имя.
                patronymic (str): Отчество.
                birth_date (str): Дата рождения в формате YYYY-MM-DD.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {
                'searchType': 'fio',
                'surname': surname,
                'name': name,
                'patronymic': patronymic,
                'birthDate': birth_date,
            }
            return await self._make_request(params)

        async def enp(self, enp_number):
            """
            Поиск по единому номеру полиса.

            Args:
                enp_number (str): Единый номер полиса.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {
                'searchType': 'enp',
                'enp': enp_number
            }
            return await self._make_request(params)

        async def polis(self, polis_number, seria_number=None):
            """
            Поиск по номеру полиса.

            Args:
                polis_number (str): Номер полиса.
                seria_number (str, optional): Серия полиса.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {
                'searchType': 'polis',
                'polisNum': polis_number,
                'polisSer': seria_number
            }
            return await self._make_request(params)

        async def snils(self, snils):
            """
            Поиск по номеру СНИЛС.

            Args:
                snils (str): Номер СНИЛС.

            Returns:
                dict: Информация о застрахованном лице.
            """
            params = {
                'searchType': 'snils',
                'snils': snils
            }
            return await self._make_request(params)
