import asyncio
import httpx
import time
from abc import ABC, abstractmethod


class TFOMS:
    """
    Главный класс для работы с веб-сервисом идентификации застрахованных лиц.

    Содержит два подкласса для разных способов идентификации:
    - API: работа через официальное API TFOMS (авторизация через API-ключ)
    - Parser: работа через парсинг веб-интерфейса (авторизация через Playwright)

    Ссылка на документацию API: https://www.webfoms.ru/help/50

    Пример использования:
        # Для API
        async with TFOMS.API(url, login, password) as api:
            result = await api.enp('1234567890123456')

        # Для парсера
        async with TFOMS.Parser(url, login, password) as parser:
            result = await parser.snils('123-456-789 00')

    """

    class Base(ABC):
        """
        Абстрактный базовый класс для всех способов взаимодействия с TFOMS.
        Содержит общую логику: управление сессией, ограничение частоты запросов.
        """

        # Константы класса
        REQUEST_INTERVAL = 2  # секунды между запросами
        DEFAULT_TIMEOUT = 30.0
        USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 YaBrowser/26.3.0.0 Safari/537.36'

        def __init__(self, base_url: str, login: str, password: str):
            """
            Инициализация базового клиента.

            :param base_url: Базовый URL сервиса
            :param login: Логин для аутентификации
            :param password: Пароль для аутентификации
            """
            self.base_url = base_url
            self.login = login
            self.password = password
            self.client: httpx.AsyncClient | None = None
            self.last_request_time: float = 0
            self.auth_token: str | None = None

            # Общие заголовки
            self.headers = {
                'User-Agent': self.USER_AGENT,
                'Content-Type': 'application/json; charset=utf-8'
            }

        @staticmethod
        def _check_response_status(response):
            if response.status_code == 401:
                raise RuntimeError('Ошибка авторизации. Токен устарел или недействителен')
            elif response.status_code == 429:
                raise RuntimeError('Слишком много запросов. Попробуйте позже')
            response.raise_for_status()

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
                self.client = None

        async def _wait_if_needed(self):
            """
            Ожидает, если необходимо, чтобы соблюсти ограничение на частоту запросов.
            Защищает сервер от слишком частых запросов.
            """
            current_time = time.time()
            elapsed_time = current_time - self.last_request_time
            if elapsed_time < self.REQUEST_INTERVAL:
                await asyncio.sleep(self.REQUEST_INTERVAL - elapsed_time)
            self.last_request_time = time.time()

        @abstractmethod
        async def authenticate(self):
            """Абстрактный метод аутентификации."""
            pass

        @abstractmethod
        async def _make_request(self, **kwargs):
            """
            Выполняет GET-запрос к API.

            Реализация зависит от подкласса:
                - API: принимает endpoint и params
                - Parser: принимает params

            :return: Ответ сервера в формате JSON.
            :raises RuntimeError: При ошибках HTTP или если клиент не инициализирован.
            """
            pass

        # Общие методы API
        @abstractmethod
        async def fio(self, surname: str, name: str, birth_date: str, patronymic: str | None = None):
            """
            Возвращает информацию о застрахованном лице по ФИО и дате рождения.

            :param surname: Фамилия застрахованного.
            :param name: Имя застрахованного.
            :param birth_date: Дата рождения в формате DD.MM.YYYY.
            :param patronymic: Отчество застрахованного.
            :return: Информация о застрахованном лице.
            :raises ValueError: Если дата в неверном формате.
            """
            pass

        @abstractmethod
        async def enp(self, enp_number: str):
            """
             Возвращает информацию о застрахованном лице по единому номеру полиса.

            :param enp_number: Единый номер полиса застрахованного.
            :return: Информация о застрахованном лице.
            """
            pass

        @abstractmethod
        async def polis(self, polis_number: str, seria_number: str | None = None):
            """
            Возвращает информацию о застрахованном лице по номеру полиса.

            :param polis_number: Номер полиса застрахованного.
            :param seria_number: Серия полиса застрахованного.
            :return: Информация о застрахованном лице.
            """
            pass

    class API(Base):
        """
        Класс для работы с официальным API TFOMS.

        Использует прямую HTTP-авторизацию.
        """

        def __init__(self, base_url: str, login: str, password: str):
            super().__init__(base_url, login, password)
            self.api_endpoint = f'{self.base_url}/insurance'

        async def authenticate(self):
            """
            Получает токен аутентификации через API, прямой запрос к эндпоинту /auth/login или использование Basic Auth.

            :return: Токен аутентификации.
            :raises Exception: Если не удалось получить токен.
            """
            self.client = httpx.AsyncClient(
                auth=(self.login, self.password),
                headers=self.headers,
                timeout=self.DEFAULT_TIMEOUT
            )
            return self.client

        async def _make_request(self, endpoint, params=None):
            if not self.client:
                raise RuntimeError('Клиент не инициализирован. Сначала вызовите authenticate()')

            await self._wait_if_needed()
            url = f'{self.api_endpoint}/{endpoint}'
            response = await self.client.get(url, params=params)
            self._check_response_status(response)
            return response.json()

        async def fio(self, surname: str, name: str, birth_date: str, patronymic: str | None = None, ) -> dict:
            params = {
                'firstname': name,
                'secondname': surname,
                'date_of_birth': birth_date
            }
            if patronymic:
                params['middlename'] = patronymic
            return await self._make_request('findbyfiodr', params)

        async def enp(self, enp_number: str) -> dict:
            params = {'enp': enp_number}
            return await self._make_request('findbyenp', params)

        async def polis(self, polis_number: str, seria_number: str | None = None) -> dict:
            params = {'polis_num': polis_number}
            return await self._make_request('findbypolis', params)

    class Parser(Base):
        """
        Класс для работы через парсинг веб-интерфейса TFOMS.

        Использует Playwright для авторизации через браузер и получения токена.
        """

        def __init__(self, base_url: str, login: str, password: str):
            super().__init__(base_url, login, password)
            self.api_base_url = self.base_url + '/api/insurance/search'

        async def authenticate(self):
            """
            Получает токен аутентификации через Playwright (браузерную автоматизацию).

            :return: Токен аутентификации.
            :raises Exception: Если не удалось получить токен за отведенное время.
            """
            from playwright.async_api import async_playwright
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError

            login_page = self.base_url + '/auth/login'

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, slow_mo=50)
                context = await browser.new_context()
                page = await context.new_page()

                token_event = asyncio.Event()

                async def handle_response(response):
                    auth_header = response.request.headers.get('authorization', '')
                    if auth_header and 'Bearer' in auth_header:
                        self.auth_token = auth_header.replace('Bearer ', '')
                        token_event.set()

                page.on('response', handle_response)

                try:
                    await page.goto(login_page)
                    await page.fill('#username', self.login)
                    await page.fill('#password', self.password)
                    await page.click('button[type="submit"]')

                    await page.wait_for_selector('header > div > p')
                    await page.click('header > div > p')

                    await asyncio.wait_for(token_event.wait(), timeout=10.0)
                    if not self.auth_token:
                        raise Exception('Токен не был получен, хотя событие сработало')
                except asyncio.TimeoutError:
                    raise Exception('Не удалось получить токен аутентификации за отведенное время')
                except PlaywrightTimeoutError as e:
                    raise Exception(f'Таймаут при загрузке страницы: {e}')
                finally:
                    page.remove_listener('response', handle_response)  # очистка
                    await browser.close()

            # Создаем HTTP-клиент с полученным токеном
            self.client = httpx.AsyncClient(
                headers={
                    **self.headers,
                    'Authorization': f'Bearer {self.auth_token}'
                },
                timeout=self.DEFAULT_TIMEOUT
            )

            return self.auth_token

        async def _make_request(self, params):
            if not self.client:
                raise RuntimeError('Клиент не инициализирован. Сначала вызовите authenticate()')

            await self._wait_if_needed()
            response = await self.client.get(self.api_base_url, params=params)
            self._check_response_status(response)
            return response.json()

        async def fio(self, surname: str, name: str, birth_date: str, patronymic: str | None = None) -> dict:
            from datetime import datetime
            try:
                parsed_date = datetime.strptime(birth_date, '%d.%m.%Y')
                formatted_date = parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                raise ValueError('Дата должна быть в формате DD.MM.YYYY')

            params = {
                'searchType': 'fio',
                'surname': surname,
                'name': name,
                'patronymic': patronymic,
                'birthDate': formatted_date,
            }
            return await self._make_request(params)

        async def enp(self, enp_number: str) -> dict:
            params = {
                'searchType': 'enp',
                'enp': enp_number
            }
            return await self._make_request(params)

        async def polis(self, polis_number: str, seria_number: str | None = None) -> dict:
            params = {
                'searchType': 'polis',
                'polisNum': polis_number,
                'polisSer': seria_number
            }
            return await self._make_request(params)

        async def snils(self, snils_number: str) -> dict:
            """
            Возвращает информацию о застрахованном лице по номеру СНИЛС.

            Пример: 123-456-789 00; 12345678900;

            :param snils_number: Номер СНИЛС застрахованного.
            :return: Информация о застрахованном лице.
            """
            params = {
                'searchType': 'snils',
                'snils': snils_number
            }
            return await self._make_request(params)
