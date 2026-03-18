import httpx


class AsyncECP:
    def __init__(self, server: str, login: str, password: str, timeout: float = 30.0):
        self.url = server
        self.login = login
        self.password = password
        self.timeout = timeout
        self.sess_id = None
        self.client = None
        self._is_entered = False

        self.headers = {
            # "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 YaBrowser/24.6.0.0 Safari/537.36",
        }

    async def __aenter__(self):
        """Асинхронный контекстный менеджер"""
        if self._is_entered:
            raise RuntimeError("AsyncECP instance is already entered")

        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=self.timeout,
            verify=True,
            follow_redirects=True
        )

        self.sess_id = await self.user_login()
        self._is_entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие при выходе из контекста"""
        ...
        # await self.close()

    async def user_login(self):
        """Асинхронная авторизация"""
        login_url = f"{self.url}/api/user/login"
        params = {'login': self.login, 'password': self.password}

        try:
            response = await self.client.get(login_url, params=params)
            response.raise_for_status()

            json_response = response.json()
            if json_response['error_code'] == 0:
                return json_response['data']['sess_id']
            else:
                raise Exception('Authorization error: ' + json_response['error_msg'])

        except httpx.RequestError as e:
            raise Exception(f'Network error during login: {str(e)}')
        except httpx.HTTPStatusError as e:
            raise Exception(f'HTTP error during login: {str(e)}')

    async def user_logout(self):
        """Асинхронный выход из системы"""
        if not self.sess_id or not self.client:
            return True

        logout_url = f"{self.url}/api/user/logout"
        params = {'sess_id': self.sess_id}

        try:
            response = await self.client.get(logout_url, params=params)
            response.raise_for_status()

            json_response = response.json()
            if json_response['error_code'] == 0:
                return True
            else:
                raise Exception('Logout error: ' + json_response['error_msg'])

        except httpx.RequestError as e:
            raise Exception(f'Network error during logout: {str(e)}')

    async def close(self):
        """Асинхронное закрытие клиента"""
        try:
            if self.sess_id:
                await self.user_logout()
        except:
            pass
        finally:
            if self.client:
                await self.client.aclose()

    async def _make_request(self, method: str, url, data=None, params=None, headers=None):
        """Универсальный асинхронный метод для запросов"""
        if not self.client or not self._is_entered:
            raise RuntimeError("ECP session not started. Use context manager")
        try:
            response = await self.client.request(
                method=method,
                url=url,
                data=data,
                params=params,
                headers=headers or self.headers
            )
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            raise Exception(f'Network error: {str(e)}')
        except httpx.HTTPStatusError as e:
            raise Exception(f'HTTP error {e.response.status_code}: {str(e)}')

    async def service_attachment(self, lpu_id: int, date_range: str):
        """Запускает формироваться список прикреплённого населения
        :param lpu_id: 13101871
        :param export_date_range: '01.12.2025 - 01.12.2025'
        :return: {'success': True, 'Link': 'export/attached_list//RPNM8300042512011.zip'}
        """
        url = f'{self.url}/?c=ServiceAttachment&m=runExport'
        params = {
            'sess_id': self.sess_id,
            'Lpu_id': lpu_id,
            'ExportDateRange': date_range,
            'ExportType': 'attached',
        }
        return await self._make_request(method='POST', url=url, data=params)
