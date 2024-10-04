import io
import os
import pandas as pd
import requests

from icecream import ic
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from typing import Any
from truflation.data.connectors.base import Connector


def playw_browser():
    if playw_browser.count >= 50 :
        playw_browser.count = 0
        playw_browser.browser.close()
        playw_browser.playwright.stop()
        playw_browser.playwright = None
        playw_browser.browser = None
    if playw_browser.playwright is None:
        launch_params = {}
        playw_browser.playwright = sync_playwright().start()
        if os.environ.get('PROXY_SERVER') is not None and \
           os.environ.get('PROXY_USERNAME') is not None and \
           os.environ.get('PROXY_PASSWORD') is not None:
            launch_params = {
                'proxy': {
                    'server': os.environ.get('PROXY_SERVER'),
                    'username': os.environ.get('PROXY_USERNAME'),
                    'password': os.environ.get('PROXY_PASSWORD')
                },
                'headless': True
            }
            ic('#### using proxy server with playwright ####')
        playw_browser.browser = playw_browser.playwright.chromium.launch(
            **launch_params
        )
        playw_browser.count += 1
    return playw_browser.browser

playw_browser.playwright = None
playw_browser.browser = None
playw_browser.count = 0


class ConnectorRest(Connector):
    def __init__(self, **kwargs):
        super().__init__()
        self.playwright = kwargs.get('playwright', False)
        self.json = kwargs.get('json', True)
        self.csv = kwargs.get('csv', False)
        self.no_cache = kwargs.get('no_cache', False)
        self.page = None

    def read_all(
            self,
            url, *args, **kwargs) -> Any:
        self.logging_manager.log_info(f'Fetching data from URL: {url}')
        ic(kwargs)
        if self.playwright:
            try:
                if kwargs.get('no_cache', self.no_cache):
                    with sync_playwright() as p:
                        browser_type = p.chromium
                        browser = browser_type.launch(headless=True)
                        context = browser.new_context(
                            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
                        )
                        self.page = context.new_page()
                        stealth_sync(self.page)
                        response = self.page.goto(
                            url
                        )
                        self.logging_manager.log_info('Data fetched using Playwright no cache.')
                        return self.process_response(response)
                else:
                    with playw_browser().new_context(
                            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
                    ) as context:
                        page = context.new_page()
                        stealth_sync(page)
                        response = page.goto(
                            url
                        )
                        self.logging_manager.log_info('Data fetched using Playwright.')
                        return self.process_response(response)
            except Exception as e:
                self.logging_manager.log_error(f'Error fetching data using Playwright: {e}')

        try:
            response = requests.get(os.path.join(url))
            if response.status_code == 200:
                self.logging_manager.log_info("Data fetched successfully using requests.")
                return self.process_response(response)
            else:
                self.logging_manager.log_warning(f'Unexpected status code {response.status_code} received from server.')
        except requests.exceptions.RequestException as e:
            self.logging_manager.log_error(f'Error fetching data using requests: {e}')
            return None
            
    def process_response(self, response):
        content_type = response.headers.get('content-type')
        self.logging_manager.log_info(f'Content type of response: {content_type}')
        
        if self.csv or content_type.startswith('text/csv'):
            self.logging_manager.log_info('Parsing response as CSV...')
            return pd.read_csv(
                io.StringIO(response.content.decode('utf-8')),
                dtype_backend='pyarrow'
            )
        if self.json or content_type.startswith('application/json'):
            self.logging_manager.log_info('Parsing response as JSON...')
            return self.process_json(response.json())
        if content_type == 'application/vnd.ms-excel' or \
                content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            self.logging_manager.log_info('Parsing response as Excel file...')
            return pd.read_excel(
                io.StringIO(response.content.decode('utf-8')),
                dtype_backend='pyarrow'
            )
        
        self.logging_manager.log_info('Parsing response content...')
        return self.process_content(response.content)

    @staticmethod
    def process_content(content):
        return content

    @staticmethod
    def process_json(json_obj):
        return json_obj
