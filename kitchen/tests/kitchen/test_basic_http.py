import logging
import pytest
import requests

from tests.kitchen import util

@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class TestBasicHttp:

    def test_hello(self, kitchen_server):
        """Test default 'Hello World' response"""
        req = requests.get(kitchen_server.url())
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/plain'
        assert req.text == 'Hello World'

    def test_content_type(self, kitchen_server):
        """Test default 'Hello World' response"""
        req = requests.get(kitchen_server.url(), headers={'x-kitchen-content-type': 'text/html'})
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/html'
