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

    def test_consume_chunked_post(self, kitchen_server):
        """Test that a large chunked POST payload is completely consumed before connection is closed"""
        total_chunks = 100
        chunk_size = 100000  # 100KB
        chunk = b'A' * chunk_size

        def chunked_payload():
            for i in range(total_chunks):
                yield chunk

        req = requests.post(kitchen_server.url('/request-info'), data=chunked_payload())
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'application/json'
        response_json = req.json()
        assert response_json['headers'].get('content-length') is None, response_json
        assert response_json['headers'].get('transfer-encoding') == 'chunked', response_json

    def test_consume_unchunked_post(self, kitchen_server):
        """Test that a large unchunked POST payload is completely consumed before connection is closed"""
        payload_size = 100000000  # 100MB
        payload = b'A' * payload_size
        req = requests.post(kitchen_server.url('/request-info'), data=payload)
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'application/json'
        response_json = req.json()
        assert response_json['headers'].get('content-length') == str(payload_size), response_json
        assert response_json['headers'].get('transfer-encoding') is None, response_json
