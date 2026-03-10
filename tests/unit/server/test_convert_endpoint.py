"""Tests for /api/convert endpoint."""

from __future__ import annotations


class TestMalformedInput:
    """Tests for handling corrupted, empty, and malformed XML input."""

    def test_convert_empty_file(self, client):
        resp = client.post(
            "/api/convert",
            files={"file": ("empty.yxmd", b"", "application/xml")},
            data={"format": "pyspark"},
        )
        assert resp.status_code == 500

    def test_convert_corrupted_xml(self, client):
        resp = client.post(
            "/api/convert",
            files={"file": ("bad.yxmd", b"<not valid xml!><<<", "application/xml")},
            data={"format": "pyspark"},
        )
        assert resp.status_code == 500

    def test_convert_valid_xml_but_not_workflow(self, client):
        resp = client.post(
            "/api/convert",
            files={"file": ("notworkflow.yxmd", b"<root><child/></root>", "application/xml")},
            data={"format": "pyspark"},
        )
        # May succeed with empty output or fail — either way should not crash
        assert resp.status_code in (200, 500)

    def test_convert_no_file(self, client):
        resp = client.post("/api/convert", data={"format": "pyspark"})
        assert resp.status_code == 422  # FastAPI validation error


def test_convert_single_file(client, simple_yxmd):
    resp = client.post(
        "/api/convert",
        files={"file": ("simple_filter.yxmd", simple_yxmd, "application/xml")},
        data={"format": "pyspark"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["workflow_name"] == "simple_filter"
    assert len(data["files"]) > 0
    assert data["node_count"] > 0
    assert "warnings" in data


def test_convert_rejects_non_yxmd(client):
    resp = client.post(
        "/api/convert",
        files={"file": ("readme.txt", b"hello", "text/plain")},
        data={"format": "pyspark"},
    )
    assert resp.status_code == 400


def test_convert_dlt_format(client, simple_yxmd):
    resp = client.post(
        "/api/convert",
        files={"file": ("simple_filter.yxmd", simple_yxmd, "application/xml")},
        data={"format": "dlt"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["files"]) > 0


def test_convert_sql_format(client, simple_yxmd):
    resp = client.post(
        "/api/convert",
        files={"file": ("simple_filter.yxmd", simple_yxmd, "application/xml")},
        data={"format": "sql"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["files"]) > 0
