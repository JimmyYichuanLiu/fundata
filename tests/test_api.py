"""
test_api.py — FastAPI 核心端点测试

覆盖范围：
  - 系统端点：health、stats、failures
  - 基金端点：list、search、detail、nav 时序
  - NAV CRUD：create、get、update、delete
  - 边界/错误：404、409 重复、参数校验
"""

import pytest
from fastapi.testclient import TestClient


# ===========================================================================
# 系统端点
# ===========================================================================

class TestHealth:
    def test_returns_ok(self, client: TestClient):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestStats:
    def test_returns_counts(self, client_with_data: TestClient):
        r = client_with_data.get("/api/stats")
        assert r.status_code == 200
        data = r.json()
        # 预置了2条基金、2条净值记录
        assert data["total_funds"] >= 2
        assert data["total_records"] >= 2

    def test_empty_db_returns_zeros(self, client: TestClient):
        r = client.get("/api/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["total_funds"] == 0
        assert data["total_records"] == 0


class TestFailures:
    def test_returns_empty_list(self, client: TestClient):
        r = client.get("/api/failures")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_pagination_params(self, client: TestClient):
        r = client.get("/api/failures?limit=10&offset=0")
        assert r.status_code == 200

    def test_invalid_limit_rejected(self, client: TestClient):
        r = client.get("/api/failures?limit=0")
        assert r.status_code == 422


# ===========================================================================
# 基金列表 / 搜索
# ===========================================================================

class TestFundList:
    def test_empty_db_returns_empty(self, client: TestClient):
        r = client.get("/api/funds")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_returns_seeded_funds(self, client_with_data: TestClient):
        r = client_with_data.get("/api/funds")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 2
        codes = [f["product_code"] for f in data["items"]]
        assert "F001" in codes
        assert "F002" in codes

    def test_returns_all_funds(self, client_with_data: TestClient):
        """list_funds 返回全量数据（无分页），total 与 items 长度一致。"""
        r = client_with_data.get("/api/funds")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == len(data["items"])

    def test_supports_strategy_filter(self, client_with_data: TestClient):
        """strategy1 参数不存在时返回空列表，不报错。"""
        r = client_with_data.get("/api/funds?strategy1=不存在的策略")
        assert r.status_code == 200
        assert r.json()["items"] == []


class TestFundSearch:
    def test_search_by_name(self, client_with_data: TestClient):
        r = client_with_data.get("/api/funds/search?q=测试基金A")
        assert r.status_code == 200
        results = r.json()["items"]
        assert any(f["product_code"] == "F001" for f in results)

    def test_search_by_code(self, client_with_data: TestClient):
        r = client_with_data.get("/api/funds/search?q=F002")
        assert r.status_code == 200
        results = r.json()["items"]
        assert any(f["product_code"] == "F002" for f in results)

    def test_search_no_match(self, client_with_data: TestClient):
        r = client_with_data.get("/api/funds/search?q=不存在的基金XYZ")
        assert r.status_code == 200
        assert r.json()["items"] == []


# ===========================================================================
# 基金详情 / NAV 时序
# ===========================================================================

class TestFundDetail:
    def test_get_existing_fund(self, client_with_data: TestClient):
        # 先找到 fund_id
        funds = client_with_data.get("/api/funds").json()["items"]
        fund_id = next(f["fund_id"] for f in funds if f["product_code"] == "F001")

        r = client_with_data.get(f"/api/funds/{fund_id}")
        assert r.status_code == 200
        data = r.json()
        assert data["product_code"] == "F001"
        assert data["product_name"] == "测试基金A"

    def test_get_nonexistent_fund_returns_404(self, client: TestClient):
        r = client.get("/api/funds/99999")
        assert r.status_code == 404

    def test_nav_series_returns_records(self, client_with_data: TestClient):
        funds = client_with_data.get("/api/funds").json()["items"]
        fund_id = next(f["fund_id"] for f in funds if f["product_code"] == "F001")

        r = client_with_data.get(f"/api/funds/{fund_id}/nav")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 1
        record = data["items"][0]
        assert "unit_nav" in record
        assert "nav_date" in record


# ===========================================================================
# NAV CRUD
# ===========================================================================

class TestNavCreate:
    def test_create_nav_record(self, client: TestClient):
        payload = {
            "product_code": "F003",
            "product_name": "新建基金C",
            "nav_date": "2024-06-01",
            "unit_nav": 1.500,
            "accumulated_nav": 1.500,
        }
        r = client.post("/api/nav", json=payload)
        assert r.status_code == 201
        data = r.json()
        assert data["product_code"] == "F003"
        assert data["unit_nav"] == pytest.approx(1.5)
        assert data["nav_date"] == "2024-06-01"
        assert data["source_id"] is None   # source_id=NULL → 手工录入

    def test_duplicate_record_returns_409(self, client: TestClient):
        payload = {
            "product_code": "F003",
            "product_name": "新建基金C",
            "nav_date": "2024-06-01",
            "unit_nav": 1.5,
            "accumulated_nav": 1.5,
        }
        client.post("/api/nav", json=payload)
        r = client.post("/api/nav", json=payload)
        assert r.status_code == 409

    def test_missing_required_field_returns_422(self, client: TestClient):
        r = client.post("/api/nav", json={"product_code": "F003"})
        assert r.status_code == 422


class TestNavGetUpdateDelete:
    def _create(self, client: TestClient) -> int:
        """辅助：创建一条记录并返回其 id。"""
        payload = {
            "product_code": "F_CRUD",
            "product_name": "CRUD测试基金",
            "nav_date": "2024-07-01",
            "unit_nav": 2.0,
            "accumulated_nav": 2.0,
        }
        r = client.post("/api/nav", json=payload)
        assert r.status_code == 201
        return r.json()["id"]

    def test_get_nav_by_id(self, client: TestClient):
        nav_id = self._create(client)
        r = client.get(f"/api/nav/{nav_id}")
        assert r.status_code == 200
        assert r.json()["id"] == nav_id

    def test_get_nonexistent_nav_returns_404(self, client: TestClient):
        r = client.get("/api/nav/99999")
        assert r.status_code == 404

    def test_update_nav_unit_value(self, client: TestClient):
        nav_id = self._create(client)
        r = client.put(f"/api/nav/{nav_id}", json={"unit_nav": 3.14})
        assert r.status_code == 200
        assert r.json()["unit_nav"] == pytest.approx(3.14)

    def test_delete_nav_record(self, client: TestClient):
        nav_id = self._create(client)
        r = client.delete(f"/api/nav/{nav_id}")
        assert r.status_code == 204
        # 验证已删除
        r2 = client.get(f"/api/nav/{nav_id}")
        assert r2.status_code == 404
