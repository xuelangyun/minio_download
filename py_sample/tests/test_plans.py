# -*- coding: utf-8 -*-
"""任务标签与方案库：配置库层（标签、方案 CRUD、过滤 / 合并、旧库升级）和 Web 接口。"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")

import config_db                                  # noqa: E402
import minio_task_downloader_by_count as dl       # noqa: E402
import web_app                                    # noqa: E402

SCENES = [("场景A", [("任务1", 40, []), ("任务2", 10, ["旧名2"], "目录2")]), ("场景B", [("任务3", 5, ["x"])])]
TAGS = {"任务1": ["桌面", "分拣"], "任务3": ["桌面"]}


def payload(*scenes, version="V1"):
    """scenes: (场景名, [(任务名, 条数, 标签列表, 别名列表)])"""
    return {"data_version": version, "scenes": [
        {"scene": sc, "tasks": [{"name": n, "count": c, "aliases": a, "dir": "", "tags": t} for n, c, t, a in tasks]}
        for sc, tasks in scenes]}


class StoreBase(unittest.TestCase):
    """只提供 setUp（临时配置库），自己没有测试用例，避免被子类继承后重复执行。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "c.sqlite3"
        self.cs = config_db.ConfigStore(self.path)


class TagsAndStorage(StoreBase):

    def test_normalize_tags(self):
        self.assertEqual(config_db.normalize_tags("桌面, 分拣，桌面 ;清洁"), ["桌面", "分拣", "清洁"])
        self.assertEqual(config_db.normalize_tags(["a", " a ", "", "b"]), ["a", "b"])
        self.assertEqual(config_db.normalize_tags(None), [])
        with self.assertRaises(ValueError):
            config_db.normalize_tags(["x" * 25])
        with self.assertRaises(ValueError):
            config_db.normalize_tags([f"t{i}" for i in range(13)])

    def test_tags_roundtrip_and_preserved_when_not_given(self):
        self.cs.set_tasks("v", SCENES, TAGS)
        self.assertEqual(self.cs.get_task_tags(), TAGS)
        self.assertEqual(self.cs.snapshot()["tags"], TAGS)
        self.assertEqual(self.cs.get_tasks(), ("v", SCENES), "标签不影响下载器看到的任务结构")
        self.cs.set_tasks("v2", SCENES)                          # 不传 tags（如“恢复默认”、下载器侧调用）：沿用
        self.assertEqual(self.cs.get_task_tags(), TAGS)
        self.cs.set_tasks("v3", [("场景A", [("任务1", 1, [])])])   # 被删掉的任务，标签随之消失
        self.assertEqual(self.cs.get_task_tags(), {"任务1": ["桌面", "分拣"]})
        self.cs.set_tasks("v4", SCENES, {})                      # 显式空字典：清空
        self.assertEqual(self.cs.get_task_tags(), {})

    def test_tasks_dict_roundtrip(self):
        self.cs.set_tasks("v", SCENES, TAGS)
        d = self.cs.get_tasks_dict()
        self.assertEqual(d["scenes"][0]["tasks"][1], {"name": "任务2", "count": 10, "aliases": ["旧名2"], "dir": "目录2", "tags": []})
        self.assertEqual(config_db.tasks_from_dict(d), ("v", SCENES, TAGS))

    def test_upgrade_from_v1_database_keeps_data(self):
        old = sqlite3.connect(self.path.with_name("old.sqlite3"))
        old.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE scenes (id INTEGER PRIMARY KEY AUTOINCREMENT, position INTEGER NOT NULL, name TEXT NOT NULL UNIQUE);
        CREATE TABLE tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, scene_id INTEGER NOT NULL, position INTEGER NOT NULL,
            name TEXT NOT NULL UNIQUE, target_count INTEGER NOT NULL, directory TEXT NOT NULL DEFAULT '');
        CREATE TABLE task_aliases (task_id INTEGER NOT NULL, position INTEGER NOT NULL, alias TEXT NOT NULL, PRIMARY KEY (task_id, alias));
        CREATE TABLE disks (name TEXT PRIMARY KEY, position INTEGER NOT NULL, path TEXT NOT NULL, mountpoint TEXT NOT NULL);
        INSERT INTO meta VALUES('schema_version','1'); INSERT INTO meta VALUES('seeded_at','x');
        INSERT INTO scenes(position,name) VALUES(0,'旧场景'); INSERT INTO tasks(scene_id,position,name,target_count) VALUES(1,0,'旧任务',7);
        """)
        old.commit(); old.close()
        cs = config_db.ConfigStore(self.path.with_name("old.sqlite3"))
        self.assertEqual(cs.get_tasks(), ("", [("旧场景", [("旧任务", 7, [])])]))
        cs.set_tasks("v", [("旧场景", [("旧任务", 7, [])])], {"旧任务": ["新标签"]})
        self.assertEqual(cs.get_task_tags(), {"旧任务": ["新标签"]})
        self.assertEqual(cs.list_plans(), [])
        cs.save_plan("p", cs.get_tasks_dict())
        self.assertEqual(len(cs.list_plans()), 1)


class PlanStore(StoreBase):
    def test_plan_crud_and_summary(self):
        pl = payload(("场景A", [("t1", 40, ["桌面"], []), ("t2", 10, ["清洁", "桌面"], [])]), ("场景B", [("t3", 5, [], [])]))
        pid = self.cs.save_plan("  我的方案 ", pl, note="  备注 ")
        got = self.cs.get_plan(pid)
        self.assertEqual((got["name"], got["note"]), ("我的方案", "备注"))
        self.assertEqual((got["scene_count"], got["task_count"], got["total_target"], got["tags"]), (2, 3, 55, ["桌面", "清洁"]))
        self.assertEqual(self.cs.list_plans()[0]["task_count"], 3)
        with self.assertRaises(ValueError):
            self.cs.save_plan("我的方案", pl)                          # 同名
        pid2 = self.cs.save_plan("我的方案", payload(("S", [("only", 1, [], [])])), overwrite=True)
        self.assertEqual(pid2, pid)
        self.assertEqual(self.cs.get_plan(pid)["task_count"], 1)
        self.assertEqual(self.cs.get_plan(pid)["note"], "备注", "覆盖时不给备注则保留原备注")
        other = self.cs.save_plan("另一个", pl)
        with self.assertRaises(ValueError):
            self.cs.update_plan(other, name="我的方案")
        self.assertTrue(self.cs.update_plan(other, name="改名了", note="n"))
        self.assertEqual(self.cs.get_plan(other)["name"], "改名了")
        self.assertTrue(self.cs.delete_plan(other))
        self.assertFalse(self.cs.delete_plan(other))
        self.assertIsNone(self.cs.get_plan(other))
        for bad in ("", "   ", "x" * 61):
            with self.assertRaises(ValueError):
                self.cs.save_plan(bad, pl)
        with self.assertRaises(ValueError):
            self.cs.save_plan("坏的", {"scenes": "oops"})

    def test_filter_by_tags(self):
        pl = payload(("A", [("t1", 1, ["x"], []), ("t2", 1, ["y"], [])]), ("B", [("t3", 1, ["y", "z"], [])]), ("C", [("t4", 1, [], [])]))
        f = config_db.filter_by_tags(pl, ["y"])
        self.assertEqual([(s["scene"], [t["name"] for t in s["tasks"]]) for s in f["scenes"]], [("A", ["t2"]), ("B", ["t3"])])
        self.assertEqual(len(config_db.filter_by_tags(pl, [])["scenes"]), 3, "不给标签 = 不过滤")
        self.assertEqual(config_db.filter_by_tags(pl, ["不存在"])["scenes"], [])
        self.assertEqual(len(pl["scenes"]), 3, "不修改入参")

    def test_merge_adds_only_missing_tasks(self):
        cur = payload(("A", [("t1", 40, [], [])]))
        inc = payload(("A", [("t1", 99, [], []), ("t2", 5, ["x"], [])]), ("新场景", [("t3", 7, [], [])]))
        merged, rep = config_db.merge_payloads(cur, inc)
        self.assertEqual(rep["added"], ["t2", "t3"])
        self.assertEqual(rep["skipped"], [{"name": "t1", "reason": "当前配置里已有同名任务"}])
        self.assertEqual([(s["scene"], [t["name"] for t in s["tasks"]]) for s in merged["scenes"]], [("A", ["t1", "t2"]), ("新场景", ["t3"])])
        self.assertEqual(merged["scenes"][0]["tasks"][0]["count"], 40, "已有任务的条数不被覆盖")
        self.assertEqual(len(cur["scenes"][0]["tasks"]), 1, "不修改入参")

    def test_merge_skips_conflicting_tasks_with_reason_and_leaves_no_empty_scene(self):
        cur = payload(("A", [("t1", 40, [], [])]))
        inc = payload(("新场景", [("bad", 5, [], ["t1"])]), ("A", [("good", 5, [], [])]))

        def validate(d):
            names = [t["name"] for s in d["scenes"] for t in s["tasks"]]
            for s in d["scenes"]:
                for t in s["tasks"]:
                    if set(t["aliases"]) & set(names):
                        raise ValueError(f"别名 {t['aliases']} 与已有任务名冲突")
        merged, rep = config_db.merge_payloads(cur, inc, validate)
        self.assertEqual(rep["added"], ["good"])
        self.assertEqual(rep["skipped"][0]["name"], "bad")
        self.assertIn("冲突", rep["skipped"][0]["reason"])
        self.assertEqual([s["scene"] for s in merged["scenes"]], ["A"], "全被跳过的新场景不应留下空壳")


class PlansWebApi(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        for p in (mock.patch.object(dl, "CONFIG_DB", root / "cfg.sqlite3"), mock.patch.object(dl, "STATE_DB", root / "state.sqlite3"),
                  mock.patch.object(dl, "LOG_DIR", root / "logs"), mock.patch.object(dl, "PID_FILE", root / "x.pid")):
            p.start()
            self.addCleanup(p.stop)
        web_app._cs_cache.clear()
        web_app._cache.update(at=0.0, key=None, data=None)
        self.c = web_app.app.test_client()
        self.cs = web_app.get_cs()
        self.put_tasks(payload(("场景A", [("t1", 40, ["桌面", "分拣"], []), ("t2", 10, ["清洁"], [])]), ("场景B", [("t3", 5, ["桌面"], [])]), version="当前版本"))

    def put_tasks(self, p):
        r = self.c.put("/api/config/tasks", json=p)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))

    def names(self):
        return [t["name"] for s in self.c.get("/api/config").get_json()["tasks"]["scenes"] for t in s["tasks"]]

    def test_first_start_creates_an_initial_plan(self):
        plans = self.c.get("/api/plans").get_json()["plans"]
        self.assertIn("初始配置", [p["name"] for p in plans])
        self.assertGreater(next(p for p in plans if p["name"] == "初始配置")["task_count"], 10)

    def test_tags_are_saved_listed_and_reach_status(self):
        cfg = self.c.get("/api/config").get_json()["tasks"]
        self.assertEqual(cfg["scenes"][0]["tasks"][0]["tags"], ["桌面", "分拣"])
        st = self.c.get("/api/status?fresh=1").get_json()
        self.assertEqual(st["scenes"][0]["tasks"][0]["tags"], ["桌面", "分拣"])
        r = self.c.put("/api/config/tasks", json=payload(("S", [("x", 1, ["x" * 30], [])])))
        self.assertEqual(r.status_code, 400)
        self.assertIn("标签太长", r.get_json()["error"])

    def test_save_current_or_unsaved_editor_state(self):
        r = self.c.post("/api/plans", json={"name": "桌面类", "note": "常用"})
        self.assertEqual(r.status_code, 200)
        plan = self.c.get(f"/api/plans/{r.get_json()['id']}").get_json()
        self.assertEqual((plan["task_count"], plan["tags"]), (3, ["桌面", "分拣", "清洁"]))
        edited = payload(("编辑中", [("未保存的任务", 3, ["新"], [])]))            # 编辑器里还没保存的内容也能存成方案
        r = self.c.post("/api/plans", json={"name": "草稿", "payload": edited})
        self.assertEqual(self.c.get(f"/api/plans/{r.get_json()['id']}").get_json()["payload"]["scenes"][0]["tasks"][0]["name"], "未保存的任务")
        self.assertEqual(self.names(), ["t1", "t2", "t3"], "存方案不改动当前配置")

    def test_duplicate_name_needs_explicit_overwrite(self):
        self.c.post("/api/plans", json={"name": "A"})
        r = self.c.post("/api/plans", json={"name": "A"})
        self.assertEqual((r.status_code, r.get_json()["code"]), (409, "exists"))
        self.assertEqual(self.c.post("/api/plans", json={"name": "A", "overwrite": True}).status_code, 200)
        self.assertEqual(self.c.post("/api/plans", json={"name": ""}).status_code, 400)
        bad = payload(("S", [("a", 1, [], []), ("a", 2, [], [])]))
        self.assertEqual(self.c.post("/api/plans", json={"name": "坏", "payload": bad}).status_code, 400, "重名任务不能存成方案")

    def test_apply_replace_with_tag_filter(self):
        pid = self.c.post("/api/plans", json={"name": "全量"}).get_json()["id"]
        self.put_tasks(payload(("别的", [("other", 1, [], [])]), version="另一个版本"))
        r = self.c.post(f"/api/plans/{pid}/apply", json={"mode": "replace", "tags": ["桌面"]}).get_json()
        self.assertEqual((r["added"], r["total_tasks"]), (2, 2))
        self.assertEqual(self.names(), ["t1", "t3"])
        self.assertEqual(self.c.get("/api/config").get_json()["tasks"]["data_version"], "当前版本", "替换时采用方案里的版本目录")
        self.assertEqual(self.c.post(f"/api/plans/{pid}/apply", json={"mode": "replace", "tags": ["没有这个标签"]}).status_code, 400)

    def test_apply_replace_keeps_current_version_when_plan_has_none(self):
        pid = self.cs.save_plan("无版本", payload(("S", [("p1", 1, [], [])]), version=""))
        self.c.post(f"/api/plans/{pid}/apply", json={"mode": "replace"})
        self.assertEqual(self.c.get("/api/config").get_json()["tasks"]["data_version"], "当前版本")

    def test_apply_merge_adds_only_new_and_reports_skips(self):
        incoming = payload(("场景A", [("t1", 99, [], []), ("新任务", 7, ["桌面"], [])]), ("新场景", [("冲突的", 3, [], ["t2"])]))
        pid = self.cs.save_plan("增量", incoming)
        r = self.c.post(f"/api/plans/{pid}/apply", json={"mode": "merge"}).get_json()
        self.assertEqual(r["added"], 1)
        reasons = {s["name"]: s["reason"] for s in r["skipped"]}
        self.assertEqual(reasons["t1"], "当前配置里已有同名任务")
        self.assertIn("冲突", reasons["冲突的"])
        self.assertEqual(self.names(), ["t1", "t2", "新任务", "t3"])
        cfg = self.c.get("/api/config").get_json()["tasks"]
        self.assertEqual(cfg["scenes"][0]["tasks"][0]["count"], 40, "已有任务的条数不变")
        self.assertEqual(cfg["scenes"][0]["tasks"][2]["tags"], ["桌面"])

    def test_apply_unknown_plan_or_mode(self):
        self.assertEqual(self.c.post("/api/plans/9999/apply", json={"mode": "replace"}).status_code, 404)
        pid = self.c.post("/api/plans", json={"name": "A"}).get_json()["id"]
        self.assertEqual(self.c.post(f"/api/plans/{pid}/apply", json={"mode": "rm -rf"}).status_code, 400)

    def test_rename_overwrite_from_current_and_delete(self):
        a = self.c.post("/api/plans", json={"name": "A"}).get_json()["id"]
        b = self.c.post("/api/plans", json={"name": "B"}).get_json()["id"]
        self.assertEqual(self.c.put(f"/api/plans/{b}", json={"name": "A"}).status_code, 409)
        self.assertEqual(self.c.put(f"/api/plans/{b}", json={"name": "B2", "note": "x"}).status_code, 200)
        self.put_tasks(payload(("S", [("only", 1, [], [])])))
        self.assertEqual(self.c.put(f"/api/plans/{a}", json={"from_current": True}).status_code, 200)
        self.assertEqual(self.c.get(f"/api/plans/{a}").get_json()["task_count"], 1)
        self.assertEqual(self.c.delete(f"/api/plans/{a}", json={}).status_code, 200)
        self.assertEqual(self.c.delete(f"/api/plans/{a}", json={}).status_code, 404)
        self.assertEqual(self.c.get(f"/api/plans/{a}").status_code, 404)

    def test_export_and_import_roundtrip(self):
        pid = self.c.post("/api/plans", json={"name": "要导出的", "note": "说明"}).get_json()["id"]
        r = self.c.get(f"/api/plans/{pid}/export")
        self.assertIn("attachment", r.headers["Content-Disposition"])
        doc = r.get_json()
        self.assertEqual((doc["format"], doc["name"]), ("minio-downloader-plan", "要导出的"))
        imported = self.c.post("/api/plans/import", json=doc).get_json()
        self.assertEqual(imported["name"], "要导出的 (2)", "同名不覆盖，自动加序号")
        again = self.c.post("/api/plans/import", json={"plan": doc}).get_json()
        self.assertEqual(again["name"], "要导出的 (3)")
        got = self.c.get(f"/api/plans/{imported['id']}").get_json()
        self.assertEqual((got["task_count"], got["note"], got["tags"]), (3, "说明", ["桌面", "分拣", "清洁"]))
        for bad in ({"nope": 1}, {"payload": "x"}, {"payload": payload(("S", [("a", 1, [], []), ("a", 1, [], [])]))}):
            self.assertEqual(self.c.post("/api/plans/import", json=bad).status_code, 400)

    def test_plan_writes_require_json(self):
        self.assertEqual(self.c.post("/api/plans", data="{}", content_type="text/plain").status_code, 415)


if __name__ == "__main__":
    unittest.main()
