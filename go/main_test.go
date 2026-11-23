package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/hex"
	"errors"
	"net/url"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestSafeComponent(t *testing.T) {
	for _, in := range []string{"", ".", "..", "a/b", "a:b", "name. "} {
		got := safe(in)
		if got == "" || got == "." || got == ".." || filepath.IsAbs(got) {
			t.Fatalf("unsafe component for %q: %q", in, got)
		}
	}
}

func TestSafeJoinRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"../escape", "/absolute", "..", "a/../../b"} {
		if _, err := safeJoin(root, rel); err == nil {
			t.Errorf("expected path traversal to be rejected: %q", rel)
		}
	}
}

func TestSafeJoinAllowsNestedObject(t *testing.T) {
	root := t.TempDir()
	got, err := safeJoin(root, "camera/left/video.mp4")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "camera", "left", "video.mp4")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMD5ETagRecognition(t *testing.T) {
	if !isMD5ETag("d41d8cd98f00b204e9800998ecf8427e") {
		t.Fatal("standard MD5 ETag should be recognized")
	}
	if isMD5ETag("d41d8cd98f00b204e9800998ecf8427e-2") {
		t.Fatal("multipart ETag must not be treated as MD5")
	}
	if !sameETag("\"abcd\"", "abcd") {
		t.Fatal("quoted and unquoted ETags should match")
	}
}

func TestWindowsObjectNameRules(t *testing.T) {
	if !windowsReserved("CON.txt") || !windowsReserved("CON.archive.txt") || !windowsReserved("lpt1") {
		t.Fatal("Windows reserved device names were not recognized")
	}
	if invalidWindowsObjectPart("bad:name") == false {
		t.Fatal("Windows-invalid object name should be rejected")
	}
	if invalidWindowsObjectPart("camera_left.mp4") {
		t.Fatal("normal object filename should be accepted")
	}
}

func validTestConfig(t *testing.T) Config {
	return Config{
		Endpoint: "http://127.0.0.1:9000", AccessKey: "access", SecretKey: "secret", Bucket: "bucket",
		PGHost: "127.0.0.1", PGDB: "db", PGUser: "user", PGPassword: "password",
		Disks: []DiskConfig{{Name: "disk1", Path: t.TempDir()}},
		Tasks: []Task{{Name: "task-a", Scene: "scene-a", Dir: "dir-a", TargetCount: 1}},
	}
}

func TestConfigRejectsAliasCollision(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Tasks = append(cfg.Tasks, Task{Name: "task-b", Scene: "scene-b", Dir: "dir-b", Aliases: []string{"task-a"}})
	if err := cfg.normalize("count"); err == nil {
		t.Fatal("expected alias/current task name collision to fail")
	}
}

func TestConfigRejectsDuplicateSceneDirectory(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Tasks = append(cfg.Tasks, Task{Name: "task-b", Scene: "scene-a", Dir: "dir-a"})
	if err := cfg.normalize("count"); err == nil {
		t.Fatal("expected duplicate scene directory to fail")
	}
}

func TestConfigAcceptsSeparateSceneDirectories(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Tasks = append(cfg.Tasks, Task{Name: "task-b", Scene: "scene-b", Dir: "dir-a"})
	if err := cfg.normalize("count"); err != nil {
		t.Fatalf("different scenes may use the same directory name: %v", err)
	}
}

func TestSweepPartialKeepsExpectedFileAndResumeTemp(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"camera/video.mp4", "camera/video.mp4.minio-tmp", "stale/old.txt"} {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	objects := []minio.ObjectInfo{{Key: "episode/camera/video.mp4", Size: 1}}
	if err := sweepPartial(root, "episode", objects); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"camera/video.mp4", "camera/video.mp4.minio-tmp"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Errorf("expected resume file to remain %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "stale/old.txt")); !os.IsNotExist(err) {
		t.Errorf("stale file should be removed, got err=%v", err)
	}
}

func TestConfigRejectsMismatchedMode(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Mode = "duration"
	if err := cfg.normalize("count"); err == nil {
		t.Fatal("expected duration config to reject count mode")
	}
}

func TestEpisodePathsUseParentKeyLeaf(t *testing.T) {
	d := DiskConfig{Name: "disk1", Path: t.TempDir()}
	task := Task{Name: "task", Scene: "scene", Dir: "task-dir"}
	final, partial := episodePaths("v1", task, d, "bucket/prefix/episode-folder/")
	if filepath.Base(final) != "episode-folder" {
		t.Fatalf("final leaf should be parent_key basename, got %q", final)
	}
	if filepath.Base(partial) != ".episode-folder.partial" {
		t.Fatalf("partial leaf mismatch: %q", partial)
	}
}

func TestNormalizeParentKey(t *testing.T) {
	got, err := normalizeParentKey("/prefix/episode/")
	if err != nil || got != "prefix/episode" {
		t.Fatalf("normalization failed: got=%q err=%v", got, err)
	}
	for _, bad := range []string{"", "/", "../escape", "prefix/../escape", "./episode"} {
		if _, err := normalizeParentKey(bad); err == nil {
			t.Errorf("expected invalid parent_key %q to fail", bad)
		}
	}
}

func TestAddSpaceReservations(t *testing.T) {
	got, ok := addSpaceReservations(100, 200, 30)
	if !ok || got != 330 {
		t.Fatalf("expected 330 bytes required, got=%d ok=%v", got, ok)
	}
	if _, ok := addSpaceReservations(^uint64(0)-1, 2, 0); ok {
		t.Fatal("expected reservation overflow to be rejected")
	}
	if _, ok := addSpaceReservations(^uint64(0)-10, 1, 10); ok {
		t.Fatal("expected minimum-free overflow to be rejected")
	}
}

func TestPathIsDirIncludesEmptyFinalDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "empty-final")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if !pathIsDir(dir) {
		t.Fatal("an existing final directory should be recognized even when empty")
	}
	if pathIsDir(filepath.Join(dir, "missing")) {
		t.Fatal("a missing directory must not be recognized")
	}
}

func TestCompletedHonorsTrustedLedger(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec("CREATE TABLE downloads(episode_id TEXT PRIMARY KEY,status TEXT,target_path TEXT); CREATE TABLE downloaded_episodes(episode_id TEXT PRIMARY KEY,target_path TEXT,disk_name TEXT);")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO downloaded_episodes(episode_id,target_path,disk_name) VALUES('ep-1','/archived/disk/episode','old-disk')")
	if err != nil {
		t.Fatal(err)
	}
	app := &App{db: db, cfg: Config{TrustCompletedRecords: true}}
	if !app.completed("ep-1") {
		t.Fatal("trusted historical ledger entry should count as completed even when archived path is unavailable")
	}
	app.cfg.TrustCompletedRecords = false
	if app.completed("ep-1") {
		t.Fatal("untrusted ledger entry with a missing path must not count as completed")
	}
}

func TestPostgresDSNEscapesCredentials(t *testing.T) {
	cfg := Config{PGHost: "::1", PGPort: 5432, PGDB: "db name", PGUser: "user@name", PGPassword: "p@ss word#"}
	raw := postgresDSN(cfg)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("invalid PostgreSQL DSN: %v", err)
	}
	user := u.User.Username()
	password, ok := u.User.Password()
	if !ok || user != cfg.PGUser || password != cfg.PGPassword {
		t.Fatalf("credentials did not round-trip: user=%q password_ok=%v", user, ok)
	}
	if u.Host != "[::1]:5432" {
		t.Fatalf("IPv6 host was not formatted correctly: %q", u.Host)
	}
	if u.Query().Get("sslmode") != "prefer" || u.Query().Get("connect_timeout") != "10" {
		t.Fatalf("connection options missing: %s", u.RawQuery)
	}
}

func TestChooseTaskDiskKeepsSharedOutputFolderTogether(t *testing.T) {
	disks := []DiskConfig{
		{Name: "disk1", Path: t.TempDir()},
		{Name: "disk2", Path: t.TempDir()},
	}
	loads := map[string]int{"disk1": 0, "disk2": 0}
	folderDisk := map[string]*DiskConfig{}
	first := Task{Name: "task-a", Scene: "scene-a", Dir: "shared-folder"}
	second := Task{Name: "task-b", Scene: "scene-a", Dir: "shared-folder"}
	chosenFirst := chooseTaskDisk(first, disks, loads, folderDisk)
	chosenSecond := chooseTaskDisk(second, disks, loads, folderDisk)
	if chosenFirst == nil || chosenSecond == nil || chosenFirst.Name != chosenSecond.Name {
		t.Fatalf("tasks sharing a folder must share a disk: first=%v second=%v", chosenFirst, chosenSecond)
	}
	if loads[chosenFirst.Name] != 1 {
		t.Fatalf("shared folder should count as one disk load, got loads=%v", loads)
	}
}

func TestSQLiteStopRequestCanBeSetAndCleared(t *testing.T) {
 db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "control.db"))
 if err != nil { t.Fatal(err) }
 defer db.Close()
 db.SetMaxOpenConns(1)
 if err := writeStopRequest(db); err != nil { t.Fatal(err) }
 var value string
 if err := db.QueryRow("SELECT value FROM control WHERE key='stop_requested'").Scan(&value); err != nil { t.Fatal(err) }
 if value != "1" { t.Fatalf("expected stop_requested=1, got %q", value) }
 app := &App{db: db}
 if err := app.clearStopRequest(); err != nil { t.Fatal(err) }
 if err := db.QueryRow("SELECT value FROM control WHERE key='stop_requested'").Scan(&value); err != sql.ErrNoRows {
  t.Fatalf("expected pause control row to be deleted, value=%q err=%v", value, err)
 }
}

func TestStopWatcherCancelsApplicationContext(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "watcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	app := &App{db: db, ctx: ctx, cancel: cancel}
	done := app.startStopWatcher()
	if err := writeStopRequest(db); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(2500 * time.Millisecond):
		cancel()
		t.Fatal("pause watcher did not cancel the context")
	}
	<-done
}

func TestPIDFileLifecycle(t *testing.T) {
	name := filepath.Join(t.TempDir(), "downloader.pid")
	if err := writePIDFile(name, "count"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var record pidRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.PID != os.Getpid() || record.Mode != "count" || record.Created <= 0 || record.CreatedAt == "" {
		t.Fatalf("unexpected PID record: %+v", record)
	}
	removePIDFile(name)
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("PID file should be removed, got err=%v", err)
	}
}

func TestIsStopRequestedAcceptsPythonTimestamp(t *testing.T) {
	for _, value := range []string{"1", "2026-11-23T03:05:30+00:00", "  2026-11-23T03:05:30Z  "} {
		if !isStopRequested(value) {
			t.Fatalf("expected pause request for %q", value)
		}
	}
	for _, value := range []string{"", " ", "0", " 0 "} {
		if isStopRequested(value) {
			t.Fatalf("did not expect pause request for %q", value)
		}
	}
}


func TestChooseTaskDiskSeparatesSameFolderAcrossScenes(t *testing.T) {
	disks := []DiskConfig{{Name: "disk1"}, {Name: "disk2"}}
	loads := map[string]int{"disk1": 0, "disk2": 0}
	folderDisk := map[string]*DiskConfig{}
	first := chooseTaskDisk(Task{Name: "task-a", Scene: "scene-a", Dir: "shared"}, disks, loads, folderDisk)
	second := chooseTaskDisk(Task{Name: "task-b", Scene: "scene-b", Dir: "shared"}, disks, loads, folderDisk)
	if first == nil || second == nil || first.Name == second.Name {
		t.Fatalf("different scenes should be scheduled independently: first=%v second=%v", first, second)
	}
}

func TestLoadConfigDBImportsPythonCountConfiguration(t *testing.T) {
	name := filepath.Join(t.TempDir(), "python-config.sqlite3")
	db, err := sql.Open("sqlite", name)
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		"CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",
		"CREATE TABLE scenes(id INTEGER PRIMARY KEY,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE)",
		"CREATE TABLE tasks(id INTEGER PRIMARY KEY,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE,target_count INTEGER NOT NULL,directory TEXT NOT NULL DEFAULT '',tags TEXT NOT NULL DEFAULT '[]')",
		"CREATE TABLE task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL,PRIMARY KEY(task_id,alias))",
		"CREATE TABLE disks(name TEXT PRIMARY KEY,position INTEGER NOT NULL,path TEXT NOT NULL,mountpoint TEXT NOT NULL)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	put := func(key string, value any) {
		t.Helper()
		raw, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, execErr := db.Exec("INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)", key, string(raw), "now"); execErr != nil {
			t.Fatal(execErr)
		}
	}
	put("minio.endpoint", "http://minio.example:9000")
	put("minio.access_key", "access")
	put("minio.secret_key", "secret")
	put("minio.bucket", "rocp-data")
	put("minio.secure", false)
	put("minio.cert_check", false)
	put("pg.host", "db.example")
	put("pg.port", 5433)
	put("pg.database", "minio_event")
	put("pg.user", "postgres")
	put("pg.password", "p@ss")
	put("tasks.data_version", "银河2.2数据")
	put("run.min_free_gb", 0.0)
	put("run.space_safety_factor", 1.15)
	put("run.max_writes_per_disk", 2)
	put("run.require_mcap", true)
	put("run.include_null_quality", false)
	put("run.validate_workers", 16)
	put("run.mcap_pattern", "*.SYNC.mcap")
	put("transfer.file_workers", 4)
	put("transfer.file_retries", 5)
	put("transfer.retry_delay", 0.0)
	put("transfer.verify_md5", true)
	put("transfer.fsync", false)
	if _, err := db.Exec("INSERT INTO scenes(id,position,name) VALUES(1,0,'Household')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO tasks(id,scene_id,position,name,target_count,directory) VALUES(7,1,0,'桌面收纳',40,'桌面收纳')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO task_aliases(task_id,position,alias) VALUES(7,0,'0930桌面收纳')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO disks(name,position,path,mountpoint) VALUES('disk1',0,'/mnt/data','/mnt/data')"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := Config{ConfigDB: name}
	if err := loadConfigDB(&cfg, "count"); err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "http://minio.example:9000" || !cfg.MinioSkipVerify || cfg.Fsync == nil || *cfg.Fsync || cfg.PGHost != "db.example" || cfg.PGPort != 5433 || cfg.DataVersion != "银河2.2数据" {
		t.Fatalf("connection settings were not imported: %+v", cfg)
	}
	if len(cfg.Tasks) != 1 || cfg.Tasks[0].Name != "桌面收纳" || cfg.Tasks[0].TargetCount != 40 || len(cfg.Tasks[0].Aliases) != 1 || cfg.Tasks[0].Aliases[0] != "0930桌面收纳" {
		t.Fatalf("task and alias import failed: %+v", cfg.Tasks)
	}
	if len(cfg.Disks) != 1 || cfg.Disks[0].Name != "disk1" || cfg.Workers != 2 || cfg.FileWorkers != 4 {
		t.Fatalf("disk/runtime settings import failed: disks=%+v workers=%d fileWorkers=%d", cfg.Disks, cfg.Workers, cfg.FileWorkers)
	}
	if err := cfg.normalize("count"); err != nil {
		t.Fatal(err)
	}
	if cfg.MinFreeGB != 0 || cfg.RetryDelaySeconds != 0 {
		t.Fatalf("valid zero-valued settings were overwritten by defaults: minFree=%v retryDelay=%v", cfg.MinFreeGB, cfg.RetryDelaySeconds)
	}
}

func TestLoadConfigDBRejectsDurationMode(t *testing.T) {
	cfg := Config{ConfigDB: filepath.Join(t.TempDir(), "config.sqlite3")}
	if err := loadConfigDB(&cfg, "duration"); err == nil {
		t.Fatal("duration mode must not silently replace hour targets with count targets")
	}
}

func TestSaveLastRunForPythonWeb(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "last-run.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	app := &App{db: db, mode: "count"}
	app.saveLastRun("error", errors.New("simulated failure"))
	var raw string
	if err := db.QueryRow("SELECT value FROM control WHERE key='last_run'").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatal(err)
	}
	if result["result"] != "error" || result["error"] != "simulated failure" || result["mode"] != "count" || result["at"] == "" {
		t.Fatalf("unexpected last_run record: %#v", result)
	}
}

func TestPBKDF2SHA256PythonWebCompatibility(t *testing.T) {
	got := hex.EncodeToString(pbkdf2SHA256([]byte("password"), []byte("salt"), 1, 32))
	const want = "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
	if got != want {
		t.Fatalf("PBKDF2 mismatch: got %s want %s", got, want)
	}
}

func TestWebAPIRequiresBearerTokenWhenConfigured(t *testing.T) {
	digest := hex.EncodeToString(pbkdf2SHA256([]byte("secret-token"), []byte("salt"), 120000, 32))
	api := &WebAPI{tokenHash: "pbkdf2_sha256$120000$73616c74$" + digest}
	request := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
	request.Host = "example.test"
	if api.authorized(request) {
		t.Fatal("request without bearer token must be rejected")
	}
	request.Header.Set("Authorization", "Bearer secret-token")
	if !api.authorized(request) {
		t.Fatal("valid Python-compatible bearer token must be accepted")
	}
	request.Header.Set("Authorization", "Bearer wrong")
	if api.authorized(request) {
		t.Fatal("invalid bearer token must be rejected")
	}
}

func TestWebStatusReturnsConfiguredTasksAndProgress(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "episode-1")
	if err := os.MkdirAll(target, 0755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statements := []string{
		"CREATE TABLE downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT,content_time REAL,file_size INTEGER,target_path TEXT)",
		"CREATE TABLE downloads(episode_id TEXT PRIMARY KEY,task_name TEXT,status TEXT,error TEXT,parent_key TEXT,updated_at TEXT)",
		"CREATE TABLE task_disk_assignments(task_name TEXT,disk_name TEXT)",
		"CREATE TABLE control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO downloaded_episodes VALUES('ep-1','桌面收纳',3600,1024,?)", target); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO downloaded_episodes VALUES('ep-deleted','桌面收纳',1800,512,?)", filepath.Join(root, "deleted-episode")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO task_disk_assignments VALUES('桌面收纳','disk1')"); err != nil {
		t.Fatal(err)
	}
	api := &WebAPI{cfg: Config{Tasks: []Task{{Name: "桌面收纳", Scene: "Household", TargetCount: 40, Tags: []string{"家居"}}}}, db: db}
	recorder := httptest.NewRecorder()
	api.status(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", recorder.Code)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	scenes := response["scenes"].([]any)
	tasks := scenes[0].(map[string]any)["tasks"].([]any)
	task := tasks[0].(map[string]any)
	if response["stale_records"] != float64(1) { t.Fatalf("expected one stale record, got %#v", response["stale_records"]) }
	if task["downloaded"] != float64(1) || task["hours"] != float64(1) || task["disk"] != "disk1" {
		t.Fatalf("unexpected task progress: %#v", task)
	}
	if tags := task["tags"].([]any); len(tags) != 1 || tags[0] != "家居" {
		t.Fatalf("task tags not returned: %#v", tags)
	}
}

func TestWebAPIWithoutTokenIsLocalOnly(t *testing.T) {
	api := &WebAPI{}
	local := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8765/api/status", nil)
	local.RemoteAddr = "127.0.0.1:54321"
	if !api.authorized(local) {
		t.Fatal("loopback request without configured token should be allowed")
	}
	remote := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
	remote.Host = "example.test"
	remote.RemoteAddr = "203.0.113.10:54321"
	if api.authorized(remote) {
		t.Fatal("remote request without configured token must be rejected")
	}
	local.Header.Set("X-Forwarded-For", "203.0.113.10")
	if api.authorized(local) {
		t.Fatal("forwarded requests without token must be rejected")
	}
}

func TestWebAPIRejectsCrossOriginWhenTokenConfigured(t *testing.T) {
	digest := hex.EncodeToString(pbkdf2SHA256([]byte("secret-token"), []byte("salt"), 120000, 32))
	api := &WebAPI{tokenHash: "pbkdf2_sha256$120000$73616c74$" + digest}
	request := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
	request.Host = "example.test"
	request.Header.Set("Origin", "http://attacker.test")
	request.Header.Set("Authorization", "Bearer secret-token")
	if api.authorized(request) {
		t.Fatal("cross-origin requests must be rejected even with a valid token")
	}
}
