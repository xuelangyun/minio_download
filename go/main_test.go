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
	"strings"
	"strconv"
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

func TestAcquireNamedDiskLocksPreventsConcurrentInstance(t *testing.T) {
 dir:=t.TempDir()
 release,err:=acquireNamedDiskLocks([]string{"device-a"},dir)
 if err!=nil{t.Fatalf("first lock: %v",err)}
 if _,err=acquireNamedDiskLocks([]string{"device-a"},dir);err==nil{t.Fatal("second instance acquired an already-held disk lock")}
 release()
 release2,err:=acquireNamedDiskLocks([]string{"device-a"},dir)
 if err!=nil{t.Fatalf("lock was not released: %v",err)}
 release2()
}
func TestAcquireNamedDiskLocksSortsAndReleasesPartialSet(t *testing.T) {
 dir:=t.TempDir()
 release,err:=acquireNamedDiskLocks([]string{"device-b","device-a"},dir)
 if err!=nil{t.Fatalf("acquire sorted locks: %v",err)}
 release()
 release2,err:=acquireNamedDiskLocks([]string{"device-a","device-b"},dir)
 if err!=nil{t.Fatalf("reacquire locks after release: %v",err)}
 release2()
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

func TestWebAPITokenFailureLockout(t *testing.T) {
	digest := hex.EncodeToString(pbkdf2SHA256([]byte("secret"), []byte("salt"), 1, 32))
	api := &WebAPI{tokenHash: "pbkdf2_sha256$1$73616c74$" + digest}
	for i := 0; i < 8; i++ {
		request := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
		request.Host = "example.test"
		request.RemoteAddr = "192.0.2.8:1234"
		request.Header.Set("Authorization", "Bearer incorrect")
		if api.authorized(request) {
			t.Fatal("incorrect token unexpectedly accepted")
		}
	}
	valid := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
	valid.Host = "example.test"
	valid.RemoteAddr = "192.0.2.8:1234"
	valid.Header.Set("Authorization", "Bearer secret")
	if api.authorized(valid) {
		t.Fatal("valid token should be temporarily blocked after eight failures")
	}
}

func TestWebStopWritesSharedPauseRequest(t *testing.T) {
	root := t.TempDir()
	pidFile := filepath.Join(root, "minio_download.pid")
	if err := writePIDFile(pidFile, "count"); err != nil {
		t.Fatal(err)
	}
	defer removePIDFile(pidFile)
	db, err := sql.Open("sqlite", filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	api := &WebAPI{cfg: Config{PIDFile: pidFile}, db: db}
	request := httptest.NewRequest(http.MethodPost, "/api/stop", strings.NewReader("{}"))
	recorder := httptest.NewRecorder()
	api.stop(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected stop response %d: %s", recorder.Code, recorder.Body.String())
	}
	var value string
	if err := db.QueryRow("SELECT value FROM control WHERE key='stop_requested'").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "1" {
		t.Fatalf("expected shared stop request value 1, got %q", value)
	}
}

func TestClearStopRequestDBAndSetNewRequest(t *testing.T) {
	name := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL); INSERT INTO control VALUES('stop_requested','old','old-time')"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := clearStopRequestDB(name); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", name)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRow("SELECT value FROM control WHERE key='stop_requested'").Scan(&value); err != sql.ErrNoRows {
		t.Fatalf("expected stale pause to be cleared, got value=%q err=%v", value, err)
	}
	if err := writeStopRequest(db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT value FROM control WHERE key='stop_requested'").Scan(&value); err != nil || value != "1" {
		t.Fatalf("new pause request should survive startup clear, value=%q err=%v", value, err)
	}
}

func TestWebRunRejectsUnsupportedMode(t *testing.T) {
	api := &WebAPI{}
	request := httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader(`{"mode":"cleanup"}`))
	recorder := httptest.NewRecorder()
	api.run(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected unsupported mode to be rejected, got %d", recorder.Code)
	}
}

func TestWebConfigMasksSecretsAndReturnsTaskTags(t *testing.T) {
	name := filepath.Join(t.TempDir(), "config.sqlite3")
	db, err := sql.Open("sqlite", name)
	if err != nil { t.Fatal(err) }
	if _, err := db.Exec("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)"); err != nil { t.Fatal(err) }
	for key, value := range map[string]any{
		"minio.endpoint": "http://minio.example:9000",
		"minio.secret_key": "minio-secret",
		"pg.password": "pg-secret",
		"web.token_hash": "pbkdf2_sha256$1$73616c74$00",
	} {
		raw, marshalErr := json.Marshal(value)
		if marshalErr != nil { t.Fatal(marshalErr) }
		if _, err := db.Exec("INSERT INTO settings(key,value) VALUES(?,?)", key, string(raw)); err != nil { t.Fatal(err) }
	}
	if err := db.Close(); err != nil { t.Fatal(err) }

	api := &WebAPI{
		cfg: Config{
			ConfigDB: name, DataVersion: "银河2.2数据",
			Tasks: []Task{{Name: "桌面收纳", Scene: "Household", TargetCount: 40, Aliases: []string{"0930桌面收纳"}, Tags: []string{"家居"}}},
			Disks: []DiskConfig{{Name: "disk1", Path: "/mnt/data", Mountpoint: "/mnt/data"}},
		},
		tokenHash: "pbkdf2_sha256$1$73616c74$00",
	}
	recorder := httptest.NewRecorder()
	api.config(recorder, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if recorder.Code != http.StatusOK { t.Fatalf("unexpected status %d: %s", recorder.Code, recorder.Body.String()) }
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil { t.Fatal(err) }
	values := response["values"].(map[string]any)
	if values["minio.endpoint"] != "http://minio.example:9000" { t.Fatalf("regular setting missing: %#v", values["minio.endpoint"]) }
	if values["minio.secret_key"] != "" || values["pg.password"] != "" { t.Fatalf("secret leaked to response: %#v", values) }
	secrets := response["secrets_set"].([]any)
	if len(secrets) != 2 { t.Fatalf("expected two masked secrets, got %#v", secrets) }
	if response["token_set"] != true { t.Fatalf("expected token_set=true, got %#v", response["token_set"]) }
	tasks := response["tasks"].(map[string]any)["scenes"].([]any)
	task := tasks[0].(map[string]any)["tasks"].([]any)[0].(map[string]any)
	if task["count"] != float64(40) || task["tags"].([]any)[0] != "家居" { t.Fatalf("task config/tags missing: %#v", task) }
	if len(response["schema"].([]any)) < 20 { t.Fatalf("configuration schema incomplete: %d", len(response["schema"].([]any))) }
}

func TestForceStopRejectsNoActiveDownloader(t *testing.T) {
	api := &WebAPI{cfg: Config{PIDFile: filepath.Join(t.TempDir(), "missing.pid")}}
	req := httptest.NewRequest(http.MethodPost, "/api/stop", strings.NewReader(`{"force":true}`))
	rec := httptest.NewRecorder()
	api.stop(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 when no downloader is active, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestKillProcessTreeRejectsUnsafePID(t *testing.T) {
	for _, pid := range []int32{0, 1, -1} {
		if err := killProcessTree(pid); err == nil {
			t.Fatalf("expected PID %d to be rejected", pid)
		}
	}
}

func newPlanTestAPI(t *testing.T) *WebAPI {
	t.Helper()
	return &WebAPI{cfg: Config{
		ConfigDB: filepath.Join(t.TempDir(), "config.sqlite3"),
		DataVersion: "银河2.2数据",
		Tasks: []Task{{Scene: "Household", Name: "桌面收纳", Dir: "桌面收纳", TargetCount: 40, Aliases: []string{"0930桌面收纳"}, Tags: []string{"家居"}}},
	}}
}

func TestWebPlanCRUDAndImportExport(t *testing.T) {
	api := newPlanTestAPI(t)
	payload := `{"data_version":"银河2.2数据","scenes":[{"scene":"Household","tasks":[{"name":"桌面收纳","count":40,"aliases":["0930桌面收纳"],"dir":"桌面收纳","tags":["家居"]}]}]}`
	create := httptest.NewRecorder()
	api.planCreate(create, httptest.NewRequest(http.MethodPost, "/api/plans", strings.NewReader(`{"name":"第一批","note":"测试方案","payload":`+payload+`}`)))
	if create.Code != http.StatusOK { t.Fatalf("create failed: %d %s", create.Code, create.Body.String()) }
	var created map[string]any
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil { t.Fatal(err) }
	id := int64(created["id"].(float64))

	list := httptest.NewRecorder()
	api.plansList(list, httptest.NewRequest(http.MethodGet, "/api/plans", nil))
	if list.Code != http.StatusOK { t.Fatalf("list failed: %d %s", list.Code, list.Body.String()) }
	var listed map[string]any
	if err := json.Unmarshal(list.Body.Bytes(), &listed); err != nil { t.Fatal(err) }
	plans := listed["plans"].([]any)
	if len(plans) != 1 { t.Fatalf("expected one plan, got %d", len(plans)) }
	summary := plans[0].(map[string]any)
	if summary["task_count"] != float64(1) || summary["total_target"] != float64(40) || summary["tags"].([]any)[0] != "家居" { t.Fatalf("incorrect plan summary: %#v", summary) }

	get := httptest.NewRequest(http.MethodGet, "/api/plans/"+strconv.FormatInt(id, 10), nil)
	get.SetPathValue("id", strconv.FormatInt(id, 10))
	got := httptest.NewRecorder()
	api.planGet(got, get)
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), "桌面收纳") { t.Fatalf("get failed: %d %s", got.Code, got.Body.String()) }

	exportReq := httptest.NewRequest(http.MethodGet, "/api/plans/"+strconv.FormatInt(id, 10)+"/export", nil)
	exportReq.SetPathValue("id", strconv.FormatInt(id, 10))
	exported := httptest.NewRecorder()
	api.planExport(exported, exportReq)
	if exported.Code != http.StatusOK || !strings.Contains(exported.Header().Get("Content-Disposition"), "attachment") { t.Fatalf("export failed: %d %s", exported.Code, exported.Body.String()) }

	imported := httptest.NewRecorder()
	api.planImport(imported, httptest.NewRequest(http.MethodPost, "/api/plans/import", strings.NewReader(exported.Body.String())))
	if imported.Code != http.StatusOK { t.Fatalf("import failed: %d %s", imported.Code, imported.Body.String()) }
	var importResult map[string]any
	if err := json.Unmarshal(imported.Body.Bytes(), &importResult); err != nil { t.Fatal(err) }
	if importResult["name"] != "第一批 (2)" { t.Fatalf("expected duplicate import name suffix, got %#v", importResult["name"]) }

	updateReq := httptest.NewRequest(http.MethodPut, "/api/plans/"+strconv.FormatInt(id, 10), strings.NewReader(`{"name":"第二批","note":"已更新"}`))
	updateReq.SetPathValue("id", strconv.FormatInt(id, 10))
	updated := httptest.NewRecorder()
	api.planUpdate(updated, updateReq)
	if updated.Code != http.StatusOK { t.Fatalf("update failed: %d %s", updated.Code, updated.Body.String()) }

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/plans/"+strconv.FormatInt(id, 10), nil)
	deleteReq.SetPathValue("id", strconv.FormatInt(id, 10))
	deleted := httptest.NewRecorder()
	api.planDelete(deleted, deleteReq)
	if deleted.Code != http.StatusOK { t.Fatalf("delete failed: %d %s", deleted.Code, deleted.Body.String()) }
	missingReq := httptest.NewRequest(http.MethodGet, "/api/plans/"+strconv.FormatInt(id, 10), nil)
	missingReq.SetPathValue("id", strconv.FormatInt(id, 10))
	missing := httptest.NewRecorder()
	api.planGet(missing, missingReq)
	if missing.Code != http.StatusNotFound { t.Fatalf("expected 404 after delete, got %d", missing.Code) }
}

func TestValidatePlanPayloadRejectsDuplicateTasksAndInvalidCounts(t *testing.T) {
	for _, raw := range []string{
		`{"data_version":"v1","scenes":[{"scene":"A","tasks":[{"name":"same","count":1},{"name":"same","count":2}]}]}`,
		`{"data_version":"v1","scenes":[{"scene":"A","tasks":[{"name":"bad","count":-1}]}]}`,
		`{"data_version":"v1","scenes":null}`,
	} {
		if _, err := validatePlanPayload(json.RawMessage(raw)); err == nil { t.Errorf("expected invalid payload: %s", raw) }
	}
}

func TestWebPlanApplyReplaceAndMerge(t *testing.T) {
	api := newPlanTestAPI(t)
	db, err := sql.Open("sqlite", api.cfg.ConfigDB)
	if err != nil { t.Fatal(err) }
	schema := []string{
		"CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",
		"CREATE TABLE scenes(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE)",
		"CREATE TABLE tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE,target_count INTEGER NOT NULL,directory TEXT NOT NULL DEFAULT '',tags TEXT NOT NULL DEFAULT '[]')",
		"CREATE TABLE task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL,PRIMARY KEY(task_id,alias))",
	}
	for _, stmt := range schema { if _, err := db.Exec(stmt); err != nil { t.Fatal(err) } }
	if err := db.Close(); err != nil { t.Fatal(err) }
	payload := `{"data_version":"v2","scenes":[{"scene":"Office","tasks":[{"name":"文件整理","count":12,"aliases":["整理文件"],"dir":"文件整理","tags":["办公"]}]}]}`
	created := httptest.NewRecorder()
	api.planCreate(created, httptest.NewRequest(http.MethodPost, "/api/plans", strings.NewReader(`{"name":"办公方案","payload":`+payload+`}`)))
	if created.Code != http.StatusOK { t.Fatalf("create failed: %d %s", created.Code, created.Body.String()) }
	var result map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &result); err != nil { t.Fatal(err) }
	id := strconv.FormatInt(int64(result["id"].(float64)), 10)
	req := httptest.NewRequest(http.MethodPost, "/api/plans/"+id+"/apply", strings.NewReader(`{"mode":"replace"}`))
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	api.planApply(rec, req)
	if rec.Code != http.StatusOK { t.Fatalf("replace apply failed: %d %s", rec.Code, rec.Body.String()) }
	if api.cfg.DataVersion != "v2" || len(api.cfg.Tasks) != 1 || api.cfg.Tasks[0].Name != "文件整理" || api.cfg.Tasks[0].Tags[0] != "办公" { t.Fatalf("config not replaced: %#v", api.cfg) }

	mergePayload := `{"name":"追加方案","payload":{"data_version":"v2","scenes":[{"scene":"Office","tasks":[{"name":"文件整理","count":99},{"name":"邮件归档","count":5,"aliases":[],"dir":"邮件归档","tags":[]}]}]}}`
	mergeCreate := httptest.NewRecorder()
	api.planCreate(mergeCreate, httptest.NewRequest(http.MethodPost, "/api/plans", strings.NewReader(mergePayload)))
	if mergeCreate.Code != http.StatusOK { t.Fatalf("merge plan create failed: %d %s", mergeCreate.Code, mergeCreate.Body.String()) }
	var mergeResult map[string]any
	if err := json.Unmarshal(mergeCreate.Body.Bytes(), &mergeResult); err != nil { t.Fatal(err) }
	mergeID := strconv.FormatInt(int64(mergeResult["id"].(float64)), 10)
	mergeReq := httptest.NewRequest(http.MethodPost, "/api/plans/"+mergeID+"/apply", strings.NewReader(`{"mode":"merge"}`))
	mergeReq.SetPathValue("id", mergeID)
	merged := httptest.NewRecorder()
	api.planApply(merged, mergeReq)
	if merged.Code != http.StatusOK { t.Fatalf("merge apply failed: %d %s", merged.Code, merged.Body.String()) }
	if len(api.cfg.Tasks) != 2 || api.cfg.Tasks[1].Name != "邮件归档" { t.Fatalf("merge did not append only new task: %#v", api.cfg.Tasks) }
}

func TestValidatePlanPayloadAllowsEmptyDataVersion(t *testing.T) {
	raw := json.RawMessage(`{"data_version":"","scenes":[{"scene":"Office","tasks":[{"name":"文件整理","count":2}]}]}`)
	p, err := validatePlanPayload(raw)
	if err != nil {
		t.Fatalf("Python-compatible plan may omit data_version: %v", err)
	}
	if p.DataVersion != "" || len(p.Scenes) != 1 {
		t.Fatalf("unexpected normalized plan: %#v", p)
	}
}

func TestPlanReplaceInheritsCurrentDataVersion(t *testing.T) {
	api := newPlanTestAPI(t)
	api.cfg.DataVersion = "current-version"
	db, err := sql.Open("sqlite", api.cfg.ConfigDB)
	if err != nil { t.Fatal(err) }
	for _, stmt := range []string{
		"CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",
		"CREATE TABLE scenes(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE)",
		"CREATE TABLE tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE,target_count INTEGER NOT NULL,directory TEXT NOT NULL DEFAULT '',tags TEXT NOT NULL DEFAULT '[]')",
		"CREATE TABLE task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL,PRIMARY KEY(task_id,alias))",
	} { if _, err := db.Exec(stmt); err != nil { t.Fatal(err) } }
	if err := db.Close(); err != nil { t.Fatal(err) }
	payload := `{"data_version":"","scenes":[{"scene":"Office","tasks":[{"name":"文件整理","count":2,"dir":"文件整理","aliases":[],"tags":[]}]}]}`
	created := httptest.NewRecorder()
	api.planCreate(created, httptest.NewRequest(http.MethodPost, "/api/plans", strings.NewReader(`{"name":"空版本方案","payload":`+payload+`}`)))
	if created.Code != http.StatusOK {
		t.Fatalf("create empty-version plan failed: %d %s", created.Code, created.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &result); err != nil { t.Fatal(err) }
	id := strconv.FormatInt(int64(result["id"].(float64)), 10)
	req := httptest.NewRequest(http.MethodPost, "/api/plans/"+id+"/apply", strings.NewReader(`{"mode":"replace"}`))
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	api.planApply(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("replace should inherit current version: %d %s", rec.Code, rec.Body.String())
	}
	if api.cfg.DataVersion != "current-version" {
		t.Fatalf("expected current version to be inherited, got %q", api.cfg.DataVersion)
	}
}

func TestWebConfigDisksPersistsAndDefaultsMountpoint(t *testing.T) {
	api := newPlanTestAPI(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/config/disks", strings.NewReader(`{"disks":[{"name":"diskA","path":"/mnt/data"}]}`))
	api.configDisks(rec, req)
	if rec.Code != http.StatusOK { t.Fatalf("disk update failed: %d %s", rec.Code, rec.Body.String()) }
	if len(api.cfg.Disks) != 1 || api.cfg.Disks[0].Mountpoint != "/mnt/data" { t.Fatalf("disk config not updated/defaulted: %#v", api.cfg.Disks) }
	db, err := sql.Open("sqlite", api.cfg.ConfigDB)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	var name, path, mount string
	if err := db.QueryRow("SELECT name,path,mountpoint FROM disks WHERE name='diskA'").Scan(&name,&path,&mount); err != nil { t.Fatal(err) }
	if path != "/mnt/data" || mount != "/mnt/data" { t.Fatalf("unexpected persisted disk: %s %s",path,mount) }
}

func TestWebConfigDisksRejectsDuplicateNamesWithoutChangingConfig(t *testing.T) {
	api := newPlanTestAPI(t)
	api.cfg.Disks = []DiskConfig{{Name:"existing",Path:"/mnt/existing",Mountpoint:"/mnt/existing"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/config/disks", strings.NewReader(`{"disks":[{"name":"same","path":"/mnt/a"},{"name":"same","path":"/mnt/b"}]}`))
	api.configDisks(rec, req)
	if rec.Code != http.StatusBadRequest { t.Fatalf("expected 400, got %d: %s",rec.Code,rec.Body.String()) }
	if len(api.cfg.Disks)!=1 || api.cfg.Disks[0].Name!="existing" { t.Fatalf("invalid update mutated in-memory config: %#v",api.cfg.Disks) }
}

func TestWebConfigTasksPersistsAndUpdatesRuntime(t *testing.T) {
	api := newPlanTestAPI(t)
	db, err := sql.Open("sqlite", api.cfg.ConfigDB)
	if err != nil { t.Fatal(err) }
	for _, stmt := range []string{
		"CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)",
		"CREATE TABLE scenes(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE)",
		"CREATE TABLE tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE,target_count INTEGER NOT NULL,directory TEXT NOT NULL DEFAULT '',tags TEXT NOT NULL DEFAULT '[]')",
		"CREATE TABLE task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL,PRIMARY KEY(task_id,alias))",
	} { if _, err := db.Exec(stmt); err != nil { t.Fatal(err) } }
	if err := db.Close(); err != nil { t.Fatal(err) }
	body := `{"data_version":"v-next","scenes":[{"scene":"Office","tasks":[{"name":"邮件归档","count":8,"aliases":["归档邮件"],"dir":"邮件归档","tags":["办公"]}]}]}`
	req := httptest.NewRequest(http.MethodPut, "/api/config/tasks", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.configTasks(rec, req)
	if rec.Code != http.StatusOK { t.Fatalf("config tasks failed: %d %s", rec.Code, rec.Body.String()) }
	if api.cfg.DataVersion != "v-next" || len(api.cfg.Tasks) != 1 || api.cfg.Tasks[0].Name != "邮件归档" || api.cfg.Tasks[0].TargetCount != 8 {
		t.Fatalf("runtime config not updated: %#v", api.cfg)
	}
	db, err = sql.Open("sqlite", api.cfg.ConfigDB)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	var value string
	if err := db.QueryRow("SELECT value FROM settings WHERE key='tasks.data_version'").Scan(&value); err != nil { t.Fatal(err) }
	if value != `"v-next"` { t.Fatalf("data_version not persisted as Python JSON setting: %s", value) }
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM tasks WHERE name='邮件归档'").Scan(&count); err != nil { t.Fatal(err) }
	if count != 1 { t.Fatalf("expected persisted task, got %d", count) }
}

func TestWebDiskCheckDoesNotWriteThroughMissingMount(t *testing.T) {
	api := newPlanTestAPI(t)
	root := t.TempDir()
	req := httptest.NewRequest(http.MethodPost, "/api/config/disks/check", strings.NewReader(`{"disks":[{"name":"disk1","path":"`+filepath.ToSlash(filepath.Join(root,"data"))+`","mountpoint":"`+filepath.ToSlash(root)+`"}]}`))
	rec := httptest.NewRecorder()
	api.configDisksCheck(rec, req)
	if rec.Code != http.StatusOK { t.Fatalf("disk check failed: %d %s",rec.Code,rec.Body.String()) }
	var response map[string]any
	if err:=json.Unmarshal(rec.Body.Bytes(),&response);err!=nil{t.Fatal(err)}
	results:=response["results"].([]any)
	if len(results)!=1 || results[0].(map[string]any)["ok"]!=false { t.Fatalf("expected unmounted disk to fail: %#v",response) }
	entries,err:=os.ReadDir(root);if err!=nil{t.Fatal(err)}
	if len(entries)!=0{t.Fatalf("disk check wrote into unmounted parent directory: %#v",entries)}
}

func TestDecodeWebSettingsValidatesAndPreservesSecretNull(t *testing.T) {
 raw:=map[string]json.RawMessage{
  "minio.secret_key":json.RawMessage("null"),
  "run.max_writes_per_disk":json.RawMessage("4"),
  "run.space_safety_factor":json.RawMessage("1.25"),
 }
 values,err:=decodeWebSettings(raw);if err!=nil{t.Fatal(err)}
 if _,ok:=values["minio.secret_key"];ok{t.Fatal("null secret must mean unchanged")}
 if values["run.max_writes_per_disk"]!=4{t.Fatalf("integer setting not decoded: %#v",values)}
 if values["run.space_safety_factor"]!=1.25{t.Fatalf("float setting not decoded: %#v",values)}
}
func TestDecodeWebSettingsRejectsUnknownAndOutOfRange(t *testing.T) {
 for _,raw:=range []map[string]json.RawMessage{
  {"not.a.setting":json.RawMessage("1")},
  {"pg.port":json.RawMessage("65536")},
  {"run.max_writes_per_disk":json.RawMessage("1.5")},
  {"minio.secure":json.RawMessage("\"yes\"")},
 }{
  if _,err:=decodeWebSettings(raw);err==nil{t.Fatalf("expected validation error for %#v",raw)}
 }
}
func TestWebConfigSettingsPersistsAndUpdatesRuntime(t *testing.T) {
 api:=newPlanTestAPI(t)
 db,err:=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)}
 if _,err=db.Exec("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)");err!=nil{t.Fatal(err)}
 db.Close()
 body:=`{"values":{"run.max_writes_per_disk":4,"run.space_safety_factor":1.25,"run.default_rounds":3,"transfer.connect_timeout":17,"transfer.read_timeout":240,"web.host":"0.0.0.0","web.port":9876,"minio.secret_key":null,"pg.password":"new-password"}}`
 req:=httptest.NewRequest(http.MethodPut,"/api/config/settings",strings.NewReader(body));rec:=httptest.NewRecorder()
 api.configSettings(rec,req)
 if rec.Code!=http.StatusOK{t.Fatalf("settings update failed: %d %s",rec.Code,rec.Body.String())}
 if api.cfg.Workers!=4||api.cfg.SafetyFactor!=1.25||api.cfg.PGPassword!="new-password"||api.cfg.DefaultRounds!=3||api.cfg.ConnectTimeout!=17||api.cfg.ReadTimeout!=240||api.cfg.WebHost!="0.0.0.0"||api.cfg.WebPort!=9876{t.Fatalf("runtime config not updated: %#v",api.cfg)}
 db,err=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)};defer db.Close()
 var got string
 if err=db.QueryRow("SELECT value FROM settings WHERE key='pg.password'").Scan(&got);err!=nil{t.Fatal(err)}
 if got!=`"new-password"`{t.Fatalf("secret not persisted as JSON string: %s",got)}
 var count int
 if err=db.QueryRow("SELECT COUNT(*) FROM settings WHERE key='minio.secret_key'").Scan(&count);err!=nil{t.Fatal(err)}
 if count!=0{t.Fatal("null secret should not create or overwrite setting")}
}
func TestWebConfigSettingsRejectsInvalidBatchAtomically(t *testing.T) {
 api:=newPlanTestAPI(t)
 db,err:=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)}
 if _,err=db.Exec("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)");err!=nil{t.Fatal(err)}
 db.Close()
 req:=httptest.NewRequest(http.MethodPut,"/api/config/settings",strings.NewReader(`{"values":{"run.max_writes_per_disk":4,"pg.port":70000}}`));rec:=httptest.NewRecorder()
 api.configSettings(rec,req)
 if rec.Code!=http.StatusBadRequest{t.Fatalf("expected 400, got %d: %s",rec.Code,rec.Body.String())}
 db,err=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)};defer db.Close()
 var count int
 if err=db.QueryRow("SELECT COUNT(*) FROM settings").Scan(&count);err!=nil{t.Fatal(err)}
 if count!=0{t.Fatalf("invalid batch partially persisted %d settings",count)}
}
func TestWebConfigTestRejectsUnknownTarget(t *testing.T) {
 api:=newPlanTestAPI(t)
 req:=httptest.NewRequest(http.MethodPost,"/api/config/test",strings.NewReader(`{"target":"redis"}`));rec:=httptest.NewRecorder()
 api.configTest(rec,req)
 if rec.Code!=http.StatusBadRequest{t.Fatalf("expected 400, got %d",rec.Code)}
}

func TestConfiguredConnectionTimeoutsAreApplied(t *testing.T) {
	cfg:=Config{ConnectTimeout:7,ReadTimeout:91,MinioSkipVerify:true}
	transport:=minioHTTPTransport(cfg)
	if transport.TLSHandshakeTimeout!=7*time.Second || transport.ResponseHeaderTimeout!=91*time.Second { t.Fatalf("transport timeouts not applied: tls=%s response=%s",transport.TLSHandshakeTimeout,transport.ResponseHeaderTimeout) }
	if transport.TLSClientConfig==nil || !transport.TLSClientConfig.InsecureSkipVerify { t.Fatal("explicit certificate skip setting was not applied") }
	u,err:=url.Parse(postgresDSN(cfg))
	if err!=nil { t.Fatal(err) }
	if u.Query().Get("connect_timeout")!="7" { t.Fatalf("PostgreSQL connect_timeout mismatch: %s",u.Query().Get("connect_timeout")) }
}

func TestApplyWebSettingCoversAllRuntimeSettings(t *testing.T) {
 cfg:=Config{}
 cases:=[]struct{key string;value any}{
  {"run.default_rounds",3},
  {"transfer.connect_timeout",17},
  {"transfer.read_timeout",240},
  {"web.host","0.0.0.0"},
  {"web.port",9876},
 }
 for _,tc:=range cases{applyWebSetting(&cfg,tc.key,tc.value)}
 if cfg.DefaultRounds!=3||cfg.ConnectTimeout!=17||cfg.ReadTimeout!=240||cfg.WebHost!="0.0.0.0"||cfg.WebPort!=9876{
  t.Fatalf("operational settings not applied: %#v",cfg)
 }
}
func TestMinioTransportUsesConfiguredTimeouts(t *testing.T) {
 tr:=minioHTTPTransport(Config{ConnectTimeout:23,ReadTimeout:321})
 defer tr.CloseIdleConnections()
 if tr.TLSHandshakeTimeout!=23*time.Second{t.Fatalf("TLS handshake timeout=%s",tr.TLSHandshakeTimeout)}
 if tr.ResponseHeaderTimeout!=321*time.Second{t.Fatalf("response header timeout=%s",tr.ResponseHeaderTimeout)}
 if tr.DialContext==nil{t.Fatal("dial timeout transport hook is missing")}
}

func TestWebConfigTasksResetRestoresDefaultsAndPreservesTags(t *testing.T) {
 api:=newPlanTestAPI(t)
 db,err:=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)}
 schema:=`CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);
 CREATE TABLE scenes(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL,name TEXT NOT NULL);
 CREATE TABLE tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL,target_count INTEGER NOT NULL,directory TEXT NOT NULL,tags TEXT NOT NULL);
 CREATE TABLE task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL);`
 if _,err=db.Exec(schema);err!=nil{t.Fatal(err)};db.Close()
 api.cfg.Tasks=append(api.cfg.Tasks,Task{Scene:"Household(家居)",Name:"桌面收纳",Dir:"桌面收纳",TargetCount:2,Tags:[]string{"用户自定义标签"}})
 rec:=httptest.NewRecorder()
 api.configTasksReset(rec,httptest.NewRequest(http.MethodPost,"/api/config/tasks/reset",strings.NewReader(`{}`)))
 if rec.Code!=http.StatusOK{t.Fatalf("reset failed: %d %s",rec.Code,rec.Body.String())}
 if len(api.cfg.Tasks)<40{t.Fatalf("expected default count-mode task set, got %d",len(api.cfg.Tasks))}
 found:=false
 for _,task:=range api.cfg.Tasks{if task.Name=="桌面收纳"{found=true;if len(task.Tags)!=1||task.Tags[0]!="用户自定义标签"{t.Fatalf("same-name task tag was not preserved: %#v",task.Tags)}}}
 if !found{t.Fatal("default task 桌面收纳 was not restored")}
 db,err:=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)};defer db.Close()
 var count int
 if err=db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&count);err!=nil{t.Fatal(err)}
 if count!=len(api.cfg.Tasks){t.Fatalf("persisted tasks=%d runtime tasks=%d",count,len(api.cfg.Tasks))}
}

func TestNewTokenHashIsPythonCompatible(t *testing.T) {
	hash, err := newTokenHash("a-long-enough-token")
	if err != nil { t.Fatal(err) }
	parts := strings.Split(hash, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2_sha256" || parts[1] != "120000" {
		t.Fatalf("unexpected token hash format: %q", hash)
	}
	salt, err := hex.DecodeString(parts[2]); if err != nil || len(salt) != 16 { t.Fatalf("invalid salt: %q %v", parts[2], err) }
	want, err := hex.DecodeString(parts[3]); if err != nil || len(want) != 32 { t.Fatalf("invalid digest: %q %v", parts[3], err) }
	api := &WebAPI{tokenHash: hash}
	req := httptest.NewRequest(http.MethodGet, "http://example.test/api/status", nil)
	req.Host = "example.test"
	req.Header.Set("Authorization", "Bearer a-long-enough-token")
	if !api.authorized(req) { t.Fatal("generated token hash must authenticate") }
}

func TestWebAuthTokenCanSetRotateAndClear(t *testing.T) {
	api := newPlanTestAPI(t)
	db, err := sql.Open("sqlite", api.cfg.ConfigDB); if err != nil { t.Fatal(err) }
	if _, err = db.Exec("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)"); err != nil { t.Fatal(err) }
	db.Close()

	call := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/api/auth/token", strings.NewReader(body))
		rec := httptest.NewRecorder()
		api.authToken(rec, req)
		return rec
	}
	if rec := call(`{"action":"set","token":"short"}`); rec.Code != http.StatusBadRequest { t.Fatalf("short token should be rejected: %d %s", rec.Code, rec.Body.String()) }
	if rec := call(`{"action":"set","token":"first-long-token-123"}`); rec.Code != http.StatusOK { t.Fatalf("set token failed: %d %s", rec.Code, rec.Body.String()) }
	first := api.tokenHash
	if first == "" || !strings.HasPrefix(first, "pbkdf2_sha256$") { t.Fatalf("token hash was not installed: %q", first) }
	if rec := call(`{"action":"set","token":"second-long-token-456"}`); rec.Code != http.StatusOK { t.Fatalf("rotate token failed: %d %s", rec.Code, rec.Body.String()) }
	if api.tokenHash == first { t.Fatal("rotation must replace the previous hash") }
	if rec := call(`{"action":"clear"}`); rec.Code != http.StatusOK { t.Fatalf("clear token failed: %d %s", rec.Code, rec.Body.String()) }
	if api.tokenHash != "" { t.Fatal("clear must disable token authentication") }

	db, err = sql.Open("sqlite", api.cfg.ConfigDB); if err != nil { t.Fatal(err) }; defer db.Close()
	var raw string
	if err = db.QueryRow("SELECT value FROM settings WHERE key='web.token_hash'").Scan(&raw); err != nil { t.Fatal(err) }
	if raw != "null" { t.Fatalf("cleared token must persist JSON null, got %q", raw) }
	var loaded string
	if err = db.QueryRow("SELECT CASE WHEN value='null' THEN '' ELSE trim(value,char(34)) END FROM settings WHERE key='web.token_hash'").Scan(&loaded); err != nil { t.Fatal(err) }
	if loaded != "" { t.Fatalf("cleared token should reload as empty, got %q", loaded) }
}

func TestAcquireSemaphoreStopsWhenContextCancelled(t *testing.T) {
 ctx,cancel:=context.WithCancel(context.Background())
 sem:=make(chan struct{},1)
 sem<-struct{}{}
 cancel()
 started:=time.Now()
 if acquireSemaphore(ctx,sem){t.Fatal("cancelled context unexpectedly acquired a full semaphore")}
 if time.Since(started)>100*time.Millisecond{t.Fatal("semaphore acquisition did not stop promptly")}
}
func TestAcquireSemaphoreAcquiresAvailableSlot(t *testing.T) {
 ctx,cancel:=context.WithCancel(context.Background());defer cancel()
 sem:=make(chan struct{},1)
 if !acquireSemaphore(ctx,sem){t.Fatal("available semaphore slot was not acquired")}
 if len(sem)!=1{t.Fatalf("semaphore occupancy=%d, want 1",len(sem))}
}

func TestUpdateTransferSampleCalculatesRateETAAndProgress(t *testing.T) {
 api:=&WebAPI{}
 start:=time.Date(2026,1,1,0,0,0,0,time.UTC)
 speed,eta,percent:=api.updateTransferSample(start,1000,3000)
 if speed!=0||eta!=nil||percent<33.33||percent>33.34{t.Fatalf("initial sample = speed %v eta %v progress %v",speed,eta,percent)}
 speed,eta,percent=api.updateTransferSample(start.Add(2*time.Second),2000,3000)
 if speed<499.9||speed>500.1{t.Fatalf("speed=%v, want 500 B/s",speed)}
 if eta!=int64(2){t.Fatalf("eta=%v, want 2 seconds",eta)}
 if percent<66.66||percent>66.67{t.Fatalf("progress=%v, want 66.67%%",percent)}
}
func TestUpdateTransferSampleResetsAfterCounterDecrease(t *testing.T) {
 api:=&WebAPI{}
 start:=time.Date(2026,1,1,0,0,0,0,time.UTC)
 api.updateTransferSample(start,5000,10000)
 speed,eta,percent:=api.updateTransferSample(start.Add(time.Second),100,10000)
 if speed!=0||eta!=nil||percent!=1{t.Fatalf("reset sample = speed %v eta %v progress %v",speed,eta,percent)}
}
func TestMaxInt64(t *testing.T) {
 if got:=maxInt64(-1,0);got!=0{t.Fatalf("maxInt64(-1,0)=%d",got)}
 if got:=maxInt64(5,3);got!=5{t.Fatalf("maxInt64(5,3)=%d",got)}
}
