package main

import (
	"os"
	"path/filepath"
	"testing"

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
