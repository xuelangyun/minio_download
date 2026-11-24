package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"sync"
	"strconv"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)


func TestDurationTargetsPersistInSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "config.sqlite3")
	if err := initializeSQLiteDatabase(dbPath); err != nil {
		t.Fatalf("initialize SQLite schema: %v", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	payload := planPayload{
		DataVersion: "test-version",
		Scenes: []planScene{{
			Scene: "test-scene",
			Tasks: []planTask{{Name: "duration-task", Count: 12, Hours: 3.75}},
		}},
	}
	if _, err = validatePlanPayload(json.RawMessage(mustJSON(payload))); err != nil {
		t.Fatalf("validate plan: %v", err)
	}
	if err = persistPlanPayload(db, payload); err != nil {
		db.Close()
		t.Fatalf("persist plan: %v", err)
	}
	cfg := Config{ConfigDB: dbPath}
	if err = loadConfigDB(&cfg, "duration"); err != nil {
		t.Fatalf("load duration config: %v", err)
	}
	if len(cfg.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(cfg.Tasks))
	}
	if got := cfg.Tasks[0].TargetHours; got != 3.75 {
		t.Fatalf("target hours did not survive SQLite round trip: got %v, want 3.75", got)
	}
	if got := cfg.Tasks[0].TargetCount; got != 12 {
		t.Fatalf("target count changed during SQLite round trip: got %d, want 12", got)
	}
}

func TestValidatePlanRejectsNegativeDurationTarget(t *testing.T) {
	payload := planPayload{
		DataVersion: "test-version",
		Scenes: []planScene{{
			Scene: "test-scene",
			Tasks: []planTask{{Name: "bad-duration", Hours: -0.5}},
		}},
	}
	if _, err := validatePlanPayload(json.RawMessage(mustJSON(payload))); err == nil {
		t.Fatal("expected negative target_hours to be rejected")
	}
}

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
func TestResumeReservationTracksOnlyRemainingBytes(t *testing.T) {
 app:=&App{cfg:Config{MinFreeGB:0}}
 dst:=filepath.Join(t.TempDir(),"object.bin")
 if err:=os.WriteFile(dst+".minio-tmp",make([]byte,40),0600);err!=nil{t.Fatal(err)}
 partial:=existingPartialBytes(dst,100)
 reservation,err:=app.reserveObjectSpace(DiskConfig{Name:"disk-a",Path:filepath.Dir(dst)},remainingObjectBytes(dst,100))
 if err!=nil{t.Fatalf("reserve remaining bytes: %v",err)}
 reservation.consumed=uint64(partial)
 if got:=app.spaceInFlight["disk-a"];got!=60{t.Fatalf("in-flight reservation=%d want 60",got)}
 // The partial may disappear between reservation and opening the temp file.
 // The downloader must retain this baseline so discard recovery restores it.
 if got:=reservation.ConsumedBytes();got!=partial{t.Fatalf("consumed baseline=%d want %d",got,partial)}
 // If the remote object changes or the temp file disappears, its previously
 // occupied bytes must become reserved again for a full restart.
 reservation.Restore(reservation.ConsumedBytes())
 if got:=app.spaceInFlight["disk-a"];got!=100{t.Fatalf("reservation after discarding partial=%d want 100",got)}
 reservation.Release()
}
func TestSpaceReservationRestoreAfterDiscardedPartial(t *testing.T) {
 app:=&App{cfg:Config{MinFreeGB:0}}
 reservation,err:=app.reserveObjectSpace(DiskConfig{Name:"disk-a",Path:t.TempDir()},100)
 if err!=nil{t.Fatal(err)}
 reservation.Consume(40)
 if app.spaceInFlight["disk-a"]!=60{t.Fatalf("after consume=%d want 60",app.spaceInFlight["disk-a"])}
 reservation.Restore(25)
 if app.spaceInFlight["disk-a"]!=85{t.Fatalf("after restore=%d want 85",app.spaceInFlight["disk-a"])}
 reservation.Restore(100)
 if app.spaceInFlight["disk-a"]!=100{t.Fatalf("restore must not exceed consumed bytes: got %d want 100",app.spaceInFlight["disk-a"])}
 reservation.Release()
 if _,ok:=app.spaceInFlight["disk-a"];ok{t.Fatal("release should clear remaining reservation")}
}
func TestDynamicSpaceExhaustionIsClassified(t *testing.T) {
 err:=fmt.Errorf("%w: disk=target",errInsufficientDiskSpace)
 if !errors.Is(err,errInsufficientDiskSpace){t.Fatal("wrapped capacity exhaustion must remain classifiable")}
}
func TestReserveObjectSpaceConcurrentAccounting(t *testing.T) {
 const workers=32
 app:=&App{cfg:Config{MinFreeGB:0}}
 diskCfg:=DiskConfig{Name:"disk-a",Path:t.TempDir()}
 reservations:=make(chan *spaceReservation,workers)
 errs:=make(chan error,workers)
 var wg sync.WaitGroup
 for i:=0;i<workers;i++{
  wg.Add(1)
  go func(){
   defer wg.Done()
   reservation,err:=app.reserveObjectSpace(diskCfg,1)
   if err!=nil{errs<-err;return}
   reservations<-reservation
  }()
 }
 wg.Wait()
 close(errs)
 for err:=range errs{t.Errorf("concurrent reservation failed: %v",err)}
 close(reservations)
 held:=uint64(0)
 all:=make([]*spaceReservation,0,workers)
 for reservation:=range reservations{held+=reservation.remaining;all=append(all,reservation)}
 if len(all)!=workers{t.Fatalf("acquired %d reservations, want %d",len(all),workers)}
 app.spaceMu.Lock()
 got:=app.spaceInFlight["disk-a"]
 app.spaceMu.Unlock()
 if got!=uint64(workers)||held!=uint64(workers){t.Fatalf("reservation ledger=%d held=%d want %d",got,held,workers)}
 for _,reservation:=range all{reservation.Release()}
 app.spaceMu.Lock()
 defer app.spaceMu.Unlock()
 if _,ok:=app.spaceInFlight["disk-a"];ok{t.Fatal("all released reservations should clear the disk ledger")}
}

func TestProgressWriterReportsOnlyWrittenBytes(t *testing.T) {
 var out bytes.Buffer
 var reported int64
 writer:=&progressWriter{writer:&out,onWrite:func(n int64){reported+=n}}
 payload:=[]byte("transfer-data")
 n,err:=writer.Write(payload)
 if err!=nil{t.Fatal(err)}
 if n!=len(payload)||reported!=int64(n)||out.String()!=string(payload){
  t.Fatalf("write n=%d reported=%d output=%q",n,reported,out.String())
 }
}

func TestProgressWriterSyncsAtConfiguredInterval(t *testing.T) {
 var out bytes.Buffer
 syncs:=0
 writer:=&progressWriter{writer:&out,syncEvery:4,syncFn:func()error{syncs++;return nil}}
 if _,err:=writer.Write([]byte("abc"));err!=nil{t.Fatal(err)}
 if syncs!=0{t.Fatalf("sync count after 3 bytes=%d want 0",syncs)}
 if _,err:=writer.Write([]byte("de"));err!=nil{t.Fatal(err)}
 if syncs!=1{t.Fatalf("sync count after crossing interval=%d want 1",syncs)}
}
func TestSpaceReservationConsumeAndRelease(t *testing.T) {
 app:=&App{cfg:Config{MinFreeGB:0}}
 diskCfg:=DiskConfig{Name:"disk-a",Path:t.TempDir()}
 reservation,err:=app.reserveObjectSpace(diskCfg,100)
 if err!=nil{t.Fatalf("reserve: %v",err)}
 if app.spaceInFlight["disk-a"]!=100{t.Fatalf("reserved=%d want 100",app.spaceInFlight["disk-a"])}
 reservation.Consume(40)
 if app.spaceInFlight["disk-a"]!=60{t.Fatalf("after consume reserved=%d want 60",app.spaceInFlight["disk-a"])}
 reservation.Consume(100)
 if _,ok:=app.spaceInFlight["disk-a"];ok{t.Fatal("consuming beyond remaining bytes should clear reservation")}
 reservation.Release()
 reservation.Release()
 if _,ok:=app.spaceInFlight["disk-a"];ok{t.Fatal("release should be idempotent")}
}
func TestReserveObjectSpaceRejectsInsufficientCapacity(t *testing.T) {
 app:=&App{cfg:Config{MinFreeGB:0}}
 _,err:=app.reserveObjectSpace(DiskConfig{Name:"disk-a",Path:t.TempDir()},int64(^uint64(0)>>1))
 if err==nil{t.Fatal("expected reservation larger than available capacity to fail")}
}
func TestExistingObjectCompleteSkipsMatchingFile(t *testing.T) {
 dir:=t.TempDir()
 dst:=filepath.Join(dir,"camera.mp4")
 modified:=time.Date(2026,11,23,10,0,0,0,time.UTC)
 if err:=os.WriteFile(dst,[]byte("12345"),0600);err!=nil{t.Fatal(err)}
 if err:=os.Chtimes(dst,modified,modified);err!=nil{t.Fatal(err)}
 expected:=minio.ObjectInfo{Size:5,LastModified:modified}
 if !existingObjectComplete(dst,expected){t.Fatal("matching size and modification time should be treated as complete")}
 expected.Size=6
 if existingObjectComplete(dst,expected){t.Fatal("different size must not be treated as complete")}
 expected.Size=5
 expected.LastModified=modified.Add(5*time.Second)
 if existingObjectComplete(dst,expected){t.Fatal("stale modification time must not be treated as complete")}
 expected.LastModified=time.Time{}
 if !existingObjectComplete(dst,expected){t.Fatal("matching size with unavailable remote modification time should be treated as complete")}
}
func TestExistingPartialBytesIgnoresInvalidOrCompleteTempFiles(t *testing.T) {
 dir:=t.TempDir();dst:=filepath.Join(dir,"object.bin")
 if got:=existingPartialBytes(dst,100);got!=0{t.Fatalf("missing temp: got %d want 0",got)}
 if err:=os.WriteFile(dst+".minio-tmp",[]byte("12345"),0600);err!=nil{t.Fatal(err)}
 if got:=existingPartialBytes(dst,100);got!=5{t.Fatalf("partial temp: got %d want 5",got)}
 if err:=os.WriteFile(dst+".minio-tmp",make([]byte,100),0600);err!=nil{t.Fatal(err)}
 if got:=existingPartialBytes(dst,100);got!=100{t.Fatalf("complete temp should be retained as resumable: got %d want 100",got)}
 if got:=remainingObjectBytes(dst,100);got!=0{t.Fatalf("complete temp remaining: got %d want 0",got)}
}
func TestExistingPartialBytesRejectsSymlinkTemp(t *testing.T) {
 dir:=t.TempDir()
 target:=filepath.Join(dir,"outside.bin")
 if err:=os.WriteFile(target,[]byte("external-data"),0600);err!=nil{t.Fatal(err)}
 temp:=filepath.Join(dir,"object.bin.minio-tmp")
 if err:=os.Symlink(target,temp);err!=nil{t.Skipf("symlink unavailable: %v",err)}
 if got:=existingPartialBytes(filepath.Join(dir,"object.bin"),100);got!=0{t.Fatalf("symlink temp must not be treated as resumable: got %d",got)}
}

func TestRemainingObjectBytesAccountsForPartialFile(t *testing.T) {
 dir:=t.TempDir();dst:=filepath.Join(dir,"object.bin")
 if got:=remainingObjectBytes(dst,100);got!=100{t.Fatalf("missing partial: got %d want 100",got)}
 if err:=os.WriteFile(dst+".minio-tmp",[]byte("12345"),0600);err!=nil{t.Fatal(err)}
 if got:=remainingObjectBytes(dst,100);got!=95{t.Fatalf("partial resume: got %d want 95",got)}
 if err:=os.WriteFile(dst+".minio-tmp",make([]byte,100),0600);err!=nil{t.Fatal(err)}
 if got:=remainingObjectBytes(dst,100);got!=0{t.Fatalf("complete temp remaining: got %d want 0",got)}
}
func TestUniqueEpisodeBackupPathDoesNotCreatePlaceholder(t *testing.T) {
 parent:=t.TempDir()
 final:=filepath.Join(parent,"episode")
 candidate,err:=uniqueEpisodeBackupPath(final);if err!=nil{t.Fatal(err)}
 if _,err=os.Lstat(candidate);!os.IsNotExist(err){t.Fatalf("backup candidate must not exist before rename, stat err=%v",err)}
}
func TestRecoverEpisodeDirectoryIgnoresEmptyInterruptedBackup(t *testing.T) {
 parent:=t.TempDir();final:=filepath.Join(parent,"episode")
 emptyBackup:=filepath.Join(parent,".episode.minio-episode-backup-empty")
 if err:=os.Mkdir(emptyBackup,0755);err!=nil{t.Fatal(err)}
 if err:=recoverEpisodeDirectory(final);err!=nil{t.Fatal(err)}
 if _,err:=os.Stat(final);!os.IsNotExist(err){t.Fatalf("empty backup must not become a final episode, stat err=%v",err)}
 if _,err:=os.Stat(emptyBackup);!os.IsNotExist(err){t.Fatalf("empty interrupted backup should be removed, stat err=%v",err)}
}

func TestRecoverEpisodeDirectoryRestoresBackupAfterInterruptedPublish(t *testing.T) {
 parent:=t.TempDir();final:=filepath.Join(parent,"episode");partial:=filepath.Join(parent,".episode.partial")
 backup:=filepath.Join(parent,".episode.minio-episode-backup-crash")
 if err:=os.Mkdir(backup,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(backup,"old.txt"),[]byte("old-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=os.Mkdir(partial,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(partial,"new.txt"),[]byte("new-partial"),0600);err!=nil{t.Fatal(err)}
 if err:=recoverEpisodeDirectory(final);err!=nil{t.Fatal(err)}
 got,err:=os.ReadFile(filepath.Join(final,"old.txt"));if err!=nil{t.Fatal(err)}
 if string(got)!="old-complete"{t.Fatalf("restored old data=%q",got)}
 if _,err=os.Stat(filepath.Join(partial,"new.txt"));err!=nil{t.Fatalf("partial download should be retained: %v",err)}
 if _,err=os.Stat(backup);!os.IsNotExist(err){t.Fatalf("backup should be moved to final, stat err=%v",err)}
}
func TestRecoverEpisodeDirectoryCleansBackupAfterSuccessfulPublish(t *testing.T) {
 parent:=t.TempDir();final:=filepath.Join(parent,"episode")
 backup:=filepath.Join(parent,".episode.minio-episode-backup-crash")
 if err:=os.Mkdir(final,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(final,"new.txt"),[]byte("new"),0600);err!=nil{t.Fatal(err)}
 if err:=os.Mkdir(backup,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(backup,"old.txt"),[]byte("old"),0600);err!=nil{t.Fatal(err)}
 if err:=recoverEpisodeDirectory(final);err!=nil{t.Fatal(err)}
 if _,err:=os.Stat(filepath.Join(final,"new.txt"));err!=nil{t.Fatalf("published final was changed: %v",err)}
 if _,err:=os.Stat(backup);!os.IsNotExist(err){t.Fatalf("stale backup should be removed, stat err=%v",err)}
}
func TestRecoverEpisodeDirectoryRejectsSymlinkBackup(t *testing.T) {
 parent:=t.TempDir();final:=filepath.Join(parent,"episode");target:=filepath.Join(parent,"target")
 if err:=os.Mkdir(target,0755);err!=nil{t.Fatal(err)}
 backup:=filepath.Join(parent,".episode.minio-episode-backup-link")
 if err:=os.Symlink(target,backup);err!=nil{t.Skipf("symlink unavailable: %v",err)}
 if err:=recoverEpisodeDirectory(final);err==nil{t.Fatal("expected symlink backup to be rejected")}
 if _,err:=os.Stat(target);err!=nil{t.Fatalf("symlink target should remain untouched: %v",err)}
}

func TestReplaceEpisodeDirectoryForWindowsReplacesExistingDirectory(t *testing.T) {
 parent:=t.TempDir()
 final:=filepath.Join(parent,"episode")
 partial:=filepath.Join(parent,".episode.partial")
 if err:=os.MkdirAll(filepath.Join(final,"old"),0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(final,"old","old.txt"),[]byte("old"),0600);err!=nil{t.Fatal(err)}
 if err:=os.MkdirAll(filepath.Join(partial,"new"),0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(partial,"new","new.txt"),[]byte("new"),0600);err!=nil{t.Fatal(err)}
 if err:=replaceEpisodeDirectoryForOS(partial,final,true);err!=nil{t.Fatal(err)}
 if _,err:=os.Stat(filepath.Join(final,"new","new.txt"));err!=nil{t.Fatalf("new episode file missing: %v",err)}
 if _,err:=os.Stat(filepath.Join(final,"old"));!os.IsNotExist(err){t.Fatalf("old episode content remains, err=%v",err)}
 entries,err:=os.ReadDir(parent);if err!=nil{t.Fatal(err)}
 if len(entries)!=1||entries[0].Name()!="episode"{t.Fatalf("unexpected leftovers: %+v",entries)}
}
func TestReplaceEpisodeDirectoryRejectsInvalidSourceBeforeMovingFinal(t *testing.T) {
 parent:=t.TempDir();final:=filepath.Join(parent,"episode");partial:=filepath.Join(parent,"missing.partial")
 if err:=os.Mkdir(final,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(final,"keep.txt"),[]byte("keep"),0600);err!=nil{t.Fatal(err)}
 if err:=replaceEpisodeDirectoryForOS(partial,final,true);err==nil{t.Fatal("expected missing source error")}
 if _,err:=os.Stat(filepath.Join(final,"keep.txt"));err!=nil{t.Fatalf("existing final directory was changed: %v",err)}
}

func TestIsTransientWindowsFileError(t *testing.T) {
 for _,code:=range []syscall.Errno{syscall.Errno(32),syscall.Errno(33)}{
  err:=&os.PathError{Op:"rename",Path:"file",Err:code}
  if !isTransientWindowsFileError(err,true){t.Fatalf("Windows error %d should be retried",code)}
  if isTransientWindowsFileError(err,false){t.Fatalf("non-Windows error %d must not be retried",code)}
 }
 if isTransientWindowsFileError(errors.New("permanent failure"),true){t.Fatal("permanent errors must not be retried")}
}
func TestRetryFileOperationRetriesWindowsSharingViolation(t *testing.T) {
 calls:=0
 err:=retryFileOperation(func()error{
  calls++
  if calls==1{return &os.PathError{Op:"rename",Path:"file",Err:syscall.Errno(32)}}
  return nil
 },true)
 if err!=nil{t.Fatal(err)}
 if calls!=2{t.Fatalf("operation calls=%d, want 2",calls)}
}

func TestRecoverDownloadedFileRestoresBackupAfterInterruptedReplacement(t *testing.T) {
 root:=t.TempDir()
 dir:=filepath.Join(root,"nested")
 if err:=os.MkdirAll(dir,0755);err!=nil{t.Fatal(err)}
 dst:=filepath.Join(dir,"final.bin")
 backup:=filepath.Join(dir,".final.bin.minio-file-backup-crash")
 if err:=os.WriteFile(backup,[]byte("old-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=recoverDownloadedFile(dst,root);err!=nil{t.Fatal(err)}
 got,err:=os.ReadFile(dst);if err!=nil{t.Fatal(err)}
 if string(got)!="old-complete"{t.Fatalf("restored file=%q",got)}
 if _,err:=os.Stat(backup);!os.IsNotExist(err){t.Fatalf("backup should be moved to destination, err=%v",err)}
}
func TestRecoverDownloadedFileCleansBackupAfterSuccessfulReplacement(t *testing.T) {
 root:=t.TempDir()
 dst:=filepath.Join(root,"final.bin")
 backup:=filepath.Join(root,".final.bin.minio-file-backup-crash")
 if err:=os.WriteFile(dst,[]byte("new-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(backup,[]byte("old-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=recoverDownloadedFile(dst,root);err!=nil{t.Fatal(err)}
 got,err:=os.ReadFile(dst);if err!=nil{t.Fatal(err)}
 if string(got)!="new-complete"{t.Fatalf("published file changed: %q",got)}
 if _,err:=os.Stat(backup);!os.IsNotExist(err){t.Fatalf("stale backup should be removed, err=%v",err)}
}

func TestReplaceDownloadedFileForWindowsReplacesExistingFile(t *testing.T) {
 dir:=t.TempDir()
 dst:=filepath.Join(dir,"final.bin")
 src:=filepath.Join(dir,"final.bin.minio-tmp")
 if err:=os.WriteFile(dst,[]byte("old-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(src,[]byte("new-complete"),0600);err!=nil{t.Fatal(err)}
 if err:=replaceDownloadedFileForOS(src,dst,true);err!=nil{t.Fatal(err)}
 got,err:=os.ReadFile(dst);if err!=nil{t.Fatal(err)}
 if string(got)!="new-complete"{t.Fatalf("destination=%q",got)}
 if _,err=os.Stat(src);!os.IsNotExist(err){t.Fatalf("source should be moved, stat err=%v",err)}
 entries,err:=os.ReadDir(dir);if err!=nil{t.Fatal(err)}
 if len(entries)!=1||entries[0].Name()!="final.bin"{t.Fatalf("unexpected leftovers: %+v",entries)}
}
func TestReplaceDownloadedFileForWindowsCreatesNewFile(t *testing.T) {
 dir:=t.TempDir();dst:=filepath.Join(dir,"new.bin");src:=filepath.Join(dir,"new.tmp")
 if err:=os.WriteFile(src,[]byte("complete"),0600);err!=nil{t.Fatal(err)}
 if err:=replaceDownloadedFileForOS(src,dst,true);err!=nil{t.Fatal(err)}
 got,err:=os.ReadFile(dst);if err!=nil{t.Fatal(err)}
 if string(got)!="complete"{t.Fatalf("destination=%q",got)}
}

func TestSameObjectVersionChecksSizeETagAndLastModified(t *testing.T) {
 modified:=time.Date(2026,1,2,3,4,5,0,time.UTC)
 expected:=minio.ObjectInfo{Size:128,ETag:"abc123",LastModified:modified}
 if !sameObjectVersion(expected,expected){t.Fatal("identical object metadata should match")}
 changedSize:=expected;changedSize.Size++
 if sameObjectVersion(expected,changedSize){t.Fatal("different size must not match")}
 changedETag:=expected;changedETag.ETag="different"
 if sameObjectVersion(expected,changedETag){t.Fatal("different ETag must not match")}
 changedTime:=expected;changedTime.LastModified=modified.Add(time.Second)
 if sameObjectVersion(expected,changedTime){t.Fatal("different modification time must not match")}
 weakExpected:=minio.ObjectInfo{Size:128}
 if !sameObjectVersion(weakExpected,changedETag){t.Fatal("unspecified ETag and modification time should not be required")}
}

func TestResumeOffsetBacksOffOnlyOnFirstAttempt(t *testing.T) {
 if got:=resumeOffset(resumeBackoffBytes+200,resumeBackoffBytes+300,true);got!=200{t.Fatalf("backoff offset=%d want 200",got)}
 if got:=resumeOffset(resumeBackoffBytes+10,resumeBackoffBytes+300,true);got!=10{t.Fatalf("first-attempt offset=%d want 10",got)}
 if got:=resumeOffset(resumeBackoffBytes+10,resumeBackoffBytes+300,false);got!=resumeBackoffBytes+10{t.Fatalf("retry offset=%d",got)}
 if got:=resumeOffset(100,90,true);got!=0{t.Fatalf("oversized partial offset=%d want 0",got)}
 if got:=resumeOffset(10,100,true);got!=0{t.Fatalf("small partial offset=%d want 0",got)}
}
func TestResumeBackoffRestoresReleasedSpaceReservation(t *testing.T) {
 app:=&App{cfg:Config{MinFreeGB:0}}
 reservation,err:=app.reserveObjectSpace(DiskConfig{Name:"disk-a",Path:t.TempDir()},100)
 if err!=nil{t.Fatal(err)}
 reservation.Consume(100)
 if got:=app.spaceInFlight["disk-a"];got!=0{t.Fatalf("after consuming partial=%d want 0",got)}
 reservation.Restore(40)
 if got:=app.spaceInFlight["disk-a"];got!=40{t.Fatalf("backoff must re-reserve truncated bytes: got %d want 40",got)}
 reservation.Release()
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

func TestCompletedEpisodeDirDoesNotFollowSymlinkedRequiredFile(t *testing.T) {
 parent:=t.TempDir();dir:=filepath.Join(parent,"episode");outside:=filepath.Join(parent,"outside.json")
 if err:=os.Mkdir(dir,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(outside,[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 if err:=os.Symlink(outside,filepath.Join(dir,"task_info.json"));err!=nil{t.Skipf("symlink unavailable: %v",err)}
 if completedEpisodeDir(dir,false,""){t.Fatal("symlinked task_info.json must not validate the episode")}
}
func TestFindExistingTaskDiskHonorsRequiredMCAP(t *testing.T) {
 diskPath:=t.TempDir();task:=Task{Name:"task",Scene:"scene",Dir:"task"}
 disk:=DiskConfig{Name:"disk1",Path:diskPath}
 ep:=Episode{ID:"ep",ParentKey:"parent"}
 final,_:=episodePaths("",task,disk,ep.ParentKey)
 if err:=os.MkdirAll(final,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(final,"task_info.json"),[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 got,err:=findExistingTaskDisk("",task,[]DiskConfig{disk},[]Episode{ep},true,"*.mcap")
 if err!=nil{t.Fatal(err)}
 if got!=""{t.Fatalf("incomplete episode should not pin disk, got %q",got)}
 if err:=os.WriteFile(filepath.Join(final,"x.mcap"),[]byte("mcap"),0600);err!=nil{t.Fatal(err)}
 got,err=findExistingTaskDisk("",task,[]DiskConfig{disk},[]Episode{ep},true,"*.mcap")
 if err!=nil{t.Fatal(err)}
 if got!="disk1"{t.Fatalf("valid episode disk=%q",got)}
}
func TestReconcileInterruptedDownloadsPreservesPartialAndRecoversFinal(t *testing.T) {
 db,err:=sql.Open("sqlite",filepath.Join(t.TempDir(),"state.db"));if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 _,err=db.Exec("CREATE TABLE downloads(episode_id TEXT PRIMARY KEY,status TEXT,target_path TEXT,partial_path TEXT,error TEXT,completed_at TEXT,updated_at TEXT);")
 if err!=nil{t.Fatal(err)}
 root:=t.TempDir();complete:=filepath.Join(root,"complete");partial:=filepath.Join(root,"partial")
 if err=os.Mkdir(complete,0755);err!=nil{t.Fatal(err)}
 if err=os.WriteFile(filepath.Join(complete,"task_info.json"),[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 if err=os.Mkdir(partial,0755);err!=nil{t.Fatal(err)}
 if err=os.WriteFile(filepath.Join(partial,"part.bin"),[]byte("partial"),0600);err!=nil{t.Fatal(err)}
 _,err=db.Exec("INSERT INTO downloads(episode_id,status,target_path,partial_path) VALUES('done','running',?,'')",complete);if err!=nil{t.Fatal(err)}
 _,err=db.Exec("INSERT INTO downloads(episode_id,status,target_path,partial_path) VALUES('resume','running',?,?)",filepath.Join(root,"missing-final"),partial);if err!=nil{t.Fatal(err)}
 app:=&App{db:db,cfg:Config{TrustCompletedRecords:true}}
 if err=app.reconcileInterruptedDownloads();err!=nil{t.Fatal(err)}
 var status,partialPath string
 if err=db.QueryRow("SELECT status,COALESCE(partial_path,'') FROM downloads WHERE episode_id='done'").Scan(&status,&partialPath);err!=nil{t.Fatal(err)}
 if status!="completed"{t.Fatalf("valid final directory status=%q",status)}
 if err=db.QueryRow("SELECT status,partial_path FROM downloads WHERE episode_id='resume'").Scan(&status,&partialPath);err!=nil{t.Fatal(err)}
 if status!="pending"{t.Fatalf("interrupted partial status=%q, want pending",status)}
 if partialPath!=partial{t.Fatalf("partial path lost: got %q want %q",partialPath,partial)}
 if _,err=os.Stat(filepath.Join(partial,"part.bin"));err!=nil{t.Fatalf("partial file should be preserved: %v",err)}
 var message string
 if err=db.QueryRow("SELECT error FROM downloads WHERE episode_id='resume'").Scan(&message);err!=nil{t.Fatal(err)}
 if !strings.Contains(message,"续传"){t.Fatalf("recovery reason not recorded: %q",message)}
}
func TestReconcileInterruptedDownloadsIsIdempotent(t *testing.T) {
 db,err:=sql.Open("sqlite",filepath.Join(t.TempDir(),"state.db"));if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 _,err=db.Exec("CREATE TABLE downloads(episode_id TEXT PRIMARY KEY,status TEXT,target_path TEXT,partial_path TEXT,error TEXT,completed_at TEXT,updated_at TEXT);");if err!=nil{t.Fatal(err)}
 _,err=db.Exec("INSERT INTO downloads(episode_id,status,target_path,partial_path) VALUES('ep','running','/missing/final','/partial/keep')");if err!=nil{t.Fatal(err)}
 app:=&App{db:db,cfg:Config{}}
 if err=app.reconcileInterruptedDownloads();err!=nil{t.Fatal(err)}
 if err=app.reconcileInterruptedDownloads();err!=nil{t.Fatal(err)}
 var status,partial string
 if err=db.QueryRow("SELECT status,partial_path FROM downloads WHERE episode_id='ep'").Scan(&status,&partial);err!=nil{t.Fatal(err)}
 if status!="pending"||partial!="/partial/keep"{t.Fatalf("unexpected reconciled state status=%q partial=%q",status,partial)}
}
func TestCompletedEpisodeDirRequiresTaskInfo(t *testing.T) {
 dir:=t.TempDir()
 if completedEpisodeDir(dir,false,""){t.Fatal("empty directory must not count as completed")}
 if err:=os.WriteFile(filepath.Join(dir,"camera.mp4"),[]byte("video"),0600);err!=nil{t.Fatal(err)}
 if completedEpisodeDir(dir,false,""){t.Fatal("directory without task_info.json must not count as completed")}
 if err:=os.WriteFile(filepath.Join(dir,"task_info.json"),[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 if !completedEpisodeDir(dir,false,""){t.Fatal("directory with task_info.json should count as completed")}
}
func TestCompletedEpisodeDirRequiresMCAPWhenConfigured(t *testing.T) {
 dir:=t.TempDir()
 if err:=os.WriteFile(filepath.Join(dir,"task_info.json"),[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 if completedEpisodeDir(dir,true,"*.mcap"){t.Fatal("missing mcap must not count as completed")}
 if err:=os.WriteFile(filepath.Join(dir,"episode.mcap"),[]byte("mcap"),0600);err!=nil{t.Fatal(err)}
 if !completedEpisodeDir(dir,true,"*.mcap"){t.Fatal("task_info plus matching mcap should count as completed")}
}
func TestCompletedEpisodeDirRejectsSymlink(t *testing.T) {
 parent:=t.TempDir();target:=filepath.Join(parent,"target");link:=filepath.Join(parent,"link")
 if err:=os.Mkdir(target,0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(target,"task_info.json"),[]byte("{}"),0600);err!=nil{t.Fatal(err)}
 if err:=os.Symlink(target,link);err!=nil{t.Skipf("symlink unavailable: %v",err)}
 if completedEpisodeDir(link,false,""){t.Fatal("symlink directory must not count as completed")}
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
	loadDone := make(chan error, 1)
	go func() { loadDone <- loadConfigDB(&cfg, "count") }()
	select {
	case loadErr := <-loadDone:
		if loadErr != nil {
			t.Fatal(loadErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loadConfigDB blocked while importing Python SQLite configuration with one connection")
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

func TestLoadConfigDBRejectsDatabaseWithoutSchema(t *testing.T) {
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

func TestWebChildProcessStateConcurrentAccess(t *testing.T) {
 api := &WebAPI{}
 var wg sync.WaitGroup
 for worker := 0; worker < 4; worker++ {
  wg.Add(1)
  go func(id int) {
   defer wg.Done()
   for i := 0; i < 1000; i++ {
    api.setChildProcess(int32(id+i), float64(i))
    _, _ = api.childProcessSnapshot()
   }
  }(worker)
 }
 wg.Wait()
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
 db,err=sql.Open("sqlite",api.cfg.ConfigDB);if err!=nil{t.Fatal(err)};defer db.Close()
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

func TestDiskLockDirectoryUsesConfiguredLockLocation(t *testing.T) {
 c:=Config{LockFile:filepath.Join("shared-state","minio_download.lock")}
 got:=diskLockDirectory(c)
 want:=filepath.Join("shared-state","disk-locks")
 if got!=want{t.Fatalf("diskLockDirectory=%q want %q",got,want)}
}
func TestDiskLockDirectoryDefaultsWhenLockFileMissing(t *testing.T) {
 got:=diskLockDirectory(Config{})
 want:="disk-locks"
 if got!=want{t.Fatalf("diskLockDirectory=%q want %q",got,want)}
}

func TestDiskLockKeysCollapseDirectoriesOnSameFilesystem(t *testing.T) {
 root:=t.TempDir()
 first:=filepath.Join(root,"first")
 second:=filepath.Join(root,"second")
 if err:=os.MkdirAll(first,0755);err!=nil{t.Fatal(err)}
 if err:=os.MkdirAll(second,0755);err!=nil{t.Fatal(err)}
 keys,err:=diskLockKeys([]DiskConfig{{Name:"one",Path:first},{Name:"two",Path:second}})
 if err!=nil{t.Fatalf("diskLockKeys: %v",err)}
 if len(keys)!=1{t.Fatalf("same filesystem should produce one lock key, got %d: %v",len(keys),keys)}
}
func TestFilesystemIdentityDetectsDirectoriesOnSameFilesystem(t *testing.T) {
	root:=t.TempDir()
	first:=filepath.Join(root,"first")
	second:=filepath.Join(root,"second")
	if err:=os.MkdirAll(first,0755);err!=nil{t.Fatal(err)}
	if err:=os.MkdirAll(second,0755);err!=nil{t.Fatal(err)}
	a,err:=filesystemIdentity(first);if err!=nil{t.Fatal(err)}
	b,err:=filesystemIdentity(second);if err!=nil{t.Fatal(err)}
	if a==""||b=="" { t.Skip("platform uses partition-device identity instead of a stat device id") }
	if a!=b { t.Fatalf("directories on one filesystem returned different identities: %q != %q",a,b) }
}

func TestFilesystemIdentityFollowsSymlinkTarget(t *testing.T) {
	root:=t.TempDir()
	target:=filepath.Join(root,"target")
	link:=filepath.Join(root,"link")
	if err:=os.MkdirAll(target,0755);err!=nil{t.Fatal(err)}
	if err:=os.Symlink(target,link);err!=nil{t.Skipf("symlink unavailable: %v",err)}
	a,err:=filesystemIdentity(target);if err!=nil{t.Fatal(err)}
	b,err:=filesystemIdentity(link);if err!=nil{t.Fatal(err)}
	if a!=""&&a!=b { t.Fatalf("symlink should resolve to target filesystem: %q != %q",a,b) }
}

func TestMissingMCAPCleanupCanRecoverAfterInterruption(t *testing.T) {
 db, err := sql.Open("sqlite", ":memory:")
 if err != nil {
  t.Fatal(err)
 }
 defer db.Close()
 db.SetMaxOpenConns(1)
 for _, stmt := range []string{
  "CREATE TABLE downloads(episode_id TEXT PRIMARY KEY,target_path TEXT,status TEXT,error TEXT,updated_at TEXT)",
  "CREATE TABLE downloaded_episodes(episode_id TEXT PRIMARY KEY)",
 } {
  if _, err = db.Exec(stmt); err != nil {
   t.Fatal(err)
  }
 }
 cleanupRoot := t.TempDir()
 app := &App{db: db, cfg: Config{MCAPPattern: "*.mcap", Disks: []DiskConfig{{Name: "test", Path: cleanupRoot}}}}

 normalDir := filepath.Join(cleanupRoot, "normal-episode")
 if err = os.MkdirAll(normalDir, 0755); err != nil {
  t.Fatal(err)
 }
 if err = os.WriteFile(filepath.Join(normalDir, "task_info.json"), []byte("{}"), 0600); err != nil {
  t.Fatal(err)
 }
 if _, err = db.Exec("INSERT INTO downloads(episode_id,target_path,status) VALUES(?,?,?)", "normal", normalDir, "completed"); err != nil {
  t.Fatal(err)
 }
 if _, err = db.Exec("INSERT INTO downloaded_episodes(episode_id) VALUES(?)", "normal"); err != nil {
  t.Fatal(err)
 }
 count, err := app.cleanup(false)
 if err != nil {
  t.Fatalf("cleanup: %v", err)
 }
 if count != 1 {
  t.Fatalf("cleanup count=%d, want 1", count)
 }
 if _, err = os.Stat(normalDir); !os.IsNotExist(err) {
  t.Fatalf("cleanup directory still exists, err=%v", err)
 }
 var downloads, ledger int
 if err = db.QueryRow("SELECT COUNT(*) FROM downloads WHERE episode_id='normal'").Scan(&downloads); err != nil {
  t.Fatal(err)
 }
 if err = db.QueryRow("SELECT COUNT(*) FROM downloaded_episodes WHERE episode_id='normal'").Scan(&ledger); err != nil {
  t.Fatal(err)
 }
 if downloads != 0 || ledger != 0 {
  t.Fatalf("cleanup records remain: downloads=%d ledger=%d", downloads, ledger)
 }

 pendingDir := filepath.Join(cleanupRoot, "pending-episode")
 if err = os.MkdirAll(pendingDir, 0755); err != nil {
  t.Fatal(err)
 }
 if _, err = db.Exec("INSERT INTO downloads(episode_id,target_path,status) VALUES(?,?,?)", "pending", pendingDir, "cleanup_pending"); err != nil {
  t.Fatal(err)
 }
 if _, err = db.Exec("INSERT INTO downloaded_episodes(episode_id) VALUES(?)", "pending"); err != nil {
  t.Fatal(err)
 }
 previewCount, err := app.cleanup(true)
 if err != nil {
  t.Fatalf("preview pending cleanup: %v", err)
 }
 if previewCount != 1 {
  t.Fatalf("pending cleanup preview count=%d, want 1", previewCount)
 }
 if _, err = os.Stat(pendingDir); err != nil {
  t.Fatalf("dry-run unexpectedly removed pending cleanup directory: %v", err)
 }
 if err = app.recoverPendingCleanup(); err != nil {
  t.Fatalf("recover pending cleanup: %v", err)
 }
 if _, err = os.Stat(pendingDir); !os.IsNotExist(err) {
  t.Fatalf("recovered cleanup directory still exists, err=%v", err)
 }
 if err = db.QueryRow("SELECT COUNT(*) FROM downloads WHERE episode_id='pending'").Scan(&downloads); err != nil {
  t.Fatal(err)
 }
 if err = db.QueryRow("SELECT COUNT(*) FROM downloaded_episodes WHERE episode_id='pending'").Scan(&ledger); err != nil {
  t.Fatal(err)
 }
 if downloads != 0 || ledger != 0 {
  t.Fatalf("pending cleanup records remain: downloads=%d ledger=%d", downloads, ledger)
 }

 outsideDir := t.TempDir()
 if _, err = db.Exec("INSERT INTO downloads(episode_id,target_path,status) VALUES(?,?,?)", "outside", outsideDir, "cleanup_pending"); err != nil {
  t.Fatal(err)
 }
 if err = app.recoverPendingCleanup(); err == nil {
  t.Fatal("expected cleanup outside configured disks to be rejected")
 }
 if _, err = os.Stat(outsideDir); err != nil {
  t.Fatalf("outside directory was unexpectedly removed: %v", err)
 }
 if err = db.QueryRow("SELECT COUNT(*) FROM downloads WHERE episode_id='outside' AND status='cleanup_pending'").Scan(&downloads); err != nil {
  t.Fatal(err)
 }
 if downloads != 1 {
  t.Fatalf("unsafe cleanup state should remain pending, got %d rows", downloads)
 }
}


func TestFirstRunUsesSingleSQLiteDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "minio_download.sqlite3")
	if err := initializeSQLiteDatabase(dbPath); err != nil {
		t.Fatalf("initialize first-run database: %v", err)
	}
	cfg := Config{ConfigDB: dbPath, StateDB: dbPath}
	if err := loadConfigDB(&cfg, "count"); err != nil {
		t.Fatalf("load first-run defaults: %v", err)
	}
	if len(cfg.Tasks) == 0 {
		t.Fatal("expected built-in task defaults to be stored in SQLite")
	}
	if len(cfg.Disks) != 0 {
		t.Fatalf("new installation should wait for web disk configuration, got %d disks", len(cfg.Disks))
	}
	if cfg.Endpoint != "" || cfg.SecretKey != "" {
		t.Fatal("new installation must not invent MinIO credentials")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writer := &sqliteLogWriter{db: db}
	if _, err = writer.Write([]byte("first-run log entry\n")); err != nil {
		t.Fatal(err)
	}
	var savedLog string
	if err = db.QueryRow("SELECT message FROM runtime_logs ORDER BY id DESC LIMIT 1").Scan(&savedLog); err != nil || savedLog != "first-run log entry" {
		t.Fatalf("expected log line in SQLite, got %q, err=%v", savedLog, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "minio_download.sqlite3" {
		t.Fatalf("expected a single persistent SQLite file, found %v", entries)
	}
}

func TestSaveLastRunPersistsPerRunStatistics(t *testing.T) {
 dbPath:=filepath.Join(t.TempDir(),"run-history.sqlite3")
 if err:=initializeSQLiteDatabase(dbPath);err!=nil{t.Fatal(err)}
 db,err:=sql.Open("sqlite",dbPath);if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 started:=time.Now().UTC().Add(-2*time.Minute)
 startedAt:=started.Format(time.RFC3339Nano)
 completedAt:=time.Now().UTC().Format(time.RFC3339Nano)
 if _,err=db.Exec("INSERT INTO downloads(episode_id,task_name,expected_bytes,status,completed_at,updated_at) VALUES('ep-1','task',4096,'completed',?,?)",completedAt,completedAt);err!=nil{t.Fatal(err)}
 if _,err=db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,file_size,content_time,downloaded_at) VALUES('ep-1','task',4096,120,?)",completedAt);err!=nil{t.Fatal(err)}
 if _,err=db.Exec("INSERT INTO run_task_history(run_id,task_name,completed_count,completed_bytes,content_seconds) VALUES('test-run-1','task',1,4096,120)");err!=nil{t.Fatal(err)}
 app:=&App{db:db,mode:"duration",runID:"test-run-1",runStartedAt:started}
 app.saveLastRun("finished",nil)
 var result string;var count,bytes int64;var seconds,elapsed float64
 if err=db.QueryRow("SELECT result,completed_count,completed_bytes,content_seconds,duration_seconds FROM run_history WHERE run_id='test-run-1'").Scan(&result,&count,&bytes,&seconds,&elapsed);err!=nil{t.Fatal(err)}
 if result!="finished"||count!=1||bytes!=4096||seconds!=120||elapsed<=0{t.Fatalf("unexpected run history: result=%s count=%d bytes=%d seconds=%v elapsed=%v",result,count,bytes,seconds,elapsed)}
}

func TestRecordAndSetForRunAccumulatesTaskStats(t *testing.T) {
 dbPath:=filepath.Join(t.TempDir(),"task-run.sqlite3")
 if err:=initializeSQLiteDatabase(dbPath);err!=nil{t.Fatal(err)}
 db,err:=sql.Open("sqlite",dbPath);if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 app:=&App{db:db,runID:"run-1"}
 episode:=Episode{ID:"episode-1",Task:"scene-task",Size:8192,Duration:90,ParentKey:"parent"}
 if err:=app.recordAndSetForRun(episode,DiskConfig{Name:"disk-a"},"/tmp/task/episode-1");err!=nil{t.Fatal(err)}
 var count,bytes int64;var seconds float64
 if err=db.QueryRow("SELECT completed_count,completed_bytes,content_seconds FROM run_task_history WHERE run_id='run-1' AND task_name='scene-task'").Scan(&count,&bytes,&seconds);err!=nil{t.Fatal(err)}
 if count!=1||bytes!=8192||seconds!=90{t.Fatalf("unexpected task stats: count=%d bytes=%d seconds=%v",count,bytes,seconds)}
 if err=app.fail(Episode{ID:"episode-2",Task:"scene-task",Size:1024},DiskConfig{Name:"disk-a"},"","",errors.New("simulated failure"));err==nil{t.Fatal("expected simulated episode failure")}
 var failed int64
 if err=db.QueryRow("SELECT failed_count FROM run_task_history WHERE run_id='run-1' AND task_name='scene-task'").Scan(&failed);err!=nil||failed!=1{t.Fatalf("failed_count=%d err=%v",failed,err)}
 var status string
 if err=db.QueryRow("SELECT status FROM downloads WHERE episode_id='episode-1'").Scan(&status);err!=nil||status!="completed"{t.Fatalf("download status=%q err=%v",status,err)}
 var successDetail,errorDetail string
 if err=db.QueryRow("SELECT status FROM run_episode_history WHERE run_id='run-1' AND episode_id='episode-1'").Scan(&successDetail);err!=nil||successDetail!="completed"{t.Fatalf("success episode detail=%q err=%v",successDetail,err)}
 if err=db.QueryRow("SELECT status||':'||error FROM run_episode_history WHERE run_id='run-1' AND episode_id='episode-2'").Scan(&errorDetail);err!=nil||errorDetail!="failed:simulated failure"{t.Fatalf("failure episode detail=%q err=%v",errorDetail,err)}
 if err=app.fail(Episode{ID:"episode-3",Task:"scene-task"},DiskConfig{Name:"disk-a"},"","",context.Canceled);err!=nil{t.Fatalf("paused episode should not be an error: %v",err)}
 var pausedStatus string
 if err=db.QueryRow("SELECT status FROM run_episode_history WHERE run_id='run-1' AND episode_id='episode-3'").Scan(&pausedStatus);err!=nil||pausedStatus!="paused"{t.Fatalf("paused episode status=%q err=%v",pausedStatus,err)}
 api:=&WebAPI{db:db}
 req:=httptest.NewRequest("GET","/api/runs/run-1/episodes?task=scene-task&limit=10",nil);req.SetPathValue("id","run-1")
 response:=httptest.NewRecorder();api.runEpisodes(response,req)
 if response.Code!=http.StatusOK{t.Fatalf("run episodes API status=%d body=%s",response.Code,response.Body.String())}
 var payload struct{Total int `json:"total"`;Episodes []struct{ID string `json:"episode_id"`;Status string `json:"status"`} `json:"episodes"`}
 if err=json.Unmarshal(response.Body.Bytes(),&payload);err!=nil{t.Fatal(err)}
 if payload.Total!=2||len(payload.Episodes)!=2{t.Fatalf("unexpected episode page: total=%d episodes=%d",payload.Total,len(payload.Episodes))}
}
