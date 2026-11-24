package main

import (
 "context"
 "crypto/tls"
 "crypto/hmac"
 "crypto/rand"
 "crypto/sha256"
 "encoding/hex"
 "crypto/md5"
 "database/sql"
 "encoding/json"
 "encoding/csv"
 "os/exec"
 "errors"
 "flag"
 "fmt"
 "io"
 "log"
 "math"
 "net"
 "net/http"
 "net/url"
 "strconv"
 "os"
 "os/signal"
 "path"
 "path/filepath"
 "runtime"
 "sort"
 "strings"
 "syscall"
 "sync"
 "sync/atomic"
 "time"

 _ "github.com/jackc/pgx/v5/stdlib"
 "github.com/gofrs/flock"
 "github.com/minio/minio-go/v7"
 "github.com/minio/minio-go/v7/pkg/credentials"
 "github.com/shirou/gopsutil/v4/disk"
 "github.com/shirou/gopsutil/v4/process"
 _ "modernc.org/sqlite"
 _ "embed"
)

type Task struct { Name string `json:"name"`; Scene string `json:"scene"`; Dir string `json:"dir"`; Aliases []string `json:"aliases"`; Tags []string `json:"tags"`; TargetHours float64 `json:"target_hours"`; TargetCount int `json:"target_count"` }
type DiskConfig struct { Name string `json:"name"`; Path string `json:"path"`; Mountpoint string `json:"mountpoint"` }
type Config struct {
 fromConfigDB bool
 Mode string `json:"mode"`
 Endpoint string `json:"minio_endpoint"`; AccessKey string `json:"minio_access_key"`; SecretKey string `json:"minio_secret_key"`; Bucket string `json:"minio_bucket"`; Region string `json:"minio_region"`; SSL bool `json:"minio_use_ssl"`; MinioSkipVerify bool `json:"minio_skip_verify"`
 PGHost string `json:"pg_host"`; PGPort int `json:"pg_port"`; PGDB string `json:"pg_database"`; PGUser string `json:"pg_user"`; PGPassword string `json:"pg_password"`
 DataVersion string `json:"data_version"`; ConfigDB string `json:"config_db"`; LockFile string `json:"lock_file"`; PIDFile string `json:"pid_file"`; StateDB string `json:"state_db"`; LogDir string `json:"log_dir"`; MinFreeGB float64 `json:"min_free_gb"`; SafetyFactor float64 `json:"space_safety_factor"`
 Workers int `json:"max_writes_per_disk"`; FileWorkers int `json:"file_workers"`; FileRetries int `json:"file_retries"`; RetryDelaySeconds float64 `json:"retry_delay_seconds"`; ValidateWorkers int `json:"validate_workers"`; DefaultRounds int `json:"default_rounds"`; ConnectTimeout int `json:"connect_timeout"`; ReadTimeout int `json:"read_timeout"`; WebHost string `json:"web_host"`; WebPort int `json:"web_port"`; RequireMCAP bool `json:"require_mcap"`; MCAPPattern string `json:"mcap_pattern"`; IncludeNull bool `json:"include_null_quality"`; VerifyMD5 *bool `json:"verify_md5"`; Fsync *bool `json:"fsync"`; TrustCompletedRecords bool `json:"trust_completed_records"`
 Disks []DiskConfig `json:"disks"`; Tasks []Task `json:"tasks"`
}
type Episode struct { ID string; JobID sql.NullInt64; Task,SourceTask,SN,ParentKey string; Size int64; Duration float64; Created sql.NullTime }
type App struct { cfg Config; mc *minio.Client; db *sql.DB; ctx context.Context; cancel context.CancelFunc; mode string; recheck bool; validated map[string]bool; spaceMu sync.Mutex; spaceInFlight map[string]uint64; processCreated float64; runID string; runStartedAt time.Time }
//go:embed config.count.example.json
var defaultCountConfigJSON []byte

var errRemoteObjectChanged = errors.New("远端对象元数据已变化")

var configPath=flag.String("config","","兼容旧版 JSON 配置；新方式无需此参数")
var dbPath=flag.String("db","minio_download.sqlite3","SQLite 数据库路径；配置、日志和下载记录统一保存在此文件")
var workerFlag=flag.Bool("worker",false,"内部参数：启动下载工作进程")
var mode=flag.String("mode","","duration 或 count；未指定时默认 count")
var rounds=flag.Int("max-rounds",0,"缺少必需文件后的补选轮数；默认使用配置项 run.default_rounds")
var recheck=flag.Bool("recheck-missing",false,"重新检查缺文件数据")
var cleanup=flag.Bool("cleanup-missing-mcap",false,"清理缺少 mcap 的已下载目录")
var dry=flag.Bool("dry-run",false,"只预览清理操作")
var stopFlag=flag.Bool("stop",false,"请求正在运行的下载器安全暂停")
var webFlag=flag.Bool("web",false,"启动 Web API 服务")
var webHost=flag.String("web-host","","覆盖配置文件中的 Web API 监听地址")
var webPort=flag.Int("web-port",0,"覆盖配置文件中的 Web API 监听端口")
var preserveStopRequest=flag.Bool("preserve-stop-request",false,"内部参数：保留 Web 启动后写入的暂停请求")

func main(){
 flag.Parse()
 if *rounds<0{log.Fatal("--max-rounds 不能为负数")}
 dbName:=strings.TrimSpace(*dbPath)
 if dbName==""{log.Fatal("--db 不能为空")}
 dbName,err:=filepath.Abs(dbName);if err!=nil{log.Fatal(err)}
 var c Config
 legacyJSON:=strings.TrimSpace(*configPath)!=""
 if legacyJSON{
  raw,e:=os.ReadFile(*configPath);if e!=nil{log.Fatalf("读取兼容配置失败: %v",e)}
  if e=json.Unmarshal(raw,&c);e!=nil{log.Fatalf("配置 JSON 无效: %v",e)}
  if c.StateDB==""{c.StateDB=dbName}
  if c.ConfigDB!=""{c.ConfigDB,e=filepath.Abs(c.ConfigDB);if e!=nil{log.Fatal(e)}}
 }else{
  c=Config{Mode:"count",ConfigDB:dbName,StateDB:dbName,WebHost:"127.0.0.1",WebPort:8765,DataVersion:"银河2.2数据",MinFreeGB:3,SafetyFactor:1.15,Workers:2,FileWorkers:4,FileRetries:5,RetryDelaySeconds:1,ValidateWorkers:16,DefaultRounds:1,ConnectTimeout:10,ReadTimeout:120,RequireMCAP:true,MCAPPattern:"*.SYNC.mcap"}
 }
 if c.StateDB==""{c.StateDB=dbName}
 c.StateDB,err=filepath.Abs(c.StateDB);if err!=nil{log.Fatal(err)}
 if !legacyJSON||*webFlag||*workerFlag{
  if c.ConfigDB==""{c.ConfigDB=c.StateDB}
  c.ConfigDB,err=filepath.Abs(c.ConfigDB);if err!=nil{log.Fatal(err)}
 }
 if *stopFlag{if err=requestStop(c);err!=nil{log.Fatal(err)};log.Print("已写入 SQLite 安全暂停请求");return}
 if err=initializeSQLiteDatabase(c.StateDB);err!=nil{log.Fatalf("初始化 SQLite 数据库失败: %v",err)}
 if c.ConfigDB!=""&&c.ConfigDB!=c.StateDB{
  if err=initializeSQLiteDatabase(c.ConfigDB);err!=nil{log.Fatalf("初始化配置 SQLite 失败: %v",err)}
 }
 logDB,err:=sql.Open("sqlite",c.StateDB);if err!=nil{log.Fatal(err)}
 logDB.SetMaxOpenConns(1)
 defer logDB.Close()
 log.SetOutput(&sqliteLogWriter{db:logDB,out:os.Stdout})
 log.SetFlags(log.Ldate|log.Ltime|log.Lmicroseconds)
 isWorker:=*workerFlag||(legacyJSON&&!*webFlag)
 if !isWorker{
  if *mode==""{*mode="count"}
  if err=loadConfigDB(&c,*mode);err!=nil{log.Fatalf("读取 SQLite 配置失败: %v",err)}
  c.Mode=*mode
  host:=c.WebHost;port:=c.WebPort
  if *webHost!=""{host=*webHost};if *webPort>0{port=*webPort}
  if err=serveWeb(c,host,port);err!=nil{log.Fatal(err)}
  return
 }
 if *mode==""{if c.Mode!=""{*mode=c.Mode}else{*mode="count"}}
 if *mode!="duration"&&*mode!="count"{log.Fatal("--mode 只能是 duration 或 count")}
 if err=loadConfigDB(&c,*mode);err!=nil{log.Fatalf("读取 SQLite 配置失败: %v",err)}
 if err=c.normalize(*mode);err!=nil{log.Fatal(err)}
 if *cleanup&&!c.RequireMCAP{log.Fatal("require_mcap 已关闭，不能清理缺少 mcap 的目录；请先在网页配置中启用 require_mcap")}
 effectiveRounds:=c.DefaultRounds;if *rounds>0{effectiveRounds=*rounds};if effectiveRounds<1||effectiveRounds>50{log.Fatal("--max-rounds / run.default_rounds 必须在 1 到 50 之间")}
 baseCtx,stop:=signal.NotifyContext(context.Background(),os.Interrupt);defer stop()
 ctx,cancel:=context.WithCancel(baseCtx);defer cancel()
 a:=&App{cfg:c,ctx:ctx,cancel:cancel,mode:*mode,recheck:*recheck,validated:map[string]bool{},spaceInFlight:map[string]uint64{}}
 if !*preserveStopRequest{if err=clearStopRequestDB(c.StateDB);err!=nil{log.Fatalf("清理旧暂停请求失败: %v",err)}}
 if err=a.open();err!=nil{log.Fatal(err)};defer a.db.Close()
 if err=a.registerProcess();err!=nil{log.Fatal(err)};defer a.unregisterProcess()
 watcherDone:=a.startStopWatcher();defer func(){cancel();<-watcherDone}()
 if err=validateDiskSet(a.cfg.Disks);err!=nil{log.Fatal(err)}
 if *dry{
  if !*cleanup{log.Fatal("--dry-run 必须与 --cleanup-missing-mcap 一起使用")}
  n,e:=a.cleanup(true);if e!=nil{log.Fatal(e)};log.Printf("清理预览结束：%d 条",n);return
 }
 a.runStartedAt=time.Now().UTC();a.runID=strconv.FormatInt(a.runStartedAt.UnixNano(),10)
 runStartText:=a.runStartedAt.Format(time.RFC3339Nano)
 if _,err=a.db.Exec("UPDATE run_history SET result='interrupted',finished_at=?,duration_seconds=MAX(0,(julianday(?) - julianday(started_at))*86400),error='下载进程未正常结束' WHERE result='running'",runStartText,runStartText);err!=nil{log.Fatalf("恢复异常中断的运行历史失败: %v",err)}
 if _,err=a.db.Exec("INSERT INTO run_history(run_id,started_at,finished_at,duration_seconds,mode,result) VALUES(?,?,?,?,?,?)",a.runID,a.runStartedAt.Format(time.RFC3339Nano),a.runStartedAt.Format(time.RFC3339Nano),0,a.mode,"running");err!=nil{log.Fatalf("创建运行历史失败: %v",err)}
 if err=a.recoverPendingCleanup();err!=nil{log.Fatalf("恢复上次未完成的缺失 mcap 清理失败: %v",err)}
 if *cleanup{n,e:=a.cleanup(false);if e!=nil{log.Fatal(e)};log.Printf("清理结束：%d 条",n)}
 rows,err:=a.fetch();if err!=nil{if ctx.Err()!=nil{log.Print("已安全暂停（候选查询期间收到暂停请求）");a.saveLastRun("paused",nil);return};log.Printf("候选查询失败: %v",err);a.saveLastRun("error",err);return}
 if err=a.run(rows,effectiveRounds);err!=nil{if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil);return};log.Printf("下载执行失败: %v",err);a.saveLastRun("error",err);return}
 if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil)}else{log.Printf("执行结束 Go=%s/%s",runtime.GOOS,runtime.GOARCH);a.saveLastRun("finished",nil)}
}

type sqliteLogWriter struct{db *sql.DB;out io.Writer;mu sync.Mutex}
func(w *sqliteLogWriter)Write(p []byte)(int,error){
 if w==nil{return len(p),nil}
 w.mu.Lock();defer w.mu.Unlock()
 if w.db!=nil{
  lines:=strings.Split(strings.TrimRight(string(p),"\r\n"),"\n")
  for _,line:=range lines{if line==""{continue};_,_=w.db.Exec("INSERT INTO runtime_logs(created_at,message) VALUES(?,?)",controlTimestamp(),line)}
 }
 if w.out!=nil{_,_=w.out.Write(p)}
 return len(p),nil
}
func initializeSQLiteDatabase(name string)error{
 if strings.TrimSpace(name)==""{return errors.New("SQLite 路径不能为空")}
 if err:=os.MkdirAll(filepath.Dir(name),0755);err!=nil{return err}
 db,err:=sql.Open("sqlite",name);if err!=nil{return err};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA busy_timeout=30000; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;"+
  "CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS scenes(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL,name TEXT NOT NULL UNIQUE);"+
  "CREATE TABLE IF NOT EXISTS tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,scene_id INTEGER NOT NULL,position INTEGER NOT NULL,name TEXT NOT NULL,target_count INTEGER NOT NULL DEFAULT 0,target_hours REAL NOT NULL DEFAULT 0,directory TEXT NOT NULL DEFAULT '',tags TEXT NOT NULL DEFAULT '[]');"+
  "CREATE TABLE IF NOT EXISTS task_aliases(task_id INTEGER NOT NULL,position INTEGER NOT NULL,alias TEXT NOT NULL,PRIMARY KEY(task_id,position));"+
  "CREATE TABLE IF NOT EXISTS disks(name TEXT PRIMARY KEY,position INTEGER NOT NULL,path TEXT NOT NULL,mountpoint TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS plans(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL UNIQUE,note TEXT NOT NULL DEFAULT '',payload TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS downloads(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,parent_key TEXT,disk_name TEXT,target_path TEXT,partial_path TEXT,expected_bytes INTEGER,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TEXT NOT NULL,started_at TEXT,completed_at TEXT);"+
  "CREATE TABLE IF NOT EXISTS task_disk_assignments(task_name TEXT NOT NULL,disk_name TEXT NOT NULL,last_used_at TEXT NOT NULL,PRIMARY KEY(task_name,disk_name));"+
  "CREATE TABLE IF NOT EXISTS downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,source_task_name TEXT,job_id INTEGER,sn_code TEXT,parent_key TEXT,disk_name TEXT,target_path TEXT,file_size INTEGER,content_time REAL,create_time TEXT,downloaded_at TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS runtime_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT NOT NULL,message TEXT NOT NULL);"+
  "CREATE TABLE IF NOT EXISTS run_history(run_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,finished_at TEXT NOT NULL,duration_seconds REAL NOT NULL,mode TEXT NOT NULL,result TEXT NOT NULL,completed_count INTEGER NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,error TEXT);"+
  "CREATE TABLE IF NOT EXISTS run_task_history(run_id TEXT NOT NULL,task_name TEXT NOT NULL,completed_count INTEGER NOT NULL DEFAULT 0,failed_count INTEGER NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,wait_seconds REAL NOT NULL DEFAULT 0,duration_seconds REAL NOT NULL DEFAULT 0,PRIMARY KEY(run_id,task_name));CREATE TABLE IF NOT EXISTS run_disk_history(run_id TEXT NOT NULL,disk_name TEXT NOT NULL,duration_seconds REAL NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,disk_name));"+
  "CREATE TABLE IF NOT EXISTS run_episode_history(run_id TEXT NOT NULL,episode_id TEXT NOT NULL,task_name TEXT NOT NULL,status TEXT NOT NULL,error TEXT,disk_name TEXT,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,updated_at TEXT NOT NULL,failure_category TEXT NOT NULL DEFAULT '',PRIMARY KEY(run_id,episode_id));"+
  "CREATE INDEX IF NOT EXISTS idx_run_episode_lookup ON run_episode_history(run_id,task_name,status,updated_at);"+
  "CREATE INDEX IF NOT EXISTS idx_run_history_started ON run_history(started_at);"+
  "CREATE TABLE IF NOT EXISTS process_state(id INTEGER PRIMARY KEY CHECK(id=1),pid INTEGER NOT NULL,created REAL NOT NULL,mode TEXT NOT NULL,updated_at TEXT NOT NULL);"+
  "CREATE INDEX IF NOT EXISTS idx_download_task_status ON downloads(task_name,status);"+
  "CREATE INDEX IF NOT EXISTS idx_downloaded_task ON downloaded_episodes(task_name);"+
  "CREATE INDEX IF NOT EXISTS idx_runtime_logs_created ON runtime_logs(created_at);");err!=nil{return err}
 for _,item:=range webConfigSchema(){
  key,ok:=item["key"].(string);if !ok{continue};value:=item["default"];if value==nil{continue}
  encoded,e:=json.Marshal(value);if e!=nil{return e}
  if _,e=db.Exec("INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",key,string(encoded),controlTimestamp());e!=nil{return e}
 }
 var taskCount int
 if err=db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&taskCount);err!=nil{return err}
 if taskCount==0{
  var defaults Config
  if err=json.Unmarshal(defaultCountConfigJSON,&defaults);err!=nil{return err}
  payload:=currentPlanPayload(defaults)
  if err=persistPlanPayload(db,payload);err!=nil{return fmt.Errorf("写入内置默认任务失败: %w",err)}
 }
 return nil
}
func processIdentity(pid int,created float64)bool{
 if pid<=0{return false}
 proc,err:=process.NewProcess(int32(pid));if err!=nil{return false}
 alive,err:=proc.IsRunning();if err!=nil||!alive{return false}
 if created>0{ms,err:=proc.CreateTime();if err!=nil||math.Abs(float64(ms)/1000-created)>=2{return false}}
 return true
}


const webUIHTML = `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MinIO 下载器</title>
<style>
:root{
 color-scheme:light;
 --bg:#f3f6fb;--surface:#fff;--surface-soft:#f8faff;--ink:#182338;--muted:#68758a;
 --line:#e2e8f0;--line-strong:#cbd5e1;--primary:#2457d6;--primary-hover:#1947b8;
 --success:#168451;--danger:#c03932;--warning:#a16207;--radius:14px;
 font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;
 color:var(--ink);background:var(--bg);font-size:14px;font-synthesis:none;
}
*{box-sizing:border-box}
body{margin:0;min-width:320px;background:linear-gradient(180deg,#edf3ff 0,#f5f7fb 240px,var(--bg) 100%);line-height:1.55}
button,input,select,textarea{font:inherit}
button{transition:background .16s,border-color .16s,box-shadow .16s,transform .16s}
.top{
 position:sticky;top:0;z-index:20;min-height:76px;padding:14px max(22px,calc((100vw - 1240px)/2));
 display:flex;justify-content:space-between;align-items:center;gap:20px;
 color:#fff;background:rgba(20,34,59,.96);border-bottom:1px solid rgba(255,255,255,.1);
 box-shadow:0 5px 22px rgba(20,34,59,.13);backdrop-filter:blur(12px)
}
.top h1{font-size:20px;letter-spacing:.2px;margin:0;font-weight:700}
.top small{display:block;margin-top:3px;color:#b9c7dc;font-size:12px}
#connection-state{padding:6px 11px;border:1px solid rgba(255,255,255,.2);border-radius:999px;color:#dce8f8;font-size:12px;white-space:nowrap}
.wrap{width:min(1240px,100%);margin:26px auto 56px;padding:0 22px}
.nav{
 position:sticky;top:76px;z-index:15;display:flex;gap:6px;overflow-x:auto;
 padding:8px;margin:0 0 20px;background:rgba(255,255,255,.9);
 border:1px solid var(--line);border-radius:13px;box-shadow:0 5px 18px rgba(26,43,75,.05);
 backdrop-filter:blur(10px);scrollbar-width:thin
}
.nav button{
 flex:0 0 auto;border:0;border-radius:9px;padding:10px 16px;background:transparent;
 color:#526078;font-weight:600;cursor:pointer
}
.nav button:hover{background:#eef3ff;color:var(--primary)}
.nav button.active{background:#e9efff;color:var(--primary);box-shadow:inset 0 0 0 1px #d8e3ff}
.panel{display:none;animation:panel-in .18s ease-out}
.panel.active{display:block}
@keyframes panel-in{from{opacity:.65;transform:translateY(4px)}to{opacity:1;transform:none}}
.card{
 min-width:0;padding:22px;margin-bottom:18px;background:var(--surface);
 border:1px solid var(--line);border-radius:var(--radius);
 box-shadow:0 5px 18px rgba(26,43,75,.045)
}
.card h2{display:flex;align-items:center;gap:9px;margin:0 0 16px;font-size:16px;font-weight:700;letter-spacing:.1px}
.card h2:before{content:"";width:4px;height:17px;border-radius:4px;background:var(--primary)}
.card h3{margin:0 0 12px;font-size:14px}
.card p{margin:0 0 14px}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
.field{min-width:0;margin:0 0 14px}
.field label{display:block;margin:0 0 7px;color:#35435a;font-size:13px;font-weight:650}
.field input,.field select,.field textarea,textarea{
 width:100%;min-width:0;min-height:40px;padding:9px 11px;color:var(--ink);
 background:#fff;border:1px solid var(--line-strong);border-radius:9px;outline:none;
 transition:border-color .15s,box-shadow .15s
}
.field input:focus,.field select:focus,.field textarea:focus,textarea:focus{
 border-color:#7196f2;box-shadow:0 0 0 3px rgba(36,87,214,.11)
}
.field input[type=checkbox]{width:auto;min-height:0;accent-color:var(--primary)}
.field small,.hint{display:block;margin-top:6px;color:var(--muted);font-size:12px;line-height:1.6}
.actions{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-top:16px}
.btn{
 display:inline-flex;align-items:center;justify-content:center;min-height:38px;
 padding:8px 14px;border:1px solid var(--line-strong);border-radius:9px;
 background:#fff;color:#344258;font-weight:600;cursor:pointer
}
.btn:hover{border-color:#9aabc3;background:#f7f9fc}
.btn:active{transform:translateY(1px)}
.btn.primary{background:var(--primary);border-color:var(--primary);color:#fff;box-shadow:0 3px 8px rgba(36,87,214,.16)}
.btn.primary:hover{background:var(--primary-hover);border-color:var(--primary-hover)}
.btn.danger{color:var(--danger);border-color:#f0c9c6;background:#fff}
.btn.danger:hover{background:#fff4f3;border-color:#e5a6a1}
.btn:disabled{opacity:.5;cursor:not-allowed}
.notice{padding:12px 14px;margin:12px 0;border:1px solid #e1e8f5;border-radius:10px;background:#f5f8ff;color:#465873}
.ok{color:var(--success)}.bad{color:var(--danger)}
.status{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:18px 0}
.metric{min-width:0;padding:15px 16px;background:linear-gradient(145deg,#f8faff,#f2f6fd);border:1px solid #e5ebf5;border-radius:11px;color:#65738a}
.metric b{display:block;margin-top:5px;color:#1c2c47;font-size:24px;font-weight:750;line-height:1.25;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
.transfer-panel{padding:15px 16px;margin:2px 0 16px;border:1px solid #e4eaf4;border-radius:12px;background:#fbfcff}
.transfer-head,.transfer-foot,.section-heading{display:flex;align-items:center;justify-content:space-between;gap:12px}
.transfer-head strong{font-size:13px;color:#33445f}.transfer-head span,.transfer-foot{font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}
.progress-track{height:10px;margin:12px 0 9px;border-radius:99px;background:#e8edf5;overflow:hidden}
.progress-fill{height:100%;border-radius:99px;background:linear-gradient(90deg,#3974e8,#64a4ff);transition:width .25s ease}
.transfer-foot{flex-wrap:wrap}
.section-heading h2{margin:0}.section-heading .hint{margin:5px 0 0}
.summary-chip{flex:0 0 auto;padding:5px 9px;border-radius:99px;background:#f0f4fb;color:#52627a;font-size:11px;font-weight:650}
.disk-monitor-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.disk-monitor{min-width:0;padding:15px;border:1px solid var(--line);border-radius:11px;background:#fbfcff}
.disk-monitor-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.disk-monitor-name{font-weight:700;color:#293a54;overflow-wrap:anywhere}
.disk-monitor-state{font-size:11px;font-weight:650;white-space:nowrap}
.disk-monitor-state.good{color:var(--success)}.disk-monitor-state.bad{color:var(--danger)}
.disk-monitor-path{margin-top:5px;color:var(--muted);font-size:11px;overflow-wrap:anywhere}
.disk-monitor-track{height:8px;margin:12px 0 8px;border-radius:99px;background:#e9edf4;overflow:hidden}
.disk-monitor-fill{height:100%;border-radius:99px;background:#6b8ed8}
.disk-monitor-fill.warn{background:#d99a3d}.disk-monitor-fill.critical{background:#cf625b}
.disk-monitor-stats{display:flex;justify-content:space-between;gap:8px;color:#65738a;font-size:11px;font-variant-numeric:tabular-nums}
.active-list{display:grid;gap:10px}
.active-item{display:block}
.active-item-top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.active-item-progress{height:6px;margin:9px 0 7px;border-radius:99px;background:#e9edf4;overflow:hidden}
.active-item-progress>span{display:block;height:100%;border-radius:99px;background:#5b8ce6}
.active-item-bottom{display:flex;justify-content:space-between;gap:10px;color:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.run-state-chip{border-color:#c8d6f4;background:#edf3ff;color:#315aab}
.run-state-chip.pending{border-color:#f0d7a9;background:#fff8e9;color:#95651b}
.history-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-top:12px}.history-card{border:1px solid var(--line);border-radius:10px;padding:12px}.history-card b{display:block;margin-bottom:5px}.history-card small{color:var(--muted)}.history-table{width:100%;border-collapse:collapse;margin-top:12px;font-size:12px}.history-table th,.history-table td{padding:8px;border-bottom:1px solid var(--line);text-align:left}.failure-list{display:grid;gap:6px;margin-top:12px}.failure-item{padding:9px 11px;border-radius:8px;background:#fff5f3;color:#8f3d35;font-size:12px;overflow-wrap:anywhere}

.active-item{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:11px 13px;border:1px solid var(--line);border-radius:9px;background:#fbfcff}
.active-item-main{min-width:0}.active-item-title{font-size:12px;font-weight:650;color:#34445d;overflow-wrap:anywhere}
.active-item-meta{margin-top:4px;color:var(--muted);font-size:11px}
.active-item-size{flex:0 0 auto;color:#52627a;font-size:12px;font-variant-numeric:tabular-nums}
@media(max-width:720px){.disk-monitor-grid{grid-template-columns:1fr}.section-heading{align-items:flex-start}}
.disk-row{padding:17px;margin:12px 0;background:#fbfcff;border:1px solid var(--line);border-radius:12px}
.disk-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.disk-actions{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-top:12px}
.editor{min-height:300px;font:12px/1.65 ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre;tab-size:2;background:#fbfcff!important}
.logs{height:440px;overflow:auto;padding:16px;border:1px solid #26364f;border-radius:11px;background:#111c2d;color:#d5e1f2;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.65 ui-monospace,SFMono-Regular,Consolas,monospace}
.tag{display:inline-flex;vertical-align:middle;align-items:center;padding:3px 9px;margin-left:7px;border:1px solid rgba(255,255,255,.18);border-radius:999px;background:rgba(255,255,255,.1);color:#d9e6ff;font-size:11px;font-weight:600}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px}
th{position:sticky;top:0;background:#f5f7fb;color:#536178;font-weight:700}
td,th{text-align:left;padding:11px 12px;border-bottom:1px solid #e8edf4}
tr:last-child td{border-bottom:0}
tbody tr:hover{background:#f8faff}
code{padding:2px 5px;border-radius:5px;background:#eef2f8;color:#33445f;font-size:.92em}
input[type=file]{max-width:260px;color:var(--muted);font-size:12px}
#task-status,#disk-status,#plan-list{overflow-x:auto}
#download-mode{font-weight:600}
@media(max-width:900px){
 .status{grid-template-columns:repeat(2,minmax(0,1fr))}
 .disk-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
}
@media(max-width:600px){
 .top{position:relative;min-height:68px;padding:13px 15px;align-items:flex-start}
 .top h1{font-size:18px}.top small{max-width:230px}
 #connection-state{font-size:11px;padding:5px 8px}
 .wrap{margin:15px auto 32px;padding:0 12px}
 .nav{position:sticky;top:0;margin-bottom:14px;padding:6px;gap:3px}
 .nav button{padding:9px 12px;font-size:12px}
 .card{padding:16px;margin-bottom:13px;border-radius:12px}
 .grid,.disk-grid{grid-template-columns:minmax(0,1fr);gap:8px}
 .status{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
 .metric{padding:12px}.metric b{font-size:20px}
 .actions{gap:7px}.btn{padding:8px 11px;font-size:12px}
 td,th{padding:9px 8px;white-space:nowrap}
 .editor{min-height:240px}
}
.task-toolbar{display:flex;justify-content:space-between;align-items:center;gap:14px;margin:18px 0 12px;padding-bottom:12px;border-bottom:1px solid var(--line)}
.task-toolbar strong{font-size:15px}.task-editor{display:grid;gap:14px}
.task-scene{min-width:0;border:1px solid var(--line);border-radius:12px;background:#fff;overflow:hidden}
.task-scene-head{display:flex;align-items:center;gap:10px;padding:12px 14px;background:#f6f8fc;border-bottom:1px solid var(--line)}
.task-scene-head label{flex:0 0 auto;color:#536178;font-size:12px;font-weight:700}
.task-scene-head input{flex:1;min-width:0;max-width:420px;padding:8px 10px;border:1px solid var(--line-strong);border-radius:8px;background:#fff}
.task-scene-head .scene-count{margin-left:auto;color:var(--muted);font-size:12px;white-space:nowrap}
.task-table-wrap{overflow-x:auto}
.task-table{min-width:900px}
.task-table input{width:100%;min-width:80px;padding:7px 8px;border:1px solid #dbe2ec;border-radius:7px;background:#fff;font-size:12px}
.task-table input:focus{outline:2px solid rgba(36,87,214,.15);border-color:#7196f2}
.task-table th{font-size:11px;white-space:nowrap}
.task-table td{padding:7px 8px;vertical-align:middle}
.task-table .task-name{min-width:150px}.task-table .task-dir{min-width:120px}
.task-table .task-actions{white-space:nowrap;text-align:right}
.task-mini{padding:5px 8px;border:1px solid var(--line);border-radius:7px;background:#fff;color:#526078;cursor:pointer}
.task-mini:hover{background:#edf2ff;border-color:#b8c9f4;color:var(--primary)}
.task-mini.remove:hover{background:#fff2f1;border-color:#f0c5c1;color:var(--danger)}
.task-scene-foot{display:flex;justify-content:space-between;align-items:center;padding:10px 13px;background:#fbfcfe;border-top:1px solid #edf0f5}
.task-scene-foot .hint{margin:0}
.json-tools{margin-top:18px;padding:12px 14px;border:1px dashed var(--line-strong);border-radius:10px;background:#fbfcff}
.json-tools summary{cursor:pointer;color:#526078;font-weight:650;font-size:13px}
.task-save-actions{padding-top:14px;border-top:1px solid var(--line)}
@media(max-width:600px){.task-toolbar{align-items:flex-start}.task-scene-head{flex-wrap:wrap}.task-scene-head input{max-width:none;flex-basis:100%}.task-scene-head .scene-count{margin-left:0}}
@media(prefers-reduced-motion:reduce){*,*:before,*:after{scroll-behavior:auto!important;animation:none!important;transition:none!important}}

/* Compact responsive layout refresh */
:root{
 --bg:#f3f6fb;--surface:#fff;--surface-soft:#f8faff;--line:#e3e9f2;
 --line-strong:#cbd5e1;--radius:12px
}
body{background:linear-gradient(180deg,#edf3fc 0,#f5f7fb 190px,var(--bg) 100%);line-height:1.48}
.top{
 min-height:62px;padding:9px max(18px,calc((100vw - 1400px)/2));
 gap:14px;background:linear-gradient(110deg,#172943,#223b60);
 box-shadow:0 3px 14px rgba(20,34,59,.12)
}
.top h1{font-size:18px}.top small{margin-top:1px;font-size:11px}
#connection-state{padding:5px 10px;font-size:11px}
.wrap{width:min(1440px,100%);margin:16px auto 34px;padding:0 18px}
.nav{
 top:62px;gap:4px;padding:6px;margin-bottom:12px;border-radius:11px;
 box-shadow:0 3px 12px rgba(26,43,75,.045)
}
.nav button{padding:8px 12px;border-radius:8px;font-size:13px}
.card{
 padding:16px;margin-bottom:12px;border-radius:12px;
 box-shadow:0 2px 10px rgba(26,43,75,.035)
}
.card h2{gap:8px;margin-bottom:11px;font-size:15px}
.card h2:before{height:15px;width:3px}
.card h3{margin-bottom:9px;font-size:13px}
.card p{margin-bottom:10px}
.card .card{padding:14px;margin:12px 0;border-radius:10px;box-shadow:none;background:#f8faff!important}
.grid{gap:11px}
.field{margin-bottom:10px}
.field label{margin-bottom:5px;font-size:12px}
.field input,.field select,.field textarea,textarea{
 min-height:36px;padding:7px 10px;border-radius:8px;font-size:13px
}
.field small,.hint{margin-top:4px;font-size:11px;line-height:1.5}
.actions{gap:7px;margin-top:11px}
.btn{min-height:34px;padding:6px 11px;border-radius:8px;font-size:12px}
.notice{padding:9px 11px;margin:9px 0;border-radius:9px;font-size:12px}
.status{grid-template-columns:repeat(5,minmax(0,1fr));gap:9px;margin:12px 0}
.metric{padding:11px 13px;border-radius:10px}
.metric b{margin-top:3px;font-size:21px;line-height:1.2}
.transfer-panel{padding:12px 14px;margin:0 0 12px;border-radius:10px}
.progress-track{height:8px;margin:9px 0 7px}
.disk-monitor-grid{gap:9px}
.disk-monitor{padding:12px;border-radius:10px}
.disk-monitor-track{margin:9px 0 7px}
.active-list{gap:7px}
.active-item{padding:9px 11px;gap:10px}
.active-item-progress{margin:7px 0 5px}
.section-heading{gap:10px}
.summary-chip{padding:4px 8px;font-size:10px}
.history-grid{gap:8px;margin-top:9px}
.history-card{padding:10px;border-radius:9px}
.history-table{margin-top:9px;font-size:11px}
.history-table th,.history-table td{padding:7px 9px}
table{font-size:12px}
td,th{padding:8px 10px}
th{z-index:1}
.disk-row{padding:12px;margin:9px 0;border-radius:10px}
.disk-grid{gap:9px}
.disk-actions{margin-top:7px}
.logs{height:min(380px,52vh);padding:12px;border-radius:9px;font-size:11px}
.task-toolbar{gap:10px;margin:13px 0 9px;padding-bottom:9px}
.task-toolbar strong{font-size:14px}
.task-editor{gap:10px}
.task-scene{border-radius:10px}
.task-scene-head{padding:9px 11px;gap:8px}
.task-scene-head input{padding:7px 9px;font-size:12px}
.task-table{min-width:820px}
.task-table td{padding:6px 7px}
.task-table input{padding:6px 7px;font-size:11px}
.task-scene-foot{padding:8px 10px}
.json-tools{margin-top:12px;padding:10px 12px}
.task-save-actions{padding-top:11px}
#dashboard.panel.active{
 display:grid;grid-template-columns:repeat(12,minmax(0,1fr));
 gap:12px;align-items:start
}
#dashboard>.card{min-width:0;margin:0}
#dashboard>.card:nth-child(1),
#dashboard>.card:nth-child(4),
#dashboard>.card:nth-child(5){grid-column:1/-1}
#dashboard>.card:nth-child(2),
#dashboard>.card:nth-child(3){grid-column:span 6}
#dashboard .status{margin:10px 0}
#dashboard #run-history{max-width:100%;overflow-x:auto}
#dashboard #task-status,#dashboard #disk-history{max-width:100%;overflow-x:auto}
#run-history .history-table{min-width:980px}
#task-status table{min-width:560px}
#connection-fields,#advanced-fields{align-items:start}
#connection-fields .field,#advanced-fields .field{padding:10px 11px;margin:0;border:1px solid #edf0f5;border-radius:9px;background:#fbfcff}
button:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,summary:focus-visible{
 outline:2px solid rgba(36,87,214,.55);outline-offset:2px
}
@media(max-width:1050px){
 .wrap{width:min(100%,100%);padding:0 14px}
 .status{grid-template-columns:repeat(3,minmax(0,1fr))}
}
@media(max-width:820px){
 #dashboard.panel.active{display:block}
 #dashboard>.card{margin-bottom:11px}
 .top{padding:9px 14px}
 .nav{top:62px}
 .status{grid-template-columns:repeat(2,minmax(0,1fr))}
}
@media(max-width:600px){
 .top{min-height:60px;padding:10px 12px}
 .top h1{font-size:16px}.top small{font-size:10px}
 #connection-state{padding:4px 7px;font-size:10px}
 .wrap{margin:11px auto 24px;padding:0 10px}
 .nav{top:0;margin-bottom:10px;padding:5px}
 .nav button{padding:8px 10px;font-size:11px}
 .card{padding:13px;margin-bottom:10px;border-radius:10px}
 .card .card{padding:11px;margin:10px 0}
 .grid,.disk-grid{grid-template-columns:minmax(0,1fr);gap:7px}
 .status{grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}
 .metric{padding:10px}.metric b{font-size:18px}
 .field{margin-bottom:8px}
 .actions{gap:6px}
 .btn{min-height:33px;padding:6px 9px;font-size:11px}
 .section-heading{align-items:flex-start}
 .transfer-head{align-items:flex-start;flex-direction:column;gap:3px}
 .task-toolbar{align-items:flex-start}
 .task-scene-head{flex-wrap:wrap}
 .task-scene-head input{max-width:none;flex-basis:100%}
 .task-scene-head .scene-count{margin-left:0}
 .disk-monitor-stats{font-size:10px}
 .logs{height:340px}
}

</style></head><body>
<header class="top"><div><h1>MinIO 下载器 <span class="tag">Go</span></h1><small>首次启动：网页配置 · SQLite 单文件存储</small></div><div id="connection-state">正在连接…</div></header>
<main class="wrap">
<div class="nav">
<button class="active" data-tab="dashboard">运行状态</button><button data-tab="connection">连接配置</button><button data-tab="disks">磁盘配置</button><button data-tab="tasks">任务配置</button><button data-tab="advanced">运行参数</button><button data-tab="records">日志记录</button>
</div>
<section id="dashboard" class="panel active">
<div class="card"><h2>下载任务</h2><div class="field" style="max-width:320px"><label for="download-mode">下载目标模式</label><select id="download-mode"><option value="count">按条数下载</option><option value="duration">按时长下载</option></select><small>按时长模式根据已完成数据的 content_time 累计，达到任务目标后停止补充。</small></div><div class="status" id="metrics"></div><div id="transfer-panel" class="transfer-panel"><div class="transfer-head"><strong>当前传输进度</strong><span id="transfer-summary">等待状态数据</span></div><div class="progress-track"><div id="transfer-bar" class="progress-fill" style="width:0%"></div></div><div class="transfer-foot"><span id="transfer-downloaded">已传输 0 GiB</span><span id="transfer-remaining">待传输 —</span></div></div><div id="setup-issues" class="notice">正在检查配置…</div><div class="actions"><button class="btn primary" id="start-run">开始下载</button><button class="btn" id="pause-run">安全暂停</button><button class="btn danger" id="force-run">强制停止</button><button class="btn" id="refresh">刷新状态</button><span id="run-control-state" class="summary-chip">等待状态</span></div></div>
<div class="card"><div class="section-heading"><div><h2>磁盘容量</h2><p class="hint">显示各下载磁盘的已用空间、可用空间和挂载状态。</p></div><span id="disk-total-summary" class="summary-chip">尚无磁盘</span></div><div id="disk-status" class="disk-monitor-grid"></div></div><div class="card"><div class="section-heading"><div><h2>正在下载</h2><p class="hint">当前正在处理的 Episode。</p></div><span id="active-count" class="summary-chip">0 个活动任务</span></div><div id="active-downloads"></div></div><div class="card"><div class="section-heading"><div><h2>运行与历史统计</h2><p class="hint">基于 SQLite 下载状态和完成台账统计，不代表单次运行增量。</p></div><span id="last-run-state" class="summary-chip">暂无运行记录</span></div><div id="run-summary" class="status"></div><div id="disk-history"></div><div class="actions"><label for="run-history-range">运行历史时间范围</label><select id="run-history-range"><option value="7">最近 7 天</option><option value="30" selected>最近 30 天</option><option value="90">最近 90 天</option><option value="365">最近一年</option><option value="all">全部（最多 50 次）</option></select></div><div id="run-history"></div><div id="last-run-detail" class="notice">暂无历史运行结果</div><div id="recent-failures"></div></div><div class="card"><h2>任务进度</h2><div id="task-status"></div></div>
</section>
<section id="connection" class="panel">
<div class="card"><h2>MinIO 和 PostgreSQL</h2><div class="hint">连接信息保存到 SQLite。Secret Key 和数据库密码不会回显；留空表示保留已保存的密码。</div><div id="connection-fields" class="grid"></div><div class="actions"><button class="btn primary" id="save-connection">保存连接配置</button><button class="btn" id="test-minio">测试 MinIO</button><button class="btn" id="test-pg">测试 PostgreSQL</button></div><div id="connection-result" class="notice">尚未测试连接</div></div>
</section>
<section id="disks" class="panel"><div class="card"><h2>下载磁盘</h2><p class="hint">通过图形界面添加磁盘、选择真实挂载点并设置下载目录。保存前会检查挂载状态、目录权限和剩余空间。</p><div id="disk-list" class="disk-list"></div><div class="actions"><button class="btn" id="detect-disks">重新检测磁盘</button><button class="btn" id="add-disk">添加磁盘</button><button class="btn" id="check-disks">检查磁盘</button><button class="btn primary" id="save-disks">保存磁盘配置</button></div><div id="disk-result" class="notice">先添加磁盘并选择挂载点，再检查和保存。</div></div></section>
<section id="tasks" class="panel"><div class="card"><h2>任务与下载目标</h2><p class="hint">任务清单保存在 SQLite。每个任务可配置目标条数和目标时长（小时）；启动时选择对应下载模式。</p><div class="card" style="background:#f8fafc"><h3>任务方案库</h3><div class="grid"><div class="field"><label for="plan-name">方案名称</label><input id="plan-name" placeholder="例如：日常采集任务"></div><div class="field"><label for="plan-note">方案备注</label><input id="plan-note" placeholder="可选"></div></div><div class="actions"><button class="btn primary" id="save-plan">保存当前任务为方案</button><button class="btn" id="refresh-plans">刷新方案</button><input id="plan-import-file" type="file" accept=".json,application/json" style="max-width:240px"><button class="btn" id="import-plan">导入方案 JSON</button></div><div id="plan-list"></div><div id="plan-result" class="notice">方案可重复载入，支持替换当前任务清单。</div></div><div class="field"><label for="data-version">数据版本目录</label><input id="data-version" placeholder="例如：银河2.2数据"><small>用于区分不同批次或版本的数据目录。</small></div>
<div class="task-toolbar"><div><strong>任务清单</strong><div class="hint">按场景组织任务。目标条数和目标时长分别保存，运行时由下载模式决定使用哪一个。</div></div><button class="btn primary" id="add-scene">＋ 添加场景</button></div>
<div id="task-editor" class="task-editor"></div>
<details class="json-tools"><summary>高级设置：查看 / 编辑 JSON</summary><p class="hint">适合批量编辑或排查问题。修改 JSON 后点击“应用 JSON”同步到上方表格。</p><textarea id="task-json" class="editor" spellcheck="false"></textarea><div class="actions"><button class="btn" id="apply-task-json">应用 JSON 到表格</button></div></details>
<div class="actions task-save-actions"><button class="btn primary" id="save-tasks">保存任务配置</button><button class="btn" id="reset-tasks">恢复内置默认任务</button><span class="hint">保存后立即写入 SQLite。</span></div><div id="task-result" class="notice"></div></div></section>
<section id="advanced" class="panel">
<div class="card"><h2>运行与传输参数</h2><div id="advanced-fields" class="grid"></div><div class="actions"><button class="btn primary" id="save-advanced">保存运行参数</button></div></div>
<div class="card"><h2>Web 访问安全</h2><p class="hint">默认仅监听本机 127.0.0.1。需要局域网访问时，先在本机设置访问口令，再将监听地址改为 0.0.0.0 并重启程序。</p><div class="field"><label for="access-token">访问口令（至少 12 个字符）</label><input id="access-token" type="password" autocomplete="new-password" placeholder="仅用于设置或轮换口令"></div><div class="actions"><button class="btn primary" id="set-token">设置 / 更新口令</button></div><div id="token-result" class="notice"></div></div>
<div class="card"><h2>数据存储</h2><div id="db-path" class="hint"></div><p class="hint">程序配置、方案、运行状态和日志均保存在这个 SQLite 数据库中；不会另外生成 JSON 配置、日志、PID 或锁文件。下载文件写入上面指定的目标磁盘。</p></div>
</section>
<section id="records" class="panel"><div class="card"><h2>运行日志（SQLite）</h2><div class="actions"><button class="btn" id="refresh-logs">刷新日志</button></div><pre id="logs" class="logs"></pre></div></section>
</main>
<script>
let configData=null,statusData=null,logOffset=0,refreshTimer=null,diskMounts=[];
const $=id=>document.getElementById(id);
async function downloadRunCSV(runID){
 try{
  const headers={};const token=sessionStorage.getItem("minio_download_token");if(token)headers.Authorization="Bearer "+token;
  const response=await fetch("/api/runs/"+encodeURIComponent(runID)+"/export.csv",{headers});
  if(!response.ok){const data=await response.json().catch(()=>({}));throw new Error(data.error||("HTTP "+response.status))}
  const blob=await response.blob(),url=URL.createObjectURL(blob),link=document.createElement("a");
  link.href=url;link.download="run-"+String(runID).replace(/[^a-zA-Z0-9_-]/g,"")+'-episodes.csv';document.body.appendChild(link);link.click();link.remove();URL.revokeObjectURL(url);
 }catch(err){alert("导出运行明细失败："+err.message)}
}
async function loadRunEpisodeDetails(el,offset,filters){
 const root=el.querySelector(".run-episode-results");if(!root)return;
 offset=Number(offset||0);filters=filters||{status:"",category:""};
 root.dataset.loading="1";
 if(offset===0)root.innerHTML='<div class="run-episode-filters"><label>状态 <select class="run-filter-status"><option value="">全部</option><option value="completed">成功</option><option value="failed">失败</option><option value="paused">暂停</option></select></label><label>失败分类 <select class="run-filter-category"><option value="">全部</option><option value="network">网络</option><option value="disk_space">磁盘空间</option><option value="not_found">对象不存在</option><option value="permission">权限</option><option value="integrity">完整性</option><option value="other">其他</option></select></label><button type="button" class="btn secondary run-filter-apply">筛选</button></div><div class="muted run-episode-count"></div><div class="muted run-failure-summary"></div><table class="history-table"><thead><tr><th>Episode ID</th><th>状态</th><th>失败分类</th><th>磁盘</th><th>容量</th><th>内容时长</th><th>错误</th></tr></thead><tbody></tbody></table><button type="button" class="btn secondary run-episode-more">加载更多</button>';
 if(offset===0){root.querySelector(".run-filter-status").value=filters.status;root.querySelector(".run-filter-category").value=filters.category;root.querySelector(".run-filter-apply").onclick=()=>loadRunEpisodeDetails(el,0,{status:root.querySelector(".run-filter-status").value,category:root.querySelector(".run-filter-category").value})}
 try{
  const q=new URLSearchParams({task:el.dataset.task,limit:"100",offset:String(offset)});
  if(filters.status)q.set("status",filters.status);if(filters.category)q.set("category",filters.category);
  const data=await api("/api/runs/"+encodeURIComponent(el.dataset.runId)+"/episodes?"+q.toString());
  const items=data.episodes||[],total=Number(data.total||0);
  const count=root.querySelector(".run-episode-count"),summary=root.querySelector(".run-failure-summary"),body=root.querySelector("tbody"),more=root.querySelector(".run-episode-more");
  const categoryNames={network:"网络",disk_space:"磁盘空间",not_found:"对象不存在",permission:"权限",integrity:"完整性",other:"其他"};
  const categoryItems=Object.entries(data.failure_categories||{}).filter(([,n])=>Number(n)>0).map(([k,n])=>(categoryNames[k]||k)+" "+n+" 次");
  summary.textContent=categoryItems.length?"失败原因："+categoryItems.join(" / "):"暂无失败记录";
  body.insertAdjacentHTML("beforeend",items.map(v=>'<tr><td>'+esc(v.episode_id)+'</td><td>'+esc(({completed:"成功",failed:"失败",paused:"已暂停"}[v.status]||v.status))+'</td><td>'+esc(({network:"网络",disk_space:"磁盘空间",not_found:"对象不存在",permission:"权限",integrity:"完整性",other:"其他",paused:"暂停"}[v.failure_category]||"-"))+'</td><td>'+esc(v.disk_name||"-")+'</td><td>'+formatGiB(v.completed_bytes||0)+'</td><td>'+(Number(v.content_seconds||0)/60).toFixed(1)+' 分钟</td><td>'+esc(v.error||"-")+'</td></tr>').join(""));
  const loaded=offset+items.length;count.textContent='共 '+total+' 条，已加载 '+loaded+' 条';more.hidden=loaded>=total;more.textContent='加载更多';more.onclick=()=>loadRunEpisodeDetails(el,loaded,filters);
  if(!total){root.querySelector("table").hidden=true;more.hidden=true;count.textContent="共 0 条 Episode"}
  el.dataset.loaded="1";el.dataset.filters=JSON.stringify(filters);
 }catch(err){root.querySelector(".run-episode-count").textContent="读取失败："+err.message}
 finally{delete root.dataset.loading}
}
async function api(path,method,body){const headers={"Content-Type":"application/json"};const token=sessionStorage.getItem("minio_download_token");if(token)headers.Authorization="Bearer "+token;const response=await fetch(path,{method:method||"GET",headers,body:body===undefined?undefined:JSON.stringify(body)});const data=await response.json().catch(()=>({error:"服务器返回内容不是 JSON"}));if(!response.ok)throw new Error(data.error||("HTTP "+response.status));return data}
function message(id,text,ok){$(id).textContent=text;$(id).className="notice "+(ok===true?"ok":ok===false?"bad":"")}
function esc(v){return String(v===undefined||v===null?"":v).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]))}
function fieldsFor(groups){const schema=configData.schema.filter(x=>groups.includes(x.group));return schema.map(item=>{const value=configData.values[item.key];let control="";if(item.kind==="bool"){control='<input type="checkbox" data-key="'+esc(item.key)+'" '+(value?"checked":"")+'>'}else{const type=item.secret?"password":item.kind==="str"?"text":"number";const extra=item.kind==="int"?' step="1"':item.kind==="float"?' step="any"':"";control='<input data-key="'+esc(item.key)+'" type="'+type+'" value="'+(item.secret?"":esc(value))+'"'+extra+(item.min!==null&&item.min!==undefined?' min="'+item.min+'"':"")+(item.max!==null&&item.max!==undefined?' max="'+item.max+'"':"")+' placeholder="'+esc(item.placeholder|| (item.secret?"留空表示不修改":""))+'">'}return '<div class="field"><label>'+esc(item.label)+(item.secret&&configData.secrets_set.includes(item.key)?'（已保存）':"")+'</label>'+control+'<small>'+esc(item.help||"")+'</small></div>'}).join("")}
function readFields(root){const values={};root.querySelectorAll("[data-key]").forEach(el=>{const key=el.dataset.key;const schema=configData.schema.find(x=>x.key===key);if(schema.secret&&!el.value)return;if(schema.kind==="bool")values[key]=el.checked;else if(schema.kind==="int")values[key]=Number.parseInt(el.value,10)||0;else if(schema.kind==="float")values[key]=Number(el.value)||0;else values[key]=el.value});return values}
async function loadDiskMounts(){try{const d=await api("/api/config/disks/detect");diskMounts=d.mounts||[]}catch(e){diskMounts=[]}}
function renderDisks(disks){const root=$("disk-list");root.innerHTML="";(disks||[]).forEach((d,i)=>{const card=document.createElement("div");card.className="disk-row";card.innerHTML='<div class="disk-actions"><b>磁盘 '+(i+1)+'</b><button type="button" class="btn danger" data-remove-disk="'+i+'">删除</button></div><div class="disk-grid"><div class="field"><label>磁盘名称</label><input data-disk-field="name" value="'+esc(d.name)+'" placeholder="例如 disk1"></div><div class="field"><label>实际挂载点</label><input data-disk-field="mountpoint" list="disk-mount-options" value="'+esc(d.mountpoint||"")+'" placeholder="选择或输入挂载点"></div><div class="field"><label>下载目录</label><input data-disk-field="path" value="'+esc(d.path)+'" placeholder="挂载点下的下载目录"></div></div>';root.appendChild(card)});let dl=document.getElementById("disk-mount-options");if(!dl){dl=document.createElement("datalist");dl.id="disk-mount-options";document.body.appendChild(dl)}dl.innerHTML=diskMounts.map(m=>'<option value="'+esc(m.mountpoint)+'">'+esc(m.device||"")+' · '+(m.free/1073741824).toFixed(1)+' GiB 可用</option>').join("");root.querySelectorAll("[data-remove-disk]").forEach(btn=>btn.onclick=()=>{const rows=readDisks();rows.splice(Number(btn.dataset.removeDisk),1);renderDisks(rows)})}
function readDisks(){return Array.from($("disk-list").querySelectorAll(".disk-row")).map(row=>{const v={};row.querySelectorAll("[data-disk-field]").forEach(el=>v[el.dataset.diskField]=el.value.trim());return v})}
function addDisk(){const rows=readDisks();rows.push({name:"disk"+(rows.length+1),mountpoint:"",path:""});renderDisks(rows)}
function resolvePlanConflicts(conflicts){
 return new Promise(resolve=>{
  const overlay=document.createElement("div");overlay.style.cssText="position:fixed;inset:0;background:rgba(15,23,42,.58);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px";
  const panel=document.createElement("div");panel.style.cssText="width:min(760px,100%);max-height:85vh;overflow:auto;background:var(--panel,#fff);color:var(--text,#111827);border-radius:14px;padding:22px;box-shadow:0 20px 60px rgba(0,0,0,.28)";
  const title=document.createElement("h3");title.textContent="处理任务方案冲突（"+conflicts.length+" 项）";panel.appendChild(title);
  const intro=document.createElement("p");intro.textContent="逐项选择如何处理同名但配置不同的任务。默认保留当前配置，不会自动覆盖。";panel.appendChild(intro);
  const rows=[];
  conflicts.forEach(c=>{
   const row=document.createElement("div");row.style.cssText="display:grid;grid-template-columns:minmax(0,1fr) 190px;gap:12px;align-items:center;padding:12px 0;border-top:1px solid var(--border,#e5e7eb)";
   const info=document.createElement("div");const name=document.createElement("strong");name.textContent=c.name;info.appendChild(name);
   const detail=document.createElement("div");detail.style.cssText="font-size:12px;opacity:.8;margin-top:4px;line-height:1.6";
   const labels={count:"目标条数",hours:"目标时长",dir:"目录",aliases:"别名",tags:"标签"};
   const keys=new Set([...Object.keys(c.old||{}),...Object.keys(c.next||{})]);const changes=[...keys].filter(k=>JSON.stringify(c.old?.[k])!==JSON.stringify(c.next?.[k])).map(k=>(labels[k]||k)+"："+(c.old?.[k]===undefined?"（未设置）":JSON.stringify(c.old[k]))+" → "+(c.next?.[k]===undefined?"（未设置）":JSON.stringify(c.next[k])));
   detail.textContent="场景："+c.oldScene+" → "+c.newScene+"；差异："+(changes.join("；")||"任务字段有变化");info.appendChild(detail);
   const select=document.createElement("select");select.style.cssText="width:100%;padding:9px;border:1px solid var(--border,#cbd5e1);border-radius:8px;background:var(--input,#fff);color:inherit";
   [["keep","保留当前配置"],["replace","使用方案配置"],["skip","跳过此任务"]].forEach(([v,label])=>{const o=document.createElement("option");o.value=v;o.textContent=label;select.appendChild(o)});
   row.append(info,select);panel.appendChild(row);rows.push({name:c.name,select});
  });
  const actions=document.createElement("div");actions.style.cssText="display:flex;justify-content:flex-end;gap:10px;margin-top:18px";
  const cancel=document.createElement("button");cancel.type="button";cancel.textContent="取消载入";cancel.className="btn";
  const apply=document.createElement("button");apply.type="button";apply.textContent="确认处理";apply.className="btn primary";
  cancel.onclick=()=>{overlay.remove();resolve(null)};
  apply.onclick=()=>{const result={};rows.forEach(x=>result[x.name]=x.select.value);overlay.remove();resolve(result)};
  actions.append(cancel,apply);panel.appendChild(actions);overlay.appendChild(panel);overlay.addEventListener("click",e=>{if(e.target===overlay){overlay.remove();resolve(null)}});document.body.appendChild(overlay);
 });
}
async function refreshPlans(){try{const d=await api("/api/plans");const root=$("plan-list");root.innerHTML=(d.plans||[]).map(p=>'<div class="notice"><b>'+esc(p.name)+'</b> · '+p.task_count+' 个任务 · '+p.scene_count+' 个场景 · 更新 '+esc(p.updated_at)+'<div class="actions"><button class="btn primary" data-plan-apply="'+p.id+'">替换载入</button><button class="btn" data-plan-merge="'+p.id+'">追加载入</button><button class="btn" data-plan-export="'+p.id+'">导出</button><button class="btn danger" data-plan-delete="'+p.id+'">删除</button></div></div>').join("")||'<p class="hint">暂无已保存方案。</p>';root.querySelectorAll("[data-plan-apply]").forEach(b=>b.onclick=async()=>{if(!confirm("用此方案替换当前任务清单？"))return;try{await api("/api/plans/"+b.dataset.planApply+"/apply","POST",{mode:"replace"});message("plan-result","方案已载入；当前任务配置已保存到 SQLite",true);await loadConfig();await refreshPlans()}catch(e){message("plan-result",e.message,false)}});root.querySelectorAll("[data-plan-merge]").forEach(b=>b.onclick=async()=>{try{
 const id=b.dataset.planMerge;const detail=await api("/api/plans/"+id);const incoming=detail.payload||{};const current=new Map();for(const scene of (configData.tasks.scenes||[])){for(const task of (scene.tasks||[])){current.set(task.name,{scene:scene.scene,task})}}
 const resolutions={};const conflicts=[];let newCount=0;let unchangedCount=0;
 for(const scene of (incoming.scenes||[])){for(const task of (scene.tasks||[])){const found=current.get(task.name);if(!found){newCount++;continue}const changed=JSON.stringify(found.task)!==JSON.stringify(task)||found.scene!==scene.scene;if(changed){conflicts.push({name:task.name,old:found.task,next:task,oldScene:found.scene,newScene:scene.scene})}else{unchangedCount++;resolutions[task.name]="keep"}}}
 if(!confirm("载入预览：新增 "+newCount+" 个任务，配置相同 "+unchangedCount+" 个，存在冲突 "+conflicts.length+" 个。\\n继续处理？"))return;
 if(conflicts.length){const chosen=await resolvePlanConflicts(conflicts);if(!chosen)return;Object.assign(resolutions,chosen)}
 const d=await api("/api/plans/"+id+"/apply","POST",{mode:"merge",resolutions});const skipped=(d.skipped||[]).length;message("plan-result","方案处理完成：新增/替换 "+d.added+" 个任务，跳过 "+skipped+" 个。",true);await loadConfig();await refreshPlans();
 }catch(e){message("plan-result",e.message,false)}});root.querySelectorAll("[data-plan-delete]").forEach(b=>b.onclick=async()=>{if(!confirm("删除这个任务方案？不会删除当前任务清单或已下载文件。"))return;try{await api("/api/plans/"+b.dataset.planDelete,"DELETE");await refreshPlans();message("plan-result","方案已删除",true)}catch(e){message("plan-result",e.message,false)}});root.querySelectorAll("[data-plan-export]").forEach(b=>b.onclick=()=>{location.href="/api/plans/"+b.dataset.planExport+"/export"})}catch(e){message("plan-result",e.message,false)}}
async function savePlan(){const name=$("plan-name").value.trim();if(!name){message("plan-result","请填写方案名称",false);return}const payload={name:name,note:$("plan-note").value};try{await api("/api/plans","POST",payload);await refreshPlans();message("plan-result","当前任务清单已保存为方案",true)}catch(e){if(e.message.includes("已有同名方案")&&confirm("方案已存在，是否覆盖原方案？")){try{payload.overwrite=true;await api("/api/plans","POST",payload);await refreshPlans();message("plan-result","方案已覆盖保存",true)}catch(x){message("plan-result",x.message,false)}}else{message("plan-result",e.message,false)}}}
async function loadConfig(){configData=await api("/api/config");$("connection-fields").innerHTML=fieldsFor(["minio","pg"]);$("advanced-fields").innerHTML=fieldsFor(["run","transfer","web"]);await loadDiskMounts();renderDisks(configData.disks||[]);$("data-version").value=configData.tasks.data_version||"";taskEditorData=JSON.parse(JSON.stringify({data_version:configData.tasks.data_version||"",scenes:configData.tasks.scenes||[]}));renderTaskEditor();$("db-path").textContent="SQLite 文件："+configData.paths.sqlite_db;$("connection-state").textContent="控制台已连接";$("connection-state").className="ok"}
let taskEditorData={data_version:"",scenes:[]};
function syncTaskJSON(value){$("task-json").value=JSON.stringify(value||taskEditorData,null,2)}
function readTaskEditor(){return {data_version:$("data-version").value,scenes:taskEditorData.scenes.map(scene=>({scene:scene.scene,tasks:(scene.tasks||[]).map(task=>({name:task.name||"",dir:task.dir||"",count:Number(task.count||0),target_hours:Number(task.target_hours||0),aliases:Array.isArray(task.aliases)?task.aliases:[],tags:Array.isArray(task.tags)?task.tags:[]}))}))}}
function renderTaskEditor(){
 const host=$("task-editor");if(!host)return;
 const scenes=taskEditorData.scenes||[];
 host.innerHTML=scenes.length?scenes.map((scene,si)=>'<section class="task-scene"><div class="task-scene-head"><label>场景名称</label><input data-scene-name="'+si+'" value="'+esc(scene.scene||"")+'" placeholder="例如：家居"><span class="scene-count">'+(scene.tasks||[]).length+' 个任务</span><button class="task-mini remove" data-action="remove-scene" data-scene="'+si+'">删除场景</button></div><div class="task-table-wrap"><table class="task-table"><thead><tr><th>任务名称</th><th>目录名（可选）</th><th>目标条数</th><th>目标时长（小时）</th><th>别名（逗号分隔）</th><th>标签（逗号分隔）</th><th>排序 / 操作</th></tr></thead><tbody>'+((scene.tasks||[]).map((task,ti)=>'<tr><td><input class="task-name" data-task-field="name" data-scene="'+si+'" data-task="'+ti+'" value="'+esc(task.name||"")+'" placeholder="任务名称"></td><td><input class="task-dir" data-task-field="dir" data-scene="'+si+'" data-task="'+ti+'" value="'+esc(task.dir||"")+'" placeholder="默认使用任务名"></td><td><input type="number" min="0" step="1" data-task-field="count" data-scene="'+si+'" data-task="'+ti+'" value="'+Number(task.count||0)+'"></td><td><input type="number" min="0" step="0.1" data-task-field="target_hours" data-scene="'+si+'" data-task="'+ti+'" value="'+Number(task.target_hours||0)+'"></td><td><input data-task-field="aliases" data-scene="'+si+'" data-task="'+ti+'" value="'+esc((task.aliases||[]).join(", "))+'" placeholder="别名1, 别名2"></td><td><input data-task-field="tags" data-scene="'+si+'" data-task="'+ti+'" value="'+esc((task.tags||[]).join(", "))+'" placeholder="标签1, 标签2"></td><td class="task-actions"><button class="task-mini" data-action="up" data-scene="'+si+'" data-task="'+ti+'" title="上移" '+(ti===0?"disabled":"")+'>↑</button> <button class="task-mini" data-action="down" data-scene="'+si+'" data-task="'+ti+'" title="下移" '+(ti===(scene.tasks||[]).length-1?"disabled":"")+'>↓</button> <button class="task-mini remove" data-action="remove-task" data-scene="'+si+'" data-task="'+ti+'">删除</button></td></tr>').join(""))+'</tbody></table></div><div class="task-scene-foot"><span class="hint">任务名全局唯一；目标设为 0 表示不按该指标限制。</span><button class="btn" data-action="add-task" data-scene="'+si+'">＋ 添加任务</button></div></section>').join(""):'<div class="notice">还没有场景。点击“添加场景”开始配置任务。</div>';
 syncTaskJSON(readTaskEditor());
}
function updateTaskField(input){
 const si=Number(input.dataset.scene),ti=Number(input.dataset.task),field=input.dataset.taskField;
 if(field==="scene-name"){taskEditorData.scenes[si].scene=input.value;return}
 const task=taskEditorData.scenes[si]?.tasks?.[ti];if(!task)return;
 if(field==="count"||field==="target_hours")task[field]=input.value===""?0:Number(input.value);
 else if(field==="aliases"||field==="tags")task[field]=input.value.split(",").map(x=>x.trim()).filter(Boolean);
 else task[field]=input.value;
 syncTaskJSON(readTaskEditor());
}
$("task-editor").addEventListener("change",event=>{const input=event.target;if(input.dataset.sceneName!==undefined){taskEditorData.scenes[Number(input.dataset.sceneName)].scene=input.value;syncTaskJSON(readTaskEditor());return}if(input.dataset.taskField)updateTaskField(input)});
$("task-editor").addEventListener("click",event=>{const button=event.target.closest("button[data-action]");if(!button)return;const si=Number(button.dataset.scene),ti=Number(button.dataset.task),action=button.dataset.action;const scenes=taskEditorData.scenes;
 if(action==="add-task"){scenes[si].tasks.push({name:"",dir:"",count:0,target_hours:0,aliases:[],tags:[]})}
 if(action==="remove-task"){if(!confirm("删除这个任务？"))return;scenes[si].tasks.splice(ti,1)}
 if(action==="up"&&ti>0){const rows=scenes[si].tasks;[rows[ti-1],rows[ti]]=[rows[ti],rows[ti-1]]}
 if(action==="down"&&ti<scenes[si].tasks.length-1){const rows=scenes[si].tasks;[rows[ti+1],rows[ti]]=[rows[ti],rows[ti+1]]}
 if(action==="remove-scene"){if(!confirm("删除整个场景及其中的任务？"))return;scenes.splice(si,1)}
 renderTaskEditor();
});
$("add-scene").onclick=()=>{taskEditorData.scenes.push({scene:"新场景",tasks:[]});renderTaskEditor();const inputs=$("task-editor").querySelectorAll("[data-scene-name]");if(inputs.length)inputs[inputs.length-1].focus()};
$("apply-task-json").onclick=()=>{try{const value=JSON.parse($("task-json").value);if(!value||!Array.isArray(value.scenes))throw new Error("JSON 必须包含 scenes 数组");taskEditorData={data_version:value.data_version||"",scenes:value.scenes};$("data-version").value=value.data_version||$("data-version").value;renderTaskEditor();message("task-result","JSON 已应用到任务表格，保存后生效",true)}catch(e){message("task-result","JSON 格式错误："+e.message,false)}};
function formatGiB(bytes){return (Math.max(0,Number(bytes)||0)/1073741824).toFixed(1)+" GiB"}
function renderStatus(d){
 statusData=d;const t=d.transfer_progress||{},running=!!(d.downloader&&d.downloader.running);
 const speed=Math.max(0,Number(t.speed_bytes_per_second)||0),downloaded=Math.max(0,Number(t.downloaded_bytes)||0),expected=Math.max(0,Number(t.expected_bytes)||0),remaining=Math.max(0,Number(t.remaining_bytes)||0),percent=Math.max(0,Math.min(100,Number(t.progress_percent)||0));
 const metrics=[["运行状态",running?"下载中":"空闲"],["下载速度",(speed/1048576).toFixed(2)+" MiB/s"],["已下载",formatGiB(downloaded)],["进度",percent.toFixed(1)+"%"],["预计剩余",t.eta_seconds===null||t.eta_seconds===undefined?"—":Math.round(Number(t.eta_seconds)/60)+" 分钟"]];
 $("metrics").innerHTML=metrics.map(x=>'<div class="metric">'+esc(x[0])+'<b>'+esc(x[1])+'</b></div>').join("");
 $("transfer-summary").textContent=running?(expected>0?percent.toFixed(1)+"% · "+(speed/1048576).toFixed(2)+" MiB/s":"传输中 · 等待对象大小统计"):"当前没有活动传输";
 $("transfer-bar").style.width=(expected>0?percent:0)+"%";
 $("transfer-downloaded").textContent="已传输 "+formatGiB(downloaded);
 $("transfer-remaining").textContent=expected>0?"预计剩余 "+formatGiB(remaining):"待传输 —";
 const stopPending=!!d.stop_requested;
 $("run-control-state").textContent=stopPending?"正在安全停止":(running?"下载运行中":"当前空闲");
 $("run-control-state").className="summary-chip "+(stopPending?"run-state-chip pending":running?"run-state-chip":"");
 $("start-run").disabled=running;
 $("pause-run").disabled=!running||stopPending;
 $("force-run").disabled=!running;
 $("pause-run").textContent=stopPending?"正在停止…":"安全暂停";
 $("setup-issues").innerHTML=(d.setup_issues||[]).length?(d.setup_issues.map(x=>'<div class="bad">· '+esc(x.text)+'</div>').join("")):'<span class="ok">配置检查通过，可启动下载。</span>';
 const disks=d.disks||[];let totalBytes=0,freeBytes=0;
 $("disk-status").innerHTML=disks.map(x=>{
  const total=Math.max(0,Number(x.total)||0),free=Math.max(0,Number(x.free)||0),used=Math.max(0,Number(x.used)||0),usedPct=total>0?Math.max(0,Math.min(100,used/total*100)):0;
  totalBytes+=total;freeBytes+=free;
  const fillClass=usedPct>=95?"critical":usedPct>=85?"warn":"";
  return '<div class="disk-monitor"><div class="disk-monitor-top"><span class="disk-monitor-name">'+esc(x.name)+'</span><span class="disk-monitor-state '+(x.mounted&&x.exists?"good":"bad")+'">'+(x.mounted&&x.exists?"挂载正常":(!x.mounted?"未挂载":"目录不可用"))+'</span></div><div class="disk-monitor-path">'+esc(x.path)+'</div><div class="disk-monitor-track"><div class="disk-monitor-fill '+fillClass+'" style="width:'+usedPct.toFixed(1)+'%"></div></div><div class="disk-monitor-stats"><span>已用 '+formatGiB(used)+' / '+formatGiB(total)+'</span><strong>可用 '+formatGiB(free)+'</strong></div></div>';
 }).join("")||'<div class="notice">尚未配置下载磁盘。</div>';
 $("disk-total-summary").textContent=disks.length?("合计可用 "+formatGiB(freeBytes)):"尚无磁盘";
 const active=d.running_episodes||[];
 $("active-count").textContent=active.length+" 个活动任务";
 $("active-downloads").innerHTML=active.length?'<div class="active-list">'+active.map(x=>{const partial=Math.max(0,Number(x.partial_bytes)||0),expected=Math.max(0,Number(x.expected_bytes)||0),pct=expected>0?Math.max(0,Math.min(100,partial/expected*100)):0;return '<div class="active-item"><div class="active-item-top"><div class="active-item-main"><div class="active-item-title">'+esc(x.task_name||"未知任务")+'</div><div class="active-item-meta">磁盘：'+esc(x.disk_name||"未分配")+' · Episode：'+esc(x.episode_id||"")+'</div></div><div class="active-item-size">'+formatGiB(partial)+'</div></div><div class="active-item-progress"><span style="width:'+pct.toFixed(1)+'%"></span></div><div class="active-item-bottom"><span>'+ (expected>0?(pct.toFixed(1)+"% · 目标 "+formatGiB(expected)):"目标容量未知")+'</span><span>开始：'+esc(x.started_at||"—")+'</span></div></div>'}).join("")+'</div>':'<div class="notice">当前没有正在下载的 Episode。</div>';
 const summary=d.history_summary||{};
 const result=d.last_run||{};
 const resultLabel={finished:"已完成",paused:"已暂停",error:"运行失败"}[result.result]||"暂无记录";
 $("last-run-state").textContent=resultLabel;
 const latestRun=(d.run_history||[])[0]||{},latestStats=latestRun.episode_stats||{};const latestEpisodeSummary=latestRun.run_id?(" · Episode：成功 "+Number(latestStats.completed||0)+" / 失败 "+Number(latestStats.failed||0)+" / 暂停 "+Number(latestStats.paused||0)):"";$("last-run-detail").textContent=result.at?("最近运行："+result.at+" · 模式："+(result.mode==="duration"?"按时长":"按条数")+latestEpisodeSummary+(result.error?" · 错误："+result.error:"")):"暂无历史运行结果";
 const summaryItems=[["完成记录",Number(summary.completed||0)+" 条"],["失败记录",Number(summary.failed||0)+" 条"],["暂停 / 待续传",Number(summary.paused||0)+ " / "+Number(summary.pending||0)],["完成数据容量",formatGiB(summary.completed_bytes||0)],["累计内容时长",(Number(summary.content_seconds||0)/3600).toFixed(1)+" 小时"]];
 $("run-summary").innerHTML=summaryItems.map(x=>'<div class="metric">'+esc(x[0])+'<b>'+esc(x[1])+'</b></div>').join("");
 const diskHistory=d.disk_history||[];
 $("disk-history").innerHTML=diskHistory.length?'<table class="history-table"><thead><tr><th>磁盘</th><th>完成数量</th><th>容量</th><th>内容时长</th></tr></thead><tbody>'+diskHistory.map(x=>'<tr><td>'+esc(x.disk_name||"未分配")+'</td><td>'+Number(x.count||0)+'</td><td>'+formatGiB(x.bytes||0)+'</td><td>'+(Number(x.seconds||0)/3600).toFixed(1)+' 小时</td></tr>').join("")+'</tbody></table>':'<div class="notice">暂无按磁盘汇总的完成记录。</div>';
 const runs=d.run_history||[];
 $("run-history").innerHTML=runs.length?'<h3>最近运行记录</h3><table class="history-table"><thead><tr><th>开始时间</th><th>结果</th><th>耗时</th><th>完成数量</th><th>容量</th><th>内容时长</th><th>Episode 统计</th><th>任务明细</th><th>导出</th></tr></thead><tbody>'+runs.map(x=>{const tasks=(x.tasks||[]).slice().sort((a,b)=>Number(b.content_seconds||0)-Number(a.content_seconds||0));const details=tasks.length?'<details><summary>查看 '+tasks.length+' 个任务 · 按内容时长排序</summary><div class="muted">* 成功率 = 成功数量 ÷（成功数量 + 失败次数）；失败按下载尝试累计，重试可能使失败次数大于 Episode 数。</div><table class="history-table"><thead><tr><th>排名</th><th>任务</th><th>成功</th><th>失败</th><th>成功率*</th><th>容量</th><th>内容时长</th><th>等待时间</th><th>运行耗时</th><th>平均吞吐</th><th>Episode</th></tr></thead><tbody>'+tasks.map((t,index)=>{const success=Number(t.completed_count||0),failed=Number(t.failed_count||0),attempts=success+failed,successRate=attempts?(success*100/attempts).toFixed(1)+"%":"-";const episodeDetails='<details class="run-episode-details" data-run-id="'+esc(x.run_id)+'" data-task="'+esc(t.task_name)+'"><summary>Episode 明细</summary><div class="run-episode-results muted">展开后加载明细</div></details>';return '<tr><td>'+(index+1)+'</td><td>'+esc(t.task_name)+'</td><td>'+success+'</td><td>'+failed+'</td><td>'+successRate+'</td><td>'+formatGiB(t.completed_bytes||0)+'</td><td>'+(Number(t.content_seconds||0)/3600).toFixed(2)+' 小时</td><td>'+(Number(t.wait_seconds||0)/60).toFixed(1)+' 分钟</td><td>'+(Number(t.duration_seconds||0)/60).toFixed(1)+' 分钟</td><td>'+(Number(t.throughput_bytes_per_second||0)/1048576).toFixed(2)+' MiB/s</td><td>'+episodeDetails+'</td></tr>'}).join("")+'</tbody></table></details>':'<span class="muted">本次无新完成任务</span>';const stats=x.episode_stats||{},categories=stats.failure_categories||{},categoryText=Object.entries(categories).map(([k,n])=>({network:"网络",disk_space:"磁盘空间",not_found:"对象不存在",permission:"权限",integrity:"完整性",other:"其他"}[k]||k)+" "+n).join(" / ");const failedTotal=Number(stats.failed||0),categorySummary=Object.entries(categories).filter(([,n])=>Number(n)>0).map(([k,n])=>({network:"网络",disk_space:"磁盘空间",not_found:"对象不存在",permission:"权限",integrity:"完整性",other:"其他"}[k]||k)+" "+n+"（"+(failedTotal?(Number(n)*100/failedTotal).toFixed(1):"0.0")+"%）").join(" / ");const episodeSummary='成功 '+Number(stats.completed||0)+' · 失败 '+failedTotal+' · 暂停 '+Number(stats.paused||0)+(categorySummary?'（'+categorySummary+'）':'');const diskSummary=(x.disk_stats||[]).map(d=>{const done=Number(d.completed||0),bad=Number(d.failed||0),attempts=done+bad,rate=attempts?(done*100/attempts).toFixed(1)+"%":"-",elapsed=Number(d.duration_seconds||0),throughput=Number(d.throughput_bytes_per_second||0);return (d.disk_name||"未分配")+': 成功 '+done+' / 失败 '+bad+' / 成功率 '+rate+' / '+formatGiB(d.completed_bytes||0)+' / 耗时 '+(elapsed/60).toFixed(1)+' 分钟 / 平均 '+(throughput/1048576).toFixed(2)+' MiB/s'}).join('；');return '<tr><td>'+esc(x.started_at||"")+'</td><td>'+esc(({running:"运行中",finished:"已完成",paused:"已暂停",error:"失败",interrupted:"异常中断"}[x.result]||x.result))+'</td><td>'+((Number(x.duration_seconds)||0)/60).toFixed(1)+' 分钟</td><td>'+Number(x.completed_count||0)+'</td><td>'+formatGiB(x.completed_bytes||0)+'</td><td>'+(Number(x.content_seconds||0)/3600).toFixed(2)+' 小时</td><td>'+esc(episodeSummary)+(diskSummary?'<div class="muted">磁盘：'+esc(diskSummary)+'</div>':'')+'</td><td>'+details+'</td><td><button type="button" class="btn secondary" onclick="downloadRunCSV(\''+esc(x.run_id)+'\')">导出 CSV</button></td></tr>'}).join("")+'</tbody></table>':'<div class="notice">暂无运行历史。</div>';
 const failures=d.recent_failures||[];
 $("run-history").querySelectorAll(".run-episode-details").forEach(el=>{el.addEventListener("toggle",()=>{if(el.open&&!el.dataset.loaded)loadRunEpisodeDetails(el)}, {once:true})});
 $("recent-failures").innerHTML=failures.length?'<div class="failure-list"><strong>最近失败记录（最多 8 条）</strong>'+failures.map(x=>'<div class="failure-item">'+esc(x.task_name||"未知任务")+' · '+esc(x.episode_id||"")+' · '+esc(x.error||"未提供错误详情")+'</div>').join("")+'</div>':'<div class="notice">没有失败记录。</div>';
 const durationMode=$("download-mode").value==="duration";let rows=[];
 (d.scenes||[]).forEach(scene=>(scene.tasks||[]).forEach(task=>{
  const progress=durationMode?(Number(task.hours||0).toFixed(2)+" / "+Number(task.target_hours||0).toFixed(2)+" 小时"):(task.downloaded+" / "+task.target+" 条");
  rows.push('<tr><td>'+esc(scene.scene)+'</td><td>'+esc(task.name)+'</td><td>'+progress+'</td><td>'+Number(task.hours||0).toFixed(2)+' 小时</td></tr>');
 }));
 $("task-status").innerHTML=rows.length?'<table><thead><tr><th>场景</th><th>任务</th><th>'+(durationMode?"累计时长 / 目标":"完成条数 / 目标")+'</th><th>累计时长</th></tr></thead><tbody>'+rows.join("")+'</tbody></table>':"尚未配置任务";
}
async function refreshStatus(){try{const range=$("run-history-range")?.value||"30";renderStatus(await api("/api/status?history_days="+encodeURIComponent(range)))}catch(e){$("connection-state").textContent=e.message;$("connection-state").className="bad"}}
async function refreshLogs(){try{const d=await api("/api/logs?offset="+logOffset);if(d.reset){$("logs").textContent=d.text||""}else if(d.text){$("logs").textContent+=($("logs").textContent?"\n":"")+d.text}$("logs").scrollTop=$("logs").scrollHeight;logOffset=d.offset||logOffset}catch(e){$("logs").textContent=e.message}}
async function testConnection(target){try{const root=target==="minio"?$("connection-fields"):$("connection-fields");const d=await api("/api/config/test","POST",{target:target,values:readFields(root)});message("connection-result",d.message,d.ok)}catch(e){message("connection-result",e.message,false)}}
$("run-history-range")?.addEventListener("change",refreshStatus);
document.querySelectorAll(".nav button").forEach(btn=>btn.addEventListener("click",()=>{document.querySelectorAll(".nav button").forEach(x=>x.classList.remove("active"));document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));btn.classList.add("active");$(btn.dataset.tab).classList.add("active");if(btn.dataset.tab==="records")refreshLogs()}));
$("save-connection").onclick=async()=>{try{await api("/api/config/settings","PUT",{values:readFields($("connection-fields"))});message("connection-result","连接配置已保存到 SQLite",true);await loadConfig()}catch(e){message("connection-result",e.message,false)}};
$("test-minio").onclick=()=>testConnection("minio");$("test-pg").onclick=()=>testConnection("pg");
$("save-advanced").onclick=async()=>{try{await api("/api/config/settings","PUT",{values:readFields($("advanced-fields"))});alert("运行参数已保存到 SQLite");await loadConfig()}catch(e){alert(e.message)}};
$("detect-disks").onclick=async()=>{await loadDiskMounts();renderDisks(readDisks());message("disk-result","已重新检测到 "+diskMounts.length+" 个挂载点",true)};$("add-disk").onclick=addDisk;
$("check-disks").onclick=async()=>{try{const d=await api("/api/config/disks/check","POST",{disks:readDisks()});message("disk-result",d.results.map(x=>x.name+": "+(x.ok?"正常":"异常")+"；"+(x.notes||[]).map(n=>n.text).join("；")+(x.ok?"；可用空间 "+(x.free/1073741824).toFixed(1)+" GiB":"")).join("\n"),d.results.every(x=>x.ok))}catch(e){message("disk-result",e.message,false)}};
$("save-disks").onclick=async()=>{try{await api("/api/config/disks","PUT",{disks:readDisks()});message("disk-result","磁盘配置已保存到 SQLite",true);await loadConfig();await refreshStatus()}catch(e){message("disk-result",e.message,false)}};
$("save-tasks").onclick=async()=>{try{const value=readTaskEditor();value.data_version=$("data-version").value;syncTaskJSON(value);await api("/api/config/tasks","PUT",value);message("task-result","任务配置已保存到 SQLite",true);await loadConfig()}catch(e){message("task-result",e.message,false)}};
$("refresh-plans").onclick=refreshPlans;$("save-plan").onclick=savePlan;
$("import-plan").onclick=async()=>{const file=$("plan-import-file").files[0];if(!file){message("plan-result","请先选择导出的方案 JSON 文件",false);return}try{const doc=JSON.parse(await file.text());const result=await api("/api/plans/import","POST",doc);$("plan-import-file").value="";await refreshPlans();message("plan-result","方案导入成功："+result.name,true)}catch(e){message("plan-result","导入失败："+e.message,false)}};
$("reset-tasks").onclick=async()=>{if(!confirm("恢复内置默认任务清单？当前任务清单将被替换。"))return;try{await api("/api/config/tasks/reset","POST",{});message("task-result","已恢复默认任务",true);await loadConfig()}catch(e){message("task-result",e.message,false)}};
$("start-run").onclick=async()=>{try{const d=await api("/api/run","POST",{mode:$("download-mode").value});message("setup-issues","下载进程已启动，PID "+d.pid,true);await refreshStatus()}catch(e){message("setup-issues",e.message,false)}};
$("pause-run").onclick=async()=>{try{await api("/api/stop","POST",{});message("setup-issues","已发送安全暂停请求",true)}catch(e){message("setup-issues",e.message,false)}};
$("force-run").onclick=async()=>{if(!confirm("强制结束下载进程？临时文件保留，下次可续传。"))return;try{await api("/api/stop","POST",{force:true});message("setup-issues","已发送强制停止",true)}catch(e){message("setup-issues",e.message,false)}};
$("refresh").onclick=refreshStatus;$("refresh-logs").onclick=refreshLogs;
$("set-token").onclick=async()=>{const token=$("access-token").value;if(token.length<12){message("token-result","口令至少 12 个字符",false);return}try{const d=await api("/api/auth/token","PUT",{action:"set",token:token});sessionStorage.setItem("minio_download_token",token);$("access-token").value="";message("token-result","访问口令已保存。启用非本机监听前需重启程序。",true)}catch(e){message("token-result",e.message,false)}};
(async()=>{try{await loadConfig();await refreshPlans();await refreshStatus();await refreshLogs();refreshTimer=setInterval(()=>{refreshStatus();refreshLogs()},5000)}catch(e){$("connection-state").textContent=e.message;$("connection-state").className="bad";$("setup-issues").textContent="初始化失败："+e.message}})();
</script></body></html>`;

type WebAPI struct{cfg Config;db *sql.DB;tokenHash string;configFile string;mode string;childPID int32;childCreated float64;childMu sync.RWMutex;runMu sync.Mutex;authMu sync.Mutex;statusMu sync.Mutex;statusSampleAt time.Time;statusSampleBytes int64;statusRateBps float64;failures map[string][]time.Time;lockouts map[string]time.Time}
func(a *WebAPI)completedPath(path string)bool{return a.cfg.TrustCompletedRecords||completedEpisodeDir(path,a.cfg.RequireMCAP,a.cfg.MCAPPattern)}
func serveWeb(c Config,host string,port int)error{
 if port<1||port>65535{return errors.New("web-port 必须在 1 到 65535 之间")}
 state,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};state.SetMaxOpenConns(4);_,_=state.Exec("PRAGMA busy_timeout=30000");defer state.Close()
 api:=&WebAPI{cfg:c,db:state,mode:*mode}
 if c.ConfigDB!=""{
  db,e:=sql.Open("sqlite",c.ConfigDB);if e!=nil{return e};defer db.Close()
  if e=db.QueryRow("SELECT CASE WHEN value='null' THEN '' ELSE trim(value,char(34)) END FROM settings WHERE key='web.token_hash'").Scan(&api.tokenHash);e!=nil&&e!=sql.ErrNoRows{return e}
 }
 if !isLoopbackHost(host)&&api.tokenHash==""{return errors.New("监听非本机地址之前必须先在 Python Web 配置库设置访问口令")}
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){writeJSON(w,map[string]any{"ok":true,"version":"go-dev"})})
 mux.HandleFunc("GET /",func(w http.ResponseWriter,r *http.Request){if r.URL.Path!="/" {http.NotFound(w,r);return};w.Header().Set("Content-Type","text/html; charset=utf-8");_,_=io.WriteString(w,webUIHTML)})
 mux.HandleFunc("GET /api/plans",api.plansList)
 mux.HandleFunc("POST /api/plans",api.planCreate)
 mux.HandleFunc("GET /api/plans/{id}",api.planGet)
 mux.HandleFunc("PUT /api/plans/{id}",api.planUpdate)
 mux.HandleFunc("DELETE /api/plans/{id}",api.planDelete)
 mux.HandleFunc("GET /api/plans/{id}/export",api.planExport)
 mux.HandleFunc("POST /api/plans/import",api.planImport)
 mux.HandleFunc("POST /api/plans/{id}/apply",api.planApply)
 mux.HandleFunc("PUT /api/auth/token",api.authToken)
 mux.HandleFunc("PUT /api/config/settings",api.configSettings)
 mux.HandleFunc("POST /api/config/test",api.configTest)
 mux.HandleFunc("PUT /api/config/tasks",api.configTasks)
 mux.HandleFunc("POST /api/config/tasks/reset",api.configTasksReset)
 mux.HandleFunc("GET /api/config/disks/detect",api.configDisksDetect)
 mux.HandleFunc("PUT /api/config/disks",api.configDisks)
 mux.HandleFunc("POST /api/config/disks/check",api.configDisksCheck)
 mux.HandleFunc("GET /api/config",api.config)
 mux.HandleFunc("GET /api/status",api.status)
 mux.HandleFunc("POST /api/run",api.run)
 mux.HandleFunc("GET /api/episodes",api.episodes)
 mux.HandleFunc("GET /api/runs/{id}/episodes",api.runEpisodes)
 mux.HandleFunc("GET /api/runs/{id}/export.csv",api.exportRunCSV)
 mux.HandleFunc("GET /api/logs",api.logs)
 mux.HandleFunc("POST /api/stop",api.stop)
 handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  w.Header().Set("X-Content-Type-Options","nosniff");w.Header().Set("X-Frame-Options","DENY");w.Header().Set("Referrer-Policy","no-referrer")
  if strings.HasPrefix(r.URL.Path,"/api/"){w.Header().Set("Cache-Control","no-store");if (r.Method=="POST"||r.Method=="PUT"||r.Method=="DELETE")&&!strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"),";")[0]),"application/json"){writeJSONStatus(w,415,map[string]any{"error":"写操作必须使用 Content-Type: application/json"});return};if !api.authorized(r){writeJSONStatus(w,401,map[string]any{"error":"口令缺失或错误"});return}}
  mux.ServeHTTP(w,r)
 })
 addr:=net.JoinHostPort(host,strconv.Itoa(port));log.Printf("Go Web API listening on http://%s",addr)
 server:=&http.Server{Addr:addr,Handler:handler,ReadHeaderTimeout:5*time.Second,ReadTimeout:15*time.Second,WriteTimeout:30*time.Second,IdleTimeout:60*time.Second}
 return server.ListenAndServe()
}
func isMountedPath(name string)bool{
 parts,err:=disk.Partitions(false);if err!=nil{return false};target:=filepath.Clean(name)
 for _,p:=range parts{mount:=filepath.Clean(p.Mountpoint);if runtime.GOOS=="windows"{if strings.EqualFold(target,mount){return true}}else if target==mount{return true}}
 return false
}
func isLoopbackHost(host string)bool{ip:=net.ParseIP(host);return host=="localhost"||(ip!=nil&&ip.IsLoopback())}
func isLoopbackRemote(addr string)bool{host,_,err:=net.SplitHostPort(addr);return err==nil&&isLoopbackHost(host)}
func writeJSON(w http.ResponseWriter,v any){writeJSONStatus(w,http.StatusOK,v)}
func writeJSONStatus(w http.ResponseWriter,status int,v any){w.Header().Set("Content-Type","application/json; charset=utf-8");w.WriteHeader(status);_ = json.NewEncoder(w).Encode(v)}
func(a *WebAPI)authorized(r *http.Request)bool{
 if origin:=r.Header.Get("Origin");origin!=""{u,err:=url.Parse(origin);if err!=nil||!strings.EqualFold(u.Host,r.Host){return false}}
 supplied:=strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"),"Bearer "));addr:=r.RemoteAddr;now:=time.Now()
 a.authMu.Lock()
 tokenHash:=a.tokenHash
 if tokenHash==""{a.authMu.Unlock();host := r.Host
  if parsed, _, err := net.SplitHostPort(host); err == nil {
   host = parsed
  } else {
   host = strings.Trim(host, "[]")
  }
  return isLoopbackHost(host)&&isLoopbackRemote(r.RemoteAddr)&&r.Header.Get("X-Forwarded-For")==""}
 if a.lockouts!=nil{if until:=a.lockouts[addr];now.Before(until){a.authMu.Unlock();return false}}
 if a.failures==nil{a.failures=map[string][]time.Time{}};if a.lockouts==nil{a.lockouts=map[string]time.Time{}}
 attempts:=a.failures[addr];kept:=attempts[:0];for _,at:=range attempts{if now.Sub(at)<=5*time.Minute{kept=append(kept,at)}};attempts=kept
 a.failures[addr]=attempts
 if len(attempts)>=8&&now.Sub(attempts[len(attempts)-1])<60*time.Second{a.lockouts[addr]=attempts[len(attempts)-1].Add(60*time.Second);a.authMu.Unlock();return false}
 a.authMu.Unlock()
 parts:=strings.Split(tokenHash,"$");if len(parts)!=4||parts[0]!="pbkdf2_sha256"{return false}
 iterations,err:=strconv.Atoi(parts[1]);if err!=nil||iterations<1||iterations>200000{return false}
 salt,err:=hex.DecodeString(parts[2]);if err!=nil{return false};want,err:=hex.DecodeString(parts[3]);if err!=nil{return false}
 got:=pbkdf2SHA256([]byte(supplied),salt,iterations,len(want));valid:=hmac.Equal(got,want)
 a.authMu.Lock();defer a.authMu.Unlock()
 if valid{delete(a.failures,addr);delete(a.lockouts,addr);return true}
 if supplied!=""{
  current:=a.failures[addr]
  recent:=current[:0]
  for _,at:=range current{if now.Sub(at)<=5*time.Minute{recent=append(recent,at)}}
  a.failures[addr]=append(recent,now)
  if len(a.failures[addr])>=8{a.lockouts[addr]=now.Add(60*time.Second)}
 }
 return false
}
func(a *WebAPI)authToken(w http.ResponseWriter,r *http.Request){
 if strings.TrimSpace(a.cfg.ConfigDB)==""{writeJSONStatus(w,400,map[string]any{"error":"未配置 SQLite 配置库，不能管理访问口令"});return}
 var body struct{Action string `json:"action"`;Token string `json:"token"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,16<<10)).Decode(&body);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 var encoded string
 switch body.Action{
 case "set":
  if len(body.Token)<12||len(body.Token)>1024{writeJSONStatus(w,400,map[string]any{"error":"新口令长度必须为 12 到 1024 字节"});return}
  hash,err:=newTokenHash(body.Token);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"生成口令摘要失败"});return}
  raw,err:=json.Marshal(hash);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"编码口令摘要失败"});return};encoded=string(raw)
 case "clear":
  if body.Token!=""{writeJSONStatus(w,400,map[string]any{"error":"clear 操作不接受 token 字段"});return}
  encoded="null"
 default:
  writeJSONStatus(w,400,map[string]any{"error":"action 只能是 set 或 clear"});return
 }
 db,err:=sql.Open("sqlite",a.cfg.ConfigDB);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"打开配置库失败"});return};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)");err!=nil{writeJSONStatus(w,500,map[string]any{"error":"初始化设置表失败"});return}
 tx,err:=db.Begin();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"开始口令更新事务失败"});return};defer tx.Rollback()
 if _,err=tx.Exec("INSERT INTO settings(key,value,updated_at) VALUES('web.token_hash',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",encoded,controlTimestamp());err!=nil{writeJSONStatus(w,500,map[string]any{"error":"保存口令摘要失败"});return}
 if err=tx.Commit();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"提交口令更新失败"});return}
 var hash string
 if body.Action!="clear"{if err=json.Unmarshal([]byte(encoded),&hash);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取口令摘要失败"});return}}
 a.authMu.Lock()
 a.tokenHash=hash
 a.failures=map[string][]time.Time{}
 a.lockouts=map[string]time.Time{}
 tokenSet:=a.tokenHash!=""
 a.authMu.Unlock()
 writeJSON(w,map[string]any{"ok":true,"token_set":tokenSet,"action":body.Action})
}
func newTokenHash(token string)(string,error){
 salt:=make([]byte,16);if _,err:=rand.Read(salt);err!=nil{return "",err}
 const iterations=120000
 digest:=pbkdf2SHA256([]byte(token),salt,iterations,32)
 return fmt.Sprintf("pbkdf2_sha256$%d$%s$%s",iterations,hex.EncodeToString(salt),hex.EncodeToString(digest)),nil
}
func pbkdf2SHA256(password,salt []byte,iterations,keyLen int)[]byte{
 hLen:=sha256.Size;blocks:=(keyLen+hLen-1)/hLen;out:=make([]byte,0,blocks*hLen)
 for block:=1;block<=blocks;block++{mac:=hmac.New(sha256.New,password);_,_=mac.Write(salt);var counter [4]byte;counter[0]=byte(block>>24);counter[1]=byte(block>>16);counter[2]=byte(block>>8);counter[3]=byte(block);_,_=mac.Write(counter[:]);u:=mac.Sum(nil);t:=append([]byte(nil),u...);for i:=1;i<iterations;i++{mac=hmac.New(sha256.New,password);_,_=mac.Write(u);u=mac.Sum(nil);for j:=range t{t[j]^=u[j]}};out=append(out,t...)}
 return out[:keyLen]
}
func(a *WebAPI)updateTransferSample(now time.Time,downloaded,expected int64)(float64,any,float64){
 a.statusMu.Lock();defer a.statusMu.Unlock()
 if downloaded<0{downloaded=0};if expected<0{expected=0}
 if a.statusSampleAt.IsZero()||now.Sub(a.statusSampleAt)<=0||downloaded<a.statusSampleBytes{a.statusSampleAt=now;a.statusSampleBytes=downloaded;a.statusRateBps=0}else{elapsed:=now.Sub(a.statusSampleAt).Seconds();if elapsed>0{a.statusRateBps=float64(downloaded-a.statusSampleBytes)/elapsed};a.statusSampleAt=now;a.statusSampleBytes=downloaded}
 var eta any=nil;remaining:=expected-downloaded;if remaining<0{remaining=0};if a.statusRateBps>0&&remaining>0{eta=int64(math.Ceil(float64(remaining)/a.statusRateBps))}else if expected>0&&remaining==0{eta=int64(0)}
 percent:=0.0;if expected>0{percent=math.Min(100,float64(downloaded)/float64(expected)*100)}
 return a.statusRateBps,eta,math.Round(percent*100)/100
}
func maxInt64(a,b int64)int64{if a>b{return a};return b}
func(a *WebAPI)status(w http.ResponseWriter,r *http.Request){
 scenes:=[]map[string]any{};sceneIndex:=map[string]int{};taskDisk:=map[string]string{};staleRecords:=0
 rowsAssign,err:=a.db.Query("SELECT task_name,disk_name FROM task_disk_assignments");if err==nil{for rowsAssign.Next(){var task,diskName string;if rowsAssign.Scan(&task,&diskName)==nil{taskDisk[task]=diskName}};rowsAssign.Close()}
 for _,t:=range a.cfg.Tasks{
  idx,ok:=sceneIndex[t.Scene];if !ok{idx=len(scenes);sceneIndex[t.Scene]=idx;scenes=append(scenes,map[string]any{"scene":t.Scene,"tasks":[]map[string]any{}})}
  rows:=scenes[idx]["tasks"].([]map[string]any);count:=0;seconds:=0.0;bytesCount:=int64(0);issues:=map[string]int{}
  records,e:=a.db.Query("SELECT content_time,file_size,target_path FROM downloaded_episodes WHERE task_name=?",t.Name)
  if e==nil{for records.Next(){var sec sql.NullFloat64;var size sql.NullInt64;var target sql.NullString;if records.Scan(&sec,&size,&target)!=nil{continue};if target.Valid&&a.completedPath(target.String){count++;seconds+=sec.Float64;bytesCount+=size.Int64}else if target.Valid{staleRecords++}};records.Close()}
  issueRows,e:=a.db.Query("SELECT status,COUNT(*) FROM downloads WHERE task_name=? AND status<>'completed' GROUP BY status",t.Name)
  if e==nil{for issueRows.Next(){var state string;var n int;if issueRows.Scan(&state,&n)==nil{issues[state]=n}};issueRows.Close()}
  var assigned any=nil;if name,ok:=taskDisk[t.Name];ok{assigned=name};aliases:=t.Aliases;if aliases==nil{aliases=[]string{}};tags:=t.Tags;if tags==nil{tags=[]string{}}
  rows=append(rows,map[string]any{"name":t.Name,"dir":taskDir(t),"target":t.TargetCount,"target_hours":t.TargetHours,"aliases":aliases,"tags":tags,"downloaded":count,"hours":math.Round(seconds/3600*100)/100,"bytes":bytesCount,"issues":issues,"disk":assigned});scenes[idx]["tasks"]=rows
 }
 disks:=[]map[string]any{}
 diskTasks:=map[string][]string{};for task,name:=range taskDisk{diskTasks[name]=append(diskTasks[name],task)}
 for _,d:=range a.cfg.Disks{usage,e:=disk.Usage(d.Path);item:=map[string]any{"name":d.Name,"path":d.Path,"mounted":isMountedPath(d.Mountpoint),"exists":pathIsDir(d.Path),"total":uint64(0),"free":uint64(0),"used":uint64(0),"tasks":diskTasks[d.Name]};if e==nil{item["total"]=usage.Total;item["free"]=usage.Free;item["used"]=usage.Used};disks=append(disks,item)}
 pid,running,byWeb:=a.activeDownloader();var pidValue any=nil;if running{pidValue=pid};state:=map[string]any{"running":running,"pid":pidValue,"by_web":byWeb}
 control:=map[string]string{};for _,key:=range []string{"stop_requested","last_run"}{var v string;if a.db.QueryRow("SELECT value FROM control WHERE key=?",key).Scan(&v)==nil{control[key]=v}}
 var last any=map[string]any{};if raw:=control["last_run"];raw!=""{var v map[string]any;if json.Unmarshal([]byte(raw),&v)==nil{last=v}}
 runningEpisodes:=[]map[string]any{};runningBytes:=int64(0);expectedBytes:=int64(0)
 if running{rr,e:=a.db.Query("SELECT episode_id,task_name,disk_name,started_at,expected_bytes,partial_path FROM downloads WHERE status='running' ORDER BY updated_at DESC");if e==nil{for rr.Next(){var id,taskName string;var diskName,started,partial sql.NullString;var expected sql.NullInt64;if rr.Scan(&id,&taskName,&diskName,&started,&expected,&partial)==nil{partialBytes:=int64(0);if partial.Valid{partialBytes,_=directorySizeBytes(partial.String)};runningBytes+=partialBytes;if expected.Valid&&expected.Int64>0{expectedBytes+=expected.Int64};if len(runningEpisodes)<24{runningEpisodes=append(runningEpisodes,map[string]any{"episode_id":id,"task_name":taskName,"disk_name":diskName.String,"started_at":started.String,"expected_bytes":expected.Int64,"partial_bytes":partialBytes})}}};rr.Close()}}
 sampledAt:=time.Now();speedBps,etaSeconds,progressPercent:=a.updateTransferSample(sampledAt,runningBytes,expectedBytes)
 historySummary:=map[string]any{"completed":int64(0),"failed":int64(0),"paused":int64(0),"pending":int64(0),"completed_bytes":int64(0),"content_seconds":float64(0)}
 statusRows,summaryErr:=a.db.Query("SELECT status,COUNT(*) FROM downloads GROUP BY status")
 if summaryErr==nil{for statusRows.Next(){var status string;var count int64;if statusRows.Scan(&status,&count)==nil{switch status{case "failed":historySummary["failed"]=count;case "paused":historySummary["paused"]=count;case "pending":historySummary["pending"]=count}}};statusRows.Close()}
 diskTotals:=map[string]map[string]any{}
 ledgerRows,ledgerErr:=a.db.Query("SELECT COALESCE(disk_name,''),file_size,content_time,target_path FROM downloaded_episodes")
 if ledgerErr==nil{for ledgerRows.Next(){var name string;var size sql.NullInt64;var seconds sql.NullFloat64;var target sql.NullString;if ledgerRows.Scan(&name,&size,&seconds,&target)!=nil{continue};if !target.Valid||!a.completedPath(target.String){continue};historySummary["completed"]=historySummary["completed"].(int64)+1;historySummary["completed_bytes"]=historySummary["completed_bytes"].(int64)+size.Int64;historySummary["content_seconds"]=historySummary["content_seconds"].(float64)+seconds.Float64;item:=diskTotals[name];if item==nil{item=map[string]any{"disk_name":name,"count":int64(0),"bytes":int64(0),"seconds":float64(0)};diskTotals[name]=item};item["count"]=item["count"].(int64)+1;item["bytes"]=item["bytes"].(int64)+size.Int64;item["seconds"]=item["seconds"].(float64)+seconds.Float64};ledgerRows.Close()}
 diskNames:=make([]string,0,len(diskTotals));for name:=range diskTotals{diskNames=append(diskNames,name)};sort.Strings(diskNames)
 diskHistory:=make([]map[string]any,0,len(diskNames));for _,name:=range diskNames{diskHistory=append(diskHistory,diskTotals[name])}
 historyDays:=strings.TrimSpace(r.URL.Query().Get("history_days"));if historyDays!="7"&&historyDays!="30"&&historyDays!="90"&&historyDays!="365"&&historyDays!="all"{historyDays="30"}
 historyModifier:="-"+historyDays+" days";runHistory:=[]map[string]any{}
 historyRows,historyErr:=a.db.Query("SELECT run_id,started_at,finished_at,duration_seconds,mode,result,completed_count,completed_bytes,content_seconds,COALESCE(error,'') FROM run_history WHERE (?='all' OR julianday(started_at)>=julianday('now',?)) ORDER BY started_at DESC LIMIT 50",historyDays,historyModifier)
 if historyErr==nil{for historyRows.Next(){var id,started,finished,mode,result,detail string;var elapsed,contentSeconds float64;var count,bytes int64;if historyRows.Scan(&id,&started,&finished,&elapsed,&mode,&result,&count,&bytes,&contentSeconds,&detail)==nil{if result=="running"{if parsed,e:=time.Parse(time.RFC3339Nano,started);e==nil{elapsed=time.Since(parsed).Seconds()}};runHistory=append(runHistory,map[string]any{"run_id":id,"started_at":started,"finished_at":finished,"duration_seconds":elapsed,"mode":mode,"result":result,"completed_count":count,"completed_bytes":bytes,"content_seconds":contentSeconds,"error":detail})}};historyRows.Close()}
 for _,run:=range runHistory{
  tasks:=[]map[string]any{};taskRows,taskErr:=a.db.Query("SELECT task_name,completed_count,failed_count,completed_bytes,content_seconds,wait_seconds,duration_seconds FROM run_task_history WHERE run_id=? ORDER BY task_name",run["run_id"]);if taskErr==nil{for taskRows.Next(){var name string;var count,failed,bytes int64;var seconds,waitSeconds,durationSeconds float64;if taskRows.Scan(&name,&count,&failed,&bytes,&seconds,&waitSeconds,&durationSeconds)==nil{attempts:=count+failed;rate:=float64(0);if attempts>0{rate=float64(count)/float64(attempts)*100};throughput:=float64(0);if durationSeconds>0{throughput=float64(bytes)/durationSeconds};tasks=append(tasks,map[string]any{"task_name":name,"completed_count":count,"failed_count":failed,"success_rate":math.Round(rate*10)/10,"completed_bytes":bytes,"content_seconds":seconds,"wait_seconds":waitSeconds,"duration_seconds":durationSeconds,"throughput_bytes_per_second":throughput})}};taskRows.Close()};run["tasks"]=tasks
  episodeStats:=map[string]any{"total":int64(0),"completed":int64(0),"failed":int64(0),"paused":int64(0),"failure_categories":map[string]int64{}}
  categoryStats:=map[string]int64{};episodeRows,episodeErr:=a.db.Query("SELECT status,COALESCE(failure_category,''),COUNT(*) FROM run_episode_history WHERE run_id=? GROUP BY status,failure_category",run["run_id"])
  if episodeErr==nil{for episodeRows.Next(){var state,category string;var count int64;if episodeRows.Scan(&state,&category,&count)==nil{episodeStats["total"]=episodeStats["total"].(int64)+count;switch state{case "completed":episodeStats["completed"]=episodeStats["completed"].(int64)+count;case "failed":episodeStats["failed"]=episodeStats["failed"].(int64)+count;if category!=""{categoryStats[category]+=count};case "paused":episodeStats["paused"]=episodeStats["paused"].(int64)+count}}};episodeRows.Close()}
  episodeStats["failure_categories"]=categoryStats;run["episode_stats"]=episodeStats
  diskStats:=[]map[string]any{};diskRows,diskErr:=a.db.Query("SELECT COALESCE(disk_name,''),SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END),SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),SUM(CASE WHEN status='paused' THEN 1 ELSE 0 END),SUM(CASE WHEN status='completed' THEN completed_bytes ELSE 0 END),SUM(CASE WHEN status='completed' THEN content_seconds ELSE 0 END) FROM run_episode_history WHERE run_id=? GROUP BY COALESCE(disk_name,'') ORDER BY disk_name",run["run_id"])
  if diskErr==nil{for diskRows.Next(){var name string;var completed,failed,paused,bytes int64;var seconds float64;if diskRows.Scan(&name,&completed,&failed,&paused,&bytes,&seconds)==nil{diskStats=append(diskStats,map[string]any{"disk_name":name,"completed":completed,"failed":failed,"paused":paused,"completed_bytes":bytes,"content_seconds":seconds})}};diskRows.Close()}
  diskTimingRows,diskTimingErr:=a.db.Query("SELECT disk_name,duration_seconds,completed_bytes FROM run_disk_history WHERE run_id=?",run["run_id"]);if diskTimingErr==nil{timing:=map[string]map[string]any{};for diskTimingRows.Next(){var name string;var elapsed float64;var bytes int64;if diskTimingRows.Scan(&name,&elapsed,&bytes)==nil{rate:=float64(0);if elapsed>0{rate=float64(bytes)/elapsed};timing[name]=map[string]any{"duration_seconds":elapsed,"throughput_bytes_per_second":rate}}};diskTimingRows.Close();for _,item:=range diskStats{if v:=timing[item["disk_name"].(string)];v!=nil{item["duration_seconds"]=v["duration_seconds"];item["throughput_bytes_per_second"]=v["throughput_bytes_per_second"]}}}
  run["disk_stats"]=diskStats
 }
 recentFailures:=[]map[string]any{}
 failedRows,failedErr:=a.db.Query("SELECT episode_id,task_name,COALESCE(error,''),updated_at FROM downloads WHERE status='failed' ORDER BY updated_at DESC LIMIT 8")
 if failedErr==nil{for failedRows.Next(){var id,taskName,detail,updated string;if failedRows.Scan(&id,&taskName,&detail,&updated)==nil{recentFailures=append(recentFailures,map[string]any{"episode_id":id,"task_name":taskName,"error":detail,"updated_at":updated})}};failedRows.Close()}
 issues:=[]map[string]string{};if a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""||a.cfg.Bucket==""{issues=append(issues,map[string]string{"page":"connection","text":"请在连接配置中填写完整的 MinIO 地址、密钥和 Bucket"})};if a.cfg.PGHost==""||a.cfg.PGDB==""||a.cfg.PGUser==""||a.cfg.PGPassword==""{issues=append(issues,map[string]string{"page":"connection","text":"请在连接配置中填写完整的 PostgreSQL 连接信息"})};if len(a.cfg.Disks)==0{issues=append(issues,map[string]string{"page":"disks","text":"尚未配置目标磁盘"})};if len(a.cfg.Tasks)==0{issues=append(issues,map[string]string{"page":"tasks","text":"尚未配置下载任务"})}
 endpoint:=a.cfg.Endpoint;if u,e:=url.Parse(endpoint);e==nil&&u.Host!=""{endpoint=u.Scheme+"://"+u.Host}
 settings:=map[string]any{"min_free_gb":a.cfg.MinFreeGB,"writes_per_disk":a.cfg.Workers,"require_mcap":a.cfg.RequireMCAP,"mcap_pattern":a.cfg.MCAPPattern,"default_rounds":a.cfg.DefaultRounds}
 writeJSON(w,map[string]any{"version":"go-dev","data_version":a.cfg.DataVersion,"downloader":state,"stop_requested":running&&isStopRequested(control["stop_requested"]),"setup_issues":issues,"minio":map[string]any{"endpoint":endpoint,"bucket":a.cfg.Bucket},"settings":settings,"last_run":last,"history_summary":historySummary,"disk_history":diskHistory,"run_history":runHistory,"recent_failures":recentFailures,"stale_records":staleRecords,"running_episodes":runningEpisodes,"transfer_progress":map[string]any{"downloaded_bytes":runningBytes,"expected_bytes":expectedBytes,"remaining_bytes":maxInt64(expectedBytes-runningBytes,0),"speed_bytes_per_second":math.Round(speedBps),"eta_seconds":etaSeconds,"progress_percent":progressPercent},"disks":disks,"scenes":scenes})
}
func webConfigSchema() []map[string]any {
 return []map[string]any{
  {"key":"minio.endpoint","kind":"str","group":"minio","label":"服务地址","help":"例如 http://192.168.1.10:9000","secret":false,"default":"","placeholder":"http://127.0.0.1:9000"},
  {"key":"minio.access_key","kind":"str","group":"minio","label":"Access Key","secret":false,"default":""},
  {"key":"minio.secret_key","kind":"str","group":"minio","label":"Secret Key","help":"保存后不会再显示；留空表示不修改","secret":true,"default":nil},
  {"key":"minio.secure","kind":"bool","group":"minio","label":"使用 HTTPS","secret":false,"default":true},
  {"key":"minio.cert_check","kind":"bool","group":"minio","label":"校验证书","secret":false,"default":true},
  {"key":"minio.region","kind":"str","group":"minio","label":"区域","secret":false,"default":""},
  {"key":"minio.bucket","kind":"str","group":"minio","label":"Bucket","secret":false,"default":"rocp-data"},
  {"key":"pg.host","kind":"str","group":"pg","label":"主机","secret":false,"default":"127.0.0.1"},
  {"key":"pg.port","kind":"int","group":"pg","label":"端口","secret":false,"min":1,"max":65535,"default":5432},
  {"key":"pg.database","kind":"str","group":"pg","label":"数据库","secret":false,"default":"minio_event"},
  {"key":"pg.user","kind":"str","group":"pg","label":"用户名","secret":false,"default":"postgres"},
  {"key":"pg.password","kind":"str","group":"pg","label":"密码","help":"保存后不会再显示；留空表示不修改","secret":true,"default":nil},
  {"key":"run.min_free_gb","kind":"float","group":"run","label":"每块盘保留空间（GiB）","secret":false,"min":0,"max":1000000,"default":3.0},
  {"key":"run.space_safety_factor","kind":"float","group":"run","label":"空间安全系数","secret":false,"min":1,"max":5,"default":1.15},
  {"key":"run.max_writes_per_disk","kind":"int","group":"run","label":"每块盘并发写入数","secret":false,"min":1,"max":16,"default":2},
  {"key":"run.require_mcap","kind":"bool","group":"run","label":"要求每条数据含 mcap","secret":false,"default":true},
  {"key":"run.include_null_quality","kind":"bool","group":"run","label":"包含质量状态为空的数据","secret":false,"default":false},
  {"key":"run.mcap_pattern","kind":"str","group":"run","label":"mcap 文件名规则","secret":false,"default":"*.SYNC.mcap"},
  {"key":"run.validate_workers","kind":"int","group":"run","label":"检查 MinIO 目录的并发数","secret":false,"min":1,"max":64,"default":16},
  {"key":"run.default_rounds","kind":"int","group":"run","label":"默认补齐轮数","secret":false,"min":1,"max":50,"default":1},
  {"key":"transfer.file_workers","kind":"int","group":"transfer","label":"单条数据内并行文件数","secret":false,"min":1,"max":16,"default":4},
  {"key":"transfer.file_retries","kind":"int","group":"transfer","label":"单个文件最大尝试次数","secret":false,"min":1,"max":20,"default":5},
  {"key":"transfer.retry_delay","kind":"float","group":"transfer","label":"重试间隔基数（秒）","secret":false,"min":0,"max":60,"default":1.0},
  {"key":"transfer.connect_timeout","kind":"int","group":"transfer","label":"连接超时（秒）","secret":false,"min":1,"max":300,"default":10},
  {"key":"transfer.read_timeout","kind":"int","group":"transfer","label":"读取超时（秒）","secret":false,"min":5,"max":3600,"default":120},
  {"key":"transfer.fsync","kind":"bool","group":"transfer","label":"改名前 fsync","secret":false,"default":true},
  {"key":"transfer.verify_md5","kind":"bool","group":"transfer","label":"校验 MD5","secret":false,"default":true},
  {"key":"web.host","kind":"str","group":"web","label":"监听地址","secret":false,"default":"127.0.0.1"},
  {"key":"web.port","kind":"int","group":"web","label":"监听端口","secret":false,"min":1,"max":65535,"default":8765},
 }
}

type planTask struct {
 Name string `json:"name"`
 Count int `json:"count"`
 Hours float64 `json:"target_hours"`
 Aliases []string `json:"aliases"`
 Dir string `json:"dir"`
 Tags []string `json:"tags"`
}
type planScene struct {
 Scene string `json:"scene"`
 Tasks []planTask `json:"tasks"`
}
type planPayload struct {
 DataVersion string `json:"data_version"`
 Scenes []planScene `json:"scenes"`
}
func currentPlanPayload(c Config) planPayload {
 out:=planPayload{DataVersion:c.DataVersion,Scenes:[]planScene{}}
 index:=map[string]int{}
 for _,t:=range c.Tasks {
  i,ok:=index[t.Scene];if !ok{i=len(out.Scenes);index[t.Scene]=i;out.Scenes=append(out.Scenes,planScene{Scene:t.Scene,Tasks:[]planTask{}})}
  aliases:=append([]string{},t.Aliases...);tags:=append([]string{},t.Tags...)
  out.Scenes[i].Tasks=append(out.Scenes[i].Tasks,planTask{Name:t.Name,Count:t.TargetCount,Hours:t.TargetHours,Aliases:aliases,Dir:t.Dir,Tags:tags})
 }
 return out
}
func validatePlanPayload(raw json.RawMessage)(planPayload,error){
 var p planPayload
 if err:=json.Unmarshal(raw,&p);err!=nil{return p,fmt.Errorf("任务内容 JSON 无效：%w",err)}
 // Python plans allow an empty data_version; apply(replace) inherits the current value.
 p.DataVersion=strings.TrimSpace(p.DataVersion)
 if p.Scenes==nil{return p,errors.New("任务内容格式不对（需要 data_version 和 scenes）")}
 seenTasks:=map[string]bool{};seenAliases:=map[string]string{};dirOwners:=map[string]string{};sceneSeen:=map[string]bool{}
 for si:=range p.Scenes {
  scene:=&p.Scenes[si];scene.Scene=strings.TrimSpace(scene.Scene)
  if scene.Scene==""{return p,errors.New("场景名不能为空")}
  if sceneSeen[scene.Scene]{return p,fmt.Errorf("场景名重复：%q",scene.Scene)};sceneSeen[scene.Scene]=true
  if scene.Tasks==nil{scene.Tasks=[]planTask{}}
  for ti:=range scene.Tasks {
   t:=&scene.Tasks[ti];t.Name=strings.TrimSpace(t.Name)
   if t.Name==""{return p,fmt.Errorf("场景 %q 下有任务名为空",scene.Scene)}
   if seenTasks[t.Name]{return p,fmt.Errorf("任务名重复：%q（任务名必须全局唯一）",t.Name)}
   seenTasks[t.Name]=true
   if t.Count<0{return p,fmt.Errorf("任务 %q 的目标条数必须是非负整数，当前为 %d",t.Name,t.Count)}
   if t.Hours<0||math.IsNaN(t.Hours)||math.IsInf(t.Hours,0){return p,fmt.Errorf("任务 %q 的目标时长必须是有效的非负数字",t.Name)}
   t.Dir=strings.TrimSpace(t.Dir);dirName:=t.Dir;if dirName==""{dirName=t.Name}
   dirKey:=scene.Scene+"\x00"+dirName;if owner,ok:=dirOwners[dirKey];ok{return p,fmt.Errorf("场景 %q 下目录名 %q 被任务 %q 和 %q 同时使用",scene.Scene,t.Dir,owner,t.Name)};dirOwners[dirKey]=t.Name
   if t.Aliases==nil{t.Aliases=[]string{}};if t.Tags==nil{t.Tags=[]string{}}
   aliasSeen:=map[string]bool{}
   for i,a:=range t.Aliases {a=strings.TrimSpace(a);if a==""{return p,fmt.Errorf("任务 %q 存在空别名",t.Name)};if aliasSeen[a]{return p,fmt.Errorf("任务 %q 的别名重复：%q",t.Name,a)};aliasSeen[a]=true;if owner,ok:=seenAliases[a];ok&&owner!=t.Name{return p,fmt.Errorf("别名 %q 重复出现在任务 %q 和 %q 中",a,owner,t.Name)};seenAliases[a]=t.Name;t.Aliases[i]=a}
   tagSeen:=map[string]bool{}
   for i,tag:=range t.Tags {tag=strings.TrimSpace(tag);if tag==""{continue};if len([]rune(tag))>24{return p,fmt.Errorf("标签太长（最多 24 个字符）：%s",tag)};if !tagSeen[tag]{tagSeen[tag]=true}else{t.Tags[i]=""}}
   clean:=make([]string,0,len(t.Tags));for _,tag:=range t.Tags{if tag!=""{clean=append(clean,tag)}};t.Tags=clean
   if len(t.Tags)>12{return p,fmt.Errorf("任务 %q 一个任务最多 12 个标签",t.Name)}
  }
 }
 for alias,owner:=range seenAliases{if seenTasks[alias]{return p,fmt.Errorf("别名 %q（任务 %q）与某个当前任务名冲突",alias,owner)}}
 return p,nil
}
func planSummary(p planPayload)map[string]any{
 tags:=[]string{};seen:=map[string]bool{};tasks:=0;target:=0
 for _,scene:=range p.Scenes{for _,t:=range scene.Tasks{tasks++;target+=t.Count;for _,tag:=range t.Tags{if !seen[tag]{seen[tag]=true;tags=append(tags,tag)}}}}
 return map[string]any{"scene_count":len(p.Scenes),"task_count":tasks,"total_target":target,"tags":tags}
}
func(a *WebAPI)openPlanDB()(*sql.DB,error){
 if strings.TrimSpace(a.cfg.ConfigDB)==""{return nil,errors.New("未配置 SQLite 配置库")}
 db,err:=sql.Open("sqlite",a.cfg.ConfigDB);if err!=nil{return nil,err};db.SetMaxOpenConns(1);_,_=db.Exec("PRAGMA busy_timeout=30000")
 _,err=db.Exec("CREATE TABLE IF NOT EXISTS plans(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL UNIQUE,note TEXT NOT NULL DEFAULT '',payload TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
 if err!=nil{db.Close();return nil,err};return db,nil
}
func planName(raw string)(string,error){
 name:=strings.TrimSpace(raw);if name==""{return "",errors.New("方案名称不能为空")};if len([]rune(name))>60{return "",errors.New("方案名称最长 60 个字符")};return name,nil
}
func(a *WebAPI)plansList(w http.ResponseWriter,r *http.Request){
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 rows,err:=db.Query("SELECT id,name,note,payload,created_at,updated_at FROM plans ORDER BY updated_at DESC,id DESC")
 if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取方案失败："+err.Error()});return};defer rows.Close()
 plans:=[]map[string]any{}
 for rows.Next(){var id int64;var name,note,raw,created,updated string;if err=rows.Scan(&id,&name,&note,&raw,&created,&updated);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取方案失败："+err.Error()});return}
  p,e:=validatePlanPayload(json.RawMessage(raw));if e!=nil{continue};item:=map[string]any{"id":id,"name":name,"note":note,"created_at":created,"updated_at":updated};for k,v:=range planSummary(p){item[k]=v};plans=append(plans,item)
 }
 if err=rows.Err();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取方案失败："+err.Error()});return}
 current:=currentPlanPayload(a.cfg)
 writeJSON(w,map[string]any{"plans":plans,"current":planSummary(current)})
}
func(a *WebAPI)planGet(w http.ResponseWriter,r *http.Request){
 id,err:=strconv.ParseInt(r.PathValue("id"),10,64);if err!=nil||id<=0{writeJSONStatus(w,400,map[string]any{"error":"方案 ID 无效"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 var name,note,raw,created,updated string;err=db.QueryRow("SELECT name,note,payload,created_at,updated_at FROM plans WHERE id=?",id).Scan(&name,&note,&raw,&created,&updated)
 if err==sql.ErrNoRows{writeJSONStatus(w,404,map[string]any{"error":"方案不存在"});return};if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取方案失败："+err.Error()});return}
 p,err:=validatePlanPayload(json.RawMessage(raw));if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"方案内容损坏："+err.Error()});return}
 out:=map[string]any{"id":id,"name":name,"note":note,"created_at":created,"updated_at":updated,"payload":p};for k,v:=range planSummary(p){out[k]=v};writeJSON(w,out)
}
func(a *WebAPI)planCreate(w http.ResponseWriter,r *http.Request){
 var body struct{Name string `json:"name"`;Note string `json:"note"`;Payload json.RawMessage `json:"payload"`;Overwrite bool `json:"overwrite"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,10<<20)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 name,err:=planName(body.Name);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 p:=currentPlanPayload(a.cfg);if len(body.Payload)>0&&string(body.Payload)!="null"{p,err=validatePlanPayload(body.Payload);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}}
 payload,err:=json.Marshal(p);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":"序列化任务内容失败"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 now:=time.Now().UTC().Format(time.RFC3339)
 tx,err:=db.Begin();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer tx.Rollback()
 var id int64
 e:=tx.QueryRow("SELECT id FROM plans WHERE name=?",name).Scan(&id)
 if e==nil{
  if !body.Overwrite{writeJSONStatus(w,409,map[string]any{"error":"已有同名方案："+name,"code":"exists"});return}
  note:=strings.TrimSpace(body.Note);_,err=tx.Exec("UPDATE plans SET payload=?,note=CASE WHEN ?<>'' THEN ? ELSE note END,updated_at=? WHERE id=?",string(payload),note,note,now,id)
 }else if e==sql.ErrNoRows{
  res,e2:=tx.Exec("INSERT INTO plans(name,note,payload,created_at,updated_at) VALUES(?,?,?,?,?)",name,strings.TrimSpace(body.Note),string(payload),now,now);err=e2;if err==nil{id,_=res.LastInsertId()}
 }else{err=e}
 if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"保存方案失败："+err.Error()});return}
 if err=tx.Commit();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"保存方案失败："+err.Error()});return}
 writeJSON(w,map[string]any{"ok":true,"id":id})
}
func(a *WebAPI)planUpdate(w http.ResponseWriter,r *http.Request){
 id,err:=strconv.ParseInt(r.PathValue("id"),10,64);if err!=nil||id<=0{writeJSONStatus(w,400,map[string]any{"error":"方案 ID 无效"});return}
 var body struct{Name *string `json:"name"`;Note *string `json:"note"`;Payload json.RawMessage `json:"payload"`;FromCurrent bool `json:"from_current"`}
 if err=json.NewDecoder(io.LimitReader(r.Body,10<<20)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 sets:=[]string{};args:=[]any{}
 if body.Name!=nil{name,e:=planName(*body.Name);if e!=nil{writeJSONStatus(w,400,map[string]any{"error":e.Error()});return};sets=append(sets,"name=?");args=append(args,name)}
 if body.Note!=nil{sets=append(sets,"note=?");args=append(args,strings.TrimSpace(*body.Note))}
 var payload planPayload;hasPayload:=false
 if len(body.Payload)>0&&string(body.Payload)!="null"{payload,err=validatePlanPayload(body.Payload);hasPayload=true}else if body.FromCurrent{payload=currentPlanPayload(a.cfg);hasPayload=true}
 if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if hasPayload{raw,_:=json.Marshal(payload);sets=append(sets,"payload=?");args=append(args,string(raw))}
 if len(sets)==0{writeJSONStatus(w,400,map[string]any{"error":"没有提供需要修改的字段"});return}
 sets=append(sets,"updated_at=?");args=append(args,time.Now().UTC().Format(time.RFC3339));args=append(args,id)
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 res,err:=db.Exec("UPDATE plans SET "+strings.Join(sets,",")+" WHERE id=?",args...)
 if err!=nil{if strings.Contains(strings.ToLower(err.Error()),"unique"){writeJSONStatus(w,409,map[string]any{"error":"已有同名方案"});return};writeJSONStatus(w,500,map[string]any{"error":"更新方案失败："+err.Error()});return}
 n,_:=res.RowsAffected();if n==0{var exists int;if db.QueryRow("SELECT 1 FROM plans WHERE id=?",id).Scan(&exists)==sql.ErrNoRows{writeJSONStatus(w,404,map[string]any{"error":"方案不存在或已被删除"});return}}
 writeJSON(w,map[string]any{"ok":true})
}
func(a *WebAPI)planDelete(w http.ResponseWriter,r *http.Request){
 id,err:=strconv.ParseInt(r.PathValue("id"),10,64);if err!=nil||id<=0{writeJSONStatus(w,400,map[string]any{"error":"方案 ID 无效"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 res,err:=db.Exec("DELETE FROM plans WHERE id=?",id);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"删除方案失败："+err.Error()});return};n,_:=res.RowsAffected();if n==0{writeJSONStatus(w,404,map[string]any{"error":"方案不存在或已被删除"});return};writeJSON(w,map[string]any{"ok":true})
}
func(a *WebAPI)planExport(w http.ResponseWriter,r *http.Request){
 id,err:=strconv.ParseInt(r.PathValue("id"),10,64);if err!=nil||id<=0{writeJSONStatus(w,400,map[string]any{"error":"方案 ID 无效"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 var name,note,raw string;err=db.QueryRow("SELECT name,note,payload FROM plans WHERE id=?",id).Scan(&name,&note,&raw);if err==sql.ErrNoRows{writeJSONStatus(w,404,map[string]any{"error":"方案不存在"});return};if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"导出方案失败："+err.Error()});return}
 p,err:=validatePlanPayload(json.RawMessage(raw));if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"方案内容损坏："+err.Error()});return}
 doc:=map[string]any{"format":"minio-downloader-plan","version":1,"name":name,"note":note,"payload":p};w.Header().Set("Content-Type","application/json; charset=utf-8");w.Header().Set("Content-Disposition","attachment; filename*=UTF-8''"+url.QueryEscape("方案-"+name)+".json");_ = json.NewEncoder(w).Encode(doc)
}

func persistPlanPayload(db *sql.DB,p planPayload)error{
 tx,err:=db.Begin();if err!=nil{return err};defer tx.Rollback()
 if _,err=tx.Exec("DELETE FROM task_aliases");err!=nil{return err}
 if _,err=tx.Exec("DELETE FROM tasks");err!=nil{return err}
 if _,err=tx.Exec("DELETE FROM scenes");err!=nil{return err}
 for si,scene:=range p.Scenes{
  res,e:=tx.Exec("INSERT INTO scenes(position,name) VALUES(?,?)",si,scene.Scene);if e!=nil{return e};sceneID,e:=res.LastInsertId();if e!=nil{return e}
  for ti,t:=range scene.Tasks{
   tags,_:=json.Marshal(t.Tags)
   res,e=tx.Exec("INSERT INTO tasks(scene_id,position,name,target_count,target_hours,directory,tags) VALUES(?,?,?,?,?,?,?)",sceneID,ti,t.Name,t.Count,t.Hours,t.Dir,string(tags));if e!=nil{return e};taskID,e:=res.LastInsertId();if e!=nil{return e}
   for ai,alias:=range t.Aliases{if _,e=tx.Exec("INSERT INTO task_aliases(task_id,position,alias) VALUES(?,?,?)",taskID,ai,alias);e!=nil{return e}}
  }
 }
 now:=time.Now().UTC().Format(time.RFC3339)
 if _,err=tx.Exec("INSERT INTO settings(key,value,updated_at) VALUES('tasks.data_version',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",mustJSON(p.DataVersion),now);err!=nil{return err}
 return tx.Commit()
}
func mustJSON(v any)string{b,_:=json.Marshal(v);return string(b)}
func planHasTasks(p planPayload)bool{for _,s:=range p.Scenes{if len(s.Tasks)>0{return true}};return false}
func(a *WebAPI)planApply(w http.ResponseWriter,r *http.Request){
 id,err:=strconv.ParseInt(r.PathValue("id"),10,64);if err!=nil||id<=0{writeJSONStatus(w,400,map[string]any{"error":"方案 ID 无效"});return}
 var body struct{Mode string `json:"mode"`;Tags []string `json:"tags"`;Resolutions map[string]string `json:"resolutions"`}
 if err=json.NewDecoder(io.LimitReader(r.Body,1<<20)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Mode==""{body.Mode="replace"};if body.Mode!="replace"&&body.Mode!="merge"{writeJSONStatus(w,400,map[string]any{"error":"mode 只能是 replace 或 merge"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 var raw string;if err=db.QueryRow("SELECT payload FROM plans WHERE id=?",id).Scan(&raw);err==sql.ErrNoRows{writeJSONStatus(w,404,map[string]any{"error":"方案不存在或已被删除"});return}else if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取方案失败："+err.Error()});return}
 incoming,err:=validatePlanPayload(json.RawMessage(raw));if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"方案内容损坏："+err.Error()});return}
 wanted:=map[string]bool{};for _,tag:=range body.Tags{tag=strings.TrimSpace(tag);if tag!=""{wanted[tag]=true}}
 if len(wanted)>0{filtered:=planPayload{DataVersion:incoming.DataVersion,Scenes:[]planScene{}};for _,scene:=range incoming.Scenes{kept:=[]planTask{};for _,t:=range scene.Tasks{match:=false;for _,tag:=range t.Tags{if wanted[tag]{match=true}};if match{kept=append(kept,t)}};if len(kept)>0{filtered.Scenes=append(filtered.Scenes,planScene{Scene:scene.Scene,Tasks:kept})}};incoming=filtered}
 if !planHasTasks(incoming){msg:="这个方案是空的";if len(wanted)>0{msg="方案里没有符合所选标签的任务"};writeJSONStatus(w,400,map[string]any{"error":msg});return}
 if body.Mode=="replace"&&incoming.DataVersion==""{incoming.DataVersion=strings.TrimSpace(a.cfg.DataVersion)}
 if body.Mode=="replace"&&incoming.DataVersion==""{writeJSONStatus(w,400,map[string]any{"error":"版本目录不能为空（方案未设置，当前配置也为空）"});return}
 result:=incoming;added:=[]string{};skipped:=[]map[string]string{}
 if body.Mode=="replace"&&result.DataVersion==""{result.DataVersion=a.cfg.DataVersion}
 if body.Mode=="merge"{
  current:=currentPlanPayload(a.cfg);result=planPayload{DataVersion:current.DataVersion,Scenes:append([]planScene{},current.Scenes...)}
  existing:=map[string]bool{};for _,scene:=range result.Scenes{for _,t:=range scene.Tasks{existing[t.Name]=true}}
  for _,scene:=range incoming.Scenes{for _,task:=range scene.Tasks{
   resolution:=strings.ToLower(strings.TrimSpace(body.Resolutions[task.Name]))
   if existing[task.Name]{
    if resolution!="replace"{reason:="当前配置里已有同名任务";if resolution=="keep"{reason="用户选择保留当前任务"}else if resolution=="skip"{reason="用户选择跳过方案任务"};skipped=append(skipped,map[string]string{"name":task.Name,"reason":reason});continue}
   }
   candidate:=planPayload{DataVersion:result.DataVersion,Scenes:[]planScene{}}
   for _,currentScene:=range result.Scenes{copyScene:=planScene{Scene:currentScene.Scene,Tasks:[]planTask{}};for _,currentTask:=range currentScene.Tasks{if existing[task.Name]&&currentTask.Name==task.Name{continue};copyScene.Tasks=append(copyScene.Tasks,currentTask)};if len(copyScene.Tasks)>0{candidate.Scenes=append(candidate.Scenes,copyScene)}}
   target:=-1;for i:=range candidate.Scenes{if candidate.Scenes[i].Scene==scene.Scene{target=i;break}}
   if target<0{candidate.Scenes=append(candidate.Scenes,planScene{Scene:scene.Scene,Tasks:[]planTask{}});target=len(candidate.Scenes)-1}
   candidate.Scenes[target].Tasks=append(candidate.Scenes[target].Tasks,task)
   if _,e:=validatePlanPayload(json.RawMessage(mustJSON(candidate)));e!=nil{skipped=append(skipped,map[string]string{"name":task.Name,"reason":e.Error()});continue}
   result=candidate;existing[task.Name]=true;added=append(added,task.Name)
  }}
 }else{for _,scene:=range incoming.Scenes{for _,task:=range scene.Tasks{added=append(added,task.Name)}}}
 if _,err=validatePlanPayload(json.RawMessage(mustJSON(result)));err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if body.Mode=="replace"||len(added)>0{
  if err=persistPlanPayload(db,result);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"应用方案失败："+err.Error()});return}
  tasks:=[]Task{};for _,scene:=range result.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,TargetHours:t.Hours,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}};a.cfg.Tasks=tasks;a.cfg.DataVersion=result.DataVersion
 }
 total:=0;for _,scene:=range result.Scenes{total+=len(scene.Tasks)}
 writeJSON(w,map[string]any{"ok":true,"mode":body.Mode,"added":len(added),"skipped":skipped,"total_tasks":total,"running":func()bool{_,running,_:=a.activeDownloader();return running}()})
}
func(a *WebAPI)planImport(w http.ResponseWriter,r *http.Request){
 var doc struct{Format string `json:"format"`;Version int `json:"version"`;Name string `json:"name"`;Note string `json:"note"`;Payload json.RawMessage `json:"payload"`;Plan json.RawMessage `json:"plan"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,10<<20)).Decode(&doc);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"方案 JSON 无效"});return}
 if len(doc.Plan)>0&&string(doc.Plan)!="null"{if err:=json.Unmarshal(doc.Plan,&doc);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"方案 JSON 无效"});return}}
 if len(doc.Payload)==0||string(doc.Payload)=="null"{writeJSONStatus(w,400,map[string]any{"error":"不是有效的方案文件（缺少 payload）"});return}
 p,err:=validatePlanPayload(doc.Payload);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 base:=strings.TrimSpace(doc.Name);if base==""{base="导入的方案"};runes:=[]rune(base);if len(runes)>54{base=string(runes[:54])}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 name:=base;for n:=2;n<100000;n++{var exists int;e:=db.QueryRow("SELECT 1 FROM plans WHERE name=?",name).Scan(&exists);if e==sql.ErrNoRows{break};if e!=nil{writeJSONStatus(w,500,map[string]any{"error":"检查方案名称失败："+e.Error()});return};suffix:=fmt.Sprintf(" (%d)",n);max:=60-len([]rune(suffix));rr:=[]rune(base);if len(rr)>max{rr=rr[:max]};name=string(rr)+suffix}
 payload,_:=json.Marshal(p);now:=time.Now().UTC().Format(time.RFC3339)
 res,err:=db.Exec("INSERT INTO plans(name,note,payload,created_at,updated_at) VALUES(?,?,?,?,?)",name,strings.TrimSpace(doc.Note),string(payload),now,now);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"导入方案失败："+err.Error()});return};id,_:=res.LastInsertId()
 writeJSON(w,map[string]any{"ok":true,"id":id,"name":name})
}


func decodeWebSettings(raw map[string]json.RawMessage)(map[string]any,error){
 schema:=webConfigSchema();allowed:=map[string]map[string]any{}
 for _,item:=range schema{key,_:=item["key"].(string);allowed[key]=item}
 clean:=map[string]any{}
 for key,data:=range raw{
  item,ok:=allowed[key];if !ok{return nil,fmt.Errorf("未知设置项：%s",key)}
  if item["secret"]==true&&string(data)=="null"{continue}
  kind,_:=item["kind"].(string)
  switch kind{
  case "str":
   var v string;if err:=json.Unmarshal(data,&v);err!=nil{return nil,fmt.Errorf("%s：必须是字符串",key)};clean[key]=v
  case "bool":
   var v bool;if err:=json.Unmarshal(data,&v);err!=nil{return nil,fmt.Errorf("%s：必须是布尔值",key)};clean[key]=v
  case "int","float":
   var v float64;if err:=json.Unmarshal(data,&v);err!=nil{return nil,fmt.Errorf("%s：必须是数字",key)}
   if min,ok:=item["min"].(int);ok&&v<float64(min){return nil,fmt.Errorf("%s：不能小于 %v",item["label"],min)}
   if max,ok:=item["max"].(int);ok&&v>float64(max){return nil,fmt.Errorf("%s：不能大于 %v",item["label"],max)}
   if kind=="int"&&v!=float64(int64(v)){return nil,fmt.Errorf("%s：必须是整数",item["label"])}
   if kind=="int"{clean[key]=int(v)}else{clean[key]=v}
  default:return nil,fmt.Errorf("不支持的设置类型：%s",kind)
  }
 }
 return clean,nil
}
func applyWebSetting(c *Config,key string,v any){
 switch key{
 case "minio.endpoint":c.Endpoint=v.(string)
 case "minio.access_key":c.AccessKey=v.(string)
 case "minio.secret_key":c.SecretKey=v.(string)
 case "minio.secure":c.SSL=v.(bool)
 case "minio.cert_check":c.MinioSkipVerify=!v.(bool)
 case "minio.region":c.Region=v.(string)
 case "minio.bucket":c.Bucket=v.(string)
 case "pg.host":c.PGHost=v.(string)
 case "pg.port":c.PGPort=v.(int)
 case "pg.database":c.PGDB=v.(string)
 case "pg.user":c.PGUser=v.(string)
 case "pg.password":c.PGPassword=v.(string)
 case "run.min_free_gb":c.MinFreeGB=v.(float64)
 case "run.space_safety_factor":c.SafetyFactor=v.(float64)
 case "run.max_writes_per_disk":c.Workers=v.(int)
 case "run.require_mcap":c.RequireMCAP=v.(bool)
 case "run.include_null_quality":c.IncludeNull=v.(bool)
 case "run.mcap_pattern":c.MCAPPattern=v.(string)
 case "run.validate_workers":c.ValidateWorkers=v.(int)
 case "transfer.file_workers":c.FileWorkers=v.(int)
 case "transfer.file_retries":c.FileRetries=v.(int)
 case "transfer.retry_delay":c.RetryDelaySeconds=v.(float64)
 case "transfer.verify_md5":b:=v.(bool);c.VerifyMD5=&b
 case "transfer.fsync":b:=v.(bool);c.Fsync=&b
 case "run.default_rounds":c.DefaultRounds=v.(int)
 case "transfer.connect_timeout":c.ConnectTimeout=v.(int)
 case "transfer.read_timeout":c.ReadTimeout=v.(int)
 case "web.host":c.WebHost=v.(string)
 case "web.port":c.WebPort=v.(int)
 }
}
func(a *WebAPI)configSettings(w http.ResponseWriter,r *http.Request){
 var body struct{Values map[string]json.RawMessage `json:"values"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,2<<20)).Decode(&body);err!=nil||body.Values==nil{writeJSONStatus(w,400,map[string]any{"error":"请求内容不是有效的 JSON"});return}
 clean,err:=decodeWebSettings(body.Values);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if strings.TrimSpace(a.cfg.ConfigDB)==""{writeJSONStatus(w,500,map[string]any{"error":"未配置 SQLite 配置库"});return}
 db,err:=sql.Open("sqlite",a.cfg.ConfigDB);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)");err!=nil{writeJSONStatus(w,500,map[string]any{"error":"初始化设置表失败："+err.Error()});return}
 tx,err:=db.Begin();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer tx.Rollback()
 saved:=[]string{}
 for key,value:=range clean{
  encoded,e:=json.Marshal(value);if e!=nil{writeJSONStatus(w,500,map[string]any{"error":"编码设置失败："+e.Error()});return}
  if _,e=tx.Exec("INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",key,string(encoded),controlTimestamp());e!=nil{writeJSONStatus(w,500,map[string]any{"error":"保存设置失败："+e.Error()});return}
  saved=append(saved,key)
 }
 if err=tx.Commit();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"提交设置失败："+err.Error()});return}
 sort.Strings(saved)
 for key,value:=range clean{applyWebSetting(&a.cfg,key,value)}
 _,running,_:=a.activeDownloader()
 writeJSON(w,map[string]any{"ok":true,"saved":saved,"running":running})
}
func(a *WebAPI)configTest(w http.ResponseWriter,r *http.Request){
 var body struct{Target string `json:"target"`;Values map[string]json.RawMessage `json:"values"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,2<<20)).Decode(&body);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Target!="minio"&&body.Target!="pg"{writeJSONStatus(w,400,map[string]any{"error":"未知的测试目标"});return}
 clean,err:=decodeWebSettings(body.Values);if err!=nil{writeJSON(w,map[string]any{"ok":false,"message":err.Error()});return}
 cfg:=a.cfg
 for key,value:=range clean{applyWebSetting(&cfg,key,value)}
 timeout:=time.Duration(cfg.ConnectTimeout)*time.Second;if timeout<=0{timeout=10*time.Second};ctx,cancel:=context.WithTimeout(r.Context(),timeout);defer cancel()
 if body.Target=="minio"{
  endpoint:=strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint,"https://"),"http://")
  secure:=cfg.SSL||strings.HasPrefix(cfg.Endpoint,"https://")
  transport:=minioHTTPTransport(cfg)
  client,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(cfg.AccessKey,cfg.SecretKey,""),Secure:secure,Region:cfg.Region,Transport:transport})
  if e==nil{var exists bool;exists,e=client.BucketExists(ctx,cfg.Bucket);if e==nil&&!exists{e=fmt.Errorf("MinIO bucket 不存在：%s",cfg.Bucket)}}
  if e!=nil{writeJSON(w,map[string]any{"ok":false,"message":fmt.Sprintf("%T: %v",e,e)});return}
  writeJSON(w,map[string]any{"ok":true,"message":fmt.Sprintf("连接成功：%s，bucket=%s",cfg.Endpoint,cfg.Bucket)});return
 }
 db,e:=sql.Open("pgx",postgresDSN(cfg))
 if e==nil{defer db.Close();e=db.PingContext(ctx)}
 if e==nil{var exists bool;e=db.QueryRowContext(ctx,"SELECT to_regclass('public.minio_galbot_scene_task_jobs') IS NOT NULL").Scan(&exists);if e==nil&&!exists{writeJSON(w,map[string]any{"ok":false,"message":"连接成功，但没有找到表 public.minio_galbot_scene_task_jobs"});return}}
 if e!=nil{writeJSON(w,map[string]any{"ok":false,"message":fmt.Sprintf("%T: %v",e,e)});return}
 writeJSON(w,map[string]any{"ok":true,"message":"连接成功，已找到任务数据表"})
}
func(a *WebAPI)configTasksReset(w http.ResponseWriter,r *http.Request){
 var defaults Config
 if err:=json.Unmarshal(defaultCountConfigJSON,&defaults);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取内置默认任务失败"});return}
 if defaults.DataVersion==""||len(defaults.Tasks)==0{writeJSONStatus(w,500,map[string]any{"error":"内置默认任务配置不完整"});return}
 oldTags:=map[string][]string{}
 for _,task:=range a.cfg.Tasks{if len(task.Tags)>0{oldTags[task.Name]=append([]string{},task.Tags...)}}
 scenes:=[]planScene{};sceneIndex:=map[string]int{}
 for _,task:=range defaults.Tasks{
  idx,ok:=sceneIndex[task.Scene]
  if !ok{idx=len(scenes);sceneIndex[task.Scene]=idx;scenes=append(scenes,planScene{Scene:task.Scene,Tasks:[]planTask{}})}
  scenes[idx].Tasks=append(scenes[idx].Tasks,planTask{Name:task.Name,Count:task.TargetCount,Aliases:append([]string{},task.Aliases...),Dir:task.Dir,Tags:append([]string{},oldTags[task.Name]...)})
 }
 payload:=planPayload{DataVersion:defaults.DataVersion,Scenes:scenes}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 if err=persistPlanPayload(db,payload);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"恢复默认任务失败："+err.Error()});return}
 a.cfg.Tasks=[]Task{}
 for _,scene:=range payload.Scenes{for _,task:=range scene.Tasks{a.cfg.Tasks=append(a.cfg.Tasks,Task{Scene:scene.Scene,Name:task.Name,TargetCount:task.Count,TargetHours:task.Hours,Aliases:append([]string{},task.Aliases...),Dir:task.Dir,Tags:append([]string{},task.Tags...)})}}
 a.cfg.DataVersion=payload.DataVersion
 writeJSON(w,map[string]any{"ok":true})
}
func(a *WebAPI)configTasks(w http.ResponseWriter,r *http.Request){
 var raw json.RawMessage
 if err:=json.NewDecoder(io.LimitReader(r.Body,10<<20)).Decode(&raw);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"任务配置 JSON 无效"});return}
 p,err:=validatePlanPayload(raw);if err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if p.DataVersion==""{writeJSONStatus(w,400,map[string]any{"error":"版本目录不能为空"});return}
 db,err:=a.openPlanDB();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close()
 if err=persistPlanPayload(db,p);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"保存任务配置失败："+err.Error()});return}
 tasks:=[]Task{}
 for _,scene:=range p.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,TargetHours:t.Hours,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}}
 a.cfg.Tasks=tasks;a.cfg.DataVersion=p.DataVersion
 _,running,_:=a.activeDownloader()
 writeJSON(w,map[string]any{"ok":true,"running":running})
}

func nearestExistingDir(path string)string{
 cur:=filepath.Clean(path)
 for {
  info,err:=os.Stat(cur);if err==nil&&info.IsDir(){return cur}
  parent:=filepath.Dir(cur);if parent==cur{return ""}
  cur=parent
 }
}
func diskPathWithinMount(path,mount string)bool{
 p,err:=filepath.Abs(path);if err!=nil{return false};m,err:=filepath.Abs(mount);if err!=nil{return false}
 rel,err:=filepath.Rel(m,p);if err!=nil{return false}
 if rel=="."{return true}
 return rel!=".."&&!strings.HasPrefix(rel,".."+string(os.PathSeparator))&&!filepath.IsAbs(rel)
}
func probeWritable(path string)error{
 f,err:=os.CreateTemp(path,".minio-downloader-check-*");if err!=nil{return err}
 name:=f.Name();if _,err=f.Write([]byte("ok"));err!=nil{f.Close();os.Remove(name);return err}
 if err=f.Close();err!=nil{os.Remove(name);return err};return os.Remove(name)
}
func(a *WebAPI)configDisksDetect(w http.ResponseWriter,r *http.Request){
 parts,err:=disk.Partitions(false);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取磁盘挂载信息失败："+err.Error()});return}
 mounts:=[]map[string]any{};seen:=map[string]bool{}
 for _,p:=range parts{mount:=strings.TrimSpace(p.Mountpoint);if mount==""||seen[mount]{continue};seen[mount]=true;item:=map[string]any{"mountpoint":mount,"device":p.Device,"fstype":p.Fstype,"free":uint64(0),"total":uint64(0)};if u,e:=disk.Usage(mount);e==nil{item["free"]=u.Free;item["total"]=u.Total};mounts=append(mounts,item)}
 sort.Slice(mounts,func(i,j int)bool{return fmt.Sprint(mounts[i]["mountpoint"])<fmt.Sprint(mounts[j]["mountpoint"])})
 writeJSON(w,map[string]any{"mounts":mounts})
}
func(a *WebAPI)configDisksCheck(w http.ResponseWriter,r *http.Request){
 var body struct{Disks []DiskConfig `json:"disks"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,1<<20)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Disks==nil{body.Disks=append([]DiskConfig{},a.cfg.Disks...)}
 results:=[]map[string]any{}
 for _,d:=range body.Disks{
  name:=strings.TrimSpace(d.Name);path:=strings.TrimSpace(d.Path);mount:=strings.TrimSpace(d.Mountpoint);if mount==""{mount=path}
  notes:=[]map[string]string{};ok:=true;free:=uint64(0);total:=uint64(0);safeToProbe:=true
  if name==""||path==""{ok=false;safeToProbe=false;notes=append(notes,map[string]string{"level":"bad","text":"磁盘名称和路径不能为空"})}
  if _,err:=os.Stat(mount);err!=nil{ok=false;safeToProbe=false;notes=append(notes,map[string]string{"level":"bad","text":"挂载点不存在："+mount})}else if !isMountedPath(mount){ok=false;safeToProbe=false;notes=append(notes,map[string]string{"level":"bad","text":"挂载点没有真正挂载："+mount+"（下载器会拒绝写入未挂载目录）"})}else{notes=append(notes,map[string]string{"level":"ok","text":"挂载点正常"})}
  if path!=""&&mount!=""&&!diskPathWithinMount(path,mount){ok=false;safeToProbe=false;notes=append(notes,map[string]string{"level":"bad","text":"下载路径不在指定挂载点之下"})}
  probe:=""
  if safeToProbe{probe=path;if probe!=""{if info,err:=os.Stat(probe);err!=nil||!info.IsDir(){probe=nearestExistingDir(probe)}}}
  if safeToProbe&&probe==""{ok=false;notes=append(notes,map[string]string{"level":"bad","text":"下载路径及其上级目录均不存在"})}else if safeToProbe{
   if err:=probeWritable(probe);err!=nil{ok=false;notes=append(notes,map[string]string{"level":"bad","text":"目录不可写："+err.Error()})}else{notes=append(notes,map[string]string{"level":"ok","text":"目录可写"})}
   if usage,err:=disk.Usage(probe);err==nil{free=usage.Free;total=usage.Total}else{notes=append(notes,map[string]string{"level":"bad","text":"读取磁盘空间失败："+err.Error()});ok=false}
  }
  results=append(results,map[string]any{"name":name,"ok":ok,"free":free,"total":total,"notes":notes})
 }
 writeJSON(w,map[string]any{"results":results})
}
func(a *WebAPI)configDisks(w http.ResponseWriter,r *http.Request){
 var body struct{Disks []DiskConfig `json:"disks"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,1<<20)).Decode(&body);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Disks==nil{writeJSONStatus(w,400,map[string]any{"error":"请求内容不是有效的 JSON"});return}
 if len(body.Disks)==0{writeJSONStatus(w,400,map[string]any{"error":"至少需要配置一块下载磁盘"});return}
 seen:=map[string]bool{};for i:=range body.Disks{
  d:=&body.Disks[i];d.Name=strings.TrimSpace(d.Name);d.Path=strings.TrimSpace(d.Path);d.Mountpoint=strings.TrimSpace(d.Mountpoint)
  if d.Name==""||d.Path==""{writeJSONStatus(w,400,map[string]any{"error":fmt.Sprintf("第 %d 块磁盘：名称和路径都不能为空",i+1)});return}
  if seen[d.Name]{writeJSONStatus(w,400,map[string]any{"error":"磁盘名称重复："+d.Name});return};seen[d.Name]=true
  if d.Mountpoint==""{d.Mountpoint=d.Path}
 }
 if strings.TrimSpace(a.cfg.ConfigDB)==""{writeJSONStatus(w,500,map[string]any{"error":"未配置 SQLite 配置库"});return}
 db,err:=sql.Open("sqlite",a.cfg.ConfigDB);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer db.Close();db.SetMaxOpenConns(1);_,_=db.Exec("PRAGMA busy_timeout=30000")
 if _,err=db.Exec("CREATE TABLE IF NOT EXISTS disks(name TEXT PRIMARY KEY,position INTEGER NOT NULL,path TEXT NOT NULL,mountpoint TEXT NOT NULL)");err!=nil{writeJSONStatus(w,500,map[string]any{"error":"初始化磁盘配置失败："+err.Error()});return}
 tx,err:=db.Begin();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return};defer tx.Rollback()
 if _,err=tx.Exec("DELETE FROM disks");err!=nil{writeJSONStatus(w,500,map[string]any{"error":"更新磁盘配置失败："+err.Error()});return}
 for i,d:=range body.Disks{if _,err=tx.Exec("INSERT INTO disks(name,position,path,mountpoint) VALUES(?,?,?,?)",d.Name,i,d.Path,d.Mountpoint);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"保存磁盘配置失败："+err.Error()});return}}
 if err=tx.Commit();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"提交磁盘配置失败："+err.Error()});return}
 a.cfg.Disks=append([]DiskConfig{},body.Disks...)
 _,running,_:=a.activeDownloader();writeJSON(w,map[string]any{"ok":true,"running":running})
}
func(a *WebAPI)config(w http.ResponseWriter,r *http.Request) {
 groups:=map[string]string{"minio":"MinIO 连接","pg":"PostgreSQL（任务元数据）","run":"运行与空间","transfer":"传输与校验","web":"Web 控制台"}
 values:=map[string]any{}
 secretKeys:=map[string]bool{"minio.secret_key":true,"pg.password":true}
 secretsSet:=[]string{}
 schema:=webConfigSchema()
 for _,item:=range schema { values[item["key"].(string)]=item["default"];if _,ok:=item["help"];!ok{item["help"]=""};if _,ok:=item["min"];!ok{item["min"]=nil};if _,ok:=item["max"];!ok{item["max"]=nil};if _,ok:=item["placeholder"];!ok{item["placeholder"]=""} }
 values["minio.endpoint"]=a.cfg.Endpoint;values["minio.access_key"]=a.cfg.AccessKey;values["minio.bucket"]=a.cfg.Bucket;values["minio.region"]=a.cfg.Region;values["minio.secure"]=a.cfg.SSL;values["minio.cert_check"]=!a.cfg.MinioSkipVerify
 values["pg.host"]=a.cfg.PGHost;values["pg.port"]=a.cfg.PGPort;values["pg.database"]=a.cfg.PGDB;values["pg.user"]=a.cfg.PGUser
 values["minio.secret_key"]="";values["pg.password"]=""
 values["run.min_free_gb"]=a.cfg.MinFreeGB;values["run.space_safety_factor"]=a.cfg.SafetyFactor;values["run.max_writes_per_disk"]=a.cfg.Workers;values["run.require_mcap"]=a.cfg.RequireMCAP;values["run.include_null_quality"]=a.cfg.IncludeNull;values["run.mcap_pattern"]=a.cfg.MCAPPattern;values["run.validate_workers"]=a.cfg.ValidateWorkers
 values["transfer.file_workers"]=a.cfg.FileWorkers;values["transfer.file_retries"]=a.cfg.FileRetries;values["transfer.retry_delay"]=a.cfg.RetryDelaySeconds
 if a.cfg.VerifyMD5!=nil {values["transfer.verify_md5"]=*a.cfg.VerifyMD5};if a.cfg.Fsync!=nil {values["transfer.fsync"]=*a.cfg.Fsync}
 if a.cfg.SecretKey!="" {secretsSet=append(secretsSet,"minio.secret_key")};if a.cfg.PGPassword!="" {secretsSet=append(secretsSet,"pg.password")}
 if a.cfg.ConfigDB!="" {
  db,err:=sql.Open("sqlite",a.cfg.ConfigDB)
  if err==nil {
   defer db.Close()
   rows,e:=db.Query("SELECT key,value FROM settings")
   if e==nil {
    for rows.Next(){var key,raw string;if rows.Scan(&key,&raw)!=nil{continue};if key=="web.token_hash"||key=="tasks.data_version"{continue};var value any;if json.Unmarshal([]byte(raw),&value)!=nil{continue};if secretKeys[key]{if value!="" {found:=false;for _,k:=range secretsSet{if k==key{found=true}};if !found{secretsSet=append(secretsSet,key)}};values[key]=""}else{values[key]=value}}
    _=rows.Close()
   }
  }
 }
 sort.Strings(secretsSet)
 scenes:=[]map[string]any{};sceneIndex:=map[string]int{}
 for _,task:=range a.cfg.Tasks {
  i,ok:=sceneIndex[task.Scene];if !ok{i=len(scenes);sceneIndex[task.Scene]=i;scenes=append(scenes,map[string]any{"scene":task.Scene,"tasks":[]map[string]any{}})}
  aliases:=task.Aliases;if aliases==nil{aliases=[]string{}}
  tags:=task.Tags;if tags==nil{tags=[]string{}}
  items:=scenes[i]["tasks"].([]map[string]any)
  items=append(items,map[string]any{"name":task.Name,"count":task.TargetCount,"target_hours":task.TargetHours,"aliases":aliases,"dir":task.Dir,"tags":tags})
  scenes[i]["tasks"]=items
 }
 _,running,_:=a.activeDownloader()
 a.authMu.Lock()
 tokenSet:=a.tokenHash!=""
 a.authMu.Unlock()
 platform:="posix";if runtime.GOOS=="windows"{platform="windows"}
 writeJSON(w,map[string]any{
  "schema":schema,"groups":groups,"values":values,"secrets_set":secretsSet,
  "tasks":map[string]any{"data_version":a.cfg.DataVersion,"scenes":scenes},"disks":a.cfg.Disks,
  "token_set":tokenSet,"paths":map[string]any{"sqlite_db":a.cfg.StateDB},
  "platform":platform,"running":running,
 })
}
func(a *WebAPI)setChildProcess(pid int32,created float64){
 a.childMu.Lock()
 a.childPID=pid
 a.childCreated=created
 a.childMu.Unlock()
}
func(a *WebAPI)childProcessSnapshot()(int32,float64){
 a.childMu.RLock()
 defer a.childMu.RUnlock()
 return a.childPID,a.childCreated
}
func(a *WebAPI)activeDownloader()(int,bool,bool){
 childPID,childCreated:=a.childProcessSnapshot()
 if childPID>0&&processIdentity(int(childPID),childCreated){return int(childPID),true,true}
 var pid int;var created float64
 if err:=a.db.QueryRow("SELECT pid,created FROM process_state WHERE id=1").Scan(&pid,&created);err!=nil{return 0,false,false}
 if processIdentity(pid,created){return pid,true,false}
 _,_=a.db.Exec("DELETE FROM process_state WHERE id=1 AND pid=? AND created=?",pid,created)
 return 0,false,false
}
func(a *App)registerProcess()error{
 if _,err:=a.db.Exec("CREATE TABLE IF NOT EXISTS process_state(id INTEGER PRIMARY KEY CHECK(id=1),pid INTEGER NOT NULL,created REAL NOT NULL,mode TEXT NOT NULL,updated_at TEXT NOT NULL)");err!=nil{return err}
 var oldPID int;var oldCreated float64
 err:=a.db.QueryRow("SELECT pid,created FROM process_state WHERE id=1").Scan(&oldPID,&oldCreated)
 if err==nil&&processIdentity(oldPID,oldCreated){return fmt.Errorf("检测到下载进程已运行（PID %d）",oldPID)}
 if err!=nil&&err!=sql.ErrNoRows{return err}
 created:=float64(time.Now().UnixNano())/1e9
 if proc,e:=process.NewProcess(int32(os.Getpid()));e==nil{if ms,e:=proc.CreateTime();e==nil&&ms>0{created=float64(ms)/1000}}
 var result sql.Result
 if err==sql.ErrNoRows{
  result,err=a.db.Exec("INSERT OR IGNORE INTO process_state(id,pid,created,mode,updated_at) VALUES(1,?,?,?,?)",os.Getpid(),created,a.mode,controlTimestamp())
 }else{
  result,err=a.db.Exec("UPDATE process_state SET pid=?,created=?,mode=?,updated_at=? WHERE id=1 AND pid=? AND created=?",os.Getpid(),created,a.mode,controlTimestamp(),oldPID,oldCreated)
 }
 if err!=nil{return err}
 affected,_:=result.RowsAffected()
 if affected!=1{return errors.New("另一个下载进程刚刚获得了 SQLite 运行租约")}
 a.processCreated=created
 return nil
}
func(a *App)unregisterProcess(){if a.db!=nil&&a.processCreated>0{_,_=a.db.Exec("DELETE FROM process_state WHERE id=1 AND pid=? AND created=?",os.Getpid(),a.processCreated)}}

func(a *WebAPI)run(w http.ResponseWriter,r *http.Request){
 var body struct{Mode string `json:"mode"`;MaxRounds int `json:"max_rounds"`;RecheckMissing bool `json:"recheck_missing"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,4096)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Mode==""{body.Mode=a.mode};if body.Mode!="count"&&body.Mode!="duration"{writeJSONStatus(w,400,map[string]any{"error":"mode 只能是 count 或 duration"});return}
 if body.MaxRounds==0{body.MaxRounds=a.cfg.DefaultRounds;if body.MaxRounds<=0{body.MaxRounds=1}};if body.MaxRounds<1||body.MaxRounds>50{writeJSONStatus(w,400,map[string]any{"error":"补齐轮数范围 1–50"});return}
 validatedConfig:=a.cfg
 validatedConfig.Mode=body.Mode
 if err:=validatedConfig.normalize(body.Mode);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"配置未完成："+err.Error()});return}
 a.runMu.Lock();defer a.runMu.Unlock()
 if pid,running,_:=a.activeDownloader();running{writeJSONStatus(w,409,map[string]any{"error":fmt.Sprintf("下载器已在运行（PID %d）",pid)});return}
 executable,err:=os.Executable();if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"定位下载器程序失败: "+err.Error()});return}
 if err:=clearStopRequestDB(a.cfg.StateDB);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"清理旧暂停请求失败: "+err.Error()});return}
 dbName,err:=filepath.Abs(a.cfg.StateDB);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"解析 SQLite 路径失败: "+err.Error()});return}
 args:=[]string{"--worker","--db",dbName,"--mode",body.Mode,"--max-rounds",strconv.Itoa(body.MaxRounds),"--preserve-stop-request"};if body.RecheckMissing{args=append(args,"--recheck-missing")}
 cmd:=exec.Command(executable,args...);configureDetached(cmd);cmd.Stdout=io.Discard;cmd.Stderr=io.Discard;cmd.Stdin=nil
 if err=cmd.Start();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"启动下载器失败: "+err.Error()});return}
 go func(){_=cmd.Wait()}();created:=float64(0);if proc,e:=process.NewProcess(int32(cmd.Process.Pid));e==nil{if ms,e:=proc.CreateTime();e==nil{created=float64(ms)/1000}};a.setChildProcess(int32(cmd.Process.Pid),created)
 writeJSON(w,map[string]any{"ok":true,"pid":cmd.Process.Pid})
}
func(a *WebAPI)stop(w http.ResponseWriter,r *http.Request){
 var body struct{Force bool `json:"force"`};if err:=json.NewDecoder(io.LimitReader(r.Body,4096)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 pid,running,_:=a.activeDownloader();if !running||pid<=0{writeJSONStatus(w,409,map[string]any{"error":"当前没有运行中的下载器"});return}
 if body.Force{
  if err:=killProcessTree(int32(pid));err!=nil{writeJSONStatus(w,500,map[string]any{"error":"强制结束下载进程失败: "+err.Error()});return}
  writeJSON(w,map[string]any{"ok":true,"mode":"force_stop","pid":pid});return
 }
 if err:=writeStopRequest(a.db);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"写入暂停请求失败: "+err.Error()});return}
 writeJSON(w,map[string]any{"ok":true,"mode":"pause"})
}
func(a *WebAPI)episodes(w http.ResponseWriter,r *http.Request){
 task:=r.URL.Query().Get("task");rows,err:=a.db.Query("SELECT episode_id,status,error,parent_key,updated_at FROM downloads WHERE task_name=? AND status<>'completed' ORDER BY updated_at DESC LIMIT 200",task)
 if err!=nil{writeJSON(w,map[string]any{"episodes":[]any{}});return};defer rows.Close()
 episodes:=[]map[string]any{};for rows.Next(){var id,status string;var errText,parent,updated sql.NullString;if rows.Scan(&id,&status,&errText,&parent,&updated)==nil{episodes=append(episodes,map[string]any{"episode_id":id,"status":status,"error":errText.String,"parent_key":parent.String,"updated_at":updated.String})}}
 writeJSON(w,map[string]any{"episodes":episodes})
}
func(a *WebAPI)runEpisodes(w http.ResponseWriter,r *http.Request){
 runID:=strings.TrimSpace(r.PathValue("id"));if runID==""{writeJSONStatus(w,400,map[string]any{"error":"run_id 不能为空"});return}
 limit,err:=strconv.Atoi(r.URL.Query().Get("limit"));if err!=nil||limit<=0{limit=100};if limit>200{limit=200}
 offset,err:=strconv.Atoi(r.URL.Query().Get("offset"));if err!=nil||offset<0{offset=0}
 task:=strings.TrimSpace(r.URL.Query().Get("task"));status:=strings.TrimSpace(r.URL.Query().Get("status"));category:=strings.TrimSpace(r.URL.Query().Get("category"))
 allowedCategories:=map[string]bool{"network":true,"disk_space":true,"not_found":true,"permission":true,"integrity":true,"other":true,"paused":true}
 if category!=""&&!allowedCategories[category]{writeJSONStatus(w,400,map[string]any{"error":"不支持的失败分类"});return}
 clauses:=[]string{"run_id=?"};args:=[]any{runID};if task!=""{clauses=append(clauses,"task_name=?");args=append(args,task)};if status!=""{clauses=append(clauses,"status=?");args=append(args,status)};if category!=""{clauses=append(clauses,"failure_category=?");args=append(args,category)}
 where:=strings.Join(clauses," AND ");var total int
 if err:=a.db.QueryRow("SELECT COUNT(*) FROM run_episode_history WHERE "+where,args...).Scan(&total);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取运行 Episode 数量失败"});return}
 args=append(args,limit,offset);rows,err:=a.db.Query("SELECT episode_id,task_name,status,COALESCE(error,''),COALESCE(failure_category,''),COALESCE(disk_name,''),completed_bytes,content_seconds,updated_at FROM run_episode_history WHERE "+where+" ORDER BY updated_at DESC,episode_id LIMIT ? OFFSET ?",args...)
 if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取运行 Episode 明细失败"});return};defer rows.Close()
 items:=[]map[string]any{};for rows.Next(){var id,name,state,detail,category,diskName,updated string;var bytes int64;var seconds float64;if rows.Scan(&id,&name,&state,&detail,&category,&diskName,&bytes,&seconds,&updated)==nil{items=append(items,map[string]any{"episode_id":id,"task_name":name,"status":state,"error":detail,"failure_category":category,"disk_name":diskName,"completed_bytes":bytes,"content_seconds":seconds,"updated_at":updated})}}
 if err:=rows.Err();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取运行 Episode 明细失败"});return};if err:=rows.Close();err!=nil{writeJSONStatus(w,500,map[string]any{"error":"关闭运行 Episode 明细查询失败"});return}
 categoryCounts:=map[string]int{}
 categoryRows,categoryErr:=a.db.Query("SELECT COALESCE(failure_category,''),COUNT(*) FROM run_episode_history WHERE "+where+" AND status='failed' GROUP BY failure_category",args[:len(args)-2]...)
 if categoryErr==nil{for categoryRows.Next(){var category string;var count int;if categoryRows.Scan(&category,&count)==nil{categoryCounts[category]=count}};categoryRows.Close()}
 writeJSON(w,map[string]any{"run_id":runID,"total":total,"limit":limit,"offset":offset,"episodes":items,"failure_categories":categoryCounts})
}

func csvSafeCell(value string) string {
 if value=="" { return value }
 switch value[0] { case '=', '+', '-', '@', '\t', '\r': return "'"+value }
 return value
}
func(a *WebAPI)exportRunCSV(w http.ResponseWriter,r *http.Request){
 runID:=strings.TrimSpace(r.PathValue("id"));if runID==""{http.Error(w,"run_id 不能为空",http.StatusBadRequest);return}
 task:=strings.TrimSpace(r.URL.Query().Get("task"));status:=strings.TrimSpace(r.URL.Query().Get("status"));category:=strings.TrimSpace(r.URL.Query().Get("category"))
 allowedCategories:=map[string]bool{"network":true,"disk_space":true,"not_found":true,"permission":true,"integrity":true,"other":true,"paused":true}
 if category!=""&&!allowedCategories[category]{http.Error(w,"不支持的失败分类",http.StatusBadRequest);return}
 if status!=""&&status!="completed"&&status!="failed"&&status!="paused"{http.Error(w,"不支持的 Episode 状态",http.StatusBadRequest);return}
 taskRecords:=[][]string{}
 taskQuery:="SELECT task_name,completed_count,failed_count,completed_bytes,content_seconds,wait_seconds,duration_seconds FROM run_task_history WHERE run_id=?";taskArgs:=[]any{runID}
 if task!=""{taskQuery+=" AND task_name=?";taskArgs=append(taskArgs,task)}
 taskRows,err:=a.db.Query(taskQuery+" ORDER BY task_name",taskArgs...);if err!=nil{http.Error(w,"读取运行任务统计失败",http.StatusInternalServerError);return}
 for taskRows.Next(){var name string;var success,failed,bytes int64;var seconds,waitSeconds,durationSeconds float64
  if err=taskRows.Scan(&name,&success,&failed,&bytes,&seconds,&waitSeconds,&durationSeconds);err!=nil{taskRows.Close();http.Error(w,"读取运行任务统计失败",http.StatusInternalServerError);return}
  attempts:=success+failed;rate:="";if attempts>0{rate=strconv.FormatFloat(float64(success)*100/float64(attempts),'f',1,64)+"%"}
  throughput:=float64(0);if durationSeconds>0{throughput=float64(bytes)/durationSeconds};taskRecords=append(taskRecords,[]string{"task_summary",runID,name,"","","","",strconv.FormatInt(success,10),strconv.FormatInt(failed,10),rate,strconv.FormatInt(bytes,10),strconv.FormatFloat(seconds,'f',2,64),strconv.FormatFloat(waitSeconds,'f',2,64),strconv.FormatFloat(durationSeconds,'f',2,64),strconv.FormatFloat(throughput,'f',2,64),"",""})
 }
 if err=taskRows.Err();err!=nil{taskRows.Close();http.Error(w,"读取运行任务统计失败",http.StatusInternalServerError);return};if err=taskRows.Close();err!=nil{http.Error(w,"关闭运行任务统计查询失败",http.StatusInternalServerError);return}
 clauses:=[]string{"run_id=?"};args:=[]any{runID}
 if task!=""{clauses=append(clauses,"task_name=?");args=append(args,task)}
 if status!=""{clauses=append(clauses,"status=?");args=append(args,status)}
 if category!=""{clauses=append(clauses,"failure_category=?");args=append(args,category)}
 query:="SELECT episode_id,task_name,status,COALESCE(failure_category,''),COALESCE(disk_name,''),completed_bytes,content_seconds,COALESCE(error,''),updated_at FROM run_episode_history WHERE "+strings.Join(clauses," AND ")+" ORDER BY updated_at,episode_id"
 rows,err:=a.db.Query(query,args...);if err!=nil{http.Error(w,"读取运行 Episode 明细失败",http.StatusInternalServerError);return};defer rows.Close()
 safeID:=strings.Map(func(ch rune)rune{if ch>='a'&&ch<='z'||ch>='A'&&ch<='Z'||ch>='0'&&ch<='9'||ch=='-'||ch=='_'{return ch};return -1},runID);if safeID==""{safeID="run"}
 w.Header().Set("Content-Type","text/csv; charset=utf-8");w.Header().Set("Content-Disposition",fmt.Sprintf("attachment; filename=\"run-%s-episodes.csv\"",safeID))
 _,_=w.Write([]byte{0xef,0xbb,0xbf});writer:=csv.NewWriter(w)
 if err=writer.Write([]string{"记录类型","运行 ID","任务","Episode ID","状态","失败分类","磁盘","成功数量","失败尝试数","成功率","完成字节","内容秒数","等待秒数","运行秒数","平均字节每秒","错误详情","更新时间"});err!=nil{return}
 for _,record:=range taskRecords{for i:=range record{record[i]=csvSafeCell(record[i])};if err=writer.Write(record);err!=nil{return}}
 for rows.Next(){var id,name,state,category,diskName,detail,updated string;var bytes int64;var seconds float64
  if err=rows.Scan(&id,&name,&state,&category,&diskName,&bytes,&seconds,&detail,&updated);err!=nil{return}
  record:=[]string{"episode",runID,name,id,state,category,diskName,"","","",strconv.FormatInt(bytes,10),strconv.FormatFloat(seconds,'f',2,64),"","","",detail,updated};for i:=range record{record[i]=csvSafeCell(record[i])}
  if err=writer.Write(record);err!=nil{return}
 }
 if err=rows.Err();err!=nil{return};writer.Flush()
}

func(a *WebAPI)logs(w http.ResponseWriter,r *http.Request){
 offset,err:=strconv.ParseInt(r.URL.Query().Get("offset"),10,64);initial:=err!=nil||offset<=0
 if initial{offset=0}
 query:="SELECT id,message FROM runtime_logs WHERE id>? ORDER BY id LIMIT 500"
 var args []any
 if initial{query="SELECT id,message FROM (SELECT id,message FROM runtime_logs ORDER BY id DESC LIMIT 500) ORDER BY id"}else{args=[]any{offset}}
 rows,err:=a.db.Query(query,args...);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取 SQLite 日志失败: "+err.Error()});return};defer rows.Close()
 lines:=[]string{};next:=offset
 for rows.Next(){var id int64;var message string;if rows.Scan(&id,&message)==nil{lines=append(lines,message);if id>next{next=id}}}
 writeJSON(w,map[string]any{"text":strings.Join(lines,"\n"),"offset":next,"reset":initial})
}

func loadConfigDB(c *Config,runMode string)error{
 if strings.TrimSpace(c.ConfigDB)==""{return nil}
  db,err:=sql.Open("sqlite",c.ConfigDB);if err!=nil{return err};defer db.Close();db.SetMaxOpenConns(1)
 settings:=map[string]string{}
 rows,err:=db.Query("SELECT key,value FROM settings");if err!=nil{return err}
 for rows.Next(){var key,value string;if err=rows.Scan(&key,&value);err!=nil{rows.Close();return err};settings[key]=value};if err=rows.Err();err!=nil{rows.Close();return err};if err=rows.Close();err!=nil{return err}
 getString:=func(key string,dst *string){if raw,ok:=settings[key];ok{var v string;if json.Unmarshal([]byte(raw),&v)==nil{*dst=v}}}
 getBool:=func(key string,dst *bool){if raw,ok:=settings[key];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{*dst=v}}}
 getInt:=func(key string,dst *int){if raw,ok:=settings[key];ok{var v int;if json.Unmarshal([]byte(raw),&v)==nil{*dst=v}}}
 getFloat:=func(key string,dst *float64){if raw,ok:=settings[key];ok{var v float64;if json.Unmarshal([]byte(raw),&v)==nil{*dst=v}}}
 getString("minio.endpoint",&c.Endpoint);getString("minio.access_key",&c.AccessKey);getString("minio.secret_key",&c.SecretKey);getString("minio.bucket",&c.Bucket);getString("minio.region",&c.Region)
 getBool("minio.secure",&c.SSL);certCheck:=true;getBool("minio.cert_check",&certCheck);c.MinioSkipVerify=!certCheck;getString("pg.host",&c.PGHost);getInt("pg.port",&c.PGPort);getString("pg.database",&c.PGDB);getString("pg.user",&c.PGUser);getString("pg.password",&c.PGPassword)
 getFloat("run.min_free_gb",&c.MinFreeGB);getFloat("run.space_safety_factor",&c.SafetyFactor);getInt("run.max_writes_per_disk",&c.Workers);getBool("run.require_mcap",&c.RequireMCAP);getBool("run.include_null_quality",&c.IncludeNull);getInt("run.validate_workers",&c.ValidateWorkers);getString("run.mcap_pattern",&c.MCAPPattern)
 getInt("transfer.file_workers",&c.FileWorkers);getInt("transfer.file_retries",&c.FileRetries);getFloat("transfer.retry_delay",&c.RetryDelaySeconds);getInt("run.default_rounds",&c.DefaultRounds);getInt("transfer.connect_timeout",&c.ConnectTimeout);getInt("transfer.read_timeout",&c.ReadTimeout);getString("web.host",&c.WebHost);getInt("web.port",&c.WebPort)
 if raw,ok:=settings["transfer.verify_md5"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.VerifyMD5=&v}}
 if raw,ok:=settings["transfer.fsync"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.Fsync=&v}}
 if raw,ok:=settings["tasks.data_version"];ok{var v string;if json.Unmarshal([]byte(raw),&v)==nil&&v!=""{c.DataVersion=v}}
 // Read and close aliases before opening taskRows. The database is intentionally
 // limited to one connection; keeping taskRows open here would block Query below.
 aliases:=map[int64][]string{};aliasRows,err:=db.Query("SELECT task_id,alias FROM task_aliases ORDER BY task_id,position");if err!=nil{return err};for aliasRows.Next(){var id int64;var alias string;if err=aliasRows.Scan(&id,&alias);err!=nil{aliasRows.Close();return err};aliases[id]=append(aliases[id],alias)};if err=aliasRows.Err();err!=nil{aliasRows.Close();return err};if err=aliasRows.Close();err!=nil{return err}
 taskRows,err:=db.Query("SELECT s.name,t.name,t.target_count,t.target_hours,t.directory,t.id,t.tags FROM tasks t JOIN scenes s ON s.id=t.scene_id ORDER BY s.position,t.position");if err!=nil{return err}
 tasks:=[]Task{};for taskRows.Next(){var t Task;var id int64;var tags string;if err=taskRows.Scan(&t.Scene,&t.Name,&t.TargetCount,&t.TargetHours,&t.Dir,&id,&tags);err!=nil{taskRows.Close();return err};t.Aliases=aliases[id];if tags!=""{_ = json.Unmarshal([]byte(tags),&t.Tags)};tasks=append(tasks,t)};if err=taskRows.Err();err!=nil{taskRows.Close();return err};if err=taskRows.Close();err!=nil{return err};c.Tasks=tasks
 diskRows,err:=db.Query("SELECT name,path,mountpoint FROM disks ORDER BY position");if err!=nil{return err};disks:=[]DiskConfig{};for diskRows.Next(){var d DiskConfig;if err=diskRows.Scan(&d.Name,&d.Path,&d.Mountpoint);err!=nil{diskRows.Close();return err};disks=append(disks,d)};if err=diskRows.Err();err!=nil{diskRows.Close();return err};if err=diskRows.Close();err!=nil{return err};c.Disks=disks
 c.fromConfigDB=true;log.Printf("已从 SQLite 配置库加载配置: %s（任务 %d 个，磁盘 %d 块）",c.ConfigDB,len(c.Tasks),len(c.Disks));return nil
}
func(c *Config)normalize(m string)error{
 if c.Mode!=""&&c.Mode!=m{return fmt.Errorf("配置文件 mode=%s 与命令行 --mode=%s 不一致",c.Mode,m)}
 if c.Endpoint==""||c.AccessKey==""||c.SecretKey==""||c.Bucket==""{return errors.New("MinIO endpoint/access_key/secret_key/bucket 必须填写")}
 if c.PGHost==""||c.PGDB==""||c.PGUser==""||c.PGPassword==""{return errors.New("PostgreSQL 连接参数必须填写")}
 if c.PGPort==0{c.PGPort=5432};if c.DataVersion==""{c.DataVersion="银河2.2数据"};if c.LockFile==""{c.LockFile="minio_download.lock"};if c.PIDFile==""{c.PIDFile="minio_download.pid"};if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"};if c.LogDir==""{c.LogDir="logs"}
 if c.MinFreeGB==0&&!c.fromConfigDB{c.MinFreeGB=3};if c.SafetyFactor==0{c.SafetyFactor=1.15};if c.Workers<=0{c.Workers=2};if c.FileWorkers<=0{c.FileWorkers=4};if c.FileRetries<=0{c.FileRetries=5};if c.RetryDelaySeconds<=0&&!c.fromConfigDB{c.RetryDelaySeconds=1};if c.ValidateWorkers<=0{c.ValidateWorkers=16};if c.DefaultRounds<=0{c.DefaultRounds=1};if c.ConnectTimeout<=0{c.ConnectTimeout=10};if c.ReadTimeout<=0{c.ReadTimeout=120};if c.WebHost==""{c.WebHost="127.0.0.1"};if c.WebPort<=0{c.WebPort=8765};if c.Workers>16||c.FileWorkers>16||c.ValidateWorkers>32||c.FileRetries>20{return errors.New("并发数不能超过 16，文件重试次数不能超过 20")};if c.MinFreeGB<0||c.MinFreeGB>1_000_000{return errors.New("min_free_gb 必须在 0 到 1000000 之间")};if c.SafetyFactor<1||c.SafetyFactor>5{return errors.New("space_safety_factor 必须在 1 到 5 之间")};if c.RetryDelaySeconds<0||c.RetryDelaySeconds>60{return errors.New("retry_delay_seconds 必须在 0 到 60 之间")};if c.PGPort<1||c.PGPort>65535{return errors.New("pg_port 必须在 1 到 65535 之间")};if c.DefaultRounds<1||c.DefaultRounds>50||c.ConnectTimeout<1||c.ConnectTimeout>300||c.ReadTimeout<5||c.ReadTimeout>3600||c.WebPort<1||c.WebPort>65535{return errors.New("default_rounds / connect_timeout / read_timeout / web_port 超出允许范围")};if c.MCAPPattern==""{c.MCAPPattern="*.SYNC.mcap"};if c.VerifyMD5==nil{v:=true;c.VerifyMD5=&v};if c.Fsync==nil{v:=true;c.Fsync=&v}
 if len(c.Disks)==0||len(c.Tasks)==0{return errors.New("disks/tasks 不能为空")}
 seen:=map[string]bool{};aliases:=map[string]string{};dirs:=map[string]string{}
 for _,t:=range c.Tasks{
  if strings.TrimSpace(t.Name)==""||seen[t.Name]{return fmt.Errorf("任务名为空或重复: %q",t.Name)}
  seen[t.Name]=true
  if t.TargetHours<0||t.TargetCount<0{return fmt.Errorf("任务 %s 的目标不能为负数",t.Name)}
  if strings.TrimSpace(t.Scene)==""{return fmt.Errorf("任务 %s 的 scene 不能为空",t.Name)}
  dir:=taskDir(t);key:=t.Scene+"\x00"+dir
  if old,ok:=dirs[key];ok{return fmt.Errorf("场景 %s 下目录 %s 同时分配给 %s 和 %s",t.Scene,dir,old,t.Name)}
  dirs[key]=t.Name
 }
 for _,t:=range c.Tasks{for _,alias:=range t.Aliases{
  if alias==""||seen[alias]{return fmt.Errorf("任务 %s 的别名为空或与当前任务名冲突: %q",t.Name,alias)}
  if owner,ok:=aliases[alias];ok{return fmt.Errorf("别名 %s 同时属于 %s 和 %s",alias,owner,t.Name)}
  aliases[alias]=t.Name
 }}
 diskNames:=map[string]bool{};diskPaths:=map[string]bool{}
 for _,d:=range c.Disks{
  if strings.TrimSpace(d.Name)==""||strings.TrimSpace(d.Path)==""{return errors.New("磁盘 name/path 不能为空")}
  if diskNames[d.Name]{return fmt.Errorf("磁盘名称重复: %s",d.Name)};diskNames[d.Name]=true
  clean:=filepath.Clean(d.Path);if diskPaths[clean]{return fmt.Errorf("磁盘路径重复: %s",clean)};diskPaths[clean]=true
 }
 _=m;return nil
}
type pidRecord struct{PID int `json:"pid"`;Mode string `json:"mode"`;Created float64 `json:"created"`;CreatedAt string `json:"created_at"`}
func writePIDFile(name,runMode string)error{
 if err:=os.MkdirAll(filepath.Dir(name),0755);err!=nil{return err}
 created:=float64(time.Now().UnixNano())/1e9
 if proc,e:=process.NewProcess(int32(os.Getpid()));e==nil{if ms,e:=proc.CreateTime();e==nil&&ms>0{created=float64(ms)/1000}}
 data,err:=json.Marshal(pidRecord{PID:os.Getpid(),Mode:runMode,Created:created,CreatedAt:controlTimestamp()});if err!=nil{return err}
 tmp:=name+".tmp"
 f,err:=os.OpenFile(tmp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if err!=nil{return err}
 if _,err=f.Write(data);err!=nil{f.Close();os.Remove(tmp);return err}
 if err=f.Sync();err!=nil{f.Close();os.Remove(tmp);return err}
 if err=f.Close();err!=nil{os.Remove(tmp);return err}
 if err=replaceDownloadedFile(tmp,name);err!=nil{os.Remove(tmp);return err}
 return nil
}
func removePIDFile(name string){
 data,err:=os.ReadFile(name);if err!=nil{return}
 var record pidRecord
 if json.Unmarshal(data,&record)==nil&&record.PID==os.Getpid(){_=os.Remove(name)}
}
func controlTimestamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func writeStopRequest(db *sql.DB) error {
 if _,err:=db.Exec("CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL)");err!=nil{return err}
 _,err:=db.Exec("INSERT INTO control(key,value,updated_at) VALUES('stop_requested','1',?) ON CONFLICT(key) DO UPDATE SET value='1',updated_at=excluded.updated_at",controlTimestamp())
 return err
}
func clearStopRequestDB(name string)error{
 if name==""{name="minio_download_state.sqlite3"}
 db,err:=sql.Open("sqlite",name);if err!=nil{return err};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA busy_timeout=30000; CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL)");err!=nil{return err}
 _,err=db.Exec("DELETE FROM control WHERE key='stop_requested'");return err
}
func requestStop(c Config) error {
 if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"}
 if err:=os.MkdirAll(filepath.Dir(c.StateDB),0755);err!=nil{return err}
 db,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};defer db.Close()
 db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA busy_timeout=30000");err!=nil{return err}
 return writeStopRequest(db)
}
func(a *App)saveLastRun(result string,runErr error){
 finished:=time.Now().UTC();finishedAt:=finished.Format(time.RFC3339Nano);startedAt:=a.runStartedAt.UTC().Format(time.RFC3339Nano);elapsed:=finished.Sub(a.runStartedAt).Seconds();if a.runStartedAt.IsZero(){a.runStartedAt=finished;startedAt=finishedAt;elapsed=0}
 var completedCount,completedBytes int64;var contentSeconds float64
 if a.runID!=""{_=a.db.QueryRow("SELECT COALESCE(SUM(completed_count),0),COALESCE(SUM(completed_bytes),0),COALESCE(SUM(content_seconds),0) FROM run_task_history WHERE run_id=?",a.runID).Scan(&completedCount,&completedBytes,&contentSeconds)}
 payload:=map[string]any{"result":result,"at":finishedAt,"started_at":startedAt,"duration_seconds":math.Round(elapsed*100)/100,"mode":a.mode,"completed_count":completedCount,"completed_bytes":completedBytes,"content_seconds":contentSeconds}
 errorText:="";if runErr!=nil{errorText=runErr.Error();payload["error"]=errorText}
 raw,err:=json.Marshal(payload);if err!=nil{log.Printf("保存上次运行结果失败: %v",err);return}
 _,err=a.db.Exec("INSERT INTO control(key,value,updated_at) VALUES('last_run',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",string(raw),finishedAt)
 if err!=nil{log.Printf("保存上次运行结果失败: %v",err)}
 if a.runID!=""{_,err=a.db.Exec("INSERT INTO run_history(run_id,started_at,finished_at,duration_seconds,mode,result,completed_count,completed_bytes,content_seconds,error) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET finished_at=excluded.finished_at,duration_seconds=excluded.duration_seconds,result=excluded.result,completed_count=excluded.completed_count,completed_bytes=excluded.completed_bytes,content_seconds=excluded.content_seconds,error=excluded.error",a.runID,startedAt,finishedAt,elapsed,a.mode,result,completedCount,completedBytes,contentSeconds,nullable(errorText));if err!=nil{log.Printf("保存运行历史失败: %v",err)}}
}
func(a *App)clearStopRequest()error{
 _,err:=a.db.Exec("DELETE FROM control WHERE key='stop_requested'")
 return err
}
func isStopRequested(value string)bool{value=strings.TrimSpace(value);return value!=""&&value!="0"}
func(a *App)startStopWatcher()<-chan struct{}{
 done:=make(chan struct{})
 go func(){
  defer close(done)
  ticker:=time.NewTicker(time.Second);defer ticker.Stop()
  for{
   select{
   case <-a.ctx.Done():return
   case <-ticker.C:
    var value string
    err:=a.db.QueryRowContext(a.ctx,"SELECT value FROM control WHERE key='stop_requested'").Scan(&value)
    if err==nil&&isStopRequested(value){log.Print("检测到外部暂停请求，正在安全停止并保留断点");a.cancel();return}
   }
  }
 }()
 return done
}
func postgresDSN(c Config) string {
 q:=url.Values{}
 q.Set("sslmode","prefer")
 connectTimeout:=c.ConnectTimeout;if connectTimeout<=0{connectTimeout=10};q.Set("connect_timeout",strconv.Itoa(connectTimeout))
 q.Set("application_name","minio_task_downloader_go")
 u:=url.URL{Scheme:"postgres",User:url.UserPassword(c.PGUser,c.PGPassword),Host:net.JoinHostPort(c.PGHost,strconv.Itoa(c.PGPort)),Path:"/"+c.PGDB,RawQuery:q.Encode()}
 return u.String()
}
func minioHTTPTransport(c Config)*http.Transport{
 t:=http.DefaultTransport.(*http.Transport).Clone()
 connect:=c.ConnectTimeout;if connect<=0{connect=10}
 read:=c.ReadTimeout;if read<=0{read=120}
 dialer:=&net.Dialer{Timeout:time.Duration(connect)*time.Second,KeepAlive:30*time.Second}
 t.DialContext=dialer.DialContext
 t.TLSHandshakeTimeout=time.Duration(connect)*time.Second
 t.ResponseHeaderTimeout=time.Duration(read)*time.Second
 if c.MinioSkipVerify{t.TLSClientConfig=&tls.Config{InsecureSkipVerify:true} /* #nosec: explicitly configured by operator */}
 return t
}
func(a *App)open()error{
 endpoint:=strings.TrimPrefix(strings.TrimPrefix(a.cfg.Endpoint,"https://"),"http://");secure:=a.cfg.SSL||strings.HasPrefix(a.cfg.Endpoint,"https://")
 transport:=minioHTTPTransport(a.cfg)
 mc,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(a.cfg.AccessKey,a.cfg.SecretKey,""),Secure:secure,Region:a.cfg.Region,Transport:transport});if e!=nil{return e};a.mc=mc
 pg:=postgresDSN(a.cfg)
 p,e:=sql.Open("pgx",pg);if e!=nil{return e};defer p.Close();connectTimeout:=a.cfg.ConnectTimeout;if connectTimeout<=0{connectTimeout=10};ctx,cancel:=context.WithTimeout(a.ctx,time.Duration(connectTimeout)*time.Second);defer cancel();if e=p.PingContext(ctx);e!=nil{return fmt.Errorf("PostgreSQL 连接失败: %w",e)}
 ok,e:=mc.BucketExists(ctx,a.cfg.Bucket);if e!=nil{return e};if !ok{return fmt.Errorf("MinIO bucket 不存在: %s",a.cfg.Bucket)}
 a.db,e=sql.Open("sqlite",a.cfg.StateDB);if e!=nil{return e};a.db.SetMaxOpenConns(1)
 _,e=a.db.Exec("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=30000;"+
 "CREATE TABLE IF NOT EXISTS downloads(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,parent_key TEXT,disk_name TEXT,target_path TEXT,partial_path TEXT,expected_bytes INTEGER,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TEXT NOT NULL,started_at TEXT,completed_at TEXT);"+
 "CREATE INDEX IF NOT EXISTS idx_download_task_status ON downloads(task_name,status);"+
 "CREATE TABLE IF NOT EXISTS task_disk_assignments(task_name TEXT NOT NULL,disk_name TEXT NOT NULL,last_used_at TEXT NOT NULL,PRIMARY KEY(task_name,disk_name));"+
 "CREATE TABLE IF NOT EXISTS downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,source_task_name TEXT,job_id INTEGER,sn_code TEXT,parent_key TEXT,disk_name TEXT,target_path TEXT,file_size INTEGER,content_time REAL,create_time TEXT,downloaded_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_downloaded_task ON downloaded_episodes(task_name);"+
 "CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL);"+
 "CREATE TABLE IF NOT EXISTS run_history(run_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,finished_at TEXT NOT NULL,duration_seconds REAL NOT NULL,mode TEXT NOT NULL,result TEXT NOT NULL,completed_count INTEGER NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,error TEXT);"+
  "CREATE TABLE IF NOT EXISTS run_task_history(run_id TEXT NOT NULL,task_name TEXT NOT NULL,completed_count INTEGER NOT NULL DEFAULT 0,failed_count INTEGER NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,wait_seconds REAL NOT NULL DEFAULT 0,duration_seconds REAL NOT NULL DEFAULT 0,PRIMARY KEY(run_id,task_name));CREATE TABLE IF NOT EXISTS run_disk_history(run_id TEXT NOT NULL,disk_name TEXT NOT NULL,duration_seconds REAL NOT NULL DEFAULT 0,completed_bytes INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,disk_name));"+
  "CREATE TABLE IF NOT EXISTS run_episode_history(run_id TEXT NOT NULL,episode_id TEXT NOT NULL,task_name TEXT NOT NULL,status TEXT NOT NULL,error TEXT,disk_name TEXT,completed_bytes INTEGER NOT NULL DEFAULT 0,content_seconds REAL NOT NULL DEFAULT 0,updated_at TEXT NOT NULL,failure_category TEXT NOT NULL DEFAULT '',PRIMARY KEY(run_id,episode_id));"+
  "CREATE INDEX IF NOT EXISTS idx_run_episode_lookup ON run_episode_history(run_id,task_name,status,updated_at);"+
 "CREATE INDEX IF NOT EXISTS idx_run_history_started ON run_history(started_at);")
 if e==nil{
  if _,e=a.db.Exec("CREATE TABLE IF NOT EXISTS runtime_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT NOT NULL,message TEXT NOT NULL); CREATE TABLE IF NOT EXISTS process_state(id INTEGER PRIMARY KEY CHECK(id=1),pid INTEGER NOT NULL,created REAL NOT NULL,mode TEXT NOT NULL,updated_at TEXT NOT NULL)");e!=nil{return e}
  log.Printf("MinIO=%s bucket=%s；PostgreSQL=%s:%d/%s；SQLite=%s",a.cfg.Endpoint,a.cfg.Bucket,a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.StateDB)
 };return e
}
type sqlExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func setWithExec(db sqlExecer, e Episode, d DiskConfig, final, partial, status, msg string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := db.Exec("INSERT INTO downloads(episode_id,task_name,parent_key,disk_name,target_path,partial_path,expected_bytes,status,error,started_at,completed_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
		"ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,partial_path=excluded.partial_path,expected_bytes=excluded.expected_bytes,status=excluded.status,error=excluded.error,"+
		"started_at=CASE WHEN excluded.status='running' THEN excluded.updated_at ELSE downloads.started_at END,"+
		"completed_at=CASE WHEN excluded.status='completed' THEN excluded.updated_at WHEN excluded.status='running' THEN NULL ELSE downloads.completed_at END,updated_at=excluded.updated_at",
		e.ID, e.Task, e.ParentKey, d.Name, nullable(final), nullable(partial), e.Size, status, nullable(msg), nullableTime(status == "running", now), nullableTime(status == "completed", now), now)
	return err
}

func (a *App) set(e Episode, d DiskConfig, final, partial, status, msg string) error {
	return setWithExec(a.db, e, d, final, partial, status, msg)
}

func recordWithExec(db sqlExecer, e Episode, d DiskConfig, final string) error {
	_, err := db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,source_task_name,job_id,sn_code,parent_key,disk_name,target_path,file_size,content_time,create_time,downloaded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
		"ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,source_task_name=excluded.source_task_name,job_id=excluded.job_id,sn_code=excluded.sn_code,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,file_size=excluded.file_size,content_time=excluded.content_time,create_time=excluded.create_time,downloaded_at=excluded.downloaded_at",
		e.ID, e.Task, e.SourceTask, e.JobID, e.SN, e.ParentKey, d.Name, final, e.Size, e.Duration, e.Created, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (a *App) record(e Episode, d DiskConfig, final string) error {
	return recordWithExec(a.db, e, d, final)
}

func (a *App) recordAndSetForRun(e Episode,d DiskConfig,final string)error{
 if err:=a.recordAndSet(e,d,final);err!=nil{return err}
 if a.runID==""{return nil}
 now:=time.Now().UTC().Format(time.RFC3339Nano)
 if _,err:=a.db.Exec("INSERT INTO run_task_history(run_id,task_name,completed_count,completed_bytes,content_seconds) VALUES(?,?,1,?,?) ON CONFLICT(run_id,task_name) DO UPDATE SET completed_count=completed_count+1,completed_bytes=completed_bytes+excluded.completed_bytes,content_seconds=content_seconds+excluded.content_seconds",a.runID,e.Task,e.Size,e.Duration);err!=nil{return fmt.Errorf("保存本次任务统计失败: %w",err)}
 if _,err:=a.db.Exec("INSERT INTO run_episode_history(run_id,episode_id,task_name,status,disk_name,completed_bytes,content_seconds,updated_at) VALUES(?,?,?,'completed',?,?,?,?) ON CONFLICT(run_id,episode_id) DO UPDATE SET task_name=excluded.task_name,status=excluded.status,error=NULL,disk_name=excluded.disk_name,completed_bytes=excluded.completed_bytes,content_seconds=excluded.content_seconds,updated_at=excluded.updated_at",a.runID,e.ID,e.Task,d.Name,e.Size,e.Duration,now);err!=nil{return fmt.Errorf("保存本次 Episode 明细失败: %w",err)}
 return nil
}

func (a *App) recordAndSet(e Episode, d DiskConfig, final string) error {
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = recordWithExec(tx, e, d, final); err != nil {
		return fmt.Errorf("写入完成台账失败: %w", err)
	}
	if err = setWithExec(tx, e, d, final, "", "completed", ""); err != nil {
		return fmt.Errorf("更新下载完成状态失败: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("提交下载完成记录失败: %w", err)
	}
	return nil
}
func nullableTime(ok bool,v string)any{if ok{return v};return nil}
func nullable(s string)any{if s==""{return nil};return s}
func completedEpisodeDir(dir string,requireMCAP bool,mcapPattern string)bool{
 info,err:=os.Lstat(dir)
 if err!=nil||!info.IsDir()||info.Mode()&os.ModeSymlink!=0{return false}
 entries,err:=os.ReadDir(dir);if err!=nil{return false}
 taskInfoFound:=false;mcapFound:=!requireMCAP
 for _,entry:=range entries{
  info,err:=os.Lstat(filepath.Join(dir,entry.Name()));if err!=nil||!info.Mode().IsRegular()||info.Mode()&os.ModeSymlink!=0{continue}
  if entry.Name()=="task_info.json"{taskInfoFound=true}
  if requireMCAP{
   matched,matchErr:=path.Match(mcapPattern,entry.Name())
   if matchErr==nil&&matched{mcapFound=true}
  }
 }
 return taskInfoFound&&mcapFound
}
func(a *App)completedPath(path string)bool{
 return a.cfg.TrustCompletedRecords||completedEpisodeDir(path,a.cfg.RequireMCAP,a.cfg.MCAPPattern)
}
func(a *App)completed(id string)bool{
 var p sql.NullString
 err:=a.db.QueryRow("SELECT target_path FROM downloads WHERE episode_id=? AND status='completed'",id).Scan(&p)
 if err==nil&&p.Valid&&a.completedPath(p.String){return true}
 p=sql.NullString{}
 err=a.db.QueryRow("SELECT target_path FROM downloaded_episodes WHERE episode_id=?",id).Scan(&p)
 return err==nil&&p.Valid&&a.completedPath(p.String)
}
func(a *App)stats(task string)(float64,int,error){
 rows,err:=a.db.Query("SELECT content_time,target_path FROM downloaded_episodes WHERE task_name=?",task)
 if err!=nil{return 0,0,fmt.Errorf("读取任务 %s 的完成统计失败: %w",task,err)}
 defer rows.Close()
 var seconds float64
 count:=0
 for rows.Next(){
  var v sql.NullFloat64
  var p sql.NullString
  if err:=rows.Scan(&v,&p);err!=nil{return 0,0,fmt.Errorf("读取任务 %s 的完成统计记录失败: %w",task,err)}
  if p.Valid&&a.completedPath(p.String){seconds+=v.Float64;count++}
 }
 if err:=rows.Err();err!=nil{return 0,0,fmt.Errorf("遍历任务 %s 的完成统计失败: %w",task,err)}
 return seconds,count,nil
}
func(a *App)assign(task,disk string)error{tx,err:=a.db.Begin();if err!=nil{return err};defer tx.Rollback();if _,err=tx.Exec("DELETE FROM task_disk_assignments WHERE task_name=?",task);err!=nil{return err};if _,err=tx.Exec("INSERT INTO task_disk_assignments(task_name,disk_name,last_used_at) VALUES(?,?,?)",task,disk,time.Now().UTC().Format(time.RFC3339Nano));err!=nil{return err};return tx.Commit()}
func(a *App)assigned(task string)string{var d string;_=a.db.QueryRow("SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC LIMIT 1",task).Scan(&d);return d}

func(a *App)fetch()([]Episode,error){
 names:=[]string{};aliases:=map[string]string{};for _,t:=range a.cfg.Tasks{names=append(names,t.Name);for _,x:=range t.Aliases{names=append(names,x);aliases[x]=t.Name}}
 pg:=postgresDSN(a.cfg)
 db,e:=sql.Open("pgx",pg);if e!=nil{return nil,e};defer db.Close()
 quality:="j.quality_status NOT IN ('不合格','未解析','未质检')";if a.cfg.IncludeNull{quality="(j.quality_status IS NULL OR "+quality+")"}
 q:="SELECT j.episode_id::text,j.job_id,j.task_name,COALESCE(j.sn_code,''),j.parent_key,COALESCE(j.file_size,0),COALESCE(j.content_time,0),j.create_time "+
 "FROM public.minio_galbot_scene_task_jobs j WHERE j.del_flag=0 AND "+quality+
 " AND j.job_id IN (SELECT t.job_id FROM public.minio_galbot_scene_task t WHERE t.task_name=ANY($1)) ORDER BY j.task_name,j.create_time NULLS LAST,j.episode_id"
 rows,e:=db.QueryContext(a.ctx,q,names);if e!=nil{return nil,e};defer rows.Close()
 out:=[]Episode{}
 for rows.Next(){var x Episode;var src string;var key sql.NullString;var duration sql.NullFloat64
  if e=rows.Scan(&x.ID,&x.JobID,&src,&x.SN,&key,&x.Size,&duration,&x.Created);e!=nil{return nil,e}
  x.SourceTask=src;x.Task=src;if v,ok:=aliases[src];ok{x.Task=v};x.Duration=duration.Float64
  normalized,normalizeErr:=normalizeParentKey(key.String)
  if normalizeErr!=nil{log.Printf("跳过无效 parent_key episode_id=%s: %v",x.ID,normalizeErr);continue}
  x.ParentKey=normalized
  if x.ID==""{continue};out=append(out,x)
 }
 if e=rows.Err();e!=nil{return nil,e}
 order:=map[string]int{};for i,t:=range a.cfg.Tasks{order[t.Name]=i}
 sort.SliceStable(out,func(i,j int)bool{
  if order[out[i].Task]!=order[out[j].Task]{return order[out[i].Task]<order[out[j].Task]}
  if out[i].Created.Valid!=out[j].Created.Valid{return out[i].Created.Valid}
  if out[i].Created.Valid&&!out[i].Created.Time.Equal(out[j].Created.Time){return out[i].Created.Time.Before(out[j].Created.Time)}
  return out[i].ID<out[j].ID
 })
 ids:=map[string]bool{};keys:=map[string]bool{};unique:=make([]Episode,0,len(out))
 for _,x:=range out{if ids[x.ID]||keys[x.ParentKey]{continue};ids[x.ID]=true;keys[x.ParentKey]=true;unique=append(unique,x)}
 log.Printf("查询并去重后 %d 条候选",len(unique));return unique,nil
}
func(a *App)checkRequiredFiles(parent string)(string,string,error){
 direct:=map[string]bool{}
 for obj:=range a.mc.ListObjects(a.ctx,a.cfg.Bucket,minio.ListObjectsOptions{Prefix:parent+"/",Recursive:false}){
  if obj.Err!=nil{return "error","MinIO 目录检查失败",obj.Err}
  if !strings.HasSuffix(obj.Key,"/"){direct[path.Base(obj.Key)]=true}
 }
 if !direct["task_info.json"]{return "missing_task_info","缺少 task_info.json",nil}
 if a.cfg.RequireMCAP{
  found:=false;for name:=range direct{if ok,_:=path.Match(a.cfg.MCAPPattern,name);ok{found=true;break}}
  if !found{return "missing_mcap","缺少 mcap",nil}
 }
 return "ok","",nil
}
func(a *App)syncLocalCopies(groups map[string][]Episode)error{
 for _,task:=range a.cfg.Tasks{
  for _,ep:=range groups[task.Name]{
   // Reconcile historical ledger/state records first. Python trusts these records
   // when configured, even if the original disk has been archived or replaced.
   var ledgerPath,ledgerDisk sql.NullString
   ledgerErr:=a.db.QueryRow("SELECT target_path,disk_name FROM downloaded_episodes WHERE episode_id=?",ep.ID).Scan(&ledgerPath,&ledgerDisk)
   if ledgerErr==nil&&ledgerPath.Valid&&a.completedPath(ledgerPath.String){
    d:=DiskConfig{Name:ledgerDisk.String}
    if err:=a.set(ep,d,ledgerPath.String,"","completed","");err!=nil{return err}
   }else{
    var statePath,stateDisk sql.NullString
    var state string
    stateErr:=a.db.QueryRow("SELECT status,target_path,disk_name FROM downloads WHERE episode_id=?",ep.ID).Scan(&state,&statePath,&stateDisk)
    if stateErr==nil&&state=="completed"&&statePath.Valid&&a.completedPath(statePath.String){
     d:=DiskConfig{Name:stateDisk.String}
     if err:=a.recordAndSet(ep,d,statePath.String);err!=nil{return err}
    }
   }
   diskName,err:=findExistingTaskDisk(a.cfg.DataVersion,task,a.cfg.Disks,[]Episode{ep},a.cfg.RequireMCAP,a.cfg.MCAPPattern)
   if err!=nil{return err};if diskName==""{continue}
   var chosen *DiskConfig
   for i:=range a.cfg.Disks{if a.cfg.Disks[i].Name==diskName{chosen=&a.cfg.Disks[i];break}}
   if chosen==nil{return fmt.Errorf("找不到 episode %s 的本地磁盘 %s",ep.ID,diskName)}
   pinned:=a.assigned(task.Name)
   if pinned!=""&&pinned!=diskName{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode %s 已存在于 %s",task.Name,pinned,ep.ID,diskName)}
   if pinned==""{if err:=a.assign(task.Name,diskName);err!=nil{return err}}
   final,_:=episodePaths(a.cfg.DataVersion,task,*chosen,ep.ParentKey)
   if err:=a.recordAndSet(ep,*chosen,final);err!=nil{return err}
  }
 }
 return nil
}
func(a *App)prevalidateCount(groups map[string][]Episode)(map[string][]Episode,error){
 validGroups:=map[string][]Episode{}
 for _,task:=range a.cfg.Tasks{
  if a.ctx.Err()!=nil{return nil,a.ctx.Err()}
  candidates:=groups[task.Name];if len(candidates)==0{continue}
  selected:=make([]Episode,0)
  todo:=make([]Episode,0,len(candidates));skipped:=0
  for _,ep:=range candidates{
   if a.completed(ep.ID){selected=append(selected,ep);continue}
   if ep.Size<=0{d:=a.cfg.Disks[0];if err:=a.set(ep,d,"","","invalid_size","file_size 无效");err!=nil{return nil,err};skipped++;continue}
   var state string
   _=a.db.QueryRow("SELECT status FROM downloads WHERE episode_id=?",ep.ID).Scan(&state)
   if !a.recheck&&(state=="missing_task_info"||state=="missing_mcap"){skipped++;continue}
   todo=append(todo,ep)
  }
  _,downloaded,statsErr:=a.stats(task.Name);if statsErr!=nil{return nil,statsErr};gap:=task.TargetCount-downloaded
  if gap<=0{validGroups[task.Name]=selected;continue}
  chosenCount:=0
  for pos:=0;pos<len(todo)&&chosenCount<gap;{
   if a.ctx.Err()!=nil{return nil,a.ctx.Err()}
   batchSize:=gap-chosenCount;if batchSize>len(todo)-pos{batchSize=len(todo)-pos}
   batch:=todo[pos:pos+batchSize];pos+=batchSize
   type checkResult struct{episode Episode;status,detail string;err error}
   results:=make([]checkResult,len(batch));sem:=make(chan struct{},a.cfg.ValidateWorkers);var wg sync.WaitGroup
   for i,ep:=range batch{i,ep:=i,ep;sem<-struct{}{};wg.Add(1);go func(){defer wg.Done();defer func(){<-sem}();status,detail,err:=a.checkRequiredFiles(ep.ParentKey);results[i]=checkResult{ep,status,detail,err}}()}
   wg.Wait()
   for _,result:=range results{
    if result.err!=nil{log.Printf("条数预检查失败，跳过并留待下次重试 %s: %v",result.episode.ID,result.err);continue}
    if result.status=="ok"{a.validated[result.episode.ID]=true;selected=append(selected,result.episode);chosenCount++;continue}
    d:=a.cfg.Disks[0];if name:=a.assigned(task.Name);name!=""{for _,candidate:=range a.cfg.Disks{if candidate.Name==name{d=candidate;break}}}
    if err:=a.set(result.episode,d,"","",result.status,result.detail);err!=nil{return nil,err}
    skipped++
   }
  }
  validGroups[task.Name]=selected
  if chosenCount<gap{log.Printf("任务 %s 条数预检查后仍缺 %d 条：需要 %d，找到 %d 个有效候选，跳过 %d 条",task.Name,gap-chosenCount,gap,chosenCount,skipped)}
 }
 return validGroups,nil
}
func(a *App)isMissingStatus(id string)bool{
 var status string
 err:=a.db.QueryRow("SELECT status FROM downloads WHERE episode_id=?",id).Scan(&status)
 return err==nil&&(status=="missing_task_info"||status=="missing_mcap")
}
func(a *App)reconcileInterruptedDownloads()error{
 tx,err:=a.db.Begin()
 if err!=nil{return err}
 defer tx.Rollback()
 rows,err:=tx.Query("SELECT episode_id,target_path FROM downloads WHERE status='running'")
 if err!=nil{return err}
 type staleDownload struct{id string;target sql.NullString}
 stale:=[]staleDownload{}
 for rows.Next(){
  var item staleDownload
  if err=rows.Scan(&item.id,&item.target);err!=nil{rows.Close();return err}
  stale=append(stale,item)
 }
 if err=rows.Err();err!=nil{rows.Close();return err}
 if err=rows.Close();err!=nil{return err}
 now:=time.Now().UTC().Format(time.RFC3339Nano)
 recovered,reset:=0,0
 for _,item:=range stale{
  status:="pending"
  message:="检测到上次运行异常中断；保留临时文件，等待续传"
  completedAt:=any(nil)
  if item.target.Valid&&completedEpisodeDir(item.target.String,a.cfg.RequireMCAP,a.cfg.MCAPPattern){
   status="completed"
   message=""
   completedAt=now
   recovered++
  }else{reset++}
  _,err=tx.Exec("UPDATE downloads SET status=?,error=?,completed_at=?,updated_at=? WHERE episode_id=? AND status='running'",
   status,nullable(message),completedAt,now,item.id)
  if err!=nil{return fmt.Errorf("恢复中断下载记录 %s 失败: %w",item.id,err)}
 }
 if err=tx.Commit();err!=nil{return err}
 if len(stale)>0{log.Printf("SQLite 中断任务恢复：已完成目录 %d 条，重置为待续传 %d 条",recovered,reset)}
 return nil
}
func(a *App)run(all []Episode,nround int)error{
 if err:=a.reconcileInterruptedDownloads();err!=nil{return fmt.Errorf("恢复上次中断的下载状态失败: %w",err)}
 groups:=map[string][]Episode{};for _,e:=range all{groups[e.Task]=append(groups[e.Task],e)}
 if err:=a.syncLocalCopies(groups);err!=nil{return err}
 if a.mode=="count"{var err error;groups,err=a.prevalidateCount(groups);if err!=nil{if a.ctx.Err()!=nil{return nil};return err}}
 for r:=1;r<=nround;r++{if a.ctx.Err()!=nil{return nil};selected:=map[string][]Episode{}
  for _,t:=range a.cfg.Tasks{s,n,statsErr:=a.stats(t.Name);if statsErr!=nil{return statsErr};target,used:=t.TargetHours*3600,s;if a.mode=="count"{target=float64(t.TargetCount);used=float64(n)}
   candidates:=groups[t.Name]
   if a.mode=="duration"{missing:=map[string]bool{};for _,ep:=range candidates{missing[ep.ID]=a.isMissingStatus(ep.ID)};sort.SliceStable(candidates,func(i,j int)bool{return !missing[candidates[i].ID]&&missing[candidates[j].ID]})}
   addSec:=0.0;addN:=0
   for _,e:=range candidates{if a.completed(e.ID){continue};if a.mode=="count"&&!a.recheck&&a.isMissingStatus(e.ID){continue}
    if (a.mode=="duration"&&addSec>=target-used)||(a.mode=="count"&&float64(addN)>=target-used){break};selected[t.Name]=append(selected[t.Name],e);addSec+=e.Duration;addN++}
   if a.mode=="duration"{log.Printf("按时长 %s: 目标 %.2fh 已完成 %.2fh 新选 %d 条/%.2fh",t.Name,target/3600,s/3600,addN,addSec/3600)}else{log.Printf("按条数 %s: 目标 %d 已完成 %d 新选 %d 条",t.Name,int(target),n,addN)}
  }
  if e:=a.downloadSelected(selected);e!=nil{return e};log.Printf("第 %d 轮结束",r)
 };return nil
}
type batch struct{t Task;d DiskConfig;rows []Episode;queuedAt time.Time}
func findExistingTaskDisk(version string,t Task,disks []DiskConfig,rows []Episode,requireMCAP bool,mcapPattern string)(string,error){
 found:=""
 for _,ep:=range rows{
  for _,d:=range disks{
   final,_:=episodePaths(version,t,d,ep.ParentKey)
   if completedEpisodeDir(final,requireMCAP,mcapPattern){
    if found!=""&&found!=d.Name{return "",fmt.Errorf("任务 %s 的已有数据分布在多块磁盘（%s、%s）",t.Name,found,d.Name)}
    found=d.Name
   }
  }
 }
 return found,nil
}
func resolvePathWithMissingTail(p string)(string,error){
 abs,err:=filepath.Abs(p);if err!=nil{return "",err}
 cur:=filepath.Clean(abs);tail:=[]string{}
 for{
  resolved,e:=filepath.EvalSymlinks(cur)
  if e==nil{for i:=len(tail)-1;i>=0;i--{resolved=filepath.Join(resolved,tail[i])};return filepath.Clean(resolved),nil}
  parent:=filepath.Dir(cur);if parent==cur{return "",e}
  tail=append(tail,filepath.Base(cur));cur=parent
 }
}
func validateDisk(d DiskConfig)error{
 mount:=d.Mountpoint;if mount==""{mount=d.Path}
 mountResolved,err:=resolvePathWithMissingTail(mount);if err!=nil{return fmt.Errorf("磁盘挂载点不存在 %s: %w",mount,err)}
 pathResolved,err:=resolvePathWithMissingTail(d.Path);if err!=nil{return fmt.Errorf("无法解析目标路径 %s: %w",d.Path,err)}
 rel,err:=filepath.Rel(mountResolved,pathResolved)
 if err!=nil||rel==".."||strings.HasPrefix(rel,".."+string(os.PathSeparator)){return fmt.Errorf("目标路径 %s 不在挂载点 %s 之下",d.Path,mount)}
 parts,err:=disk.Partitions(false);if err!=nil{return fmt.Errorf("读取磁盘挂载信息失败: %w",err)}
 found:=false;want:=filepath.Clean(mountResolved);if runtime.GOOS=="windows"{want=strings.ToLower(want)}
 for _,part:=range parts{got,resolveErr:=resolvePathWithMissingTail(part.Mountpoint);if resolveErr!=nil{continue};got=filepath.Clean(got);if runtime.GOOS=="windows"{got=strings.ToLower(got)};if got==want{found=true;break}}
 if !found{return fmt.Errorf("路径存在但不是已挂载的磁盘挂载点: %s",mount)}
 return nil
}
func validateDiskSet(disks []DiskConfig)error{
 parts,err:=disk.Partitions(false);if err!=nil{return fmt.Errorf("读取磁盘挂载信息失败: %w",err)}
 identities:=map[string]string{}
 for _,d:=range disks{
  if err:=validateDisk(d);err!=nil{return err}
  mount:=d.Mountpoint;if mount==""{mount=d.Path}
  resolved,err:=resolvePathWithMissingTail(mount);if err!=nil{return err}
  identity,err:=filesystemIdentity(resolved);if err!=nil{return fmt.Errorf("读取磁盘 %s 的文件系统标识失败: %w",d.Name,err)}
  keys:=[]string{}
  if identity!=""{keys=append(keys,identity)}
  for _,part:=range parts{
   got,e:=resolvePathWithMissingTail(part.Mountpoint);if e!=nil{continue}
   same:=filepath.Clean(got)==filepath.Clean(resolved);if runtime.GOOS=="windows"{same=strings.EqualFold(filepath.Clean(got),filepath.Clean(resolved))}
   if same&&part.Device!=""{
    device:=strings.TrimSpace(part.Device)
    if runtime.GOOS=="windows"{device=strings.ToLower(device)}
    keys=append(keys,"device:"+device)
   }
  }
  for _,key:=range keys{
   if old,ok:=identities[key];ok{return fmt.Errorf("磁盘 %s 和 %s 指向同一个文件系统（%s）",old,d.Name,key)}
  }
  for _,key:=range keys{identities[key]=d.Name}
 }
 return nil
}
func taskRootPath(version string,t Task,d DiskConfig)string{
 return filepath.Join(d.Path,safe(version),safe(t.Scene),safe(taskDir(t)))
}
func taskRootHasEntries(version string,t Task,d DiskConfig)(bool,error){
 root:=taskRootPath(version,t,d)
 info,err:=os.Lstat(root)
 if os.IsNotExist(err){return false,nil};if err!=nil{return false,err}
 if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return false,fmt.Errorf("任务目录不是普通目录: %s",root)}
 entries,err:=os.ReadDir(root);if err!=nil{return false,err}
 return len(entries)>0,nil
}
func directorySizeBytes(root string)(int64,error){
 info,err:=os.Lstat(root);if os.IsNotExist(err){return 0,nil};if err!=nil{return 0,err}
 if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return 0,fmt.Errorf("暂存路径不是普通目录: %s",root)}
 var total int64
 err=filepath.Walk(root,func(p string,info os.FileInfo,walkErr error)error{
  if walkErr!=nil{return walkErr}
  if info.Mode()&os.ModeSymlink!=0{return nil}
  if info.Mode().IsRegular(){total+=info.Size()}
  return nil
 })
 return total,err
}
func addSpaceReservations(reserved,additional,minFree uint64)(uint64,bool){
 const max=^uint64(0)
 if max-reserved<additional{return 0,false}
 total:=reserved+additional
 if max-total<minFree{return 0,false}
 return total+minFree,true
}
func chooseTaskDisk(task Task,disks []DiskConfig,loads map[string]int,folderDisk map[string]*DiskConfig)*DiskConfig{
 folder:=taskFolderKey(task)
 if assigned:=folderDisk[folder];assigned!=nil{return assigned}
 var chosen *DiskConfig
 for i:=range disks{candidate:=&disks[i];if chosen==nil||loads[candidate.Name]<loads[chosen.Name]{chosen=candidate}}
 if chosen!=nil{folderDisk[folder]=chosen;loads[chosen.Name]++}
 return chosen
}
func diskLockKeys(disks []DiskConfig) ([]string,error) {
 parts,err:=disk.Partitions(false);if err!=nil{return nil,fmt.Errorf("读取磁盘挂载信息失败，无法建立跨进程磁盘锁: %w",err)}
 keys:=map[string]bool{}
 for _,d:=range disks {
  resolved,err:=resolvePathWithMissingTail(d.Path);if err!=nil{return nil,fmt.Errorf("解析磁盘路径失败 %s: %w",d.Path,err)}
  key:=""
  // On Unix, st_dev identifies the actual filesystem, including bind mounts
  // whose mount source strings may differ. Windows falls back to the volume
  // device reported by the partition table.
  if identity,e:=filesystemIdentity(resolved);e==nil&&identity!=""{key=identity}
  if key=="" {
   target:=filepath.Clean(resolved);bestLen:=-1
   for _,p:=range parts {
    mount,e:=resolvePathWithMissingTail(p.Mountpoint);if e!=nil{continue};mount=filepath.Clean(mount)
    rel,e:=filepath.Rel(mount,target)
    if e!=nil||rel==".."||strings.HasPrefix(rel,".."+string(os.PathSeparator)){continue}
    if runtime.GOOS=="windows"&&!strings.EqualFold(mount,target)&&!strings.HasPrefix(strings.ToLower(target),strings.ToLower(mount+string(os.PathSeparator))){continue}
    if len(mount)>bestLen&&p.Device!=""{bestLen=len(mount);key=p.Device}
   }
  }
  if key==""{key=resolved}
  sum:=sha256.Sum256([]byte(strings.ToLower(key)))
  keys[hex.EncodeToString(sum[:])]=true
 }
 out:=make([]string,0,len(keys));for k:=range keys{out=append(out,k)};sort.Strings(out);return out,nil
}
func diskLockDirectory(c Config)string{
 base:=strings.TrimSpace(c.LockFile)
 if base==""{base="minio_download.lock"}
 return filepath.Join(filepath.Dir(base),"disk-locks")
}
func acquireNamedDiskLocks(keys []string,lockDir string)(func(),error) {
 if err:=os.MkdirAll(lockDir,0700);err!=nil{return nil,fmt.Errorf("创建磁盘锁目录失败: %w",err)}
 locks:=make([]*flock.Flock,0,len(keys))
 for _,key:=range keys {
  lock:=flock.New(filepath.Join(lockDir,"minio-download-disk-"+key+".lock"))
  ok,err:=lock.TryLock()
  if err!=nil||!ok {
   for i:=len(locks)-1;i>=0;i--{_ = locks[i].Unlock()}
   if err!=nil{return nil,fmt.Errorf("获取磁盘并发锁失败: %w",err)}
   return nil,fmt.Errorf("目标文件系统正在被另一个下载器实例使用（锁 %s）",key)
  }
  locks=append(locks,lock)
 }
 return func(){for i:=len(locks)-1;i>=0;i--{_ = locks[i].Unlock()}},nil
}
func(a *App)downloadSelected(sel map[string][]Episode)error{
 // A SQLite process lease prevents concurrent downloader instances; no lock files are created.
 loads:=map[string]int{};existing:=map[string]*DiskConfig{};folderDisk:=map[string]*DiskConfig{}
 // Existing task footprints (including .partial directories) are authoritative.
 // Stale SQLite assignments without any on-disk data are deliberately ignored.
 for _,task:=range a.cfg.Tasks{
  var found *DiskConfig
  for i:=range a.cfg.Disks{
   has,err:=taskRootHasEntries(a.cfg.DataVersion,task,a.cfg.Disks[i]);if err!=nil{return err}
   if has{if found!=nil{return fmt.Errorf("任务 %s 的目录同时存在于多块磁盘（%s、%s）",task.Name,found.Name,a.cfg.Disks[i].Name)};found=&a.cfg.Disks[i]}
  }
  if found!=nil{existing[task.Name]=found;folder:=taskFolderKey(task);if assigned,ok:=folderDisk[folder];ok&&assigned.Name!=found.Name{return fmt.Errorf("共用目录 %s 的任务分布在不同磁盘（%s、%s）",folder,assigned.Name,found.Name)};if _,ok:=folderDisk[folder];!ok{folderDisk[folder]=found;loads[found.Name]++}}
 }
 byDisk:=map[string][]batch{};waitingSpace:=false;reservedBytes:=map[string]uint64{}
 for _,task:=range a.cfg.Tasks{
  rows:=sel[task.Name];if len(rows)==0{continue}
  d:=existing[task.Name]
  if d==nil{d=chooseTaskDisk(task,a.cfg.Disks,loads,folderDisk)}
  if d==nil{return errors.New("没有配置目标磁盘")}
  if err:=os.MkdirAll(d.Path,0755);err!=nil{return err}
  usage,err:=disk.Usage(d.Path);if err!=nil{return err}
  var bytes int64
  for _,ep:=range rows{
   final,partial:=episodePaths(a.cfg.DataVersion,task,*d,ep.ParentKey)
   if pathIsDir(final){continue}
   partialBytes,sizeErr:=directorySizeBytes(partial);if sizeErr!=nil{return sizeErr}
   remaining:=ep.Size-partialBytes;if remaining<0{remaining=0};bytes+=remaining
  }
  estimated:=uint64(math.Ceil(float64(bytes)*a.cfg.SafetyFactor))
  minFree:=uint64(math.Ceil(a.cfg.MinFreeGB*1024*1024*1024))
  need,ok:=addSpaceReservations(reservedBytes[d.Name],estimated,minFree)
  if !ok{return fmt.Errorf("磁盘空间预检数值溢出: %s",d.Name)}
  if usage.Free<need{
   log.Printf("任务整体空间预检不通过，未启动: %s disk=%s free=%d need=%d",task.Name,d.Name,usage.Free,need)
   for _,ep:=range rows{if err:=a.set(ep,*d,"","","waiting_space","磁盘剩余空间不足，任务整体未启动");err!=nil{return err}}
   waitingSpace=true;continue
  }
  reservedBytes[d.Name]+=estimated
  if err:=a.assign(task.Name,d.Name);err!=nil{return fmt.Errorf("保存任务磁盘分配失败 %s: %w",task.Name,err)}
  byDisk[d.Name]=append(byDisk[d.Name],batch{t:task,d:*d,rows:rows,queuedAt:time.Now()})
 }
 var wg sync.WaitGroup;errs:=make(chan error,len(byDisk))
 for diskName,batches:=range byDisk{
  diskName,batches:=diskName,batches;wg.Add(1)
  go func(){started:=time.Now();defer wg.Done();defer func(){if err:=a.recordRunDiskTiming(diskName,time.Since(started).Seconds());err!=nil{log.Printf("保存磁盘运行统计失败 %s: %v",diskName,err)}}();for _,b:=range batches{if a.ctx.Err()!=nil{return};if err:=a.downloadTask(b);err!=nil{errs<-err;return}}}()
 }
 wg.Wait();close(errs)
 for err:=range errs{if err!=nil{return err}}
 if waitingSpace{return errors.New("部分任务因磁盘空间不足未启动；请释放空间后重新运行")}
 return nil
}
func acquireSemaphore(ctx context.Context,sem chan struct{})bool{
 select{case sem<-struct{}{}:return true;case <-ctx.Done():return false}
}
func(a *App)recordRunTaskTiming(taskName string,waitSeconds,durationSeconds float64)error{
 if a.runID==""{return nil}
 _,err:=a.db.Exec("INSERT INTO run_task_history(run_id,task_name,wait_seconds,duration_seconds) VALUES(?,?,?,?) ON CONFLICT(run_id,task_name) DO UPDATE SET wait_seconds=run_task_history.wait_seconds+excluded.wait_seconds,duration_seconds=run_task_history.duration_seconds+excluded.duration_seconds",a.runID,taskName,waitSeconds,durationSeconds)
 return err
}
func(a *App)recordRunDiskTiming(diskName string,durationSeconds float64)error{
 if a.runID==""{return nil}
 var bytes int64
 if err:=a.db.QueryRow("SELECT COALESCE(SUM(completed_bytes),0) FROM run_episode_history WHERE run_id=? AND disk_name=? AND status='completed'",a.runID,diskName).Scan(&bytes);err!=nil{return err}
 _,err:=a.db.Exec("INSERT INTO run_disk_history(run_id,disk_name,duration_seconds,completed_bytes) VALUES(?,?,?,?) ON CONFLICT(run_id,disk_name) DO UPDATE SET duration_seconds=run_disk_history.duration_seconds+excluded.duration_seconds,completed_bytes=MAX(run_disk_history.completed_bytes,excluded.completed_bytes)",a.runID,diskName,durationSeconds,bytes)
 return err
}
func(a *App)downloadTask(b batch)error{
 started:=time.Now();waitSeconds:=0.0;if !b.queuedAt.IsZero(){waitSeconds=math.Max(0,started.Sub(b.queuedAt).Seconds())}
 defer func(){if err:=a.recordRunTaskTiming(b.t.Name,waitSeconds,time.Since(started).Seconds());err!=nil{log.Printf("保存任务耗时统计失败 %s: %v",b.t.Name,err)}}()
 sem:=make(chan struct{},a.cfg.Workers);var wg sync.WaitGroup;var mu sync.Mutex;var first error
 var stopStarting atomic.Bool
 for _,ep:=range b.rows{
  if !acquireSemaphore(a.ctx,sem){break}
  wg.Add(1)
  go func(e Episode){
   defer wg.Done();defer func(){<-sem}()
   if stopStarting.Load(){return}
   if err:=a.downloadOne(b.t,b.d,e);err!=nil{
    if errors.Is(err,errInsufficientDiskSpace){stopStarting.Store(true)}
    mu.Lock();if first==nil{first=err};mu.Unlock()
   }
  }(ep)
 }
 wg.Wait();return first
}
func episodePaths(version string,t Task,d DiskConfig,parentKey string)(string,string){
 root:=filepath.Join(d.Path,safe(version),safe(t.Scene),safe(taskDir(t)))
 leaf:=safe(path.Base(strings.Trim(parentKey,"/")))
 return filepath.Join(root,leaf),filepath.Join(root,"."+leaf+".partial")
}
var errInsufficientDiskSpace=errors.New("动态磁盘空间不足")
type spaceReservation struct { app *App; diskName string; mu sync.Mutex; remaining uint64; consumed uint64 }
func(a *App)reserveObjectSpace(d DiskConfig,size int64)(*spaceReservation,error){
 if size<0{return nil,fmt.Errorf("对象大小无效: %d",size)}
 a.spaceMu.Lock()
 usage,err:=disk.Usage(d.Path)
 if err!=nil{a.spaceMu.Unlock();return nil,fmt.Errorf("动态读取磁盘空间失败 %s: %w",d.Name,err)}
 if a.spaceInFlight==nil{a.spaceInFlight=map[string]uint64{}}
 reserved:=a.spaceInFlight[d.Name]
 minFree:=uint64(math.Ceil(a.cfg.MinFreeGB*1024*1024*1024))
 next,ok:=addSpaceReservations(reserved,uint64(size),minFree)
 if !ok||usage.Free<next{a.spaceMu.Unlock();return nil,fmt.Errorf("%w，暂停启动新对象: disk=%s free=%d reserved=%d object=%d min_free=%d",errInsufficientDiskSpace,d.Name,usage.Free,reserved,size,minFree)}
 a.spaceInFlight[d.Name]=reserved+uint64(size)
 a.spaceMu.Unlock()
 return &spaceReservation{app:a,diskName:d.Name,remaining:uint64(size)},nil
}
func(r *spaceReservation)Consume(n int64){
 if r==nil||n<=0{return}
 r.mu.Lock();defer r.mu.Unlock()
 amount:=uint64(n);if amount>r.remaining{amount=r.remaining}
 if amount==0{return}
 r.app.spaceMu.Lock();current:=r.app.spaceInFlight[r.diskName]
 if current<=amount{delete(r.app.spaceInFlight,r.diskName)}else{r.app.spaceInFlight[r.diskName]=current-amount}
 r.app.spaceMu.Unlock();r.remaining-=amount;r.consumed+=amount
}
func(r *spaceReservation)Restore(n int64){
 if r==nil||n<=0{return}
 r.mu.Lock();defer r.mu.Unlock()
 amount:=uint64(n);if amount>r.consumed{amount=r.consumed}
 if amount==0{return}
 r.app.spaceMu.Lock()
 current:=r.app.spaceInFlight[r.diskName]
 if ^uint64(0)-current<amount{current=^uint64(0)}else{current+=amount}
 r.app.spaceInFlight[r.diskName]=current
 r.app.spaceMu.Unlock()
 r.remaining+=amount;r.consumed-=amount
}
func(r *spaceReservation)ConsumedBytes()int64{
 if r==nil{return 0}
 r.mu.Lock();defer r.mu.Unlock()
 if r.consumed>uint64(math.MaxInt64){return math.MaxInt64}
 return int64(r.consumed)
}
func(r *spaceReservation)Release(){
 if r==nil{return}
 r.mu.Lock();defer r.mu.Unlock()
 if r.remaining==0{return}
 r.app.spaceMu.Lock();current:=r.app.spaceInFlight[r.diskName]
 if current<=r.remaining{delete(r.app.spaceInFlight,r.diskName)}else{r.app.spaceInFlight[r.diskName]=current-r.remaining}
 r.app.spaceMu.Unlock();r.remaining=0
}
const fsyncIntervalBytes int64=32*1024*1024
type progressWriter struct {
 writer io.Writer
 onWrite func(int64)
 syncEvery int64
 sinceSync int64
 syncFn func()error
}
func(w *progressWriter)Write(p []byte)(int,error){
 n,err:=w.writer.Write(p)
 if n>0{
  if w.onWrite!=nil{w.onWrite(int64(n))}
  if w.syncEvery>0&&w.syncFn!=nil{
   w.sinceSync+=int64(n)
   if w.sinceSync>=w.syncEvery{
    if syncErr:=w.syncFn();err==nil{err=syncErr}
    w.sinceSync=0
   }
  }
 }
 return n,err
}
func existingObjectComplete(dst string,expected minio.ObjectInfo)bool{
 st,err:=os.Lstat(dst)
 if err!=nil||!st.Mode().IsRegular()||st.Size()!=expected.Size{return false}
 if expected.LastModified.IsZero(){return true}
 delta:=st.ModTime().Sub(expected.LastModified)
 if delta<0{delta=-delta}
 return delta<2*time.Second
}
const resumeBackoffBytes int64=64*1024*1024
func existingPartialBytes(dst string,expected int64)int64{
 if expected<=0{return 0}
 if st,err:=os.Lstat(dst+".minio-tmp");err==nil&&st.Mode().IsRegular()&&st.Size()>0&&st.Size()<=expected{return st.Size()}
 return 0
}
func resumeOffset(have,expected int64,firstAttempt bool)int64{
 if have<0||have>expected{return 0}
 if firstAttempt{
  if have>resumeBackoffBytes{return have-resumeBackoffBytes}
  return 0
 }
 return have
}
func truncateResumeFile(name string,size int64)error{
 f,err:=os.OpenFile(name,os.O_WRONLY,0)
 if err!=nil{return err}
 truncateErr:=f.Truncate(size)
 closeErr:=f.Close()
 if truncateErr!=nil{return truncateErr}
 return closeErr
}
func remainingObjectBytes(dst string,expected int64)int64{
 return expected-existingPartialBytes(dst,expected)
}
func(a *App)downloadOne(t Task,d DiskConfig,e Episode)error{
 if a.ctx.Err()!=nil{return nil};if e.Size<=0{_=a.set(e,d,"","","invalid_size","file_size 无效");return nil}
 var existing *DiskConfig;var existingPath string
 for i:=range a.cfg.Disks{
  candidate:=&a.cfg.Disks[i];candidateFinal,_:=episodePaths(a.cfg.DataVersion,t,*candidate,e.ParentKey)
  if err:=recoverEpisodeDirectory(candidateFinal);err!=nil{return a.fail(e,*candidate,candidateFinal,"",err)}
  if completedEpisodeDir(candidateFinal,a.cfg.RequireMCAP,a.cfg.MCAPPattern){
   if existing!=nil{return fmt.Errorf("episode %s 在多个磁盘存在正式目录，违反单副本规则",e.ID)}
   existing=candidate;existingPath=candidateFinal
  }
 }
 if existing!=nil{
  pinned:=a.assigned(t.Name)
  if pinned!=""&&pinned!=existing.Name{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode 已存在于 %s",t.Name,pinned,existing.Name)}
  if pinned==""{if err:=a.assign(t.Name,existing.Name);err!=nil{return err}}
  return a.recordAndSet(e,*existing,existingPath)
 }
 final,partial:=episodePaths(a.cfg.DataVersion,t,d,e.ParentKey)
 if err:=os.MkdirAll(partial,0755);err!=nil{return a.fail(e,d,final,partial,err)}
 if !a.validated[e.ID]{
  status,detail,checkErr:=a.checkRequiredFiles(e.ParentKey)
  if checkErr!=nil{return a.fail(e,d,final,partial,checkErr)}
  if status!="ok"{_=os.RemoveAll(partial);_=a.set(e,d,final,"",status,detail);return nil}
 }
 _=a.set(e,d,final,partial,"running","")
 objects:=make([]minio.ObjectInfo,0)
 for obj:=range a.mc.ListObjects(a.ctx,a.cfg.Bucket,minio.ListObjectsOptions{Prefix:e.ParentKey+"/",Recursive:true}){
  if obj.Err!=nil{return a.fail(e,d,final,partial,obj.Err)}
  if strings.HasSuffix(obj.Key,"/"){continue}
  objects=append(objects,obj)
 }
 if err:=sweepPartial(partial,e.ParentKey,objects);err!=nil{return a.fail(e,d,final,partial,err)}
 sem:=make(chan struct{},a.cfg.FileWorkers);var wg sync.WaitGroup;var completedObjects atomic.Int64;var mu sync.Mutex;var first error
 // Serialize the admission check with the first capacity failure so workers
 // that have not reserved space yet cannot start additional object downloads.
 var admissionMu sync.Mutex
 stopStarting:=false
 for _,item:=range objects{
  item:=item
  if !acquireSemaphore(a.ctx,sem){break}
  wg.Add(1)
  go func(){
   defer wg.Done();defer func(){<-sem}()
   rel:=strings.TrimPrefix(item.Key,e.ParentKey+"/")
   dst,err:=safeJoin(partial,rel)
   if err==nil{err=os.MkdirAll(filepath.Dir(dst),0755)}
   if err==nil&&existingObjectComplete(dst,item){
    completedObjects.Add(1)
    return
   }
   if err==nil{
    admissionMu.Lock()
    if stopStarting {
     admissionMu.Unlock()
     return
    }
    partialBytes:=existingPartialBytes(dst,item.Size)
    reservation,spaceErr:=a.reserveObjectSpace(d,remainingObjectBytes(dst,item.Size))
    if errors.Is(spaceErr,errInsufficientDiskSpace){stopStarting=true}
    if spaceErr==nil{reservation.consumed=uint64(partialBytes)}
    admissionMu.Unlock()
    if spaceErr!=nil{err=spaceErr}else{defer reservation.Release();err=a.fetchObject(item.Key,dst,item,reservation)}
   }
   if err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock();return}
   completedObjects.Add(1)
  }()
 }
 wg.Wait()
 if first!=nil{if errors.Is(first,errInsufficientDiskSpace){_=a.set(e,d,final,partial,"waiting_space",first.Error());return first};return a.fail(e,d,final,partial,first)}
 if a.ctx.Err()!=nil{return a.fail(e,d,final,partial,a.ctx.Err())}
 if completedObjects.Load()==0{return a.fail(e,d,final,partial,errors.New("MinIO 前缀为空"))};if err:=os.MkdirAll(filepath.Dir(final),0755);err!=nil{return a.fail(e,d,final,partial,err)}
 if err:=replaceEpisodeDirectory(partial,final);err!=nil{return a.fail(e,d,final,partial,err)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(final))}
 return a.recordAndSetForRun(e,d,final)
}
func sweepPartial(root,parent string,objects []minio.ObjectInfo)error{
 info,err:=os.Lstat(root);if err!=nil{return err};if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return fmt.Errorf("暂存路径不是普通目录: %s",root)}
 keep:=make(map[string]bool,len(objects)*2)
 for _,obj:=range objects{
  rel:=strings.TrimPrefix(obj.Key,parent+"/")
  dst,err:=safeJoin(root,rel);if err!=nil{return err}
  if err:=recoverDownloadedFile(dst,root);err!=nil{return err}
  keep[dst]=true;keep[dst+".minio-tmp"]=true
 }
 return filepath.Walk(root,func(p string,info os.FileInfo,walkErr error)error{
  if walkErr!=nil{return walkErr};if p==root{return nil}
  if info.Mode()&os.ModeSymlink!=0{return os.Remove(p)}
  if info.IsDir(){return nil}
  if !keep[p]{return os.Remove(p)}
  return nil
 })
}
func syncDirectory(dir string){
 f,err:=os.Open(dir);if err!=nil{return};_=f.Sync();_=f.Close()
}
func isTransientWindowsFileError(err error,windows bool)bool{
 if !windows||err==nil{return false}
 // Windows ERROR_SHARING_VIOLATION (32) and ERROR_LOCK_VIOLATION (33)
 // are not consistently classified as os.IsPermission by all Go versions.
 return os.IsPermission(err)||errors.Is(err,syscall.Errno(32))||errors.Is(err,syscall.Errno(33))
}
func retryFileOperation(operation func()error,windows bool)error{
 for attempt:=0;attempt<6;attempt++{
  err:=operation()
  if err==nil{return nil}
  if !isTransientWindowsFileError(err,windows)||attempt==5{return err}
  time.Sleep(time.Duration(attempt+1)*200*time.Millisecond)
 }
 return nil
}
func uniqueEpisodeBackupPath(final string)(string,error){
 parent:=filepath.Dir(final)
 prefix:="."+filepath.Base(final)+".minio-episode-backup-"
 for attempt:=0;attempt<10;attempt++{
  var random [12]byte
  if _,err:=rand.Read(random[:]);err!=nil{return "",err}
  candidate:=filepath.Join(parent,prefix+hex.EncodeToString(random[:]))
  if _,err:=os.Lstat(candidate);os.IsNotExist(err){return candidate,nil}else if err!=nil{return "",err}
 }
 return "",errors.New("无法生成唯一的 episode 备份路径")
}
func recoverEpisodeDirectory(final string)error{
 parent:=filepath.Dir(final)
 entries,err:=os.ReadDir(parent)
 if os.IsNotExist(err){return nil}
 if err!=nil{return err}
 prefix:="."+filepath.Base(final)+".minio-episode-backup-"
 backups:=make([]os.FileInfo,0)
 for _,entry:=range entries{
  if !strings.HasPrefix(entry.Name(),prefix){continue}
  path:=filepath.Join(parent,entry.Name())
  info,statErr:=os.Lstat(path)
  if statErr!=nil{return statErr}
  if !info.IsDir()||info.Mode()&os.ModeSymlink!=0{return fmt.Errorf("episode 备份不是普通目录，拒绝自动恢复: %s",path)}
  contents,readErr:=os.ReadDir(path)
  if readErr!=nil{return readErr}
  if len(contents)==0{
   if removeErr:=os.Remove(path);removeErr!=nil{return fmt.Errorf("清理未完成的空 episode 备份失败: %w",removeErr)}
   continue
  }
  backups=append(backups,info)
 }
 if len(backups)==0{return nil}
 _,finalErr:=os.Lstat(final)
 if finalErr==nil{
  for _,backup:=range backups{if err:=os.RemoveAll(filepath.Join(parent,backup.Name()));err!=nil{return fmt.Errorf("清理已发布 episode 的旧备份失败: %w",err)}}
  return nil
 }
 if !os.IsNotExist(finalErr){return finalErr}
 sort.Slice(backups,func(i,j int)bool{return backups[i].ModTime().After(backups[j].ModTime())})
 chosen:=backups[0]
 backupPath:=filepath.Join(parent,chosen.Name())
 if err:=retryFileOperation(func()error{return os.Rename(backupPath,final)},runtime.GOOS=="windows");err!=nil{return fmt.Errorf("恢复崩溃前的 episode 正式目录失败: %w",err)}
 for _,backup:=range backups[1:]{if err:=os.RemoveAll(filepath.Join(parent,backup.Name()));err!=nil{return fmt.Errorf("清理多余 episode 备份失败: %w",err)}}
 log.Printf("检测到上次进程中断，已恢复 episode 正式目录: %s",final)
 return nil
}
func replaceEpisodeDirectory(partial,final string)error{
 return replaceEpisodeDirectoryForOS(partial,final,runtime.GOOS=="windows")
}
func replaceEpisodeDirectoryForOS(partial,final string,windows bool)error{
 info,err:=os.Lstat(partial)
 if err!=nil{return err}
 if !info.IsDir()||info.Mode()&os.ModeSymlink!=0{return fmt.Errorf("episode 暂存路径不是普通目录: %s",partial)}
 _,statErr:=os.Lstat(final)
 if os.IsNotExist(statErr){return retryFileOperation(func()error{return os.Rename(partial,final)},windows)}
 if statErr!=nil{return statErr}
 backup,err:=uniqueEpisodeBackupPath(final)
 if err!=nil{return err}
 if err=retryFileOperation(func()error{return os.Rename(final,backup)},windows);err!=nil{return err}
 if err=retryFileOperation(func()error{return os.Rename(partial,final)},windows);err!=nil{
  rollbackErr:=retryFileOperation(func()error{return os.Rename(backup,final)},windows)
  if rollbackErr!=nil{return fmt.Errorf("发布 episode 目录失败: %v；恢复旧目录也失败: %w",err,rollbackErr)}
  return err
 }
 if err=os.RemoveAll(backup);err!=nil{log.Printf("episode 已发布，但旧目录备份清理失败 %s: %v",backup,err)}
 return nil
}
func replaceDownloadedFile(src,dst string)error{
 return replaceDownloadedFileForOS(src,dst,runtime.GOOS=="windows")
}
func uniqueDownloadedFileBackupPath(dst string)(string,error){
 parent:=filepath.Dir(dst)
 prefix:="."+filepath.Base(dst)+".minio-file-backup-"
 for attempt:=0;attempt<10;attempt++{
  var random [12]byte
  if _,err:=rand.Read(random[:]);err!=nil{return "",err}
  candidate:=filepath.Join(parent,prefix+hex.EncodeToString(random[:]))
  if _,err:=os.Lstat(candidate);os.IsNotExist(err){return candidate,nil}else if err!=nil{return "",err}
 }
 return "",errors.New("无法生成唯一的文件备份路径")
}
func recoverDownloadedFile(dst,root string)error{
 parent:=filepath.Dir(dst)
 rel,err:=filepath.Rel(root,parent);if err!=nil{return err}
 current:=root
 checkDir:=func(path string)error{
  info,err:=os.Lstat(path)
  if os.IsNotExist(err){return os.ErrNotExist}
  if err!=nil{return err}
  if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return fmt.Errorf("文件备份父路径不是普通目录: %s",path)}
  return nil
 }
 if err:=checkDir(current);err!=nil{return err}
 if rel!="."{
  for _,part:=range strings.Split(rel,string(os.PathSeparator)){
   current=filepath.Join(current,part)
   if err:=checkDir(current);os.IsNotExist(err){return nil}else if err!=nil{return err}
  }
 }
 entries,err:=os.ReadDir(parent);if os.IsNotExist(err){return nil};if err!=nil{return err}
 prefix:="."+filepath.Base(dst)+".minio-file-backup-"
 backups:=make([]os.FileInfo,0)
 for _,entry:=range entries{
  if !strings.HasPrefix(entry.Name(),prefix){continue}
  path:=filepath.Join(parent,entry.Name())
  info,statErr:=os.Lstat(path);if statErr!=nil{return statErr}
  if !info.Mode().IsRegular()||info.Mode()&os.ModeSymlink!=0{return fmt.Errorf("文件备份不是普通文件，拒绝自动恢复: %s",path)}
  backups=append(backups,info)
 }
 if len(backups)==0{return nil}
 _,finalErr:=os.Lstat(dst)
 if finalErr==nil{
  for _,backup:=range backups{if err:=os.Remove(filepath.Join(parent,backup.Name()));err!=nil{return fmt.Errorf("清理已替换文件的旧备份失败: %w",err)}}
  return nil
 }
 if !os.IsNotExist(finalErr){return finalErr}
 sort.Slice(backups,func(i,j int)bool{return backups[i].ModTime().After(backups[j].ModTime())})
 chosen:=backups[0]
 backupPath:=filepath.Join(parent,chosen.Name())
 if err:=retryFileOperation(func()error{return os.Rename(backupPath,dst)},runtime.GOOS=="windows");err!=nil{return fmt.Errorf("恢复中断替换的文件失败: %w",err)}
 for _,backup:=range backups[1:]{if err:=os.Remove(filepath.Join(parent,backup.Name()));err!=nil{return fmt.Errorf("清理多余文件备份失败: %w",err)}}
 log.Printf("检测到上次进程中断，已恢复文件: %s",dst)
 return nil
}
func replaceDownloadedFileForOS(src,dst string,windows bool)error{
 if !windows{return os.Rename(src,dst)}
 _,statErr:=os.Lstat(dst)
 if os.IsNotExist(statErr){return retryFileOperation(func()error{return os.Rename(src,dst)},true)}
 if statErr!=nil{return statErr}
 backup,err:=uniqueDownloadedFileBackupPath(dst)
 if err!=nil{return err}
 if err=retryFileOperation(func()error{return os.Rename(dst,backup)},true);err!=nil{return err}
 if err=retryFileOperation(func()error{return os.Rename(src,dst)},true);err!=nil{
  rollbackErr:=retryFileOperation(func()error{return os.Rename(backup,dst)},true)
  if rollbackErr!=nil{return fmt.Errorf("替换目标文件失败: %v；恢复旧文件也失败: %w",err,rollbackErr)}
  return err
 }
 if err=retryFileOperation(func()error{return os.Remove(backup)},true);err!=nil{
  log.Printf("新文件已发布，但清理旧文件备份失败 %s: %v",backup,err)
 }
 return nil
}
func(a *App)fetchObject(key,dst string,expected minio.ObjectInfo,reservation *spaceReservation)error{
 if existingObjectComplete(dst,expected){return nil}
 tmp:=dst+".minio-tmp"
 var writtenThisCall int64=reservation.ConsumedBytes()
 onProgress:=func(n int64){if n>0{reservation.Consume(n);writtenThisCall+=n}}
 restoreWritten:=func(){if writtenThisCall>0{reservation.Restore(writtenThisCall);writtenThisCall=0}}
 discardTemp:=func()error{
  err:=os.Remove(tmp)
  if err==nil||os.IsNotExist(err){restoreWritten();return nil}
  return err
 }
 attempts:=a.cfg.FileRetries
 for attempt:=1;attempt<=attempts;attempt++{
  if a.ctx.Err()!=nil{return a.ctx.Err()}
  offset:=int64(0)
  if st,err:=os.Lstat(tmp);err==nil{
   if !st.Mode().IsRegular()||st.Size()>expected.Size{if err:=discardTemp();err!=nil{return err}}else{
    offset=resumeOffset(st.Size(),expected.Size,attempt==1)
    if offset<st.Size(){
     if err:=truncateResumeFile(tmp,offset);err!=nil{return fmt.Errorf("回退续传临时文件失败 %s: %w",key,err)}
     backedOff:=st.Size()-offset
     reservation.Restore(backedOff)
     writtenThisCall-=backedOff
    }
   }
  }else if os.IsNotExist(err){restoreWritten()}else{return err}
  if offset>=expected.Size&&offset>0{if err:=discardTemp();err!=nil{return err};offset=0}
  if offset>0{
   current,statErr:=a.mc.StatObject(a.ctx,a.cfg.Bucket,key,minio.StatObjectOptions{})
   if statErr!=nil{if attempt<attempts{select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):};continue};return statErr}
   if !sameObjectVersion(expected,current){if discardErr:=discardTemp();discardErr!=nil{return fmt.Errorf("远端对象已变化且删除暂存文件失败 %s: %w",key,discardErr)};return fmt.Errorf("%w：续传前发现对象已变化，已丢弃旧临时文件: %s",errRemoteObjectChanged,key)}
  }
  opts:=minio.GetObjectOptions{}
  if expected.ETag!=""{if err:=opts.SetMatchETag(strings.Trim(expected.ETag,"\""));err!=nil{return err}}
  if offset>0{if err:=opts.SetRange(offset,0);err!=nil{return err}}
  obj,err:=a.mc.GetObject(a.ctx,a.cfg.Bucket,key,opts)
  if err==nil{
   var info minio.ObjectInfo
   info,err=obj.Stat()
   if err==nil&&!sameObjectVersion(expected,info){err=fmt.Errorf("%w：GetObject 元数据不匹配: %s",errRemoteObjectChanged,key)}
   if err==nil{
    flags:=os.O_CREATE|os.O_WRONLY
    if offset>0{flags|=os.O_APPEND}else{flags|=os.O_TRUNC}
    var out *os.File
    out,err=os.OpenFile(tmp,flags,0644)
    if err==nil{
     var n int64
     writer:=&progressWriter{writer:out,onWrite:onProgress};if *a.cfg.Fsync{writer.syncEvery=fsyncIntervalBytes;writer.syncFn=out.Sync};n,copyErr:=io.Copy(writer,struct{io.Reader}{obj})
     var syncErr error
     if *a.cfg.Fsync{syncErr=out.Sync()}
     closeErr:=out.Close()
     if copyErr!=nil{err=copyErr}else if syncErr!=nil{err=syncErr}else if closeErr!=nil{err=closeErr}
     if err==nil&&n!=expected.Size-offset{err=fmt.Errorf("对象分段长度不符 %s: got=%d want=%d",key,n,expected.Size-offset)}
    }
   }
   _=obj.Close()
  }
  if err==nil{
   var st os.FileInfo
   st,err=os.Stat(tmp)
   if err==nil&&st.Size()!=expected.Size{err=fmt.Errorf("对象大小校验失败 %s: %d != %d",key,st.Size(),expected.Size)}
  }
  if err==nil{
   latest,statErr:=a.mc.StatObject(a.ctx,a.cfg.Bucket,key,minio.StatObjectOptions{})
   if statErr!=nil{err=statErr}else if !sameObjectVersion(expected,latest){err=fmt.Errorf("%w：下载期间对象版本发生变化: %s",errRemoteObjectChanged,key)}
  }
  if err==nil&&*a.cfg.VerifyMD5&&isMD5ETag(expected.ETag){
   var sum string
   sum,err=fileMD5(tmp)
   if err==nil&&!strings.EqualFold(sum,strings.Trim(expected.ETag,"\"")){err=fmt.Errorf("MD5 校验失败 %s: %s != %s",key,sum,expected.ETag)}
  }
  if err==nil{
   if err=replaceDownloadedFile(tmp,dst);err==nil{if !expected.LastModified.IsZero(){_=os.Chtimes(dst,expected.LastModified,expected.LastModified)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(dst))}}
   if err==nil{return nil}
  }
  if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){return err}
  if errors.Is(err,errRemoteObjectChanged){if discardErr:=discardTemp();discardErr!=nil{return fmt.Errorf("远端对象已变化，且删除失效暂存文件失败 %s: %w",key,discardErr)};return err}
  if attempt<attempts{
   log.Printf("对象下载重试 %d/%d: %s: %v",attempt,attempts,key,err)
   select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):}
  }
 }
 return fmt.Errorf("对象下载重试 %d 次仍失败: %s",attempts,key)
}
func sameETag(a,b string)bool{return strings.Trim(a,"\"")==strings.Trim(b,"\"")}
func sameObjectVersion(expected,current minio.ObjectInfo)bool{
 return expected.Size==current.Size&&(expected.ETag==""||sameETag(expected.ETag,current.ETag))&&(expected.LastModified.IsZero()||expected.LastModified.Equal(current.LastModified))
}
func isMD5ETag(etag string)bool{v:=strings.Trim(etag,"\"");if len(v)!=32||strings.Contains(v,"-"){return false};_,err:=hex.DecodeString(v);return err==nil}
func fileMD5(name string)(string,error){f,err:=os.Open(name);if err!=nil{return "",err};defer f.Close();h:=md5.New();if _,err=io.Copy(h,f);err!=nil{return "",err};return hex.EncodeToString(h.Sum(nil)),nil}
func(a *App)fail(e Episode,d DiskConfig,final,partial string,err error)error{
 if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){pauseText:="已暂停，保留暂存文件供下次续传";_=a.set(e,d,final,partial,"paused",pauseText);if a.runID!=""{now:=time.Now().UTC().Format(time.RFC3339Nano);if _,detailErr:=a.db.Exec("INSERT INTO run_episode_history(run_id,episode_id,task_name,status,error,updated_at,failure_category) VALUES(?,?,?,'paused',?,?, 'paused') ON CONFLICT(run_id,episode_id) DO UPDATE SET task_name=excluded.task_name,status=excluded.status,error=excluded.error,updated_at=excluded.updated_at,failure_category=excluded.failure_category",a.runID,e.ID,e.Task,pauseText,now);detailErr!=nil{log.Printf("保存本次 Episode 暂停明细失败: %v",detailErr)}};return nil}
 _=a.set(e,d,final,partial,"failed",err.Error());if a.runID!=""{now:=time.Now().UTC().Format(time.RFC3339Nano);_,statsErr:=a.db.Exec("INSERT INTO run_task_history(run_id,task_name,failed_count) VALUES(?,?,1) ON CONFLICT(run_id,task_name) DO UPDATE SET failed_count=failed_count+1",a.runID,e.Task);if statsErr!=nil{log.Printf("保存本次任务失败统计失败: %v",statsErr)};_,detailErr:=a.db.Exec("INSERT INTO run_episode_history(run_id,episode_id,task_name,status,error,updated_at,failure_category) VALUES(?,?,?,'failed',?,?,?) ON CONFLICT(run_id,episode_id) DO UPDATE SET task_name=excluded.task_name,status=excluded.status,error=excluded.error,updated_at=excluded.updated_at,failure_category=excluded.failure_category",a.runID,e.ID,e.Task,err.Error(),now,classifyFailure(err));if detailErr!=nil{log.Printf("保存本次 Episode 失败明细失败: %v",detailErr)}};log.Printf("下载失败 %s: %v",e.ID,err);return err
}
func classifyFailure(err error) string {
 if err == nil { return "" }
 message := strings.ToLower(err.Error())
 switch {
 case strings.Contains(message, "no space left"), strings.Contains(message, "disk full"), strings.Contains(message, "not enough space"), strings.Contains(message, "insufficient space"):
  return "disk_space"
 case strings.Contains(message, "permission denied"), strings.Contains(message, "access denied"), strings.Contains(message, "status code: 403"), strings.Contains(message, "http 403"):
  return "permission"
 case strings.Contains(message, "nosuchkey"), strings.Contains(message, "no such key"), strings.Contains(message, "does not exist"), strings.Contains(message, "not found"), strings.Contains(message, "status code: 404"), strings.Contains(message, "http 404"):
  return "not_found"
 case strings.Contains(message, "checksum"), strings.Contains(message, "hash mismatch"), strings.Contains(message, "size mismatch"), strings.Contains(message, "corrupt"), strings.Contains(message, "unexpected eof"):
  return "integrity"
 case strings.Contains(message, "timeout"), strings.Contains(message, "connection reset"), strings.Contains(message, "connection refused"), strings.Contains(message, "i/o timeout"), strings.Contains(message, "no such host"), strings.Contains(message, "tls"), strings.Contains(message, "broken pipe"), strings.Contains(message, "network"):
  return "network"
 default:
  return "other"
 }
}

func (a *App) markCleanupPending(episodeID string) error {
 tx, err := a.db.Begin()
 if err != nil {
  return err
 }
 defer tx.Rollback()
 result, err := tx.Exec(
  "UPDATE downloads SET status='cleanup_pending',error=?,updated_at=? WHERE episode_id=? AND status='completed'",
  "缺少 mcap，等待清理目录", time.Now().UTC().Format(time.RFC3339Nano), episodeID,
 )
 if err != nil {
  return err
 }
 affected, err := result.RowsAffected()
 if err != nil {
  return err
 }
 if affected == 0 {
  return fmt.Errorf("episode %s 的完成状态已变化", episodeID)
 }
 if _, err = tx.Exec("DELETE FROM downloaded_episodes WHERE episode_id=?", episodeID); err != nil {
  return err
 }
 return tx.Commit()
}

func (a *App) validateCleanupTarget(target string) error {
 resolvedTarget, err := resolvePathWithMissingTail(target)
 if err != nil {
  return fmt.Errorf("解析清理目标路径失败 %s: %w", target, err)
 }
 for _, configuredDisk := range a.cfg.Disks {
  resolvedRoot, rootErr := resolvePathWithMissingTail(configuredDisk.Path)
  if rootErr != nil {
   continue
  }
  rel, relErr := filepath.Rel(resolvedRoot, resolvedTarget)
  if relErr != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
   continue
  }
  return nil
 }
 return fmt.Errorf("拒绝清理配置磁盘之外的路径: %s", target)
}

func (a *App) completePendingCleanup(episodeID string, target sql.NullString) error {
 if target.Valid && target.String != "" {
  if err := a.validateCleanupTarget(target.String); err != nil {
   return err
  }
  if err := os.RemoveAll(target.String); err != nil {
   return fmt.Errorf("删除 episode 目录 %s 失败: %w", target.String, err)
  }
 }
 tx, err := a.db.Begin()
 if err != nil {
  return err
 }
 defer tx.Rollback()
 if _, err = tx.Exec("DELETE FROM downloaded_episodes WHERE episode_id=?", episodeID); err != nil {
  return err
 }
 if _, err = tx.Exec("DELETE FROM downloads WHERE episode_id=? AND status='cleanup_pending'", episodeID); err != nil {
  return err
 }
 return tx.Commit()
}

func (a *App) recoverPendingCleanup() error {
 rows, err := a.db.Query("SELECT episode_id,target_path FROM downloads WHERE status='cleanup_pending'")
 if err != nil {
  return err
 }
 type item struct {
  id string
  path sql.NullString
 }
 items := []item{}
 for rows.Next() {
  var entry item
  if err = rows.Scan(&entry.id, &entry.path); err != nil {
   rows.Close()
   return err
  }
  items = append(items, entry)
 }
 if err = rows.Err(); err != nil {
  rows.Close()
  return err
 }
 if err = rows.Close(); err != nil {
  return err
 }
 for _, entry := range items {
  if err = a.completePendingCleanup(entry.id, entry.path); err != nil {
   return fmt.Errorf("恢复 episode %s 的清理失败: %w", entry.id, err)
  }
 }
 if len(items) > 0 {
  log.Printf("已恢复并完成 %d 条中断的缺失 mcap 清理记录", len(items))
 }
 return nil
}

func (a *App) cleanup(preview bool) (int, error) {
 rows, err := a.db.Query("SELECT episode_id,target_path,status FROM downloads WHERE status IN ('completed','cleanup_pending')")
 if err != nil {
  return 0, err
 }
 type item struct {
  id string
  path sql.NullString
  status string
 }
 items := []item{}
 for rows.Next() {
  var entry item
  if err = rows.Scan(&entry.id, &entry.path, &entry.status); err != nil {
   rows.Close()
   return 0, err
  }
  items = append(items, entry)
 }
 if err = rows.Err(); err != nil {
  rows.Close()
  return 0, err
 }
 if err = rows.Close(); err != nil {
  return 0, err
 }
 n := 0
 for _, entry := range items {
  pending := entry.status == "cleanup_pending"
  if pending || !entry.path.Valid || !hasPattern(entry.path.String, a.cfg.MCAPPattern) {
   n++
   if preview {
    if pending {
     log.Printf("[dry-run] 待恢复的清理项: %s", entry.path.String)
    } else {
     log.Printf("[dry-run] 缺 mcap: %s", entry.path.String)
    }
    continue
   }
   if !pending {
    if err = a.markCleanupPending(entry.id); err != nil {
     return n, fmt.Errorf("标记 episode %s 清理状态失败: %w", entry.id, err)
    }
   }
   if err = a.completePendingCleanup(entry.id, entry.path); err != nil {
    return n, fmt.Errorf("episode %s 已标记待清理，稍后会自动恢复: %w", entry.id, err)
   }
  }
 }
 return n, nil
}
func pathIsDir(p string)bool{st,e:=os.Stat(p);return e==nil&&st.IsDir()}
func dirHasFiles(p string)bool{x,e:=os.ReadDir(p);return e==nil&&len(x)>0}
func hasPattern(p,pat string)bool{x,e:=os.ReadDir(p);if e!=nil{return false};for _,v:=range x{if !v.IsDir(){if ok,_:=path.Match(pat,v.Name());ok{return true}}};return false}
func normalizeParentKey(value string)(string,error){
 key:=strings.TrimSpace(value);key=strings.Trim(key,"/")
 if key==""{return "",errors.New("parent_key 为空")}
 for _,part:=range strings.Split(key,"/"){if part=="."||part==".."{return "",fmt.Errorf("parent_key 含有不允许的路径段: %q",key)}}
 return key,nil
}
func safe(s string)string{
 s=strings.TrimSpace(s)
 if s==""||s=="."||s==".."{return "_"}
 r:=strings.NewReplacer("/","_","\\","_",":","_","*","_","?","_","\"","_","<","_",">","_","|","_")
 s=strings.TrimRight(r.Replace(s),". ")
 if s==""{return "_"}
 if runtime.GOOS=="windows"&&windowsReserved(s){s="_"+s}
 return s
}
func windowsReserved(s string)bool{
 base:=strings.ToUpper(strings.SplitN(s,".",2)[0])
 if base=="CON"||base=="PRN"||base=="AUX"||base=="NUL"{return true}
 if len(base)==4&&(strings.HasPrefix(base,"COM")||strings.HasPrefix(base,"LPT"))&&base[3]>='1'&&base[3]<='9'{return true}
 return false
}
func invalidWindowsObjectPart(s string)bool{
 if strings.TrimRight(s," .")!=s||windowsReserved(s)||strings.ContainsAny(s,`<>:"\\|?*`){return true}
 for _,r:=range s{if r<32{return true}}
 return false
}
func safeJoin(root,rel string)(string,error){
 normalized:=strings.ReplaceAll(rel,"\\","/")
 c:=path.Clean(normalized)
 if c=="."||c==".."||strings.HasPrefix(c,"../")||strings.HasPrefix(c,"/"){return "",fmt.Errorf("非法对象路径 %q",rel)}
 if runtime.GOOS=="windows"{for _,part:=range strings.Split(c,"/"){if invalidWindowsObjectPart(part){return "",fmt.Errorf("Windows 不支持的对象文件名 %q",part)}}}
 p:=filepath.Join(root,filepath.FromSlash(c))
 r,e:=filepath.Rel(root,p)
 if e!=nil||r==".."||strings.HasPrefix(r,".."+string(os.PathSeparator)){return "",fmt.Errorf("对象路径越界 %q",rel)}
 return p,nil
}
func taskDir(t Task)string{if t.Dir!=""{return t.Dir};return t.Name}
func taskFolderKey(t Task)string{return safe(t.Scene)+"\x00"+safe(taskDir(t))}
