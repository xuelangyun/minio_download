package main

import (
 "context"
 "crypto/tls"
 "crypto/hmac"
 "crypto/sha256"
 "encoding/hex"
 "bytes"
 "crypto/md5"
 "database/sql"
 "encoding/json"
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
)

type Task struct { Name string `json:"name"`; Scene string `json:"scene"`; Dir string `json:"dir"`; Aliases []string `json:"aliases"`; Tags []string `json:"tags"`; TargetHours float64 `json:"target_hours"`; TargetCount int `json:"target_count"` }
type DiskConfig struct { Name string `json:"name"`; Path string `json:"path"`; Mountpoint string `json:"mountpoint"` }
type Config struct {
 fromConfigDB bool
 Mode string `json:"mode"`
 Endpoint string `json:"minio_endpoint"`; AccessKey string `json:"minio_access_key"`; SecretKey string `json:"minio_secret_key"`; Bucket string `json:"minio_bucket"`; Region string `json:"minio_region"`; SSL bool `json:"minio_use_ssl"`; MinioSkipVerify bool `json:"minio_skip_verify"`
 PGHost string `json:"pg_host"`; PGPort int `json:"pg_port"`; PGDB string `json:"pg_database"`; PGUser string `json:"pg_user"`; PGPassword string `json:"pg_password"`
 DataVersion string `json:"data_version"`; ConfigDB string `json:"config_db"`; LockFile string `json:"lock_file"`; PIDFile string `json:"pid_file"`; StateDB string `json:"state_db"`; LogDir string `json:"log_dir"`; MinFreeGB float64 `json:"min_free_gb"`; SafetyFactor float64 `json:"space_safety_factor"`
 Workers int `json:"max_writes_per_disk"`; FileWorkers int `json:"file_workers"`; FileRetries int `json:"file_retries"`; RetryDelaySeconds float64 `json:"retry_delay_seconds"`; ValidateWorkers int `json:"validate_workers"`; RequireMCAP bool `json:"require_mcap"`; MCAPPattern string `json:"mcap_pattern"`; IncludeNull bool `json:"include_null_quality"`; VerifyMD5 *bool `json:"verify_md5"`; Fsync *bool `json:"fsync"`; TrustCompletedRecords bool `json:"trust_completed_records"`
 Disks []DiskConfig `json:"disks"`; Tasks []Task `json:"tasks"`
}
type Episode struct { ID string; JobID sql.NullInt64; Task,SourceTask,SN,ParentKey string; Size int64; Duration float64; Created sql.NullTime }
type App struct { cfg Config; mc *minio.Client; db *sql.DB; ctx context.Context; cancel context.CancelFunc; mode string; recheck bool; validated map[string]bool }
var configPath=flag.String("config","config.json","JSON 配置文件")
var mode=flag.String("mode","","duration 或 count；未指定时根据配置自动选择")
var rounds=flag.Int("max-rounds",1,"缺少必需文件后的补选轮数")
var recheck=flag.Bool("recheck-missing",false,"重新检查缺文件数据")
var cleanup=flag.Bool("cleanup-missing-mcap",false,"清理缺少 mcap 的已下载目录")
var dry=flag.Bool("dry-run",false,"只预览清理操作")
var stopFlag=flag.Bool("stop",false,"请求正在运行的下载器安全暂停")
var webFlag=flag.Bool("web",false,"启动只读 Web API 服务")
var webHost=flag.String("web-host","127.0.0.1","Web API 监听地址")
var webPort=flag.Int("web-port",8765,"Web API 监听端口")

func main(){
 flag.Parse()
 if *rounds<1{log.Fatal("--max-rounds 必须 >= 1")}
 b,e:=os.ReadFile(*configPath);if e!=nil{log.Fatalf("读取配置失败: %v；请复制 config.example.json 为 config.json",e)}
 var c Config;if e=json.Unmarshal(b,&c);e!=nil{log.Fatalf("配置 JSON 无效: %v",e)}
 if *stopFlag{if e=requestStop(c);e!=nil{log.Fatal(e)};log.Print("已写入安全暂停请求");return}
 if *mode==""{if c.ConfigDB!=""{*mode="count"}else if c.Mode!=""{*mode=c.Mode}else{*mode="duration"}}
 if *mode!="duration"&&*mode!="count"{log.Fatal("--mode 只能是 duration 或 count")}
 if e=loadConfigDB(&c,*mode);e!=nil{log.Fatalf("读取 SQLite 配置库失败: %v",e)}
 if e=c.normalize(*mode);e!=nil{log.Fatal(e)}
 if *webFlag{if e=serveWeb(c,*webHost,*webPort);e!=nil{log.Fatal(e)};return}
 _=os.MkdirAll(filepath.Dir(c.StateDB),0755);_=os.MkdirAll(filepath.Dir(c.LockFile),0755);_=os.MkdirAll(c.LogDir,0755)
 f,e:=os.OpenFile(filepath.Join(c.LogDir,"minio_task_downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if e!=nil{log.Fatal(e)};defer f.Close()
 log.SetOutput(io.MultiWriter(os.Stdout,f));log.SetFlags(log.Ldate|log.Ltime|log.Lmicroseconds)
 baseCtx,stop:=signal.NotifyContext(context.Background(),os.Interrupt);defer stop()
 ctx,cancel:=context.WithCancel(baseCtx);defer cancel()
 a:=&App{cfg:c,ctx:ctx,cancel:cancel,mode:*mode,recheck:*recheck,validated:map[string]bool{}}
 lock:=flock.New(c.LockFile);locked,lockErr:=lock.TryLock();if lockErr!=nil{log.Fatalf("创建下载锁失败: %v",lockErr)};if !locked{log.Fatal("检测到另一个下载器实例正在运行")};defer lock.Unlock()
 if e=a.open();e!=nil{log.Fatal(e)};defer a.db.Close()
 if e=a.clearStopRequest();e!=nil{log.Fatal(e)}
 if e=writePIDFile(c.PIDFile,*mode);e!=nil{log.Fatal(e)};defer removePIDFile(c.PIDFile)
 watcherDone:=a.startStopWatcher();defer func(){cancel();<-watcherDone}()
 if e=validateDiskSet(a.cfg.Disks);e!=nil{log.Fatal(e)}
 if *cleanup{n,err:=a.cleanup(*dry);if err!=nil{log.Fatal(err)};log.Printf("清理结束：%d 条",n);if *dry{return}}else if *dry{log.Fatal("--dry-run 必须与 --cleanup-missing-mcap 一起使用")}
 rows,e:=a.fetch();if e!=nil{if ctx.Err()!=nil{log.Print("已安全暂停（候选查询期间收到暂停请求）");a.saveLastRun("paused",nil);return};log.Printf("候选查询失败: %v",e);a.saveLastRun("error",e);return}
 if e=a.run(rows,*rounds);e!=nil{if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil);return};log.Printf("下载执行失败: %v",e);a.saveLastRun("error",e);return}
 if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil)}else{log.Printf("执行结束 Go=%s/%s",runtime.GOOS,runtime.GOARCH);a.saveLastRun("finished",nil)}
}

type WebAPI struct{cfg Config;db *sql.DB;tokenHash string;authMu sync.Mutex;failures map[string][]time.Time;lockouts map[string]time.Time}
func serveWeb(c Config,host string,port int)error{
 if port<1||port>65535{return errors.New("web-port 必须在 1 到 65535 之间")}
 if err:=os.MkdirAll(filepath.Dir(c.StateDB),0755);err!=nil{return err}
 state,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};state.SetMaxOpenConns(4);_,_=state.Exec("PRAGMA busy_timeout=30000");defer state.Close()
 api:=&WebAPI{cfg:c,db:state}
 if c.ConfigDB!=""{db,e:=sql.Open("sqlite",c.ConfigDB);if e!=nil{return e};defer db.Close();_ = db.QueryRow("SELECT json_extract(value,'
 if strings.TrimSpace(c.ConfigDB)==""{return nil}
 if runMode!="count"{return errors.New("config_db 当前只支持 count 模式；duration 模式请使用独立 JSON 任务目标")}
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
 getInt("transfer.file_workers",&c.FileWorkers);getInt("transfer.file_retries",&c.FileRetries);getFloat("transfer.retry_delay",&c.RetryDelaySeconds)
 if raw,ok:=settings["transfer.verify_md5"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.VerifyMD5=&v}}
 if raw,ok:=settings["transfer.fsync"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.Fsync=&v}}
 if raw,ok:=settings["tasks.data_version"];ok{var v string;if json.Unmarshal([]byte(raw),&v)==nil&&v!=""{c.DataVersion=v}}
 taskRows,err:=db.Query("SELECT s.name,t.name,t.target_count,t.directory,t.id,t.tags FROM tasks t JOIN scenes s ON s.id=t.scene_id ORDER BY s.position,t.position");if err!=nil{return err}
 aliases:=map[int64][]string{};aliasRows,err:=db.Query("SELECT task_id,alias FROM task_aliases ORDER BY task_id,position");if err!=nil{taskRows.Close();return err};for aliasRows.Next(){var id int64;var alias string;if err=aliasRows.Scan(&id,&alias);err!=nil{aliasRows.Close();taskRows.Close();return err};aliases[id]=append(aliases[id],alias)};if err=aliasRows.Err();err!=nil{aliasRows.Close();taskRows.Close();return err};if err=aliasRows.Close();err!=nil{taskRows.Close();return err}
 tasks:=[]Task{};for taskRows.Next(){var t Task;var id int64;var tags string;if err=taskRows.Scan(&t.Scene,&t.Name,&t.TargetCount,&t.Dir,&id,&tags);err!=nil{taskRows.Close();return err};t.Aliases=aliases[id];if tags!=""{_ = json.Unmarshal([]byte(tags),&t.Tags)};tasks=append(tasks,t)};if err=taskRows.Err();err!=nil{taskRows.Close();return err};if err=taskRows.Close();err!=nil{return err};if len(tasks)==0{return errors.New("SQLite 配置库中没有任务")};c.Tasks=tasks
 diskRows,err:=db.Query("SELECT name,path,mountpoint FROM disks ORDER BY position");if err!=nil{return err};disks:=[]DiskConfig{};for diskRows.Next(){var d DiskConfig;if err=diskRows.Scan(&d.Name,&d.Path,&d.Mountpoint);err!=nil{diskRows.Close();return err};disks=append(disks,d)};if err=diskRows.Err();err!=nil{diskRows.Close();return err};if err=diskRows.Close();err!=nil{return err};if len(disks)==0{return errors.New("SQLite 配置库中没有目标磁盘")};c.Disks=disks
 c.Mode="count";c.fromConfigDB=true;log.Printf("已从 SQLite 配置库加载配置: %s（任务 %d 个，磁盘 %d 块）",c.ConfigDB,len(c.Tasks),len(c.Disks));return nil
}
func(c *Config)normalize(m string)error{
 if c.Mode!=""&&c.Mode!=m{return fmt.Errorf("配置文件 mode=%s 与命令行 --mode=%s 不一致",c.Mode,m)}
 if c.Endpoint==""||c.AccessKey==""||c.SecretKey==""||c.Bucket==""{return errors.New("MinIO endpoint/access_key/secret_key/bucket 必须填写")}
 if c.PGHost==""||c.PGDB==""||c.PGUser==""||c.PGPassword==""{return errors.New("PostgreSQL 连接参数必须填写")}
 if c.PGPort==0{c.PGPort=5432};if c.DataVersion==""{c.DataVersion="银河2.2数据"};if c.LockFile==""{c.LockFile="minio_download.lock"};if c.PIDFile==""{c.PIDFile="minio_download.pid"};if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"};if c.LogDir==""{c.LogDir="logs"}
 if c.MinFreeGB==0&&!c.fromConfigDB{c.MinFreeGB=3};if c.SafetyFactor==0{c.SafetyFactor=1.15};if c.Workers<=0{c.Workers=2};if c.FileWorkers<=0{c.FileWorkers=4};if c.FileRetries<=0{c.FileRetries=5};if c.RetryDelaySeconds<=0&&!c.fromConfigDB{c.RetryDelaySeconds=1};if c.ValidateWorkers<=0{c.ValidateWorkers=16};if c.Workers>16||c.FileWorkers>16||c.ValidateWorkers>32||c.FileRetries>20{return errors.New("并发数不能超过 16，文件重试次数不能超过 20")};if c.MinFreeGB<0||c.MinFreeGB>1_000_000{return errors.New("min_free_gb 必须在 0 到 1000000 之间")};if c.SafetyFactor<1||c.SafetyFactor>5{return errors.New("space_safety_factor 必须在 1 到 5 之间")};if c.RetryDelaySeconds<0||c.RetryDelaySeconds>60{return errors.New("retry_delay_seconds 必须在 0 到 60 之间")};if c.PGPort<1||c.PGPort>65535{return errors.New("pg_port 必须在 1 到 65535 之间")};if c.MCAPPattern==""{c.MCAPPattern="*.SYNC.mcap"};if c.VerifyMD5==nil{v:=true;c.VerifyMD5=&v};if c.Fsync==nil{v:=true;c.Fsync=&v}
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
 if runtime.GOOS=="windows"{if err=os.Remove(name);err!=nil&&!os.IsNotExist(err){os.Remove(tmp);return err}}
 if err=os.Rename(tmp,name);err!=nil{os.Remove(tmp);return err}
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
func requestStop(c Config) error {
 if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"}
 if err:=os.MkdirAll(filepath.Dir(c.StateDB),0755);err!=nil{return err}
 db,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};defer db.Close()
 db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA busy_timeout=30000");err!=nil{return err}
 return writeStopRequest(db)
}
func(a *App)saveLastRun(result string,runErr error){
 payload:=map[string]any{"result":result,"at":controlTimestamp(),"mode":a.mode}
 if runErr!=nil{payload["error"]=runErr.Error()}
 raw,err:=json.Marshal(payload);if err!=nil{log.Printf("保存上次运行结果失败: %v",err);return}
 _,err=a.db.Exec("INSERT INTO control(key,value,updated_at) VALUES('last_run',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",string(raw),controlTimestamp())
 if err!=nil{log.Printf("保存上次运行结果失败: %v",err)}
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
 q.Set("connect_timeout","10")
 q.Set("application_name","minio_task_downloader_go")
 u:=url.URL{Scheme:"postgres",User:url.UserPassword(c.PGUser,c.PGPassword),Host:net.JoinHostPort(c.PGHost,strconv.Itoa(c.PGPort)),Path:"/"+c.PGDB,RawQuery:q.Encode()}
 return u.String()
}
func(a *App)open()error{
 endpoint:=strings.TrimPrefix(strings.TrimPrefix(a.cfg.Endpoint,"https://"),"http://");secure:=a.cfg.SSL||strings.HasPrefix(a.cfg.Endpoint,"https://")
 transport:=http.DefaultTransport.(*http.Transport).Clone()
 if a.cfg.MinioSkipVerify{transport.TLSClientConfig=&tls.Config{InsecureSkipVerify:true} /* #nosec: explicitly configured to match Python minio.cert_check=false */}
 mc,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(a.cfg.AccessKey,a.cfg.SecretKey,""),Secure:secure,Region:a.cfg.Region,Transport:transport});if e!=nil{return e};a.mc=mc
 pg:=postgresDSN(a.cfg)
 p,e:=sql.Open("pgx",pg);if e!=nil{return e};defer p.Close();ctx,cancel:=context.WithTimeout(a.ctx,15*time.Second);defer cancel();if e=p.PingContext(ctx);e!=nil{return fmt.Errorf("PostgreSQL 连接失败: %w",e)}
 ok,e:=mc.BucketExists(ctx,a.cfg.Bucket);if e!=nil{return e};if !ok{return fmt.Errorf("MinIO bucket 不存在: %s",a.cfg.Bucket)}
 a.db,e=sql.Open("sqlite",a.cfg.StateDB);if e!=nil{return e};a.db.SetMaxOpenConns(1)
 _,e=a.db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000;"+
 "CREATE TABLE IF NOT EXISTS downloads(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,parent_key TEXT,disk_name TEXT,target_path TEXT,partial_path TEXT,expected_bytes INTEGER,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_download_task_status ON downloads(task_name,status);"+
 "CREATE TABLE IF NOT EXISTS task_disk_assignments(task_name TEXT NOT NULL,disk_name TEXT NOT NULL,last_used_at TEXT NOT NULL,PRIMARY KEY(task_name,disk_name));"+
 "CREATE TABLE IF NOT EXISTS downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,source_task_name TEXT,job_id INTEGER,sn_code TEXT,parent_key TEXT,disk_name TEXT,target_path TEXT,file_size INTEGER,content_time REAL,create_time TEXT,downloaded_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_downloaded_task ON downloaded_episodes(task_name);"+
 "CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL);")
 if e==nil{
  if e=ensureColumn(a.db,"downloads","started_at","TEXT");e!=nil{return e}
  if e=ensureColumn(a.db,"downloads","completed_at","TEXT");e!=nil{return e}
  log.Printf("MinIO=%s bucket=%s；PostgreSQL=%s:%d/%s；SQLite=%s",a.cfg.Endpoint,a.cfg.Bucket,a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.StateDB)
 };return e
}
func(a *App)set(e Episode,d DiskConfig,final,partial,status,msg string)error{
 now:=time.Now().UTC().Format(time.RFC3339Nano)
 _,err:=a.db.Exec("INSERT INTO downloads(episode_id,task_name,parent_key,disk_name,target_path,partial_path,expected_bytes,status,error,started_at,completed_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,partial_path=excluded.partial_path,expected_bytes=excluded.expected_bytes,status=excluded.status,error=excluded.error,"+
 "started_at=CASE WHEN excluded.status='running' THEN excluded.updated_at ELSE downloads.started_at END,"+
 "completed_at=CASE WHEN excluded.status='completed' THEN excluded.updated_at WHEN excluded.status='running' THEN NULL ELSE downloads.completed_at END,updated_at=excluded.updated_at",
 e.ID,e.Task,e.ParentKey,d.Name,nullable(final),nullable(partial),e.Size,status,nullable(msg),nullableTime(status=="running",now),nullableTime(status=="completed",now),now)
 return err
}
func nullableTime(ok bool,v string)any{if ok{return v};return nil}
func ensureColumn(db *sql.DB,table,column,typ string)error{
 rows,err:=db.Query("PRAGMA table_info("+table+")");if err!=nil{return err}
 found:=false
 for rows.Next(){var cid,notnull,pk int;var name,kind string;var def sql.NullString;if err=rows.Scan(&cid,&name,&kind,&notnull,&def,&pk);err!=nil{rows.Close();return err};if name==column{found=true}}
 if err=rows.Err();err!=nil{rows.Close();return err};if err=rows.Close();err!=nil{return err}
 if !found{_,err=db.Exec("ALTER TABLE "+table+" ADD COLUMN "+column+" "+typ)}
 return err
}
func nullable(s string)any{if s==""{return nil};return s}
func(a *App)completed(id string)bool{
 var p sql.NullString
 err:=a.db.QueryRow("SELECT target_path FROM downloads WHERE episode_id=? AND status='completed'",id).Scan(&p)
 if err==nil&&p.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(p.String)){return true}
 p=sql.NullString{}
 err=a.db.QueryRow("SELECT target_path FROM downloaded_episodes WHERE episode_id=?",id).Scan(&p)
 return err==nil&&p.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(p.String))
}
func(a *App)stats(task string)(float64,int){rows,err:=a.db.Query("SELECT content_time,target_path FROM downloaded_episodes WHERE task_name=?",task);if err!=nil{return 0,0};defer rows.Close();var seconds float64;count:=0;for rows.Next(){var v sql.NullFloat64;var p sql.NullString;if rows.Scan(&v,&p)!=nil{continue};if a.cfg.TrustCompletedRecords||(p.Valid&&pathIsDir(p.String)){seconds+=v.Float64;count++}};return seconds,count}
func(a *App)assign(task,disk string)error{tx,err:=a.db.Begin();if err!=nil{return err};defer tx.Rollback();if _,err=tx.Exec("DELETE FROM task_disk_assignments WHERE task_name=?",task);err!=nil{return err};if _,err=tx.Exec("INSERT INTO task_disk_assignments(task_name,disk_name,last_used_at) VALUES(?,?,?)",task,disk,time.Now().UTC().Format(time.RFC3339Nano));err!=nil{return err};return tx.Commit()}
func(a *App)assigned(task string)string{var d string;_=a.db.QueryRow("SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC LIMIT 1",task).Scan(&d);return d}
func(a *App)record(e Episode,d DiskConfig,final string)error{
 _,err:=a.db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,source_task_name,job_id,sn_code,parent_key,disk_name,target_path,file_size,content_time,create_time,downloaded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,source_task_name=excluded.source_task_name,job_id=excluded.job_id,sn_code=excluded.sn_code,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,file_size=excluded.file_size,content_time=excluded.content_time,create_time=excluded.create_time,downloaded_at=excluded.downloaded_at",
 e.ID,e.Task,e.SourceTask,e.JobID,e.SN,e.ParentKey,d.Name,final,e.Size,e.Duration,e.Created,time.Now().UTC().Format(time.RFC3339Nano));return err
}
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
  if !obj.IsDir&&!strings.HasSuffix(obj.Key,"/"){direct[path.Base(obj.Key)]=true}
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
   if ledgerErr==nil&&ledgerPath.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(ledgerPath.String)){
    d:=DiskConfig{Name:ledgerDisk.String}
    if err:=a.set(ep,d,ledgerPath.String,"","completed","");err!=nil{return err}
   }else{
    var statePath,stateDisk sql.NullString
    var state string
    stateErr:=a.db.QueryRow("SELECT status,target_path,disk_name FROM downloads WHERE episode_id=?",ep.ID).Scan(&state,&statePath,&stateDisk)
    if stateErr==nil&&state=="completed"&&statePath.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(statePath.String)){
     d:=DiskConfig{Name:stateDisk.String}
     if err:=a.record(ep,d,statePath.String);err!=nil{return err}
     if err:=a.set(ep,d,statePath.String,"","completed","");err!=nil{return err}
    }
   }
   diskName,err:=findExistingTaskDisk(a.cfg.DataVersion,task,a.cfg.Disks,[]Episode{ep})
   if err!=nil{return err};if diskName==""{continue}
   var chosen *DiskConfig
   for i:=range a.cfg.Disks{if a.cfg.Disks[i].Name==diskName{chosen=&a.cfg.Disks[i];break}}
   if chosen==nil{return fmt.Errorf("找不到 episode %s 的本地磁盘 %s",ep.ID,diskName)}
   pinned:=a.assigned(task.Name)
   if pinned!=""&&pinned!=diskName{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode %s 已存在于 %s",task.Name,pinned,ep.ID,diskName)}
   if pinned==""{if err:=a.assign(task.Name,diskName);err!=nil{return err}}
   final,_:=episodePaths(a.cfg.DataVersion,task,*chosen,ep.ParentKey)
   if err:=a.record(ep,*chosen,final);err!=nil{return err}
   if err:=a.set(ep,*chosen,final,"","completed","");err!=nil{return err}
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
  _,downloaded:=a.stats(task.Name);gap:=task.TargetCount-downloaded
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
func(a *App)run(all []Episode,nround int)error{
 groups:=map[string][]Episode{};for _,e:=range all{groups[e.Task]=append(groups[e.Task],e)}
 if err:=a.syncLocalCopies(groups);err!=nil{return err}
 if a.mode=="count"{var err error;groups,err=a.prevalidateCount(groups);if err!=nil{if a.ctx.Err()!=nil{return nil};return err}}
 for r:=1;r<=nround;r++{if a.ctx.Err()!=nil{return nil};selected:=map[string][]Episode{}
  for _,t:=range a.cfg.Tasks{s,n:=a.stats(t.Name);target,used:=t.TargetHours*3600,s;if a.mode=="count"{target=float64(t.TargetCount);used=float64(n)}
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
type batch struct{t Task;d DiskConfig;rows []Episode}
func findExistingTaskDisk(version string,t Task,disks []DiskConfig,rows []Episode)(string,error){
 found:=""
 for _,ep:=range rows{
  for _,d:=range disks{
   final,_:=episodePaths(version,t,d,ep.ParentKey)
   if pathIsDir(final){
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
 devices:=map[string]string{}
 for _,d:=range disks{
  if err:=validateDisk(d);err!=nil{return err}
  mount:=d.Mountpoint;if mount==""{mount=d.Path};resolved,err:=resolvePathWithMissingTail(mount);if err!=nil{return err}
  for _,part:=range parts{
   got,e:=resolvePathWithMissingTail(part.Mountpoint);if e!=nil{continue}
   same:=filepath.Clean(got)==filepath.Clean(resolved);if runtime.GOOS=="windows"{same=strings.EqualFold(filepath.Clean(got),filepath.Clean(resolved))}
   if same&&part.Device!=""{if old,ok:=devices[part.Device];ok{return fmt.Errorf("磁盘 %s 和 %s 指向同一个文件系统设备 %s",old,d.Name,part.Device)};devices[part.Device]=d.Name}
  }
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
func(a *App)downloadSelected(sel map[string][]Episode)error{
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
  byDisk[d.Name]=append(byDisk[d.Name],batch{task,*d,rows})
 }
 var wg sync.WaitGroup;errs:=make(chan error,len(byDisk))
 for _,batches:=range byDisk{
  batches:=batches;wg.Add(1)
  go func(){defer wg.Done();for _,b:=range batches{if a.ctx.Err()!=nil{return};if err:=a.downloadTask(b);err!=nil{errs<-err;return}}}()
 }
 wg.Wait();close(errs)
 for err:=range errs{if err!=nil{return err}}
 if waitingSpace{return errors.New("部分任务因磁盘空间不足未启动；请释放空间后重新运行")}
 return nil
}
func(a *App)downloadTask(b batch)error{
 sem:=make(chan struct{},a.cfg.Workers);var wg sync.WaitGroup;var mu sync.Mutex;var first error
 for _,ep:=range b.rows{if a.ctx.Err()!=nil{break};sem<-struct{}{};wg.Add(1);go func(e Episode){defer wg.Done();defer func(){<-sem}();if err:=a.downloadOne(b.t,b.d,e);err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock()}}(ep)}
 wg.Wait();return first
}
func episodePaths(version string,t Task,d DiskConfig,parentKey string)(string,string){
 root:=filepath.Join(d.Path,safe(version),safe(t.Scene),safe(taskDir(t)))
 leaf:=safe(path.Base(strings.Trim(parentKey,"/")))
 return filepath.Join(root,leaf),filepath.Join(root,"."+leaf+".partial")
}
func(a *App)downloadOne(t Task,d DiskConfig,e Episode)error{
 if a.ctx.Err()!=nil{return nil};if e.Size<=0{_=a.set(e,d,"","","invalid_size","file_size 无效");return nil}
 var existing *DiskConfig;var existingPath string
 for i:=range a.cfg.Disks{
  candidate:=&a.cfg.Disks[i];candidateFinal,_:=episodePaths(a.cfg.DataVersion,t,*candidate,e.ParentKey)
  if pathIsDir(candidateFinal){
   if existing!=nil{return fmt.Errorf("episode %s 在多个磁盘存在正式目录，违反单副本规则",e.ID)}
   existing=candidate;existingPath=candidateFinal
  }
 }
 if existing!=nil{
  pinned:=a.assigned(t.Name)
  if pinned!=""&&pinned!=existing.Name{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode 已存在于 %s",t.Name,pinned,existing.Name)}
  if pinned==""{if err:=a.assign(t.Name,existing.Name);err!=nil{return err}}
  if err:=a.record(e,*existing,existingPath);err!=nil{return err}
  return a.set(e,*existing,existingPath,"","completed","")
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
  if obj.IsDir||strings.HasSuffix(obj.Key,"/"){continue}
  objects=append(objects,obj)
 }
 if err:=sweepPartial(partial,e.ParentKey,objects);err!=nil{return a.fail(e,d,final,partial,err)}
 sem:=make(chan struct{},a.cfg.FileWorkers);var wg sync.WaitGroup;var total atomic.Int64;var mu sync.Mutex;var first error
 for _,item:=range objects{
  item:=item
  if a.ctx.Err()!=nil{break}
  sem<-struct{}{};wg.Add(1)
  go func(){
   defer wg.Done();defer func(){<-sem}()
   rel:=strings.TrimPrefix(item.Key,e.ParentKey+"/")
   dst,err:=safeJoin(partial,rel)
   if err==nil{err=os.MkdirAll(filepath.Dir(dst),0755)}
   if err==nil{err=a.fetchObject(item.Key,dst,item)}
   if err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock();return}
   total.Add(item.Size)
  }()
 }
 wg.Wait()
 if first!=nil{return a.fail(e,d,final,partial,first)}
 if a.ctx.Err()!=nil{return a.fail(e,d,final,partial,a.ctx.Err())}
 if total.Load()==0{return a.fail(e,d,final,partial,errors.New("MinIO 前缀为空"))};if err:=os.MkdirAll(filepath.Dir(final),0755);err!=nil{return a.fail(e,d,final,partial,err)}
 if err:=os.RemoveAll(final);err!=nil{return a.fail(e,d,final,partial,err)};if err:=os.Rename(partial,final);err!=nil{return a.fail(e,d,final,partial,err)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(final))}
 if err:=a.record(e,d,final);err!=nil{return err};return a.set(e,d,final,"","completed","")
}
func sweepPartial(root,parent string,objects []minio.ObjectInfo)error{
 info,err:=os.Lstat(root);if err!=nil{return err};if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return fmt.Errorf("暂存路径不是普通目录: %s",root)}
 keep:=make(map[string]bool,len(objects)*2)
 for _,obj:=range objects{
  rel:=strings.TrimPrefix(obj.Key,parent+"/")
  dst,err:=safeJoin(root,rel);if err!=nil{return err}
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
func(a *App)fetchObject(key,dst string,expected minio.ObjectInfo)error{
 tmp:=dst+".minio-tmp"
 attempts:=a.cfg.FileRetries
 for attempt:=1;attempt<=attempts;attempt++{
  if a.ctx.Err()!=nil{return a.ctx.Err()}
  offset:=int64(0)
  if st,err:=os.Stat(tmp);err==nil{
   if st.Size()>expected.Size{_=os.Remove(tmp)}else{offset=st.Size()}
  }else if !os.IsNotExist(err){return err}
  if offset>=expected.Size&&offset>0{_=os.Remove(tmp);offset=0}
  if offset>0{
   current,statErr:=a.mc.StatObject(a.ctx,a.cfg.Bucket,key,minio.StatObjectOptions{})
   if statErr!=nil{if attempt<attempts{select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):};continue};return statErr}
   changed:=current.Size!=expected.Size||(expected.ETag!=""&&!sameETag(current.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!current.LastModified.Equal(expected.LastModified))
   if changed{_=os.Remove(tmp);return fmt.Errorf("续传前发现远端对象已变化，已丢弃旧临时文件: %s",key)}
  }
  opts:=minio.GetObjectOptions{}
  if expected.ETag!=""{if err:=opts.SetMatchETag(strings.Trim(expected.ETag,"\""));err!=nil{return err}}
  if offset>0{if err:=opts.SetRange(offset,0);err!=nil{return err}}
  obj,err:=a.mc.GetObject(a.ctx,a.cfg.Bucket,key,opts)
  if err==nil{
   var info minio.ObjectInfo
   info,err=obj.Stat()
   if err==nil&&(info.Size!=expected.Size||(expected.ETag!=""&&!sameETag(info.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!info.LastModified.Equal(expected.LastModified))){err=fmt.Errorf("对象元数据发生变化: %s",key)}
   if err==nil{
    flags:=os.O_CREATE|os.O_WRONLY
    if offset>0{flags|=os.O_APPEND}else{flags|=os.O_TRUNC}
    var out *os.File
    out,err=os.OpenFile(tmp,flags,0644)
    if err==nil{
     var n int64
     n,copyErr:=io.Copy(out,obj)
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
   if statErr!=nil{err=statErr}else if latest.Size!=expected.Size||(expected.ETag!=""&&!sameETag(latest.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!latest.LastModified.Equal(expected.LastModified)){err=fmt.Errorf("下载期间远端对象被修改: %s",key)}
  }
  if err==nil&&*a.cfg.VerifyMD5&&isMD5ETag(expected.ETag){
   var sum string
   sum,err=fileMD5(tmp)
   if err==nil&&!strings.EqualFold(sum,strings.Trim(expected.ETag,"\"")){err=fmt.Errorf("MD5 校验失败 %s: %s != %s",key,sum,expected.ETag)}
  }
  if err==nil{
   _=os.Remove(dst)
   if err=os.Rename(tmp,dst);err==nil{if !expected.LastModified.IsZero(){_=os.Chtimes(dst,expected.LastModified,expected.LastModified)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(dst))}}
   if err==nil{return nil}
  }
  if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){return err}
  if err!=nil&&strings.Contains(err.Error(),"远端对象被修改"){_=os.Remove(tmp)}
  if attempt<attempts{
   log.Printf("对象下载重试 %d/%d: %s: %v",attempt,attempts,key,err)
   select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):}
  }
 }
 return fmt.Errorf("对象下载重试 %d 次仍失败: %s",attempts,key)
}
func sameETag(a,b string)bool{return strings.Trim(a,"\"")==strings.Trim(b,"\"")}
func isMD5ETag(etag string)bool{v:=strings.Trim(etag,"\"");if len(v)!=32||strings.Contains(v,"-"){return false};_,err:=hex.DecodeString(v);return err==nil}
func fileMD5(name string)(string,error){f,err:=os.Open(name);if err!=nil{return "",err};defer f.Close();h:=md5.New();if _,err=io.Copy(h,f);err!=nil{return "",err};return hex.EncodeToString(h.Sum(nil)),nil}
func(a *App)fail(e Episode,d DiskConfig,final,partial string,err error)error{
 if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){_=a.set(e,d,final,partial,"paused","已暂停，保留暂存文件供下次续传");return nil}
 _=a.set(e,d,final,partial,"failed",err.Error());log.Printf("下载失败 %s: %v",e.ID,err);return err
}
func(a *App)cleanup(preview bool)(int,error){
 rows,err:=a.db.Query("SELECT episode_id,target_path FROM downloads WHERE status='completed'");if err!=nil{return 0,err}
 type item struct{id string;path sql.NullString}
 items:=[]item{}
 for rows.Next(){var x item;if err=rows.Scan(&x.id,&x.path);err!=nil{rows.Close();return 0,err};items=append(items,x)}
 if err=rows.Err();err!=nil{rows.Close();return 0,err};if err=rows.Close();err!=nil{return 0,err}
 n:=0
 for _,x:=range items{if !x.path.Valid||!hasPattern(x.path.String,a.cfg.MCAPPattern){
  n++;if preview{log.Printf("[dry-run] 缺 mcap: %s",x.path.String)}else{
   if x.path.Valid{if err=os.RemoveAll(x.path.String);err!=nil{return n,err}}
   if _,err=a.db.Exec("DELETE FROM downloads WHERE episode_id=?",x.id);err!=nil{return n,err}
   if _,err=a.db.Exec("DELETE FROM downloaded_episodes WHERE episode_id=?",x.id);err!=nil{return n,err}
  }
 }}
 return n,nil
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
) FROM settings WHERE key='web.token_hash'").Scan(&api.tokenHash)}
 if !isLoopbackHost(host)&&api.tokenHash==""{return errors.New("监听非本机地址之前必须先在 Python Web 配置库设置访问口令")}
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){writeJSON(w,map[string]any{"ok":true,"version":"go-dev"})})
 mux.HandleFunc("GET /",func(w http.ResponseWriter,r *http.Request){w.Header().Set("Content-Type","text/html; charset=utf-8");_,_=io.WriteString(w,"<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>MinIO Go Downloader</title><body style='font:15px system-ui;max-width:1100px;margin:40px auto;padding:0 20px'><h2>MinIO Go Downloader API</h2><p>只读 API：<a href='/api/status'>状态</a> · <a href='/api/logs'>日志</a> · <a href='/healthz'>健康检查</a></p><pre id='out'>正在读取状态…</pre><script>async function tick(){try{let r=await fetch('/api/status');document.querySelector('#out').textContent=JSON.stringify(await r.json(),null,2)}catch(e){document.querySelector('#out').textContent=String(e)}}tick();setInterval(tick,5000)</script></body></html>")})
 mux.HandleFunc("GET /api/status",api.status)
 mux.HandleFunc("GET /api/episodes",api.episodes)
 mux.HandleFunc("GET /api/logs",api.logs)
 mux.HandleFunc("POST /api/stop",api.stop)
 handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  w.Header().Set("X-Content-Type-Options","nosniff");w.Header().Set("X-Frame-Options","DENY");w.Header().Set("Referrer-Policy","no-referrer")
  if strings.HasPrefix(r.URL.Path,"/api/"){w.Header().Set("Cache-Control","no-store");if r.Method=="POST"&&r.Header.Get("Content-Type")!="application/json"{writeJSONStatus(w,415,map[string]any{"error":"写操作必须使用 Content-Type: application/json"});return};if !api.authorized(r){writeJSONStatus(w,401,map[string]any{"error":"口令缺失或错误"});return}}
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
 if a.tokenHash==""{return isLoopbackHost(r.Hostname())&&isLoopbackRemote(r.RemoteAddr)&&r.Header.Get("X-Forwarded-For")==""}
 supplied:=strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"),"Bearer "));addr:=r.RemoteAddr;now:=time.Now()
 a.authMu.Lock()
 if a.lockouts!=nil{if until:=a.lockouts[addr];now.Before(until){a.authMu.Unlock();return false}}
 if a.failures==nil{a.failures=map[string][]time.Time{}};if a.lockouts==nil{a.lockouts=map[string]time.Time{}}
 attempts:=a.failures[addr];kept:=attempts[:0];for _,at:=range attempts{if now.Sub(at)<=5*time.Minute{kept=append(kept,at)}};attempts=kept
 if len(attempts)>=8&&now.Sub(attempts[len(attempts)-1])<60*time.Second{a.lockouts[addr]=attempts[len(attempts)-1].Add(60*time.Second);a.authMu.Unlock();return false}
 a.authMu.Unlock()
 parts:=strings.Split(a.tokenHash,"$");if len(parts)!=4||parts[0]!="pbkdf2_sha256"{return false}
 iterations,err:=strconv.Atoi(parts[1]);if err!=nil||iterations<1||iterations>200000{return false}
 salt,err:=hex.DecodeString(parts[2]);if err!=nil{return false};want,err:=hex.DecodeString(parts[3]);if err!=nil{return false}
 got:=pbkdf2SHA256([]byte(supplied),salt,iterations,len(want));valid:=hmac.Equal(got,want)
 a.authMu.Lock();defer a.authMu.Unlock()
 if valid{delete(a.failures,addr);delete(a.lockouts,addr);return true}
 if supplied!=""{a.failures[addr]=append(attempts,now);if len(a.failures[addr])>=8{a.lockouts[addr]=now.Add(60*time.Second)}}
 return false
}
func pbkdf2SHA256(password,salt []byte,iterations,keyLen int)[]byte{
 hLen:=sha256.Size;blocks:=(keyLen+hLen-1)/hLen;out:=make([]byte,0,blocks*hLen)
 for block:=1;block<=blocks;block++{mac:=hmac.New(sha256.New,password);_,_=mac.Write(salt);var counter [4]byte;counter[0]=byte(block>>24);counter[1]=byte(block>>16);counter[2]=byte(block>>8);counter[3]=byte(block);_,_=mac.Write(counter[:]);u:=mac.Sum(nil);t:=append([]byte(nil),u...);for i:=1;i<iterations;i++{mac=hmac.New(sha256.New,password);_,_=mac.Write(u);u=mac.Sum(nil);for j:=range t{t[j]^=u[j]}};out=append(out,t...)}
 return out[:keyLen]
}
func(a *WebAPI)status(w http.ResponseWriter,r *http.Request){
 scenes:=[]map[string]any{};sceneIndex:=map[string]int{};taskDisk:=map[string]string{};staleRecords:=0
 rowsAssign,err:=a.db.Query("SELECT task_name,disk_name FROM task_disk_assignments");if err==nil{for rowsAssign.Next(){var task,diskName string;if rowsAssign.Scan(&task,&diskName)==nil{taskDisk[task]=diskName}};rowsAssign.Close()}
 for _,t:=range a.cfg.Tasks{
  idx,ok:=sceneIndex[t.Scene];if !ok{idx=len(scenes);sceneIndex[t.Scene]=idx;scenes=append(scenes,map[string]any{"scene":t.Scene,"tasks":[]map[string]any{}})}
  rows:=scenes[idx]["tasks"].([]map[string]any);count:=0;seconds:=0.0;bytesCount:=int64(0);issues:=map[string]int{}
  records,e:=a.db.Query("SELECT content_time,file_size,target_path FROM downloaded_episodes WHERE task_name=?",t.Name)
  if e==nil{for records.Next(){var sec sql.NullFloat64;var size sql.NullInt64;var target sql.NullString;if records.Scan(&sec,&size,&target)!=nil{continue};if target.Valid&&pathIsDir(target.String){count++;seconds+=sec.Float64;bytesCount+=size.Int64}else if target.Valid{staleRecords++}};records.Close()}
  issueRows,e:=a.db.Query("SELECT status,COUNT(*) FROM downloads WHERE task_name=? AND status<>'completed' GROUP BY status",t.Name)
  if e==nil{for issueRows.Next(){var state string;var n int;if issueRows.Scan(&state,&n)==nil{issues[state]=n}};issueRows.Close()}
  var assigned any=nil;if name,ok:=taskDisk[t.Name];ok{assigned=name};aliases:=t.Aliases;if aliases==nil{aliases=[]string{}};tags:=t.Tags;if tags==nil{tags=[]string{}}
  rows=append(rows,map[string]any{"name":t.Name,"dir":taskDir(t),"target":t.TargetCount,"target_hours":t.TargetHours,"aliases":aliases,"tags":tags,"downloaded":count,"hours":math.Round(seconds/3600*100)/100,"bytes":bytesCount,"issues":issues,"disk":assigned});scenes[idx]["tasks"]=rows
 }
 disks:=[]map[string]any{}
 diskTasks:=map[string][]string{};for task,name:=range taskDisk{diskTasks[name]=append(diskTasks[name],task)}
 for _,d:=range a.cfg.Disks{usage,e:=disk.Usage(d.Path);item:=map[string]any{"name":d.Name,"path":d.Path,"mounted":isMountedPath(d.Mountpoint),"exists":pathIsDir(d.Path),"total":uint64(0),"free":uint64(0),"used":uint64(0),"tasks":diskTasks[d.Name]};if e==nil{item["total"]=usage.Total;item["free"]=usage.Free;item["used"]=usage.Used};disks=append(disks,item)}
 state:=map[string]any{"running":false,"pid":nil,"by_web":false}
 if raw,e:=os.ReadFile(a.cfg.PIDFile);e==nil{var p pidRecord;if json.Unmarshal(raw,&p)==nil{if proc,e:=process.NewProcess(int32(p.PID));e==nil{alive,_:=proc.IsRunning();if alive{ct,_:=proc.CreateTime();if p.Created==0||math.Abs(float64(ct)/1000-p.Created)<2{state=map[string]any{"running":true,"pid":p.PID,"by_web":false}}}}}}
 running,_:=state["running"].(bool)
 control:=map[string]string{};for _,key:=range []string{"stop_requested","last_run"}{var v string;if a.db.QueryRow("SELECT value FROM control WHERE key=?",key).Scan(&v)==nil{control[key]=v}}
 var last any=map[string]any{};if raw:=control["last_run"];raw!=""{var v map[string]any;if json.Unmarshal([]byte(raw),&v)==nil{last=v}}
 runningEpisodes:=[]map[string]any{}
 if running{rr,e:=a.db.Query("SELECT episode_id,task_name,disk_name,started_at,expected_bytes,partial_path FROM downloads WHERE status='running' ORDER BY updated_at DESC LIMIT 24");if e==nil{for rr.Next(){var id,taskName string;var diskName,started,partial sql.NullString;var expected sql.NullInt64;if rr.Scan(&id,&taskName,&diskName,&started,&expected,&partial)==nil{partialBytes:=int64(0);if partial.Valid{partialBytes,_=directorySizeBytes(partial.String)};runningEpisodes=append(runningEpisodes,map[string]any{"episode_id":id,"task_name":taskName,"disk_name":diskName.String,"started_at":started.String,"expected_bytes":expected.Int64,"partial_bytes":partialBytes})}};rr.Close()}}
 issues:=[]map[string]string{};if a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""{issues=append(issues,map[string]string{"page":"connection","text":"尚未配置 MinIO 连接"})};if len(a.cfg.Disks)==0{issues=append(issues,map[string]string{"page":"disks","text":"尚未配置目标磁盘"})};if len(a.cfg.Tasks)==0{issues=append(issues,map[string]string{"page":"tasks","text":"尚未配置下载任务"})}
 endpoint:=a.cfg.Endpoint;if u,e:=url.Parse(endpoint);e==nil&&u.Host!=""{endpoint=u.Scheme+"://"+u.Host}
 settings:=map[string]any{"min_free_gb":a.cfg.MinFreeGB,"writes_per_disk":a.cfg.Workers,"require_mcap":a.cfg.RequireMCAP,"mcap_pattern":a.cfg.MCAPPattern,"default_rounds":1}
 writeJSON(w,map[string]any{"version":"go-dev","data_version":a.cfg.DataVersion,"downloader":state,"stop_requested":running&&isStopRequested(control["stop_requested"]),"setup_issues":issues,"minio":map[string]any{"endpoint":endpoint,"bucket":a.cfg.Bucket},"settings":settings,"last_run":last,"stale_records":staleRecords,"running_episodes":runningEpisodes,"disks":disks,"scenes":scenes})
}
func(a *WebAPI)stop(w http.ResponseWriter,r *http.Request){
 var body struct{Force bool `json:"force"`};if err:=json.NewDecoder(io.LimitReader(r.Body,4096)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Force{writeJSONStatus(w,400,map[string]any{"error":"Go Web API 暂未实现强制结束；请使用安全暂停"});return}
 raw,err:=os.ReadFile(a.cfg.PIDFile);if err!=nil{writeJSONStatus(w,409,map[string]any{"error":"当前没有运行中的下载器"});return}
 var record pidRecord;if json.Unmarshal(raw,&record)!=nil||record.PID<=0{writeJSONStatus(w,409,map[string]any{"error":"当前没有运行中的下载器"});return}
 proc,err:=process.NewProcess(int32(record.PID));if err!=nil{writeJSONStatus(w,409,map[string]any{"error":"当前没有运行中的下载器"});return}
 alive,err:=proc.IsRunning();if err!=nil||!alive{writeJSONStatus(w,409,map[string]any{"error":"当前没有运行中的下载器"});return}
 created,err:=proc.CreateTime();if err==nil&&record.Created>0&&math.Abs(float64(created)/1000-record.Created)>=2{writeJSONStatus(w,409,map[string]any{"error":"PID 已被其他进程复用"});return}
 if err:=writeStopRequest(a.db);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"写入暂停请求失败: "+err.Error()});return}
 writeJSON(w,map[string]any{"ok":true,"mode":"pause"})
}
func(a *WebAPI)episodes(w http.ResponseWriter,r *http.Request){
 task:=r.URL.Query().Get("task");rows,err:=a.db.Query("SELECT episode_id,status,error,parent_key,updated_at FROM downloads WHERE task_name=? AND status<>'completed' ORDER BY updated_at DESC LIMIT 200",task)
 if err!=nil{writeJSON(w,map[string]any{"episodes":[]any{}});return};defer rows.Close()
 episodes:=[]map[string]any{};for rows.Next(){var id,status string;var errText,parent,updated sql.NullString;if rows.Scan(&id,&status,&errText,&parent,&updated)==nil{episodes=append(episodes,map[string]any{"episode_id":id,"status":status,"error":errText.String,"parent_key":parent.String,"updated_at":updated.String})}}
 writeJSON(w,map[string]any{"episodes":episodes})
}
func(a *WebAPI)logs(w http.ResponseWriter,r *http.Request){
 name:=filepath.Join(a.cfg.LogDir,"minio_task_downloader.log");info,err:=os.Stat(name);if err!=nil{writeJSON(w,map[string]any{"text":"","offset":0,"reset":true});return};size:=info.Size()
 offset,err:=strconv.ParseInt(r.URL.Query().Get("offset"),10,64);initial:=err!=nil||offset<0||offset>size;start:=offset;if initial{start=size-96*1024;if start<0{start=0}}
 file,err:=os.Open(name);if err!=nil{writeJSON(w,map[string]any{"text":"","offset":0,"reset":true});return};defer file.Close();_,_=file.Seek(start,io.SeekStart)
 chunk,_:=io.ReadAll(io.LimitReader(file,512*1024));if initial&&start>0{if i:=bytes.IndexByte(chunk,'\n');i>=0{chunk=chunk[i+1:];start=size-int64(len(chunk))}}
 cut:=bytes.LastIndexByte(chunk,'\n')+1;if cut<0{cut=0};chunk=chunk[:cut]
 writeJSON(w,map[string]any{"text":string(chunk),"offset":start+int64(len(chunk)),"reset":initial})
}

func loadConfigDB(c *Config,runMode string)error{
 if strings.TrimSpace(c.ConfigDB)==""{return nil}
 if runMode!="count"{return errors.New("config_db 当前只支持 count 模式；duration 模式请使用独立 JSON 任务目标")}
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
 getInt("transfer.file_workers",&c.FileWorkers);getInt("transfer.file_retries",&c.FileRetries);getFloat("transfer.retry_delay",&c.RetryDelaySeconds)
 if raw,ok:=settings["transfer.verify_md5"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.VerifyMD5=&v}}
 if raw,ok:=settings["transfer.fsync"];ok{var v bool;if json.Unmarshal([]byte(raw),&v)==nil{c.Fsync=&v}}
 if raw,ok:=settings["tasks.data_version"];ok{var v string;if json.Unmarshal([]byte(raw),&v)==nil&&v!=""{c.DataVersion=v}}
 taskRows,err:=db.Query("SELECT s.name,t.name,t.target_count,t.directory,t.id FROM tasks t JOIN scenes s ON s.id=t.scene_id ORDER BY s.position,t.position");if err!=nil{return err}
 aliases:=map[int64][]string{};aliasRows,err:=db.Query("SELECT task_id,alias FROM task_aliases ORDER BY task_id,position");if err!=nil{taskRows.Close();return err};for aliasRows.Next(){var id int64;var alias string;if err=aliasRows.Scan(&id,&alias);err!=nil{aliasRows.Close();taskRows.Close();return err};aliases[id]=append(aliases[id],alias)};if err=aliasRows.Err();err!=nil{aliasRows.Close();taskRows.Close();return err};if err=aliasRows.Close();err!=nil{taskRows.Close();return err}
 tasks:=[]Task{};for taskRows.Next(){var t Task;var id int64;if err=taskRows.Scan(&t.Scene,&t.Name,&t.TargetCount,&t.Dir,&id);err!=nil{taskRows.Close();return err};t.Aliases=aliases[id];tasks=append(tasks,t)};if err=taskRows.Err();err!=nil{taskRows.Close();return err};if err=taskRows.Close();err!=nil{return err};if len(tasks)==0{return errors.New("SQLite 配置库中没有任务")};c.Tasks=tasks
 diskRows,err:=db.Query("SELECT name,path,mountpoint FROM disks ORDER BY position");if err!=nil{return err};disks:=[]DiskConfig{};for diskRows.Next(){var d DiskConfig;if err=diskRows.Scan(&d.Name,&d.Path,&d.Mountpoint);err!=nil{diskRows.Close();return err};disks=append(disks,d)};if err=diskRows.Err();err!=nil{diskRows.Close();return err};if err=diskRows.Close();err!=nil{return err};if len(disks)==0{return errors.New("SQLite 配置库中没有目标磁盘")};c.Disks=disks
 c.Mode="count";c.fromConfigDB=true;log.Printf("已从 SQLite 配置库加载配置: %s（任务 %d 个，磁盘 %d 块）",c.ConfigDB,len(c.Tasks),len(c.Disks));return nil
}
func(c *Config)normalize(m string)error{
 if c.Mode!=""&&c.Mode!=m{return fmt.Errorf("配置文件 mode=%s 与命令行 --mode=%s 不一致",c.Mode,m)}
 if c.Endpoint==""||c.AccessKey==""||c.SecretKey==""||c.Bucket==""{return errors.New("MinIO endpoint/access_key/secret_key/bucket 必须填写")}
 if c.PGHost==""||c.PGDB==""||c.PGUser==""||c.PGPassword==""{return errors.New("PostgreSQL 连接参数必须填写")}
 if c.PGPort==0{c.PGPort=5432};if c.DataVersion==""{c.DataVersion="银河2.2数据"};if c.LockFile==""{c.LockFile="minio_download.lock"};if c.PIDFile==""{c.PIDFile="minio_download.pid"};if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"};if c.LogDir==""{c.LogDir="logs"}
 if c.MinFreeGB==0&&!c.fromConfigDB{c.MinFreeGB=3};if c.SafetyFactor==0{c.SafetyFactor=1.15};if c.Workers<=0{c.Workers=2};if c.FileWorkers<=0{c.FileWorkers=4};if c.FileRetries<=0{c.FileRetries=5};if c.RetryDelaySeconds<=0&&!c.fromConfigDB{c.RetryDelaySeconds=1};if c.ValidateWorkers<=0{c.ValidateWorkers=16};if c.Workers>16||c.FileWorkers>16||c.ValidateWorkers>32||c.FileRetries>20{return errors.New("并发数不能超过 16，文件重试次数不能超过 20")};if c.MinFreeGB<0||c.MinFreeGB>1_000_000{return errors.New("min_free_gb 必须在 0 到 1000000 之间")};if c.SafetyFactor<1||c.SafetyFactor>5{return errors.New("space_safety_factor 必须在 1 到 5 之间")};if c.RetryDelaySeconds<0||c.RetryDelaySeconds>60{return errors.New("retry_delay_seconds 必须在 0 到 60 之间")};if c.PGPort<1||c.PGPort>65535{return errors.New("pg_port 必须在 1 到 65535 之间")};if c.MCAPPattern==""{c.MCAPPattern="*.SYNC.mcap"};if c.VerifyMD5==nil{v:=true;c.VerifyMD5=&v};if c.Fsync==nil{v:=true;c.Fsync=&v}
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
 if runtime.GOOS=="windows"{if err=os.Remove(name);err!=nil&&!os.IsNotExist(err){os.Remove(tmp);return err}}
 if err=os.Rename(tmp,name);err!=nil{os.Remove(tmp);return err}
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
func requestStop(c Config) error {
 if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"}
 if err:=os.MkdirAll(filepath.Dir(c.StateDB),0755);err!=nil{return err}
 db,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};defer db.Close()
 db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA busy_timeout=30000");err!=nil{return err}
 return writeStopRequest(db)
}
func(a *App)saveLastRun(result string,runErr error){
 payload:=map[string]any{"result":result,"at":controlTimestamp(),"mode":a.mode}
 if runErr!=nil{payload["error"]=runErr.Error()}
 raw,err:=json.Marshal(payload);if err!=nil{log.Printf("保存上次运行结果失败: %v",err);return}
 _,err=a.db.Exec("INSERT INTO control(key,value,updated_at) VALUES('last_run',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",string(raw),controlTimestamp())
 if err!=nil{log.Printf("保存上次运行结果失败: %v",err)}
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
 q.Set("connect_timeout","10")
 q.Set("application_name","minio_task_downloader_go")
 u:=url.URL{Scheme:"postgres",User:url.UserPassword(c.PGUser,c.PGPassword),Host:net.JoinHostPort(c.PGHost,strconv.Itoa(c.PGPort)),Path:"/"+c.PGDB,RawQuery:q.Encode()}
 return u.String()
}
func(a *App)open()error{
 endpoint:=strings.TrimPrefix(strings.TrimPrefix(a.cfg.Endpoint,"https://"),"http://");secure:=a.cfg.SSL||strings.HasPrefix(a.cfg.Endpoint,"https://")
 transport:=http.DefaultTransport.(*http.Transport).Clone()
 if a.cfg.MinioSkipVerify{transport.TLSClientConfig=&tls.Config{InsecureSkipVerify:true} /* #nosec: explicitly configured to match Python minio.cert_check=false */}
 mc,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(a.cfg.AccessKey,a.cfg.SecretKey,""),Secure:secure,Region:a.cfg.Region,Transport:transport});if e!=nil{return e};a.mc=mc
 pg:=postgresDSN(a.cfg)
 p,e:=sql.Open("pgx",pg);if e!=nil{return e};defer p.Close();ctx,cancel:=context.WithTimeout(a.ctx,15*time.Second);defer cancel();if e=p.PingContext(ctx);e!=nil{return fmt.Errorf("PostgreSQL 连接失败: %w",e)}
 ok,e:=mc.BucketExists(ctx,a.cfg.Bucket);if e!=nil{return e};if !ok{return fmt.Errorf("MinIO bucket 不存在: %s",a.cfg.Bucket)}
 a.db,e=sql.Open("sqlite",a.cfg.StateDB);if e!=nil{return e};a.db.SetMaxOpenConns(1)
 _,e=a.db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000;"+
 "CREATE TABLE IF NOT EXISTS downloads(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,parent_key TEXT,disk_name TEXT,target_path TEXT,partial_path TEXT,expected_bytes INTEGER,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_download_task_status ON downloads(task_name,status);"+
 "CREATE TABLE IF NOT EXISTS task_disk_assignments(task_name TEXT NOT NULL,disk_name TEXT NOT NULL,last_used_at TEXT NOT NULL,PRIMARY KEY(task_name,disk_name));"+
 "CREATE TABLE IF NOT EXISTS downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,source_task_name TEXT,job_id INTEGER,sn_code TEXT,parent_key TEXT,disk_name TEXT,target_path TEXT,file_size INTEGER,content_time REAL,create_time TEXT,downloaded_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_downloaded_task ON downloaded_episodes(task_name);"+
 "CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL);")
 if e==nil{
  if e=ensureColumn(a.db,"downloads","started_at","TEXT");e!=nil{return e}
  if e=ensureColumn(a.db,"downloads","completed_at","TEXT");e!=nil{return e}
  log.Printf("MinIO=%s bucket=%s；PostgreSQL=%s:%d/%s；SQLite=%s",a.cfg.Endpoint,a.cfg.Bucket,a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.StateDB)
 };return e
}
func(a *App)set(e Episode,d DiskConfig,final,partial,status,msg string)error{
 now:=time.Now().UTC().Format(time.RFC3339Nano)
 _,err:=a.db.Exec("INSERT INTO downloads(episode_id,task_name,parent_key,disk_name,target_path,partial_path,expected_bytes,status,error,started_at,completed_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,partial_path=excluded.partial_path,expected_bytes=excluded.expected_bytes,status=excluded.status,error=excluded.error,"+
 "started_at=CASE WHEN excluded.status='running' THEN excluded.updated_at ELSE downloads.started_at END,"+
 "completed_at=CASE WHEN excluded.status='completed' THEN excluded.updated_at WHEN excluded.status='running' THEN NULL ELSE downloads.completed_at END,updated_at=excluded.updated_at",
 e.ID,e.Task,e.ParentKey,d.Name,nullable(final),nullable(partial),e.Size,status,nullable(msg),nullableTime(status=="running",now),nullableTime(status=="completed",now),now)
 return err
}
func nullableTime(ok bool,v string)any{if ok{return v};return nil}
func ensureColumn(db *sql.DB,table,column,typ string)error{
 rows,err:=db.Query("PRAGMA table_info("+table+")");if err!=nil{return err}
 found:=false
 for rows.Next(){var cid,notnull,pk int;var name,kind string;var def sql.NullString;if err=rows.Scan(&cid,&name,&kind,&notnull,&def,&pk);err!=nil{rows.Close();return err};if name==column{found=true}}
 if err=rows.Err();err!=nil{rows.Close();return err};if err=rows.Close();err!=nil{return err}
 if !found{_,err=db.Exec("ALTER TABLE "+table+" ADD COLUMN "+column+" "+typ)}
 return err
}
func nullable(s string)any{if s==""{return nil};return s}
func(a *App)completed(id string)bool{
 var p sql.NullString
 err:=a.db.QueryRow("SELECT target_path FROM downloads WHERE episode_id=? AND status='completed'",id).Scan(&p)
 if err==nil&&p.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(p.String)){return true}
 p=sql.NullString{}
 err=a.db.QueryRow("SELECT target_path FROM downloaded_episodes WHERE episode_id=?",id).Scan(&p)
 return err==nil&&p.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(p.String))
}
func(a *App)stats(task string)(float64,int){rows,err:=a.db.Query("SELECT content_time,target_path FROM downloaded_episodes WHERE task_name=?",task);if err!=nil{return 0,0};defer rows.Close();var seconds float64;count:=0;for rows.Next(){var v sql.NullFloat64;var p sql.NullString;if rows.Scan(&v,&p)!=nil{continue};if a.cfg.TrustCompletedRecords||(p.Valid&&pathIsDir(p.String)){seconds+=v.Float64;count++}};return seconds,count}
func(a *App)assign(task,disk string)error{tx,err:=a.db.Begin();if err!=nil{return err};defer tx.Rollback();if _,err=tx.Exec("DELETE FROM task_disk_assignments WHERE task_name=?",task);err!=nil{return err};if _,err=tx.Exec("INSERT INTO task_disk_assignments(task_name,disk_name,last_used_at) VALUES(?,?,?)",task,disk,time.Now().UTC().Format(time.RFC3339Nano));err!=nil{return err};return tx.Commit()}
func(a *App)assigned(task string)string{var d string;_=a.db.QueryRow("SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC LIMIT 1",task).Scan(&d);return d}
func(a *App)record(e Episode,d DiskConfig,final string)error{
 _,err:=a.db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,source_task_name,job_id,sn_code,parent_key,disk_name,target_path,file_size,content_time,create_time,downloaded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,source_task_name=excluded.source_task_name,job_id=excluded.job_id,sn_code=excluded.sn_code,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,file_size=excluded.file_size,content_time=excluded.content_time,create_time=excluded.create_time,downloaded_at=excluded.downloaded_at",
 e.ID,e.Task,e.SourceTask,e.JobID,e.SN,e.ParentKey,d.Name,final,e.Size,e.Duration,e.Created,time.Now().UTC().Format(time.RFC3339Nano));return err
}
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
  if !obj.IsDir&&!strings.HasSuffix(obj.Key,"/"){direct[path.Base(obj.Key)]=true}
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
   if ledgerErr==nil&&ledgerPath.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(ledgerPath.String)){
    d:=DiskConfig{Name:ledgerDisk.String}
    if err:=a.set(ep,d,ledgerPath.String,"","completed","");err!=nil{return err}
   }else{
    var statePath,stateDisk sql.NullString
    var state string
    stateErr:=a.db.QueryRow("SELECT status,target_path,disk_name FROM downloads WHERE episode_id=?",ep.ID).Scan(&state,&statePath,&stateDisk)
    if stateErr==nil&&state=="completed"&&statePath.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(statePath.String)){
     d:=DiskConfig{Name:stateDisk.String}
     if err:=a.record(ep,d,statePath.String);err!=nil{return err}
     if err:=a.set(ep,d,statePath.String,"","completed","");err!=nil{return err}
    }
   }
   diskName,err:=findExistingTaskDisk(a.cfg.DataVersion,task,a.cfg.Disks,[]Episode{ep})
   if err!=nil{return err};if diskName==""{continue}
   var chosen *DiskConfig
   for i:=range a.cfg.Disks{if a.cfg.Disks[i].Name==diskName{chosen=&a.cfg.Disks[i];break}}
   if chosen==nil{return fmt.Errorf("找不到 episode %s 的本地磁盘 %s",ep.ID,diskName)}
   pinned:=a.assigned(task.Name)
   if pinned!=""&&pinned!=diskName{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode %s 已存在于 %s",task.Name,pinned,ep.ID,diskName)}
   if pinned==""{if err:=a.assign(task.Name,diskName);err!=nil{return err}}
   final,_:=episodePaths(a.cfg.DataVersion,task,*chosen,ep.ParentKey)
   if err:=a.record(ep,*chosen,final);err!=nil{return err}
   if err:=a.set(ep,*chosen,final,"","completed","");err!=nil{return err}
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
  _,downloaded:=a.stats(task.Name);gap:=task.TargetCount-downloaded
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
func(a *App)run(all []Episode,nround int)error{
 groups:=map[string][]Episode{};for _,e:=range all{groups[e.Task]=append(groups[e.Task],e)}
 if err:=a.syncLocalCopies(groups);err!=nil{return err}
 if a.mode=="count"{var err error;groups,err=a.prevalidateCount(groups);if err!=nil{if a.ctx.Err()!=nil{return nil};return err}}
 for r:=1;r<=nround;r++{if a.ctx.Err()!=nil{return nil};selected:=map[string][]Episode{}
  for _,t:=range a.cfg.Tasks{s,n:=a.stats(t.Name);target,used:=t.TargetHours*3600,s;if a.mode=="count"{target=float64(t.TargetCount);used=float64(n)}
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
type batch struct{t Task;d DiskConfig;rows []Episode}
func findExistingTaskDisk(version string,t Task,disks []DiskConfig,rows []Episode)(string,error){
 found:=""
 for _,ep:=range rows{
  for _,d:=range disks{
   final,_:=episodePaths(version,t,d,ep.ParentKey)
   if pathIsDir(final){
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
 devices:=map[string]string{}
 for _,d:=range disks{
  if err:=validateDisk(d);err!=nil{return err}
  mount:=d.Mountpoint;if mount==""{mount=d.Path};resolved,err:=resolvePathWithMissingTail(mount);if err!=nil{return err}
  for _,part:=range parts{
   got,e:=resolvePathWithMissingTail(part.Mountpoint);if e!=nil{continue}
   same:=filepath.Clean(got)==filepath.Clean(resolved);if runtime.GOOS=="windows"{same=strings.EqualFold(filepath.Clean(got),filepath.Clean(resolved))}
   if same&&part.Device!=""{if old,ok:=devices[part.Device];ok{return fmt.Errorf("磁盘 %s 和 %s 指向同一个文件系统设备 %s",old,d.Name,part.Device)};devices[part.Device]=d.Name}
  }
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
func(a *App)downloadSelected(sel map[string][]Episode)error{
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
  byDisk[d.Name]=append(byDisk[d.Name],batch{task,*d,rows})
 }
 var wg sync.WaitGroup;errs:=make(chan error,len(byDisk))
 for _,batches:=range byDisk{
  batches:=batches;wg.Add(1)
  go func(){defer wg.Done();for _,b:=range batches{if a.ctx.Err()!=nil{return};if err:=a.downloadTask(b);err!=nil{errs<-err;return}}}()
 }
 wg.Wait();close(errs)
 for err:=range errs{if err!=nil{return err}}
 if waitingSpace{return errors.New("部分任务因磁盘空间不足未启动；请释放空间后重新运行")}
 return nil
}
func(a *App)downloadTask(b batch)error{
 sem:=make(chan struct{},a.cfg.Workers);var wg sync.WaitGroup;var mu sync.Mutex;var first error
 for _,ep:=range b.rows{if a.ctx.Err()!=nil{break};sem<-struct{}{};wg.Add(1);go func(e Episode){defer wg.Done();defer func(){<-sem}();if err:=a.downloadOne(b.t,b.d,e);err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock()}}(ep)}
 wg.Wait();return first
}
func episodePaths(version string,t Task,d DiskConfig,parentKey string)(string,string){
 root:=filepath.Join(d.Path,safe(version),safe(t.Scene),safe(taskDir(t)))
 leaf:=safe(path.Base(strings.Trim(parentKey,"/")))
 return filepath.Join(root,leaf),filepath.Join(root,"."+leaf+".partial")
}
func(a *App)downloadOne(t Task,d DiskConfig,e Episode)error{
 if a.ctx.Err()!=nil{return nil};if e.Size<=0{_=a.set(e,d,"","","invalid_size","file_size 无效");return nil}
 var existing *DiskConfig;var existingPath string
 for i:=range a.cfg.Disks{
  candidate:=&a.cfg.Disks[i];candidateFinal,_:=episodePaths(a.cfg.DataVersion,t,*candidate,e.ParentKey)
  if pathIsDir(candidateFinal){
   if existing!=nil{return fmt.Errorf("episode %s 在多个磁盘存在正式目录，违反单副本规则",e.ID)}
   existing=candidate;existingPath=candidateFinal
  }
 }
 if existing!=nil{
  pinned:=a.assigned(t.Name)
  if pinned!=""&&pinned!=existing.Name{return fmt.Errorf("任务 %s 固定在磁盘 %s，但 episode 已存在于 %s",t.Name,pinned,existing.Name)}
  if pinned==""{if err:=a.assign(t.Name,existing.Name);err!=nil{return err}}
  if err:=a.record(e,*existing,existingPath);err!=nil{return err}
  return a.set(e,*existing,existingPath,"","completed","")
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
  if obj.IsDir||strings.HasSuffix(obj.Key,"/"){continue}
  objects=append(objects,obj)
 }
 if err:=sweepPartial(partial,e.ParentKey,objects);err!=nil{return a.fail(e,d,final,partial,err)}
 sem:=make(chan struct{},a.cfg.FileWorkers);var wg sync.WaitGroup;var total atomic.Int64;var mu sync.Mutex;var first error
 for _,item:=range objects{
  item:=item
  if a.ctx.Err()!=nil{break}
  sem<-struct{}{};wg.Add(1)
  go func(){
   defer wg.Done();defer func(){<-sem}()
   rel:=strings.TrimPrefix(item.Key,e.ParentKey+"/")
   dst,err:=safeJoin(partial,rel)
   if err==nil{err=os.MkdirAll(filepath.Dir(dst),0755)}
   if err==nil{err=a.fetchObject(item.Key,dst,item)}
   if err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock();return}
   total.Add(item.Size)
  }()
 }
 wg.Wait()
 if first!=nil{return a.fail(e,d,final,partial,first)}
 if a.ctx.Err()!=nil{return a.fail(e,d,final,partial,a.ctx.Err())}
 if total.Load()==0{return a.fail(e,d,final,partial,errors.New("MinIO 前缀为空"))};if err:=os.MkdirAll(filepath.Dir(final),0755);err!=nil{return a.fail(e,d,final,partial,err)}
 if err:=os.RemoveAll(final);err!=nil{return a.fail(e,d,final,partial,err)};if err:=os.Rename(partial,final);err!=nil{return a.fail(e,d,final,partial,err)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(final))}
 if err:=a.record(e,d,final);err!=nil{return err};return a.set(e,d,final,"","completed","")
}
func sweepPartial(root,parent string,objects []minio.ObjectInfo)error{
 info,err:=os.Lstat(root);if err!=nil{return err};if info.Mode()&os.ModeSymlink!=0||!info.IsDir(){return fmt.Errorf("暂存路径不是普通目录: %s",root)}
 keep:=make(map[string]bool,len(objects)*2)
 for _,obj:=range objects{
  rel:=strings.TrimPrefix(obj.Key,parent+"/")
  dst,err:=safeJoin(root,rel);if err!=nil{return err}
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
func(a *App)fetchObject(key,dst string,expected minio.ObjectInfo)error{
 tmp:=dst+".minio-tmp"
 attempts:=a.cfg.FileRetries
 for attempt:=1;attempt<=attempts;attempt++{
  if a.ctx.Err()!=nil{return a.ctx.Err()}
  offset:=int64(0)
  if st,err:=os.Stat(tmp);err==nil{
   if st.Size()>expected.Size{_=os.Remove(tmp)}else{offset=st.Size()}
  }else if !os.IsNotExist(err){return err}
  if offset>=expected.Size&&offset>0{_=os.Remove(tmp);offset=0}
  if offset>0{
   current,statErr:=a.mc.StatObject(a.ctx,a.cfg.Bucket,key,minio.StatObjectOptions{})
   if statErr!=nil{if attempt<attempts{select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):};continue};return statErr}
   changed:=current.Size!=expected.Size||(expected.ETag!=""&&!sameETag(current.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!current.LastModified.Equal(expected.LastModified))
   if changed{_=os.Remove(tmp);return fmt.Errorf("续传前发现远端对象已变化，已丢弃旧临时文件: %s",key)}
  }
  opts:=minio.GetObjectOptions{}
  if expected.ETag!=""{if err:=opts.SetMatchETag(strings.Trim(expected.ETag,"\""));err!=nil{return err}}
  if offset>0{if err:=opts.SetRange(offset,0);err!=nil{return err}}
  obj,err:=a.mc.GetObject(a.ctx,a.cfg.Bucket,key,opts)
  if err==nil{
   var info minio.ObjectInfo
   info,err=obj.Stat()
   if err==nil&&(info.Size!=expected.Size||(expected.ETag!=""&&!sameETag(info.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!info.LastModified.Equal(expected.LastModified))){err=fmt.Errorf("对象元数据发生变化: %s",key)}
   if err==nil{
    flags:=os.O_CREATE|os.O_WRONLY
    if offset>0{flags|=os.O_APPEND}else{flags|=os.O_TRUNC}
    var out *os.File
    out,err=os.OpenFile(tmp,flags,0644)
    if err==nil{
     var n int64
     n,copyErr:=io.Copy(out,obj)
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
   if statErr!=nil{err=statErr}else if latest.Size!=expected.Size||(expected.ETag!=""&&!sameETag(latest.ETag,expected.ETag))||(!expected.LastModified.IsZero()&&!latest.LastModified.Equal(expected.LastModified)){err=fmt.Errorf("下载期间远端对象被修改: %s",key)}
  }
  if err==nil&&*a.cfg.VerifyMD5&&isMD5ETag(expected.ETag){
   var sum string
   sum,err=fileMD5(tmp)
   if err==nil&&!strings.EqualFold(sum,strings.Trim(expected.ETag,"\"")){err=fmt.Errorf("MD5 校验失败 %s: %s != %s",key,sum,expected.ETag)}
  }
  if err==nil{
   _=os.Remove(dst)
   if err=os.Rename(tmp,dst);err==nil{if !expected.LastModified.IsZero(){_=os.Chtimes(dst,expected.LastModified,expected.LastModified)};if *a.cfg.Fsync{syncDirectory(filepath.Dir(dst))}}
   if err==nil{return nil}
  }
  if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){return err}
  if err!=nil&&strings.Contains(err.Error(),"远端对象被修改"){_=os.Remove(tmp)}
  if attempt<attempts{
   log.Printf("对象下载重试 %d/%d: %s: %v",attempt,attempts,key,err)
   select{case <-a.ctx.Done():return a.ctx.Err();case <-time.After(time.Duration(float64(time.Second)*a.cfg.RetryDelaySeconds*float64(attempt))):}
  }
 }
 return fmt.Errorf("对象下载重试 %d 次仍失败: %s",attempts,key)
}
func sameETag(a,b string)bool{return strings.Trim(a,"\"")==strings.Trim(b,"\"")}
func isMD5ETag(etag string)bool{v:=strings.Trim(etag,"\"");if len(v)!=32||strings.Contains(v,"-"){return false};_,err:=hex.DecodeString(v);return err==nil}
func fileMD5(name string)(string,error){f,err:=os.Open(name);if err!=nil{return "",err};defer f.Close();h:=md5.New();if _,err=io.Copy(h,f);err!=nil{return "",err};return hex.EncodeToString(h.Sum(nil)),nil}
func(a *App)fail(e Episode,d DiskConfig,final,partial string,err error)error{
 if errors.Is(err,context.Canceled)||errors.Is(err,context.DeadlineExceeded){_=a.set(e,d,final,partial,"paused","已暂停，保留暂存文件供下次续传");return nil}
 _=a.set(e,d,final,partial,"failed",err.Error());log.Printf("下载失败 %s: %v",e.ID,err);return err
}
func(a *App)cleanup(preview bool)(int,error){
 rows,err:=a.db.Query("SELECT episode_id,target_path FROM downloads WHERE status='completed'");if err!=nil{return 0,err}
 type item struct{id string;path sql.NullString}
 items:=[]item{}
 for rows.Next(){var x item;if err=rows.Scan(&x.id,&x.path);err!=nil{rows.Close();return 0,err};items=append(items,x)}
 if err=rows.Err();err!=nil{rows.Close();return 0,err};if err=rows.Close();err!=nil{return 0,err}
 n:=0
 for _,x:=range items{if !x.path.Valid||!hasPattern(x.path.String,a.cfg.MCAPPattern){
  n++;if preview{log.Printf("[dry-run] 缺 mcap: %s",x.path.String)}else{
   if x.path.Valid{if err=os.RemoveAll(x.path.String);err!=nil{return n,err}}
   if _,err=a.db.Exec("DELETE FROM downloads WHERE episode_id=?",x.id);err!=nil{return n,err}
   if _,err=a.db.Exec("DELETE FROM downloaded_episodes WHERE episode_id=?",x.id);err!=nil{return n,err}
  }
 }}
 return n,nil
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
