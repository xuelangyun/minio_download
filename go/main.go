package main

import (
 "context"
 "crypto/tls"
 "crypto/hmac"
 "crypto/rand"
 "crypto/sha256"
 "encoding/hex"
 "bytes"
 "crypto/md5"
 "database/sql"
 "encoding/json"
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
type App struct { cfg Config; mc *minio.Client; db *sql.DB; ctx context.Context; cancel context.CancelFunc; mode string; recheck bool; validated map[string]bool }
//go:embed config.count.example.json
var defaultCountConfigJSON []byte

var configPath=flag.String("config","config.json","JSON 配置文件")
var mode=flag.String("mode","","duration 或 count；未指定时根据配置自动选择")
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
 b,e:=os.ReadFile(*configPath);if e!=nil{log.Fatalf("读取配置失败: %v；请复制 config.example.json 为 config.json",e)}
 var c Config;if e=json.Unmarshal(b,&c);e!=nil{log.Fatalf("配置 JSON 无效: %v",e)}
 if *stopFlag{if e=requestStop(c);e!=nil{log.Fatal(e)};log.Print("已写入安全暂停请求");return}
 if *mode==""{if c.ConfigDB!=""{*mode="count"}else if c.Mode!=""{*mode=c.Mode}else{*mode="duration"}}
 if *mode!="duration"&&*mode!="count"{log.Fatal("--mode 只能是 duration 或 count")}
 if e=loadConfigDB(&c,*mode);e!=nil{log.Fatalf("读取 SQLite 配置库失败: %v",e)}
 if e=c.normalize(*mode);e!=nil{log.Fatal(e)}
 effectiveRounds:=c.DefaultRounds;if *rounds>0{effectiveRounds=*rounds};if effectiveRounds<1||effectiveRounds>50{log.Fatal("--max-rounds / run.default_rounds 必须在 1 到 50 之间")}
 if *webFlag{host:=c.WebHost;port:=c.WebPort;if *webHost!=""{host=*webHost};if *webPort>0{port=*webPort};if e=serveWeb(c,host,port);e!=nil{log.Fatal(e)};return}
 _=os.MkdirAll(filepath.Dir(c.StateDB),0755);_=os.MkdirAll(filepath.Dir(c.LockFile),0755);_=os.MkdirAll(c.LogDir,0755)
 f,e:=os.OpenFile(filepath.Join(c.LogDir,"minio_task_downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if e!=nil{log.Fatal(e)};defer f.Close()
 log.SetOutput(io.MultiWriter(os.Stdout,f));log.SetFlags(log.Ldate|log.Ltime|log.Lmicroseconds)
 baseCtx,stop:=signal.NotifyContext(context.Background(),os.Interrupt);defer stop()
 ctx,cancel:=context.WithCancel(baseCtx);defer cancel()
 a:=&App{cfg:c,ctx:ctx,cancel:cancel,mode:*mode,recheck:*recheck,validated:map[string]bool{}}
 lock:=flock.New(c.LockFile);locked,lockErr:=lock.TryLock();if lockErr!=nil{log.Fatalf("创建下载锁失败: %v",lockErr)};if !locked{log.Fatal("检测到另一个下载器实例正在运行")};defer lock.Unlock()
 if !*preserveStopRequest{if e=clearStopRequestDB(c.StateDB);e!=nil{log.Fatal(e)}}
 if e=a.open();e!=nil{log.Fatal(e)};defer a.db.Close()
 if e=writePIDFile(c.PIDFile,*mode);e!=nil{log.Fatal(e)};defer removePIDFile(c.PIDFile)
 watcherDone:=a.startStopWatcher();defer func(){cancel();<-watcherDone}()
 if e=validateDiskSet(a.cfg.Disks);e!=nil{log.Fatal(e)}
 if *cleanup{n,err:=a.cleanup(*dry);if err!=nil{log.Fatal(err)};log.Printf("清理结束：%d 条",n);if *dry{return}}else if *dry{log.Fatal("--dry-run 必须与 --cleanup-missing-mcap 一起使用")}
 rows,e:=a.fetch();if e!=nil{if ctx.Err()!=nil{log.Print("已安全暂停（候选查询期间收到暂停请求）");a.saveLastRun("paused",nil);return};log.Printf("候选查询失败: %v",e);a.saveLastRun("error",e);return}
 if e=a.run(rows,effectiveRounds);e!=nil{if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil);return};log.Printf("下载执行失败: %v",e);a.saveLastRun("error",e);return}
 if ctx.Err()!=nil{log.Print("已安全暂停，临时文件已保留供下次续传");a.saveLastRun("paused",nil)}else{log.Printf("执行结束 Go=%s/%s",runtime.GOOS,runtime.GOARCH);a.saveLastRun("finished",nil)}
}

type WebAPI struct{cfg Config;db *sql.DB;tokenHash string;configFile string;mode string;childPID int32;childCreated float64;runMu sync.Mutex;authMu sync.Mutex;statusMu sync.Mutex;statusSampleAt time.Time;statusSampleBytes int64;statusRateBps float64;failures map[string][]time.Time;lockouts map[string]time.Time}
func serveWeb(c Config,host string,port int)error{
 if port<1||port>65535{return errors.New("web-port 必须在 1 到 65535 之间")}
 if err:=os.MkdirAll(filepath.Dir(c.StateDB),0755);err!=nil{return err}
 state,err:=sql.Open("sqlite",c.StateDB);if err!=nil{return err};state.SetMaxOpenConns(4);_,_=state.Exec("PRAGMA busy_timeout=30000");defer state.Close()
 configFile,_:=filepath.Abs(*configPath);api:=&WebAPI{cfg:c,db:state,configFile:configFile,mode:*mode}
 if c.ConfigDB!=""{
  db,e:=sql.Open("sqlite",c.ConfigDB);if e!=nil{return e};defer db.Close()
  if e=db.QueryRow("SELECT CASE WHEN value='null' THEN '' ELSE trim(value,char(34)) END FROM settings WHERE key='web.token_hash'").Scan(&api.tokenHash);e!=nil&&e!=sql.ErrNoRows{return e}
 }
 if !isLoopbackHost(host)&&api.tokenHash==""{return errors.New("监听非本机地址之前必须先在 Python Web 配置库设置访问口令")}
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){writeJSON(w,map[string]any{"ok":true,"version":"go-dev"})})
 mux.HandleFunc("GET /",func(w http.ResponseWriter,r *http.Request){w.Header().Set("Content-Type","text/html; charset=utf-8");_,_=io.WriteString(w,"<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>MinIO Go Downloader</title><body style='font:15px system-ui;max-width:1100px;margin:40px auto;padding:0 20px'><h2>MinIO Go Downloader API</h2><p>只读 API：<a href='/api/status'>状态</a> · <a href='/api/logs'>日志</a> · <a href='/healthz'>健康检查</a></p><pre id='out'>正在读取状态…</pre><script>async function tick(){try{let r=await fetch('/api/status');document.querySelector('#out').textContent=JSON.stringify(await r.json(),null,2)}catch(e){document.querySelector('#out').textContent=String(e)}}tick();setInterval(tick,5000)</script></body></html>")})
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
 mux.HandleFunc("PUT /api/config/disks",api.configDisks)
 mux.HandleFunc("POST /api/config/disks/check",api.configDisksCheck)
 mux.HandleFunc("GET /api/config",api.config)
 mux.HandleFunc("GET /api/status",api.status)
 mux.HandleFunc("POST /api/run",api.run)
 mux.HandleFunc("GET /api/episodes",api.episodes)
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
 if body.Action=="clear"{a.tokenHash=""}else{var hash string;if err=json.Unmarshal([]byte(encoded),&hash);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取口令摘要失败"});return};a.tokenHash=hash}
 a.authMu.Lock();a.failures=map[string][]time.Time{};a.lockouts=map[string]time.Time{};a.authMu.Unlock()
 writeJSON(w,map[string]any{"ok":true,"token_set":a.tokenHash!="","action":body.Action})
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
  if e==nil{for records.Next(){var sec sql.NullFloat64;var size sql.NullInt64;var target sql.NullString;if records.Scan(&sec,&size,&target)!=nil{continue};if target.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(target.String)){count++;seconds+=sec.Float64;bytesCount+=size.Int64}else if target.Valid{staleRecords++}};records.Close()}
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
 issues:=[]map[string]string{};if a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""{issues=append(issues,map[string]string{"page":"connection","text":"尚未配置 MinIO 连接"})};if len(a.cfg.Disks)==0{issues=append(issues,map[string]string{"page":"disks","text":"尚未配置目标磁盘"})};if len(a.cfg.Tasks)==0{issues=append(issues,map[string]string{"page":"tasks","text":"尚未配置下载任务"})}
 endpoint:=a.cfg.Endpoint;if u,e:=url.Parse(endpoint);e==nil&&u.Host!=""{endpoint=u.Scheme+"://"+u.Host}
 settings:=map[string]any{"min_free_gb":a.cfg.MinFreeGB,"writes_per_disk":a.cfg.Workers,"require_mcap":a.cfg.RequireMCAP,"mcap_pattern":a.cfg.MCAPPattern,"default_rounds":1}
 writeJSON(w,map[string]any{"version":"go-dev","data_version":a.cfg.DataVersion,"downloader":state,"stop_requested":running&&isStopRequested(control["stop_requested"]),"setup_issues":issues,"minio":map[string]any{"endpoint":endpoint,"bucket":a.cfg.Bucket},"settings":settings,"last_run":last,"stale_records":staleRecords,"running_episodes":runningEpisodes,"transfer_progress":map[string]any{"downloaded_bytes":runningBytes,"expected_bytes":expectedBytes,"remaining_bytes":maxInt64(expectedBytes-runningBytes,0),"speed_bytes_per_second":math.Round(speedBps),"eta_seconds":etaSeconds,"progress_percent":progressPercent},"disks":disks,"scenes":scenes})
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
  out.Scenes[i].Tasks=append(out.Scenes[i].Tasks,planTask{Name:t.Name,Count:t.TargetCount,Aliases:aliases,Dir:t.Dir,Tags:tags})
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
   res,e=tx.Exec("INSERT INTO tasks(scene_id,position,name,target_count,directory,tags) VALUES(?,?,?,?,?,?)",sceneID,ti,t.Name,t.Count,t.Dir,string(tags));if e!=nil{return e};taskID,e:=res.LastInsertId();if e!=nil{return e}
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
 var body struct{Mode string `json:"mode"`;Tags []string `json:"tags"`}
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
   if existing[task.Name]{skipped=append(skipped,map[string]string{"name":task.Name,"reason":"当前配置里已有同名任务"});continue}
   candidate:=planPayload{DataVersion:result.DataVersion,Scenes:append([]planScene{},result.Scenes...)}
   target:=-1;for i:=range candidate.Scenes{if candidate.Scenes[i].Scene==scene.Scene{target=i;break}}
   if target<0{candidate.Scenes=append(candidate.Scenes,planScene{Scene:scene.Scene,Tasks:[]planTask{}});target=len(candidate.Scenes)-1}
   candidate.Scenes[target].Tasks=append(candidate.Scenes[target].Tasks,task)
   if _,e:=validatePlanPayload(mustJSON(candidate));e!=nil{skipped=append(skipped,map[string]string{"name":task.Name,"reason":e.Error()});continue}
   result=candidate;existing[task.Name]=true;added=append(added,task.Name)
  }}
 }else{for _,scene:=range incoming.Scenes{for _,task:=range scene.Tasks{added=append(added,task.Name)}}}
 if err=validatePlanPayload(mustJSON(result));err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if body.Mode=="replace"||len(added)>0{
  if err=persistPlanPayload(db,result);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"应用方案失败："+err.Error()});return}
  tasks:=[]Task{};for _,scene:=range result.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}};a.cfg.Tasks=tasks;a.cfg.DataVersion=result.DataVersion
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
 for _,scene:=range payload.Scenes{for _,task:=range scene.Tasks{a.cfg.Tasks=append(a.cfg.Tasks,Task{Scene:scene.Scene,Name:task.Name,TargetCount:task.Count,Aliases:append([]string{},task.Aliases...),Dir:task.Dir,Tags:append([]string{},task.Tags...)})}}
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
 for _,scene:=range p.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}}
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
  items=append(items,map[string]any{"name":task.Name,"count":task.TargetCount,"aliases":aliases,"dir":task.Dir,"tags":tags})
  scenes[i]["tasks"]=items
 }
 _,running,_:=a.activeDownloader()
 platform:="posix";if runtime.GOOS=="windows"{platform="windows"}
 writeJSON(w,map[string]any{
  "schema":schema,"groups":groups,"values":values,"secrets_set":secretsSet,
  "tasks":map[string]any{"data_version":a.cfg.DataVersion,"scenes":scenes},"disks":a.cfg.Disks,
  "token_set":a.tokenHash!="","paths":map[string]any{"config_db":a.cfg.ConfigDB,"state_db":a.cfg.StateDB,"log_dir":a.cfg.LogDir},
  "platform":platform,"running":running,
 })
}
func(a *WebAPI)activeDownloader()(int,bool,bool){
 if a.childPID>0{if proc,err:=process.NewProcess(a.childPID);err==nil{alive,_:=proc.IsRunning();if alive{created,err:=proc.CreateTime();if err!=nil||a.childCreated==0||math.Abs(float64(created)/1000-a.childCreated)<2{return int(a.childPID),true,true}}}}
 raw,err:=os.ReadFile(a.cfg.PIDFile);if err!=nil{return 0,false,false}
 var record pidRecord;if json.Unmarshal(raw,&record)!=nil||record.PID<=0{return 0,false,false}
 proc,err:=process.NewProcess(int32(record.PID));if err!=nil{return 0,false,false};alive,err:=proc.IsRunning();if err!=nil||!alive{return 0,false,false}
 created,err:=proc.CreateTime();if err==nil&&record.Created>0&&math.Abs(float64(created)/1000-record.Created)>=2{return 0,false,false}
 return record.PID,true,false
}
func(a *WebAPI)run(w http.ResponseWriter,r *http.Request){
 var body struct{Mode string `json:"mode"`;MaxRounds int `json:"max_rounds"`;RecheckMissing bool `json:"recheck_missing"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,4096)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Mode==""{body.Mode="download"};if body.Mode!="download"{writeJSONStatus(w,400,map[string]any{"error":"当前 Go Web API 只支持 download 模式"});return}
 if body.MaxRounds==0{body.MaxRounds=a.cfg.DefaultRounds;if body.MaxRounds<=0{body.MaxRounds=1}};if body.MaxRounds<1||body.MaxRounds>50{writeJSONStatus(w,400,map[string]any{"error":"补齐轮数范围 1–50"});return}
 if len(a.cfg.Tasks)==0||len(a.cfg.Disks)==0||a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""||a.cfg.PGHost==""{writeJSONStatus(w,400,map[string]any{"error":"配置未补全：需要 MinIO、PostgreSQL、任务和磁盘配置"});return}
 a.runMu.Lock();defer a.runMu.Unlock()
 if pid,running,_:=a.activeDownloader();running{writeJSONStatus(w,409,map[string]any{"error":fmt.Sprintf("下载器已在运行（PID %d）",pid)});return}
 if err:=os.MkdirAll(a.cfg.LogDir,0755);err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return}
 logFile,err:=os.OpenFile(filepath.Join(a.cfg.LogDir,"minio_task_downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"打开日志失败: "+err.Error()});return}
 executable,err:=os.Executable();if err!=nil{logFile.Close();writeJSONStatus(w,500,map[string]any{"error":"定位下载器程序失败: "+err.Error()});return}
 if err:=clearStopRequestDB(a.cfg.StateDB);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"清理旧暂停请求失败: "+err.Error()});return}
 args:=[]string{"--config",a.configFile,"--mode",a.mode,"--max-rounds",strconv.Itoa(body.MaxRounds),"--preserve-stop-request"};if body.RecheckMissing{args=append(args,"--recheck-missing")}
 cmd:=exec.Command(executable,args...);configureDetached(cmd);cmd.Stdout=logFile;cmd.Stderr=logFile;cmd.Stdin=nil
 if err=cmd.Start();err!=nil{logFile.Close();writeJSONStatus(w,500,map[string]any{"error":"启动下载器失败: "+err.Error()});return}
 _=logFile.Close();go func(){_=cmd.Wait()}();a.childPID=int32(cmd.Process.Pid);if proc,e:=process.NewProcess(a.childPID);e==nil{if ms,e:=proc.CreateTime();e==nil{a.childCreated=float64(ms)/1000}}
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
 getInt("transfer.file_workers",&c.FileWorkers);getInt("transfer.file_retries",&c.FileRetries);getFloat("transfer.retry_delay",&c.RetryDelaySeconds);getInt("run.default_rounds",&c.DefaultRounds);getInt("transfer.connect_timeout",&c.ConnectTimeout);getInt("transfer.read_timeout",&c.ReadTimeout);getString("web.host",&c.WebHost);getInt("web.port",&c.WebPort)
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
func diskLockKeys(disks []DiskConfig) ([]string,error) {
 parts,err:=disk.Partitions(false);if err!=nil{return nil,fmt.Errorf("读取磁盘挂载信息失败，无法建立跨进程磁盘锁: %w",err)}
 keys:=map[string]bool{}
 for _,d:=range disks {
  target:=filepath.Clean(d.Path);bestLen:=-1;key:=""
  for _,p:=range parts {
   mount:=filepath.Clean(p.Mountpoint);rel,e:=filepath.Rel(mount,target)
   if e!=nil||rel==".."||strings.HasPrefix(rel,".."+string(os.PathSeparator)){continue}
   if runtime.GOOS=="windows"&&!strings.EqualFold(mount,target)&&!strings.HasPrefix(strings.ToLower(target),strings.ToLower(mount+string(os.PathSeparator))){continue}
   if len(mount)>bestLen {bestLen=len(mount);key=p.Device}
  }
  if key=="" {resolved,e:=filepath.Abs(target);if e!=nil{return nil,e};key=resolved}
  sum:=sha256.Sum256([]byte(strings.ToLower(key)))
  keys[hex.EncodeToString(sum[:])]=true
 }
 out:=make([]string,0,len(keys));for k:=range keys{out=append(out,k)};sort.Strings(out);return out,nil
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
 keys,lockErr:=diskLockKeys(a.cfg.Disks);if lockErr!=nil{return lockErr}
 unlockDiskLocks,lockErr:=acquireNamedDiskLocks(keys,filepath.Join(os.TempDir(),"minio-download-locks"));if lockErr!=nil{return lockErr}
 defer unlockDiskLocks()
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
func acquireSemaphore(ctx context.Context,sem chan struct{})bool{
 select{case sem<-struct{}{}:return true;case <-ctx.Done():return false}
}
func(a *App)downloadTask(b batch)error{
 sem:=make(chan struct{},a.cfg.Workers);var wg sync.WaitGroup;var mu sync.Mutex;var first error
 for _,ep:=range b.rows{if !acquireSemaphore(a.ctx,sem){break};wg.Add(1);go func(e Episode){defer wg.Done();defer func(){<-sem}();if err:=a.downloadOne(b.t,b.d,e);err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock()}}(ep)}
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
  if !acquireSemaphore(a.ctx,sem){break}
  wg.Add(1)
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
),'') FROM settings WHERE key='web.token_hash'").Scan(&api.tokenHash);e!=nil&&e!=sql.ErrNoRows{return e}
 }
 if !isLoopbackHost(host)&&api.tokenHash==""{return errors.New("监听非本机地址之前必须先在 Python Web 配置库设置访问口令")}
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){writeJSON(w,map[string]any{"ok":true,"version":"go-dev"})})
 mux.HandleFunc("GET /",func(w http.ResponseWriter,r *http.Request){w.Header().Set("Content-Type","text/html; charset=utf-8");_,_=io.WriteString(w,"<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>MinIO Go Downloader</title><body style='font:15px system-ui;max-width:1100px;margin:40px auto;padding:0 20px'><h2>MinIO Go Downloader API</h2><p>只读 API：<a href='/api/status'>状态</a> · <a href='/api/logs'>日志</a> · <a href='/healthz'>健康检查</a></p><pre id='out'>正在读取状态…</pre><script>async function tick(){try{let r=await fetch('/api/status');document.querySelector('#out').textContent=JSON.stringify(await r.json(),null,2)}catch(e){document.querySelector('#out').textContent=String(e)}}tick();setInterval(tick,5000)</script></body></html>")})
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
 mux.HandleFunc("PUT /api/config/disks",api.configDisks)
 mux.HandleFunc("POST /api/config/disks/check",api.configDisksCheck)
 mux.HandleFunc("GET /api/config",api.config)
 mux.HandleFunc("GET /api/status",api.status)
 mux.HandleFunc("POST /api/run",api.run)
 mux.HandleFunc("GET /api/episodes",api.episodes)
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
 if body.Action=="clear"{a.tokenHash=""}else{var hash string;if err=json.Unmarshal([]byte(encoded),&hash);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"读取口令摘要失败"});return};a.tokenHash=hash}
 a.authMu.Lock();a.failures=map[string][]time.Time{};a.lockouts=map[string]time.Time{};a.authMu.Unlock()
 writeJSON(w,map[string]any{"ok":true,"token_set":a.tokenHash!="","action":body.Action})
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
func(a *WebAPI)status(w http.ResponseWriter,r *http.Request){
 scenes:=[]map[string]any{};sceneIndex:=map[string]int{};taskDisk:=map[string]string{};staleRecords:=0
 rowsAssign,err:=a.db.Query("SELECT task_name,disk_name FROM task_disk_assignments");if err==nil{for rowsAssign.Next(){var task,diskName string;if rowsAssign.Scan(&task,&diskName)==nil{taskDisk[task]=diskName}};rowsAssign.Close()}
 for _,t:=range a.cfg.Tasks{
  idx,ok:=sceneIndex[t.Scene];if !ok{idx=len(scenes);sceneIndex[t.Scene]=idx;scenes=append(scenes,map[string]any{"scene":t.Scene,"tasks":[]map[string]any{}})}
  rows:=scenes[idx]["tasks"].([]map[string]any);count:=0;seconds:=0.0;bytesCount:=int64(0);issues:=map[string]int{}
  records,e:=a.db.Query("SELECT content_time,file_size,target_path FROM downloaded_episodes WHERE task_name=?",t.Name)
  if e==nil{for records.Next(){var sec sql.NullFloat64;var size sql.NullInt64;var target sql.NullString;if records.Scan(&sec,&size,&target)!=nil{continue};if target.Valid&&(a.cfg.TrustCompletedRecords||pathIsDir(target.String)){count++;seconds+=sec.Float64;bytesCount+=size.Int64}else if target.Valid{staleRecords++}};records.Close()}
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
 runningEpisodes:=[]map[string]any{}
 if running{rr,e:=a.db.Query("SELECT episode_id,task_name,disk_name,started_at,expected_bytes,partial_path FROM downloads WHERE status='running' ORDER BY updated_at DESC LIMIT 24");if e==nil{for rr.Next(){var id,taskName string;var diskName,started,partial sql.NullString;var expected sql.NullInt64;if rr.Scan(&id,&taskName,&diskName,&started,&expected,&partial)==nil{partialBytes:=int64(0);if partial.Valid{partialBytes,_=directorySizeBytes(partial.String)};runningEpisodes=append(runningEpisodes,map[string]any{"episode_id":id,"task_name":taskName,"disk_name":diskName.String,"started_at":started.String,"expected_bytes":expected.Int64,"partial_bytes":partialBytes})}};rr.Close()}}
 issues:=[]map[string]string{};if a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""{issues=append(issues,map[string]string{"page":"connection","text":"尚未配置 MinIO 连接"})};if len(a.cfg.Disks)==0{issues=append(issues,map[string]string{"page":"disks","text":"尚未配置目标磁盘"})};if len(a.cfg.Tasks)==0{issues=append(issues,map[string]string{"page":"tasks","text":"尚未配置下载任务"})}
 endpoint:=a.cfg.Endpoint;if u,e:=url.Parse(endpoint);e==nil&&u.Host!=""{endpoint=u.Scheme+"://"+u.Host}
 settings:=map[string]any{"min_free_gb":a.cfg.MinFreeGB,"writes_per_disk":a.cfg.Workers,"require_mcap":a.cfg.RequireMCAP,"mcap_pattern":a.cfg.MCAPPattern,"default_rounds":1}
 writeJSON(w,map[string]any{"version":"go-dev","data_version":a.cfg.DataVersion,"downloader":state,"stop_requested":running&&isStopRequested(control["stop_requested"]),"setup_issues":issues,"minio":map[string]any{"endpoint":endpoint,"bucket":a.cfg.Bucket},"settings":settings,"last_run":last,"stale_records":staleRecords,"running_episodes":runningEpisodes,"disks":disks,"scenes":scenes})
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
  out.Scenes[i].Tasks=append(out.Scenes[i].Tasks,planTask{Name:t.Name,Count:t.TargetCount,Aliases:aliases,Dir:t.Dir,Tags:tags})
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
   res,e=tx.Exec("INSERT INTO tasks(scene_id,position,name,target_count,directory,tags) VALUES(?,?,?,?,?,?)",sceneID,ti,t.Name,t.Count,t.Dir,string(tags));if e!=nil{return e};taskID,e:=res.LastInsertId();if e!=nil{return e}
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
 var body struct{Mode string `json:"mode"`;Tags []string `json:"tags"`}
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
   if existing[task.Name]{skipped=append(skipped,map[string]string{"name":task.Name,"reason":"当前配置里已有同名任务"});continue}
   candidate:=planPayload{DataVersion:result.DataVersion,Scenes:append([]planScene{},result.Scenes...)}
   target:=-1;for i:=range candidate.Scenes{if candidate.Scenes[i].Scene==scene.Scene{target=i;break}}
   if target<0{candidate.Scenes=append(candidate.Scenes,planScene{Scene:scene.Scene,Tasks:[]planTask{}});target=len(candidate.Scenes)-1}
   candidate.Scenes[target].Tasks=append(candidate.Scenes[target].Tasks,task)
   if _,e:=validatePlanPayload(mustJSON(candidate));e!=nil{skipped=append(skipped,map[string]string{"name":task.Name,"reason":e.Error()});continue}
   result=candidate;existing[task.Name]=true;added=append(added,task.Name)
  }}
 }else{for _,scene:=range incoming.Scenes{for _,task:=range scene.Tasks{added=append(added,task.Name)}}}
 if err=validatePlanPayload(mustJSON(result));err!=nil{writeJSONStatus(w,400,map[string]any{"error":err.Error()});return}
 if body.Mode=="replace"||len(added)>0{
  if err=persistPlanPayload(db,result);err!=nil{writeJSONStatus(w,400,map[string]any{"error":"应用方案失败："+err.Error()});return}
  tasks:=[]Task{};for _,scene:=range result.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}};a.cfg.Tasks=tasks;a.cfg.DataVersion=result.DataVersion
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
 for _,scene:=range payload.Scenes{for _,task:=range scene.Tasks{a.cfg.Tasks=append(a.cfg.Tasks,Task{Scene:scene.Scene,Name:task.Name,TargetCount:task.Count,Aliases:append([]string{},task.Aliases...),Dir:task.Dir,Tags:append([]string{},task.Tags...)})}}
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
 for _,scene:=range p.Scenes{for _,t:=range scene.Tasks{tasks=append(tasks,Task{Scene:scene.Scene,Name:t.Name,TargetCount:t.Count,Aliases:append([]string{},t.Aliases...),Dir:t.Dir,Tags:append([]string{},t.Tags...)})}}
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
  items=append(items,map[string]any{"name":task.Name,"count":task.TargetCount,"aliases":aliases,"dir":task.Dir,"tags":tags})
  scenes[i]["tasks"]=items
 }
 _,running,_:=a.activeDownloader()
 platform:="posix";if runtime.GOOS=="windows"{platform="windows"}
 writeJSON(w,map[string]any{
  "schema":schema,"groups":groups,"values":values,"secrets_set":secretsSet,
  "tasks":map[string]any{"data_version":a.cfg.DataVersion,"scenes":scenes},"disks":a.cfg.Disks,
  "token_set":a.tokenHash!="","paths":map[string]any{"config_db":a.cfg.ConfigDB,"state_db":a.cfg.StateDB,"log_dir":a.cfg.LogDir},
  "platform":platform,"running":running,
 })
}
func(a *WebAPI)activeDownloader()(int,bool,bool){
 if a.childPID>0{if proc,err:=process.NewProcess(a.childPID);err==nil{alive,_:=proc.IsRunning();if alive{created,err:=proc.CreateTime();if err!=nil||a.childCreated==0||math.Abs(float64(created)/1000-a.childCreated)<2{return int(a.childPID),true,true}}}}
 raw,err:=os.ReadFile(a.cfg.PIDFile);if err!=nil{return 0,false,false}
 var record pidRecord;if json.Unmarshal(raw,&record)!=nil||record.PID<=0{return 0,false,false}
 proc,err:=process.NewProcess(int32(record.PID));if err!=nil{return 0,false,false};alive,err:=proc.IsRunning();if err!=nil||!alive{return 0,false,false}
 created,err:=proc.CreateTime();if err==nil&&record.Created>0&&math.Abs(float64(created)/1000-record.Created)>=2{return 0,false,false}
 return record.PID,true,false
}
func(a *WebAPI)run(w http.ResponseWriter,r *http.Request){
 var body struct{Mode string `json:"mode"`;MaxRounds int `json:"max_rounds"`;RecheckMissing bool `json:"recheck_missing"`}
 if err:=json.NewDecoder(io.LimitReader(r.Body,4096)).Decode(&body);err!=nil&&err!=io.EOF{writeJSONStatus(w,400,map[string]any{"error":"请求 JSON 无效"});return}
 if body.Mode==""{body.Mode="download"};if body.Mode!="download"{writeJSONStatus(w,400,map[string]any{"error":"当前 Go Web API 只支持 download 模式"});return}
 if body.MaxRounds==0{body.MaxRounds=a.cfg.DefaultRounds;if body.MaxRounds<=0{body.MaxRounds=1}};if body.MaxRounds<1||body.MaxRounds>50{writeJSONStatus(w,400,map[string]any{"error":"补齐轮数范围 1–50"});return}
 if len(a.cfg.Tasks)==0||len(a.cfg.Disks)==0||a.cfg.Endpoint==""||a.cfg.AccessKey==""||a.cfg.SecretKey==""||a.cfg.PGHost==""{writeJSONStatus(w,400,map[string]any{"error":"配置未补全：需要 MinIO、PostgreSQL、任务和磁盘配置"});return}
 a.runMu.Lock();defer a.runMu.Unlock()
 if pid,running,_:=a.activeDownloader();running{writeJSONStatus(w,409,map[string]any{"error":fmt.Sprintf("下载器已在运行（PID %d）",pid)});return}
 if err:=os.MkdirAll(a.cfg.LogDir,0755);err!=nil{writeJSONStatus(w,500,map[string]any{"error":err.Error()});return}
 logFile,err:=os.OpenFile(filepath.Join(a.cfg.LogDir,"minio_task_downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if err!=nil{writeJSONStatus(w,500,map[string]any{"error":"打开日志失败: "+err.Error()});return}
 executable,err:=os.Executable();if err!=nil{logFile.Close();writeJSONStatus(w,500,map[string]any{"error":"定位下载器程序失败: "+err.Error()});return}
 if err:=clearStopRequestDB(a.cfg.StateDB);err!=nil{writeJSONStatus(w,500,map[string]any{"error":"清理旧暂停请求失败: "+err.Error()});return}
 args:=[]string{"--config",a.configFile,"--mode",a.mode,"--max-rounds",strconv.Itoa(body.MaxRounds),"--preserve-stop-request"};if body.RecheckMissing{args=append(args,"--recheck-missing")}
 cmd:=exec.Command(executable,args...);configureDetached(cmd);cmd.Stdout=logFile;cmd.Stderr=logFile;cmd.Stdin=nil
 if err=cmd.Start();err!=nil{logFile.Close();writeJSONStatus(w,500,map[string]any{"error":"启动下载器失败: "+err.Error()});return}
 _=logFile.Close();go func(){_=cmd.Wait()}();a.childPID=int32(cmd.Process.Pid);if proc,e:=process.NewProcess(a.childPID);e==nil{if ms,e:=proc.CreateTime();e==nil{a.childCreated=float64(ms)/1000}}
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
 getInt("transfer.file_workers",&c.FileWorkers);getInt("transfer.file_retries",&c.FileRetries);getFloat("transfer.retry_delay",&c.RetryDelaySeconds);getInt("run.default_rounds",&c.DefaultRounds);getInt("transfer.connect_timeout",&c.ConnectTimeout);getInt("transfer.read_timeout",&c.ReadTimeout);getString("web.host",&c.WebHost);getInt("web.port",&c.WebPort)
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
