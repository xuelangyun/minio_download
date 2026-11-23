package main

import (
 "context"
 "crypto/md5"
 "encoding/hex"
 "database/sql"
 "encoding/json"
 "errors"
 "flag"
 "fmt"
 "io"
 "log"
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
 _ "modernc.org/sqlite"
)

type Task struct { Name string `json:"name"`; Scene string `json:"scene"`; Dir string `json:"dir"`; Aliases []string `json:"aliases"`; TargetHours float64 `json:"target_hours"`; TargetCount int `json:"target_count"` }
type DiskConfig struct { Name string `json:"name"`; Path string `json:"path"`; Mountpoint string `json:"mountpoint"` }
type Config struct {
 Mode string `json:"mode"`
 Endpoint string `json:"minio_endpoint"`; AccessKey string `json:"minio_access_key"`; SecretKey string `json:"minio_secret_key"`; Bucket string `json:"minio_bucket"`; Region string `json:"minio_region"`; SSL bool `json:"minio_use_ssl"`
 PGHost string `json:"pg_host"`; PGPort int `json:"pg_port"`; PGDB string `json:"pg_database"`; PGUser string `json:"pg_user"`; PGPassword string `json:"pg_password"`
 DataVersion string `json:"data_version"`; LockFile string `json:"lock_file"`; StateDB string `json:"state_db"`; LogDir string `json:"log_dir"`; MinFreeGB float64 `json:"min_free_gb"`; SafetyFactor float64 `json:"space_safety_factor"`
 Workers int `json:"max_writes_per_disk"`; FileWorkers int `json:"file_workers"`; FileRetries int `json:"file_retries"`; RetryDelaySeconds float64 `json:"retry_delay_seconds"`; ValidateWorkers int `json:"validate_workers"`; RequireMCAP bool `json:"require_mcap"`; MCAPPattern string `json:"mcap_pattern"`; IncludeNull bool `json:"include_null_quality"`; VerifyMD5 *bool `json:"verify_md5"`; TrustCompletedRecords bool `json:"trust_completed_records"`
 Disks []DiskConfig `json:"disks"`; Tasks []Task `json:"tasks"`
}
type Episode struct { ID string; JobID sql.NullInt64; Task,SourceTask,SN,ParentKey string; Size int64; Duration float64; Created sql.NullTime }
type App struct { cfg Config; mc *minio.Client; db *sql.DB; ctx context.Context; mode string; recheck bool; validated map[string]bool }
var configPath=flag.String("config","config.json","JSON 配置文件")
var mode=flag.String("mode","duration","duration 或 count")
var rounds=flag.Int("max-rounds",1,"缺少必需文件后的补选轮数")
var recheck=flag.Bool("recheck-missing",false,"重新检查缺文件数据")
var cleanup=flag.Bool("cleanup-missing-mcap",false,"清理缺少 mcap 的已下载目录")
var dry=flag.Bool("dry-run",false,"只预览清理操作")

func main(){
 flag.Parse()
 if *mode!="duration"&&*mode!="count"{log.Fatal("--mode 只能是 duration 或 count")}
 if *rounds<1{log.Fatal("--max-rounds 必须 >= 1")}
 b,e:=os.ReadFile(*configPath);if e!=nil{log.Fatalf("读取配置失败: %v；请复制 config.example.json 为 config.json",e)}
 var c Config;if e=json.Unmarshal(b,&c);e!=nil{log.Fatalf("配置 JSON 无效: %v",e)}
 if e=c.normalize(*mode);e!=nil{log.Fatal(e)}
 _=os.MkdirAll(filepath.Dir(c.StateDB),0755);_=os.MkdirAll(filepath.Dir(c.LockFile),0755);_=os.MkdirAll(c.LogDir,0755)
 f,e:=os.OpenFile(filepath.Join(c.LogDir,"go-downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if e!=nil{log.Fatal(e)};defer f.Close()
 log.SetOutput(io.MultiWriter(os.Stdout,f));log.SetFlags(log.Ldate|log.Ltime|log.Lmicroseconds)
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt);defer stop()
 a:=&App{cfg:c,ctx:ctx,mode:*mode,recheck:*recheck,validated:map[string]bool{}}
 lock:=flock.New(c.LockFile);locked,lockErr:=lock.TryLock();if lockErr!=nil{log.Fatalf("创建下载锁失败: %v",lockErr)};if !locked{log.Fatal("检测到另一个下载器实例正在运行")};defer lock.Unlock()
 if e=a.open();e!=nil{log.Fatal(e)};defer a.db.Close()
 if *cleanup{n,err:=a.cleanup(*dry);if err!=nil{log.Fatal(err)};log.Printf("清理结束：%d 条",n);if *dry{return}}else if *dry{log.Fatal("--dry-run 必须与 --cleanup-missing-mcap 一起使用")}
 rows,e:=a.fetch();if e!=nil{log.Fatal(e)}
 if e=a.run(rows,*rounds);e!=nil{log.Fatal(e)}
 log.Printf("执行结束 Go=%s/%s",runtime.GOOS,runtime.GOARCH)
}
func(c *Config)normalize(m string)error{
 if c.Mode!=""&&c.Mode!=m{return fmt.Errorf("配置文件 mode=%s 与命令行 --mode=%s 不一致",c.Mode,m)}
 if c.Endpoint==""||c.AccessKey==""||c.SecretKey==""||c.Bucket==""{return errors.New("MinIO endpoint/access_key/secret_key/bucket 必须填写")}
 if c.PGHost==""||c.PGDB==""||c.PGUser==""||c.PGPassword==""{return errors.New("PostgreSQL 连接参数必须填写")}
 if c.PGPort==0{c.PGPort=5432};if c.DataVersion==""{c.DataVersion="银河2.2数据"};if c.LockFile==""{c.LockFile="minio_download.lock"};if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"};if c.LogDir==""{c.LogDir="logs"}
 if c.MinFreeGB==0{c.MinFreeGB=3};if c.SafetyFactor==0{c.SafetyFactor=1.15};if c.Workers<=0{c.Workers=2};if c.FileWorkers<=0{c.FileWorkers=4};if c.FileRetries<=0{c.FileRetries=5};if c.RetryDelaySeconds<=0{c.RetryDelaySeconds=1};if c.ValidateWorkers<=0{c.ValidateWorkers=8};if c.Workers>16||c.FileWorkers>16||c.ValidateWorkers>32||c.FileRetries>20{return errors.New("并发数不能超过 16，文件重试次数不能超过 20")};if c.MCAPPattern==""{c.MCAPPattern="*.SYNC.mcap"};if c.VerifyMD5==nil{v:=true;c.VerifyMD5=&v}
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
func(a *App)open()error{
 endpoint:=strings.TrimPrefix(strings.TrimPrefix(a.cfg.Endpoint,"https://"),"http://");secure:=a.cfg.SSL||strings.HasPrefix(a.cfg.Endpoint,"https://")
 mc,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(a.cfg.AccessKey,a.cfg.SecretKey,""),Secure:secure,Region:a.cfg.Region});if e!=nil{return e};a.mc=mc
 pg:=fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=prefer connect_timeout=10",a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.PGUser,a.cfg.PGPassword)
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
func(a *App)completed(id string)bool{var p sql.NullString;err:=a.db.QueryRow("SELECT target_path FROM downloads WHERE episode_id=? AND status='completed'",id).Scan(&p);if err!=nil{return false};if a.cfg.TrustCompletedRecords{return true};return p.Valid&&dirHasFiles(p.String)}
func(a *App)stats(task string)(float64,int){rows,err:=a.db.Query("SELECT content_time,target_path FROM downloaded_episodes WHERE task_name=?",task);if err!=nil{return 0,0};defer rows.Close();var seconds float64;count:=0;for rows.Next(){var v sql.NullFloat64;var p sql.NullString;if rows.Scan(&v,&p)!=nil{continue};if a.cfg.TrustCompletedRecords||(p.Valid&&dirHasFiles(p.String)){seconds+=v.Float64;count++}};return seconds,count}
func(a *App)assign(task,disk string)error{tx,err:=a.db.Begin();if err!=nil{return err};defer tx.Rollback();if _,err=tx.Exec("DELETE FROM task_disk_assignments WHERE task_name=?",task);err!=nil{return err};if _,err=tx.Exec("INSERT INTO task_disk_assignments(task_name,disk_name,last_used_at) VALUES(?,?,?)",task,disk,time.Now().UTC().Format(time.RFC3339Nano));err!=nil{return err};return tx.Commit()}
func(a *App)assigned(task string)string{var d string;_=a.db.QueryRow("SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC LIMIT 1",task).Scan(&d);return d}
func(a *App)record(e Episode,d DiskConfig,final string)error{
 _,err:=a.db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,source_task_name,job_id,sn_code,parent_key,disk_name,target_path,file_size,content_time,create_time,downloaded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,source_task_name=excluded.source_task_name,job_id=excluded.job_id,sn_code=excluded.sn_code,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,file_size=excluded.file_size,content_time=excluded.content_time,create_time=excluded.create_time,downloaded_at=excluded.downloaded_at",
 e.ID,e.Task,e.SourceTask,e.JobID,e.SN,e.ParentKey,d.Name,final,e.Size,e.Duration,e.Created,time.Now().UTC().Format(time.RFC3339Nano));return err
}
func(a *App)fetch()([]Episode,error){
 names:=[]string{};aliases:=map[string]string{};for _,t:=range a.cfg.Tasks{names=append(names,t.Name);for _,x:=range t.Aliases{names=append(names,x);aliases[x]=t.Name}}
 pg:=fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=prefer connect_timeout=10",a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.PGUser,a.cfg.PGPassword)
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
  candidates:=groups[task.Name];if len(candidates)==0{continue}
  selected:=make([]Episode,0)
  todo:=make([]Episode,0,len(candidates))
  for _,ep:=range candidates{
   if a.completed(ep.ID){selected=append(selected,ep);continue}
   if ep.Size<=0{d:=a.cfg.Disks[0];if err:=a.set(ep,d,"","","invalid_size","file_size 无效");err!=nil{return nil,err};skipped++;continue}
   var state string
   _=a.db.QueryRow("SELECT status FROM downloads WHERE episode_id=?",ep.ID).Scan(&state)
   if !a.recheck&&(state=="missing_task_info"||state=="missing_mcap"){continue}
   todo=append(todo,ep)
  }
  _,downloaded:=a.stats(task.Name);gap:=task.TargetCount-downloaded
  if gap<=0{validGroups[task.Name]=selected;continue}
  chosenCount:=0;skipped:=0
  for pos:=0;pos<len(todo)&&chosenCount<gap;{
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
 if a.mode=="count"{var err error;groups,err=a.prevalidateCount(groups);if err!=nil{return err}}
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
   if dirHasFiles(final){
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
func(a *App)downloadSelected(sel map[string][]Episode)error{
 if err:=validateDiskSet(a.cfg.Disks);err!=nil{return err}
 loads:=map[string]int{};for _,t:=range a.cfg.Tasks{if a.assigned(t.Name)!=""{loads[a.assigned(t.Name)]++}}
 byDisk:=map[string][]batch{};waitingSpace:=false
 for _,t:=range a.cfg.Tasks{rs:=sel[t.Name];if len(rs)==0{continue};var d *DiskConfig;old:=a.assigned(t.Name)
  for i:=range a.cfg.Disks{if old!=""&&a.cfg.Disks[i].Name==old{d=&a.cfg.Disks[i];break}}
  if d==nil{
   existing,err:=findExistingTaskDisk(a.cfg.DataVersion,t,a.cfg.Disks,rs);if err!=nil{return err}
   if existing!=""{for i:=range a.cfg.Disks{if a.cfg.Disks[i].Name==existing{d=&a.cfg.Disks[i];break}}}
  }
  if d==nil{for i:=range a.cfg.Disks{x:=&a.cfg.Disks[i];if d==nil||loads[x.Name]<loads[d.Name]{d=x}}};if d==nil{return errors.New("没有配置目标磁盘")}
  if err:=validateDisk(*d);err!=nil{return err}
  if err:=os.MkdirAll(d.Path,0755);err!=nil{return err}
  usage,err:=disk.Usage(d.Path);if err!=nil{return err};var bytes int64
  for _,ep:=range rs{exists:=false;for _,candidate:=range a.cfg.Disks{p,_:=episodePaths(a.cfg.DataVersion,t,candidate,ep.ParentKey);if dirHasFiles(p){exists=true;break}};if !exists{bytes+=ep.Size}}
  need:=uint64(float64(bytes)*a.cfg.SafetyFactor+a.cfg.MinFreeGB*1024*1024*1024)
  if usage.Free<need{
   log.Printf("任务整体空间预检不通过，未启动: %s free=%d need=%d",t.Name,usage.Free,need)
   for _,ep:=range rs{_=a.set(ep,*d,"","","waiting_space","磁盘剩余空间不足，任务整体未启动")}
   waitingSpace=true;continue
  }
  if err:=a.assign(t.Name,d.Name);err!=nil{return fmt.Errorf("保存任务磁盘分配失败 %s: %w",t.Name,err)};loads[d.Name]++;byDisk[d.Name]=append(byDisk[d.Name],batch{t,*d,rs})
 }
 var wg sync.WaitGroup;errs:=make(chan error,len(byDisk));for _,bs:=range byDisk{bs:=bs;wg.Add(1);go func(){defer wg.Done();for _,b:=range bs{if a.ctx.Err()!=nil{return};if e:=a.downloadTask(b);e!=nil{errs<-e;return}}}()};wg.Wait();close(errs);for e:=range errs{if e!=nil{return e}};if waitingSpace{return errors.New("部分任务因磁盘空间不足未启动；请释放空间后重新运行")};return nil
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
  if dirHasFiles(candidateFinal){
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
 if err:=os.RemoveAll(final);err!=nil{return a.fail(e,d,final,partial,err)};if err:=os.Rename(partial,final);err!=nil{return a.fail(e,d,final,partial,err)};syncDirectory(filepath.Dir(final))
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
     syncErr:=out.Sync()
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
   if err=os.Rename(tmp,dst);err==nil{if !expected.LastModified.IsZero(){_=os.Chtimes(dst,expected.LastModified,expected.LastModified)};syncDirectory(filepath.Dir(dst))}
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
