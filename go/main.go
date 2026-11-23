package main

import (
 "context"
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
 "time"

 _ "github.com/jackc/pgx/v5/stdlib"
 "github.com/minio/minio-go/v7"
 "github.com/minio/minio-go/v7/pkg/credentials"
 "github.com/shirou/gopsutil/v4/disk"
 _ "modernc.org/sqlite"
)

type Task struct { Name string `json:"name"`; Scene string `json:"scene"`; Dir string `json:"dir"`; Aliases []string `json:"aliases"`; TargetHours float64 `json:"target_hours"`; TargetCount int `json:"target_count"` }
type DiskConfig struct { Name string `json:"name"`; Path string `json:"path"`; Mountpoint string `json:"mountpoint"` }
type Config struct {
 Endpoint string `json:"minio_endpoint"`; AccessKey string `json:"minio_access_key"`; SecretKey string `json:"minio_secret_key"`; Bucket string `json:"minio_bucket"`; Region string `json:"minio_region"`; SSL bool `json:"minio_use_ssl"`
 PGHost string `json:"pg_host"`; PGPort int `json:"pg_port"`; PGDB string `json:"pg_database"`; PGUser string `json:"pg_user"`; PGPassword string `json:"pg_password"`
 DataVersion string `json:"data_version"`; StateDB string `json:"state_db"`; LogDir string `json:"log_dir"`; MinFreeGB float64 `json:"min_free_gb"`; SafetyFactor float64 `json:"space_safety_factor"`
 Workers int `json:"max_writes_per_disk"`; RequireMCAP bool `json:"require_mcap"`; MCAPPattern string `json:"mcap_pattern"`; IncludeNull bool `json:"include_null_quality"`
 Disks []DiskConfig `json:"disks"`; Tasks []Task `json:"tasks"`
}
type Episode struct { ID string; JobID sql.NullInt64; Task,SourceTask,SN,ParentKey string; Size int64; Duration float64; Created sql.NullTime }
type App struct { cfg Config; mc *minio.Client; db *sql.DB; ctx context.Context; mode string; recheck bool }
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
 _=os.MkdirAll(filepath.Dir(c.StateDB),0755);_=os.MkdirAll(c.LogDir,0755)
 f,e:=os.OpenFile(filepath.Join(c.LogDir,"go-downloader.log"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644);if e!=nil{log.Fatal(e)};defer f.Close()
 log.SetOutput(io.MultiWriter(os.Stdout,f));log.SetFlags(log.Ldate|log.Ltime|log.Lmicroseconds)
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt);defer stop()
 a:=&App{cfg:c,ctx:ctx,mode:*mode,recheck:*recheck}
 if e=a.open();e!=nil{log.Fatal(e)};defer a.db.Close()
 if *cleanup{n,err:=a.cleanup(*dry);if err!=nil{log.Fatal(err)};log.Printf("清理结束：%d 条",n);if *dry{return}}else if *dry{log.Fatal("--dry-run 必须与 --cleanup-missing-mcap 一起使用")}
 rows,e:=a.fetch();if e!=nil{log.Fatal(e)}
 if e=a.run(rows,*rounds);e!=nil{log.Fatal(e)}
 log.Printf("执行结束 Go=%s/%s",runtime.GOOS,runtime.GOARCH)
}
func(c *Config)normalize(m string)error{
 if c.Endpoint==""||c.AccessKey==""||c.SecretKey==""||c.Bucket==""{return errors.New("MinIO endpoint/access_key/secret_key/bucket 必须填写")}
 if c.PGHost==""||c.PGDB==""||c.PGUser==""||c.PGPassword==""{return errors.New("PostgreSQL 连接参数必须填写")}
 if c.PGPort==0{c.PGPort=5432};if c.DataVersion==""{c.DataVersion="银河2.2数据"};if c.StateDB==""{c.StateDB="minio_download_state.sqlite3"};if c.LogDir==""{c.LogDir="logs"}
 if c.MinFreeGB==0{c.MinFreeGB=3};if c.SafetyFactor==0{c.SafetyFactor=1.15};if c.Workers<=0{c.Workers=2};if c.MCAPPattern==""{c.MCAPPattern="*.SYNC.mcap"}
 if len(c.Disks)==0||len(c.Tasks)==0{return errors.New("disks/tasks 不能为空")}
 seen:=map[string]bool{};for _,t:=range c.Tasks{if t.Name==""||seen[t.Name]{return fmt.Errorf("任务名为空或重复: %q",t.Name)};seen[t.Name]=true;if t.TargetHours<0||t.TargetCount<0{return fmt.Errorf("任务 %s 的目标不能为负数",t.Name)}}
 for _,d:=range c.Disks{if d.Name==""||d.Path==""{return errors.New("磁盘 name/path 不能为空")}}
 _=m;return nil
}
func(a *App)open()error{
 endpoint:=strings.TrimPrefix(strings.TrimPrefix(a.cfg.Endpoint,"https://"),"http://");secure:=a.cfg.SSL||strings.HasPrefix(a.cfg.Endpoint,"https://")
 mc,e:=minio.New(endpoint,&minio.Options{Creds:credentials.NewStaticV4(a.cfg.AccessKey,a.cfg.SecretKey,""),Secure:secure,Region:a.cfg.Region});if e!=nil{return e};a.mc=mc
 pg:=fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=prefer connect_timeout=10",a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.PGUser,a.cfg.PGPassword)
 p,e:=sql.Open("pgx",pg);if e!=nil{return e};defer p.Close();ctx,cancel:=context.WithTimeout(a.ctx,15*time.Second);defer cancel();if e=p.PingContext(ctx);e!=nil{return fmt.Errorf("PostgreSQL 连接失败: %w",e)}
 ok,e:=mc.BucketExists(ctx,a.cfg.Bucket);if e!=nil{return e};if !ok{return fmt.Errorf("MinIO bucket 不存在: %s",a.cfg.Bucket)}
 a.db,e=sql.Open("sqlite",a.cfg.StateDB);if e!=nil{return e};a.db.SetMaxOpenConns(8)
 _,e=a.db.Exec("PRAGMA journal_mode=WAL;"+
 "CREATE TABLE IF NOT EXISTS downloads(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,parent_key TEXT,disk_name TEXT,target_path TEXT,partial_path TEXT,expected_bytes INTEGER,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TEXT NOT NULL);"+
 "CREATE INDEX IF NOT EXISTS idx_download_task_status ON downloads(task_name,status);"+
 "CREATE TABLE IF NOT EXISTS task_disk_assignments(task_name TEXT NOT NULL,disk_name TEXT NOT NULL,last_used_at TEXT NOT NULL,PRIMARY KEY(task_name,disk_name));"+
 "CREATE TABLE IF NOT EXISTS downloaded_episodes(episode_id TEXT PRIMARY KEY,task_name TEXT NOT NULL,source_task_name TEXT,job_id INTEGER,sn_code TEXT,parent_key TEXT,disk_name TEXT,target_path TEXT,file_size INTEGER,content_time REAL,create_time TEXT,downloaded_at TEXT NOT NULL);"+
 "CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT NOT NULL);")
 if e==nil{log.Printf("MinIO=%s bucket=%s；PostgreSQL=%s:%d/%s；SQLite=%s",a.cfg.Endpoint,a.cfg.Bucket,a.cfg.PGHost,a.cfg.PGPort,a.cfg.PGDB,a.cfg.StateDB)};return e
}
func(a *App)set(e Episode,d DiskConfig,final,partial,status,msg string)error{
 _,err:=a.db.Exec("INSERT INTO downloads(episode_id,task_name,parent_key,disk_name,target_path,partial_path,expected_bytes,status,error,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,parent_key=excluded.parent_key,disk_name=excluded.disk_name,target_path=excluded.target_path,partial_path=excluded.partial_path,expected_bytes=excluded.expected_bytes,status=excluded.status,error=excluded.error,updated_at=excluded.updated_at",
 e.ID,e.Task,e.ParentKey,d.Name,nullable(final),nullable(partial),e.Size,status,nullable(msg),time.Now().UTC().Format(time.RFC3339Nano));return err
}
func nullable(s string)any{if s==""{return nil};return s}
func(a *App)completed(id string)bool{var p sql.NullString;err:=a.db.QueryRow("SELECT target_path FROM downloads WHERE episode_id=? AND status='completed'",id).Scan(&p);return err==nil&&p.Valid&&dirHasFiles(p.String)}
func(a *App)stats(task string)(float64,int){rows,err:=a.db.Query("SELECT content_time,target_path FROM downloaded_episodes WHERE task_name=?",task);if err!=nil{return 0,0};defer rows.Close();var seconds float64;count:=0;for rows.Next(){var v sql.NullFloat64;var p sql.NullString;if rows.Scan(&v,&p)!=nil{continue};if p.Valid&&dirHasFiles(p.String){seconds+=v.Float64;count++}};return seconds,count}
func(a *App)assign(task,disk string){tx,err:=a.db.Begin();if err!=nil{return};_,_=tx.Exec("DELETE FROM task_disk_assignments WHERE task_name=?",task);_,_=tx.Exec("INSERT INTO task_disk_assignments(task_name,disk_name,last_used_at) VALUES(?,?,?)",task,disk,time.Now().UTC().Format(time.RFC3339Nano));_=tx.Commit()}
func(a *App)assigned(task string)string{var d string;_=a.db.QueryRow("SELECT disk_name FROM task_disk_assignments WHERE task_name=? ORDER BY last_used_at DESC LIMIT 1",task).Scan(&d);return d}
func(a *App)record(e Episode,d DiskConfig,final string)error{
 _,err:=a.db.Exec("INSERT INTO downloaded_episodes(episode_id,task_name,source_task_name,job_id,sn_code,parent_key,disk_name,target_path,file_size,content_time,create_time,downloaded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "+
 "ON CONFLICT(episode_id) DO UPDATE SET task_name=excluded.task_name,disk_name=excluded.disk_name,target_path=excluded.target_path,downloaded_at=excluded.downloaded_at",
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
 out:=[]Episode{};ids:=map[string]bool{};keys:=map[string]bool{}
 for rows.Next(){var x Episode;var src string;var key sql.NullString;var duration sql.NullFloat64
  if e=rows.Scan(&x.ID,&x.JobID,&src,&x.SN,&key,&x.Size,&duration,&x.Created);e!=nil{return nil,e}
  x.SourceTask=src;x.Task=src;if v,ok:=aliases[src];ok{x.Task=v};x.ParentKey=strings.Trim(key.String,"/");x.Duration=duration.Float64
  if x.ID==""||x.ParentKey==""||ids[x.ID]||keys[x.ParentKey]{continue};ids[x.ID]=true;keys[x.ParentKey]=true;out=append(out,x)
 }
 if e=rows.Err();e!=nil{return nil,e};order:=map[string]int{};for i,t:=range a.cfg.Tasks{order[t.Name]=i}
 sort.SliceStable(out,func(i,j int)bool{if order[out[i].Task]!=order[out[j].Task]{return order[out[i].Task]<order[out[j].Task]};if !out[i].Created.Valid{return false};if !out[j].Created.Valid{return true};return out[i].Created.Time.Before(out[j].Created.Time)})
 log.Printf("查询并去重后 %d 条候选",len(out));return out,nil
}
func(a *App)run(all []Episode,nround int)error{
 groups:=map[string][]Episode{};for _,e:=range all{groups[e.Task]=append(groups[e.Task],e)}
 for r:=1;r<=nround;r++{if a.ctx.Err()!=nil{return nil};selected:=map[string][]Episode{}
  for _,t:=range a.cfg.Tasks{s,n:=a.stats(t.Name);target,used:=t.TargetHours*3600,s;if a.mode=="count"{target=float64(t.TargetCount);used=float64(n)}
   addSec:=0.0;addN:=0
   for _,e:=range groups[t.Name]{if a.completed(e.ID){continue};var st string;_=a.db.QueryRow("SELECT status FROM downloads WHERE episode_id=?",e.ID).Scan(&st);if !a.recheck&&(st=="missing_task_info"||st=="missing_mcap"){continue}
    if (a.mode=="duration"&&addSec>=target-used)||(a.mode=="count"&&float64(addN)>=target-used){break};selected[t.Name]=append(selected[t.Name],e);addSec+=e.Duration;addN++}
   if a.mode=="duration"{log.Printf("按时长 %s: 目标 %.2fh 已完成 %.2fh 新选 %d 条/%.2fh",t.Name,target/3600,s/3600,addN,addSec/3600)}else{log.Printf("按条数 %s: 目标 %d 已完成 %d 新选 %d 条",t.Name,int(target),n,addN)}
  }
  if e:=a.downloadSelected(selected);e!=nil{return e};log.Printf("第 %d 轮结束",r)
 };return nil
}
type batch struct{t Task;d DiskConfig;rows []Episode}
func(a *App)downloadSelected(sel map[string][]Episode)error{
 loads:=map[string]int{};for _,t:=range a.cfg.Tasks{if a.assigned(t.Name)!=""{loads[a.assigned(t.Name)]++}}
 byDisk:=map[string][]batch{}
 for _,t:=range a.cfg.Tasks{rs:=sel[t.Name];if len(rs)==0{continue};var d *DiskConfig;old:=a.assigned(t.Name)
  for i:=range a.cfg.Disks{if old!=""&&a.cfg.Disks[i].Name==old{d=&a.cfg.Disks[i];break}}
  if d==nil{for i:=range a.cfg.Disks{x:=&a.cfg.Disks[i];if d==nil||loads[x.Name]<loads[d.Name]{d=x}}};if d==nil{return errors.New("没有配置目标磁盘")}
  if err:=os.MkdirAll(d.Path,0755);err!=nil{return err};if d.Mountpoint!=""{if _,err:=os.Stat(d.Mountpoint);err!=nil{return fmt.Errorf("挂载点不存在 %s: %w",d.Mountpoint,err)}}
  usage,err:=disk.Usage(d.Path);if err!=nil{return err};var bytes int64;for _,e:=range rs{bytes+=e.Size};need:=uint64(float64(bytes)*a.cfg.SafetyFactor+a.cfg.MinFreeGB*1024*1024*1024)
  if usage.Free<need{log.Printf("任务整体空间预检不通过，未启动: %s free=%d need=%d",t.Name,usage.Free,need);continue}
  a.assign(t.Name,d.Name);loads[d.Name]++;byDisk[d.Name]=append(byDisk[d.Name],batch{t,*d,rs})
 }
 var wg sync.WaitGroup;errs:=make(chan error,len(byDisk));for _,bs:=range byDisk{bs:=bs;wg.Add(1);go func(){defer wg.Done();for _,b:=range bs{if a.ctx.Err()!=nil{return};if e:=a.downloadTask(b);e!=nil{errs<-e;return}}}()};wg.Wait();close(errs);for e:=range errs{if e!=nil{return e}};return nil
}
func(a *App)downloadTask(b batch)error{
 sem:=make(chan struct{},a.cfg.Workers);var wg sync.WaitGroup;var mu sync.Mutex;var first error
 for _,ep:=range b.rows{if a.ctx.Err()!=nil{break};sem<-struct{}{};wg.Add(1);go func(e Episode){defer wg.Done();defer func(){<-sem}();if err:=a.downloadOne(b.t,b.d,e);err!=nil{mu.Lock();if first==nil{first=err};mu.Unlock()}}(ep)}
 wg.Wait();return first
}
func(a *App)downloadOne(t Task,d DiskConfig,e Episode)error{
 if a.ctx.Err()!=nil{return nil};if e.Size<=0{_=a.set(e,d,"","","invalid_size","file_size 无效");return nil}
 root:=filepath.Join(d.Path,safe(a.cfg.DataVersion),safe(t.Scene),safe(taskDir(t)));final:=filepath.Join(root,safe(e.ID));partial:=filepath.Join(root,"."+safe(e.ID)+".partial")
 if dirHasFiles(final){_=a.set(e,d,final,"","completed","");return a.record(e,d,final)}
 if err:=os.MkdirAll(partial,0755);err!=nil{return a.fail(e,d,final,partial,err)}
 direct:=map[string]bool{};for obj:=range a.mc.ListObjects(a.ctx,a.cfg.Bucket,minio.ListObjectsOptions{Prefix:e.ParentKey+"/",Recursive:false}){if obj.Err!=nil{return a.fail(e,d,final,partial,obj.Err)};if !obj.IsDir{direct[path.Base(obj.Key)]=true}}
 if !direct["task_info.json"]{_=a.set(e,d,final,partial,"missing_task_info","缺少 task_info.json");return nil}
 if a.cfg.RequireMCAP{found:=false;for n:=range direct{if ok,_:=path.Match(a.cfg.MCAPPattern,n);ok{found=true}};if !found{_=a.set(e,d,final,partial,"missing_mcap","缺少 mcap");return nil}}
 _=a.set(e,d,final,partial,"running","");var total int64
 for obj:=range a.mc.ListObjects(a.ctx,a.cfg.Bucket,minio.ListObjectsOptions{Prefix:e.ParentKey+"/",Recursive:true}){if obj.Err!=nil{return a.fail(e,d,final,partial,obj.Err)};if obj.IsDir||strings.HasSuffix(obj.Key,"/"){continue}
  rel:=strings.TrimPrefix(obj.Key,e.ParentKey+"/");dst,err:=safeJoin(partial,rel);if err!=nil{return a.fail(e,d,final,partial,err)};if err=os.MkdirAll(filepath.Dir(dst),0755);err!=nil{return a.fail(e,d,final,partial,err)}
  tmp:=dst+".minio-tmp";if err=a.fetchObject(obj.Key,tmp);err!=nil{return a.fail(e,d,final,partial,err)};if err=os.Remove(dst);err!=nil&&!os.IsNotExist(err){return a.fail(e,d,final,partial,err)};if err=os.Rename(tmp,dst);err!=nil{return a.fail(e,d,final,partial,err)};total+=obj.Size
 }
 if total==0{return a.fail(e,d,final,partial,errors.New("MinIO 前缀为空"))};if err:=os.MkdirAll(filepath.Dir(final),0755);err!=nil{return a.fail(e,d,final,partial,err)}
 if err:=os.RemoveAll(final);err!=nil{return a.fail(e,d,final,partial,err)};if err:=os.Rename(partial,final);err!=nil{return a.fail(e,d,final,partial,err)}
 if err:=a.set(e,d,final,"","completed","");err!=nil{return err};return a.record(e,d,final)
}
func(a *App)fetchObject(key,tmp string)error{
 obj,e:=a.mc.GetObject(a.ctx,a.cfg.Bucket,key,minio.GetObjectOptions{});if e!=nil{return e};defer obj.Close();info,e:=obj.Stat();if e!=nil{return e}
 f,e:=os.OpenFile(tmp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0644);if e!=nil{return e};n,ce:=io.Copy(f,obj);se:=f.Sync();fe:=f.Close();if ce!=nil{return ce};if se!=nil{return se};if fe!=nil{return fe};if n!=info.Size{return fmt.Errorf("对象大小校验失败 %s: %d != %d",key,n,info.Size)};return nil
}
func(a *App)fail(e Episode,d DiskConfig,final,partial string,err error)error{_=a.set(e,d,final,partial,"failed",err.Error());log.Printf("下载失败 %s: %v",e.ID,err);return err}
func(a *App)cleanup(preview bool)(int,error){
 rows,e:=a.db.Query("SELECT episode_id,target_path FROM downloads WHERE status='completed'");if e!=nil{return 0,e};defer rows.Close();n:=0
 for rows.Next(){var id string;var p sql.NullString;if e=rows.Scan(&id,&p);e!=nil{return n,e};if !p.Valid||!hasPattern(p.String,a.cfg.MCAPPattern){n++;if preview{log.Printf("[dry-run] 缺 mcap: %s",p.String)}else{_=os.RemoveAll(p.String);_,_=a.db.Exec("DELETE FROM downloads WHERE episode_id=?",id);_,_=a.db.Exec("DELETE FROM downloaded_episodes WHERE episode_id=?",id)}}};return n,rows.Err()
}
func dirHasFiles(p string)bool{x,e:=os.ReadDir(p);return e==nil&&len(x)>0}
func hasPattern(p,pat string)bool{x,e:=os.ReadDir(p);if e!=nil{return false};for _,v:=range x{if !v.IsDir(){if ok,_:=path.Match(pat,v.Name());ok{return true}}};return false}
func safe(s string)string{s=strings.TrimSpace(s);if s==""||s=="."||s==".."{return "_"};r:=strings.NewReplacer("/","_","\\","_",":","_","*","_","?","_","\"","_","<","_",">","_","|","_");s=strings.TrimRight(r.Replace(s),". ");if s==""{return "_"};return s}
func safeJoin(root,rel string)(string,error){c:=path.Clean(strings.ReplaceAll(rel,"\\","/"));if c=="."||c==".."||strings.HasPrefix(c,"../")||strings.HasPrefix(c,"/"){return "",fmt.Errorf("非法对象路径 %q",rel)};p:=filepath.Join(root,filepath.FromSlash(c));r,e:=filepath.Rel(root,p);if e!=nil||r==".."||strings.HasPrefix(r,".."+string(os.PathSeparator)){return "",fmt.Errorf("对象路径越界 %q",rel)};return p,nil}
func taskDir(t Task)string{if t.Dir!=""{return t.Dir};return t.Name}
