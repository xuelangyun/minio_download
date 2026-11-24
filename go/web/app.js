let configData=null,statusData=null,logOffset=0,refreshTimer=null,logRefreshTimer=null,diskMounts=[];let statusRequestInFlight=false,logsRequestInFlight=false,statusEventsController=null,statusEventsRetryTimer=null,statusEventsRetryMs=1000;
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
function renderDisks(disks){const root=$("disk-list");root.replaceChildren();(disks||[]).forEach((d,i)=>{const card=document.createElement("div");card.className="disk-row";card.innerHTML='<div class="disk-actions"><b>磁盘 '+(i+1)+'</b><button type="button" class="btn danger" data-remove-disk="'+i+'">删除</button></div><div class="disk-grid"><div class="field"><label>磁盘名称</label><input data-disk-field="name" value="'+esc(d.name)+'" placeholder="例如 disk1"></div><div class="field"><label>实际挂载点</label><input data-disk-field="mountpoint" list="disk-mount-options" value="'+esc(d.mountpoint||"")+'" placeholder="选择或输入挂载点"></div><div class="field"><label>下载目录</label><input data-disk-field="path" value="'+esc(d.path)+'" placeholder="挂载点下的下载目录"></div></div>';root.appendChild(card)});let dl=document.getElementById("disk-mount-options");if(!dl){dl=document.createElement("datalist");dl.id="disk-mount-options";document.body.appendChild(dl)}dl.innerHTML=diskMounts.map(m=>'<option value="'+esc(m.mountpoint)+'">'+esc(m.device||"")+' · '+(m.free/1073741824).toFixed(1)+' GiB 可用</option>').join("");root.querySelectorAll("[data-remove-disk]").forEach(btn=>btn.onclick=()=>{const rows=readDisks();rows.splice(Number(btn.dataset.removeDisk),1);renderDisks(rows)})}
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
async function loadConfig(){configData=await api("/api/config");$("connection-fields").innerHTML=fieldsFor(["minio","pg"]);$("advanced-fields").innerHTML=fieldsFor(["run","transfer","web"]);await loadDiskMounts();renderDisks(configData.disks||[]);$("data-version").value=configData.tasks.data_version||"";taskEditorData=structuredClone({data_version:configData.tasks.data_version||"",scenes:configData.tasks.scenes||[]});renderTaskEditor();$("db-path").textContent="SQLite 文件："+configData.paths.sqlite_db;$("connection-state").textContent="控制台已连接";$("connection-state").className="ok"}
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
 $("transfer-bar").style.width=(expected>0?percent:0)+"%";$("transfer-bar").parentElement.setAttribute("aria-valuenow",String(expected>0?percent:0));
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
async function refreshStatus(){if(statusRequestInFlight)return;statusRequestInFlight=true;try{const range=$("run-history-range")?.value||"30";renderStatus(await api("/api/status?history_days="+encodeURIComponent(range)))}catch(e){$("connection-state").textContent=e.message;$("connection-state").className="bad"}finally{statusRequestInFlight=false}}
function renderLiveStatus(live){
 if(!live)return;
 const t=live.transfer_progress||{},running=!!live.running;
 const speed=Math.max(0,Number(t.speed_bytes_per_second)||0),downloaded=Math.max(0,Number(t.downloaded_bytes)||0),expected=Math.max(0,Number(t.expected_bytes)||0),remaining=Math.max(0,Number(t.remaining_bytes)||0),percent=Math.max(0,Math.min(100,Number(t.progress_percent)||0));
 const metrics=[["运行状态",running?"下载中":"空闲"],["下载速度",(speed/1048576).toFixed(2)+" MiB/s"],["已下载",formatGiB(downloaded)],["进度",percent.toFixed(1)+"%"],["预计剩余",t.eta_seconds===null||t.eta_seconds===undefined?"—":Math.round(Number(t.eta_seconds)/60)+" 分钟"]];
 $("metrics").innerHTML=metrics.map(x=>'<div class="metric">'+esc(x[0])+'<b>'+esc(x[1])+'</b></div>').join("");
 $("transfer-summary").textContent=running?(expected>0?percent.toFixed(1)+"% · "+(speed/1048576).toFixed(2)+" MiB/s":"传输中 · 等待对象大小统计"):"当前没有活动传输";
 $("transfer-bar").style.width=(expected>0?percent:0)+"%";
 $("transfer-downloaded").textContent="已传输 "+formatGiB(downloaded);
 $("transfer-remaining").textContent=expected>0?"预计剩余 "+formatGiB(remaining):"待传输 —";
 const stopPending=!!(statusData&&statusData.stop_requested);
 $("run-control-state").textContent=stopPending?"正在安全停止":(running?"下载运行中":"当前空闲");
 $("run-control-state").className="summary-chip "+(stopPending?"run-state-chip pending":running?"run-state-chip":"");
 $("start-run").disabled=running;$("pause-run").disabled=!running||stopPending;$("force-run").disabled=!running;
 $("pause-run").textContent=stopPending?"正在停止…":"安全暂停";
 const active=live.running_episodes||[];
 $("active-count").textContent=active.length+" 个活动任务";
 $("active-downloads").innerHTML=active.length?'<div class="active-list">'+active.map(x=>{const partial=Math.max(0,Number(x.partial_bytes)||0),expected=Math.max(0,Number(x.expected_bytes)||0),pct=expected>0?Math.max(0,Math.min(100,partial/expected*100)):0;return '<div class="active-item"><div class="active-item-top"><div class="active-item-main"><div class="active-item-title">'+esc(x.task_name||"未知任务")+'</div><div class="active-item-meta">磁盘：'+esc(x.disk_name||"未分配")+' · Episode：'+esc(x.episode_id||"")+'</div></div><div class="active-item-size">'+formatGiB(partial)+'</div></div><div class="active-item-progress"><span style="width:'+pct.toFixed(1)+'%"></span></div><div class="active-item-bottom"><span>'+(expected>0?(pct.toFixed(1)+"% · 目标 "+formatGiB(expected)):"目标容量未知")+'</span><span>开始：'+esc(x.started_at||"—")+'</span></div></div>'}).join("")+'</div>':'<div class="notice">当前没有正在下载的 Episode。</div>';
}
function closeStatusEvents(){
 if(statusEventsRetryTimer){clearTimeout(statusEventsRetryTimer);statusEventsRetryTimer=null}
 if(statusEventsController){const current=statusEventsController;statusEventsController=null;current.abort()}
}
async function connectStatusEvents(){
 if(statusEventsController||document.visibilityState!=="visible"||document.querySelector(".nav button.active")?.dataset.tab!=="dashboard")return;
 const controller=new AbortController();statusEventsController=controller;
 try{
  const headers={"Accept":"text/event-stream"};const token=sessionStorage.getItem("minio_download_token");if(token)headers.Authorization="Bearer "+token;
  const response=await fetch("/api/events",{headers,signal:controller.signal,cache:"no-store"});
  if(!response.ok||!response.body)throw new Error("状态推送连接失败："+response.status);
  statusEventsRetryMs=1000;
  const reader=response.body.getReader(),decoder=new TextDecoder();let buffer="";
  while(!controller.signal.aborted){
   const part=await reader.read();if(part.done)break;
   buffer+=decoder.decode(part.value,{stream:true});
   const blocks=buffer.split(/\r?\n\r?\n/);buffer=blocks.pop()||"";
   for(const block of blocks){
    let eventName="message";const data=[];
    for(const line of block.split(/\r?\n/)){if(line.startsWith("event:"))eventName=line.slice(6).trim();else if(line.startsWith("data:"))data.push(line.slice(5).trimStart())}
    if(eventName==="status"&&data.length){try{renderLiveStatus(JSON.parse(data.join("\n")))}catch(e){}}
   }
  }
  if(!controller.signal.aborted)throw new Error("状态推送连接已关闭");
 }catch(e){
  if(!controller.signal.aborted&&statusData){$("connection-state").textContent="状态推送断开，5 秒轮询继续工作";$("connection-state").className="bad"}
 }finally{
  if(statusEventsController===controller)statusEventsController=null;
  if(!controller.signal.aborted&&document.visibilityState==="visible"&&document.querySelector(".nav button.active")?.dataset.tab==="dashboard"){
   statusEventsRetryTimer=setTimeout(connectStatusEvents,statusEventsRetryMs);statusEventsRetryMs=Math.min(10000,statusEventsRetryMs*2);
  }
 }
}
async function refreshLogs(){if(logsRequestInFlight)return;logsRequestInFlight=true;try{const d=await api("/api/logs?offset="+logOffset);if(d.reset){$("logs").textContent=d.text||""}else if(d.text){$("logs").textContent+=($("logs").textContent?"\n":"")+d.text}$("logs").scrollTop=$("logs").scrollHeight;logOffset=d.offset||logOffset}catch(e){$("logs").textContent=e.message}finally{logsRequestInFlight=false}}
async function testConnection(target){try{const root=target==="minio"?$("connection-fields"):$("connection-fields");const d=await api("/api/config/test","POST",{target:target,values:readFields(root)});message("connection-result",d.message,d.ok)}catch(e){message("connection-result",e.message,false)}}
$("run-history-range")?.addEventListener("change",refreshStatus);
document.querySelectorAll(".nav button").forEach(btn=>btn.addEventListener("click",()=>{document.querySelectorAll(".nav button").forEach(x=>{x.classList.toggle("active",x===btn);x.setAttribute("aria-pressed",String(x===btn))});document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));btn.classList.add("active");$(btn.dataset.tab).classList.add("active");if(btn.dataset.tab==="records"){closeStatusEvents();refreshLogs()}else if(btn.dataset.tab==="dashboard"){refreshStatus();connectStatusEvents()}}));
document.addEventListener("visibilitychange",()=>{if(document.visibilityState!=="visible")closeStatusEvents();else if(document.querySelector(".nav button.active")?.dataset.tab==="dashboard")connectStatusEvents()});
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
(async()=>{try{await loadConfig();await refreshPlans();await refreshStatus();connectStatusEvents();
 refreshTimer=setInterval(()=>{if(document.visibilityState==="visible"&&document.querySelector(".nav button.active")?.dataset.tab==="dashboard")refreshStatus()},5000);logRefreshTimer=setInterval(()=>{if(document.visibilityState==="visible"&&document.querySelector(".nav button.active")?.dataset.tab==="records")refreshLogs()},20000)}catch(e){$("connection-state").textContent=e.message;$("connection-state").className="bad";$("setup-issues").textContent="初始化失败："+e.message}})();
