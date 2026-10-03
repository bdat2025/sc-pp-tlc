//! TeleCloud API 上传后处理模块 / TeleCloud Upload API Post-processing Module
//!
//! 将录制的视频文件通过 TeleCloud HTTP API 上传到远程存储。
//! 支持同步/异步上传、进度跟踪、文件分享链接生成。
//!
//! Uploads recorded video files to TeleCloud storage via HTTP API.
//! Supports sync/async uploads, progress tracking, and share link generation.
//!
//! # 协议 / Protocol
//! - `--describe`: 输出 JSON 格式的模块元数据 / Output module metadata as JSON
//! - 环境变量 `PP_INPUT`: 输入视频文件路径 / Input video file path via env var
//! - 标准输出 `OUTPUT:{path}`: 成功后输出视频路径 / Output video path on success
//! - 标准输出 `PROGRESS:{done}/{total}`: 进度上报 / Progress reporting
//! - 标准输出 `STATUS:{speed}`: 上传速度上报 / Upload speed reporting
//! - 标准输出 `SHARE_LINK:{url}`: 分享链接 / Share link (if auto-share enabled)
//! - 标准输出 `TASK_ID:{id}`: 任务 ID（异步上传） / Task ID (async upload)
//!
//! # API 行为 / API Behavior
//! - 同步上传 (async=false): 立即返回 status="done", file_id, path
//! - 异步上传 (async=true): 返回 status="processing", task_id，模块继续轮询查询进度
//! - 支持 overwrite: 覆盖已存在的文件
//! - 支持分享: auto_share=true 时，返回 share_link, direct_link
//!
//! 详见: https://telecloud/api/docs

use pp_utils::{
    ModuleInput, describe_with_version, find_cover, format_bytes, format_duration,
    output_ok, parse_stem, video_duration,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

/// 任务状态持久化 / Task state persistence
#[derive(Debug, Serialize, Deserialize, Clone)]
struct UploadTaskState {
    task_id: String,
    file_path: String,
    api_url: String,
    file_size: u64,
    created_at: u64,
    updated_at: u64,
}

/// 模块元数据 JSON
const DESCRIBE: &str = r#"{
    "id": "telecloud_upload",
    "name": "TeleCloud API Upload",
    "description": "Upload recorded video to TeleCloud via HTTP API (supports sync/async, share links, progress tracking)",
    "inputTypes": ["any_file"],
    "outputTypes": ["any_file"],
    "official": true,
    "params": [
        {
        "key": "api_url",
        "label": "API 地址（例如 https://telecloud/）",
        "type": "string",
        "default": ""
        },
        {
        "key": "api_token",
        "label": "API Token（Bearer Token，保密）",
        "type": "string",
        "default": ""
        },
        {
        "key": "remote_path",
        "label": "远程存储路径（例如 /recordings/）",
        "type": "string",
        "default": "/"
        },
        {
        "key": "organize_by_model",
        "label": "按主播名自动创建子文件夹",
        "type": "boolean",
        "default": true
        },
        {
        "key": "async_upload",
        "label": "异步上传（后台上传，立即返回）",
        "type": "boolean",
        "default": false
        },
        {
        "key": "auto_share",
        "label": "自动生成分享链接",
        "type": "boolean",
        "default": false
        },
        {
        "key": "share_type",
        "label": "分享类型（public 或 folder）",
        "type": "string",
        "default": "public"
        },
        {
        "key": "overwrite",
        "label": "存在时覆盖文件",
        "type": "boolean",
        "default": false
        },
        {
        "key": "proxy",
        "label": "代理地址（支持 http://、socks5://）",
        "type": "string",
        "default": ""
        },
        {
        "key": "max_retries",
        "label": "最大重试次数",
        "type": "string",
        "default": "3"
        },
        {
        "key": "timeout_seconds",
        "label": "超时时间（秒，0=自动计算）",
        "type": "string",
        "default": "0"
        },
        {
        "key": "upload_cover",
        "label": "同时上传封面图",
        "type": "boolean",
        "default": true
        },
        {
        "key": "delete_after_upload",
        "label": "上传成功后删除本地文件",
        "type": "boolean",
        "default": false
        }
    ],
    "i18n": {
        "en-US": {
        "name": "TeleCloud API Upload 0.1.0",
        "description": "Upload recorded video to TeleCloud via HTTP API (supports sync/async, share links, progress tracking)",
        "params": {
            "api_url": { "label": "API URL (e.g. https://telecloud/)" },
            "api_token": { "label": "API Token (Bearer Token, keep secret)" },
            "remote_path": { "label": "Remote storage path (e.g. /recordings/)" },
            "organize_by_model": { "label": "Auto-create folder by model name" },
            "async_upload": { "label": "Async upload (background, return immediately)" },
            "auto_share": { "label": "Auto-generate share link" },
            "share_type": { "label": "Share type (public or folder)" },
            "overwrite": { "label": "Overwrite if file exists" },
            "proxy": { "label": "Proxy (http:// or socks5://)" },
            "max_retries": { "label": "Max retry attempts" },
            "timeout_seconds": { "label": "Timeout in seconds (0=auto-calculate based on file size)" },
            "upload_cover": { "label": "Also upload cover image" },
            "delete_after_upload": { "label": "Delete local file after successful upload" }
        }
        }
    }
}"#;

/// API 上传响应
/// 
/// 根据不同情况返回不同格式:
/// - 同步上传: status="done", 包含 file_id, filename, path
/// - 异步上传: status="processing", 包含 task_id (需要通过 GET /tasks/:id 查询进度)
/// - 带分享: 额外包含 share_token, share_link, direct_link
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct UploadResponse {
    status: String,
    #[serde(default)]
    filename: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    file_id: i32,
    #[serde(default)]
    task_id: String,
    #[serde(default)]
    share_token: String,
    #[serde(default)]
    share_link: String,
    #[serde(default)]
    direct_link: String,
    #[serde(default)]
    error: String,
}

/// 任务进度响应
#[derive(Debug, Serialize, Deserialize)]
struct TaskProgress {
    #[serde(default)]
    task_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    percent: u32,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    message: String,  // 别名
    #[serde(default)]
    file_id: i32,
    #[serde(default)]
    filename: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    uploaded: u64,
    #[serde(default)]
    speed: u64,  // 上传速度（字节/秒）/ Upload speed in bytes/second
    #[serde(default)]
    eta: u32,    // 预估剩余时间（秒）/ Estimated time remaining in seconds
    #[serde(default)]
    error: String,
}

/// 分享链接响应
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct ShareResponse {
    #[serde(default)]
    share_token: String,
    #[serde(default)]
    share_link: String,
    #[serde(default)]
    error: String,
}

const LOW_SPEED_THRESHOLD_BYTES_PER_SEC: u64 = 1024 * 1024;
const LOW_SPEED_ABORT_AFTER: Duration = Duration::from_secs(10);
const RETRY_REASON_LOW_SPEED: &str = "__RETRY_LOW_SPEED__";
const RETRY_REASON_TIMEOUT: &str = "__RETRY_TIMEOUT__";

/// 检测 MIME 类型
fn detect_mime_type(file_path: &Path) -> &'static str {
    match file_path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase().as_str() {
        "mkv" => "video/x-matroska",
        "ts" => "video/mp2t",
        "m3u8" => "application/vnd.apple.mpegurl",
        _ => "video/mp4",
    }
}

/// 构建 HTTP 客户端
fn build_client(proxy: &str, timeout: u64) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout))
        .pool_max_idle_per_host(2)  // 连接池优化 / Connection pool optimization
        .http1_only();               // 使用 HTTP/1.1 仅用于最大兼容性 / HTTP/1.1 only for compatibility

    // 配置代理
    if !proxy.is_empty() {
        let proxy_obj = if proxy.starts_with("socks5://")
            || proxy.starts_with("http://")
            || proxy.starts_with("https://")
        {
            reqwest::Proxy::all(proxy)
        } else {
            return Err(format!("Invalid proxy format: {}", proxy));
        };

        builder = builder.proxy(proxy_obj.map_err(|e| format!("Proxy error: {}", e))?);
    }

    builder.build().map_err(|e| format!("Failed to build HTTP client: {}", e))
}

struct UploadFileRequest<'a> {
    client: &'a reqwest::Client,
    api_url: &'a str,
    api_token: &'a str,
    file_path: &'a Path,
    remote_path: &'a str,
    auto_share: bool,
    share_type: &'a str,
    overwrite: bool,
    async_upload: bool,
}

/// 上传文件到 TeleCloud API
async fn upload_file(req: UploadFileRequest<'_>) -> Result<UploadResponse, String> {
    let client = req.client;
    let api_url = req.api_url;
    let api_token = req.api_token;
    let file_path = req.file_path;
    let remote_path = req.remote_path;
    let auto_share = req.auto_share;
    let share_type = req.share_type;
    let overwrite = req.overwrite;
    let async_upload = req.async_upload;
    let file_size = fs::metadata(file_path)
        .map_err(|e| format!("Failed to get file metadata: {}", e))?.len();
    
    let filename = file_path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.mp4")
        .to_string();

    eprintln!("[上传开始] / [Upload Start]");
    eprintln!("  文件 / File: {}", filename);
    eprintln!("  大小 / Size: {}", format_bytes(file_size));
    eprintln!("  目录 / Path: {}", remote_path);

    // 构建请求
    let upload_url = format!("{}/api/upload-api/upload", api_url.trim_end_matches('/'));
    let mut form = reqwest::multipart::Form::new()
        .text("path", remote_path.to_string())
        .text("overwrite", overwrite.to_string());

    if auto_share {
        form = form.text("share", share_type.to_string());
    }

    if async_upload {
        form = form.text("async", "true");
    }

    // 添加文件（流式读取以支持大文件）/ Stream file to support large files
    // 注意：multipart form 需要在内存中构建完整消息体
    // Note: multipart form requires the entire message body in memory before sending
    eprintln!("[内存检查] / [Memory Check]: 准备读取 {} MB 文件", file_size / 1024 / 1024);
    
    let file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| format!("Failed to open file: {}", e))?;
    
    // 使用较小的缓冲区逐步读取，而不是一次性分配 / Read in small chunks rather than allocating all at once
    let mut reader = tokio::io::BufReader::with_capacity(5 * 1024 * 1024, file); // 5MB buffer
    let mut file_bytes = Vec::new();
    let mut buffer = [0u8; 1024 * 1024]; // 1MB read buffer
    
    eprintln!("[开始读取] / [Start Reading]: 使用流式缓冲读取");
    let start_read = Instant::now();
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                file_bytes.extend_from_slice(&buffer[..n]);
                if file_bytes.len() % (100 * 1024 * 1024) == 0 || file_bytes.len() == file_size as usize {
                    let elapsed = start_read.elapsed().as_secs();
                    let speed_mb_s = if elapsed > 0 { file_bytes.len() / 1024 / 1024 / elapsed as usize } else { 0 };
                    eprintln!("[读取进度] / [Read Progress]: {} / {} MB ({} MB/s)",
                        file_bytes.len() / 1024 / 1024,
                        file_size / 1024 / 1024,
                        speed_mb_s);
                }
            }
            Err(e) => {
                eprintln!("[读取失败] / [Read Failed]: {}", e);
                eprintln!("[已读大小] / [Read So Far]: {} MB", file_bytes.len() / 1024 / 1024);
                return Err(format!("Failed to read file: {} (read {} MB before failure)", 
                    e, file_bytes.len() / 1024 / 1024));
            }
        }
    }
    
    eprintln!("[读取完成] / [Read Complete]: {} MB in {} seconds",
        file_bytes.len() / 1024 / 1024,
        start_read.elapsed().as_secs());
    
    let mime_type = detect_mime_type(file_path);
    let file_part = reqwest::multipart::Part::bytes(file_bytes)
        .file_name(filename.clone())
        .mime_str(mime_type)
        .map_err(|e| format!("Failed to set mime type: {}", e))?;
    form = form.part("file", file_part);

    eprintln!("[连接信息] / [Connection Info]: POST {}", upload_url);
    
    let response = client
        .post(&upload_url)
        .bearer_auth(api_token)
        .multipart(form)
        .send()
        .await
        .map_err(|e| {
            let err_msg = e.to_string();
            eprintln!("[连接错误详情] / [Connection Error Details]:");
            eprintln!("  错误类型 / Error type: {}", if e.is_connect() { "连接失败" } else if e.is_timeout() { "超时" } else if e.is_status() { "HTTP状态" } else { "其他" });
            eprintln!("  错误信息 / Error message: {}", err_msg);
            eprintln!("  诊断 / Diagnosis: 检查 TeleCloud 服务是否在线，网络连接是否正常");
            format!("Upload request failed: {}", err_msg)
        })?;

    let status = response.status();
    let body = response.text().await
        .map_err(|e| format!("Failed to read response body: {}", e))?;

    if !status.is_success() {
        return Err(format!("Upload failed with status {}: {}", status, body));
    }

    let upload_resp: UploadResponse = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if !upload_resp.error.is_empty() {
        return Err(format!("API error: {}", upload_resp.error));
    }

    // 异步上传: 返回 task_id，需要后续查询进度
    if upload_resp.status.to_lowercase() == "processing" && !upload_resp.task_id.is_empty() {
        eprintln!("[异步响应] / [Async response] Task ID: {}", upload_resp.task_id);
    }

    Ok(upload_resp)
}

/// 上传覆盖图（如果存在）
async fn upload_cover(
    client: &reqwest::Client,
    api_url: &str,
    api_token: &str,
    cover_path: &Path,
    remote_path: &str,
) -> Result<Option<String>, String> {
    eprintln!("[上传覆盖图] / [Upload Cover Image]: {:?}", cover_path.file_name());
    
    let filename = cover_path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("cover.jpg")
        .to_string();

    let upload_url = format!("{}/api/upload-api/upload", api_url.trim_end_matches('/'));
    let cover_bytes = tokio::fs::read(cover_path)
        .await
        .map_err(|e| format!("Failed to read cover file: {}", e))?;
    let form = reqwest::multipart::Form::new()
        .text("path", remote_path.to_string())
        .part("file", reqwest::multipart::Part::bytes(cover_bytes)
            .file_name(filename.clone())
            .mime_str("image/jpeg")
            .map_err(|e| format!("Failed to set mime type: {}", e))?);

    let response = client
        .post(&upload_url)
        .bearer_auth(api_token)
        .multipart(form)
        .send()
        .await
        .map_err(|e| {
            eprintln!("[覆盖图上传请求失败] / [Cover upload request failed]: {}", e);
            format!("Cover upload request failed: {}", e)
        })?;

    let status = response.status();
    let body = response.text().await
        .map_err(|e| format!("Failed to read cover response body: {}", e))?;

    if !status.is_success() {
        eprintln!("[覆盖图上传失败] / [Cover upload failed]: {}", status);
        return Ok(None);
    }

    let upload_resp: UploadResponse = serde_json::from_str(&body)
        .map_err(|e| {
            eprintln!("[覆盖图解析失败] / [Failed to parse cover response]: {}", e);
            format!("Failed to parse cover response: {}", e)
        })?;

    if !upload_resp.error.is_empty() {
        eprintln!("[覆盖图 API 错误] / [Cover API error]: {}", upload_resp.error);
        return Ok(None);
    }

    eprintln!("[覆盖图上传成功] / [Cover uploaded successfully]");
    
    // 返回 cover path 供后续删除 / Return cover path for later deletion
    Ok(Some(cover_path.display().to_string()))
}

/// 等待异步上传完成
async fn cancel_upload_task(
    client: &reqwest::Client,
    api_url: &str,
    api_token: &str,
    task_id: &str,
) -> Result<(), String> {
    let cancel_url = format!(
        "{}/api/upload-api/tasks/{}",
        api_url.trim_end_matches('/'),
        task_id
    );

    let response = client
        .delete(&cancel_url)
        .bearer_auth(api_token)
        .send()
        .await
        .map_err(|e| format!("Failed to cancel task {}: {}", task_id, e))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read cancel response for task {}: {}", task_id, e))?;

    if !status.is_success() {
        return Err(format!(
            "Cancel task {} failed with status {}: {}",
            task_id, status, body
        ));
    }

    if !body.trim().is_empty() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
            if json.get("status").and_then(|v| v.as_str()) == Some("cancelled") {
                eprintln!("[任务已取消] / [Task Cancelled]: {}", task_id);
                return Ok(());
            }
            if let Some(error) = json.get("error").and_then(|v| v.as_str()) {
                return Err(format!("Cancel task {} API error: {}", task_id, error));
            }
        }
    }

    eprintln!("[任务取消请求已发送] / [Task cancel request sent]: {}", task_id);
    Ok(())
}

async fn wait_async_upload(
    client: &reqwest::Client,
    api_url: &str,
    api_token: &str,
    task_id: &str,
    max_wait: u64,
) -> Result<UploadResponse, String> {
    let start = Instant::now();
    let check_url = format!("{}/api/upload-api/tasks/{}", api_url.trim_end_matches('/'), task_id);

    let mut last_progress = 0u32;  // 记录上次进度，避免重复输出 / Track last progress to avoid duplicate logs
    let mut last_status_logged = 0u32;  // 记录上次状态日志进度 / Track last status log percentage
    let mut low_speed_since: Option<Instant> = None;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;  // 改为 1 秒查询一次，加快 UI 更新 / Changed to 1s for faster UI

        let response = client
            .get(&check_url)
            .bearer_auth(api_token)
            .send()
            .await
            .map_err(|e| {
                eprintln!("[进度查询失败] / [Progress query failed]: {}", e);
                format!("Failed to check progress: {}", e)
            })?;

        let body = response.text().await
            .map_err(|e| format!("Failed to read progress response: {}", e))?;

        // 解析进度 / Parse progress
        let progress: TaskProgress = serde_json::from_str(&body)
            .map_err(|e| {
                eprintln!("[错误] / [Error] 解析失败 / Parse failed: {}", e);
                format!("Failed to parse progress: {} (response: {})", e, body)
            })?;

        // 获取消息字段（处理 msg 或 message）
        let msg = if !progress.msg.is_empty() {
            &progress.msg
        } else {
            &progress.message
        };

        let progress_moved = progress.percent != last_progress;

        // 只在进度改变时输出日志，减少输出 / Only log when progress changes
        if progress_moved {
            eprintln!("[进度] / [Progress]: {} - {} - {} MB / {} MB",
                progress.status,
                msg,
                progress.uploaded / 1024 / 1024,
                progress.size / 1024 / 1024
            );
            last_progress = progress.percent;
        }

        // 输出进度到 stdout，供 Web UI 读取 / Output progress to stdout for Web UI
        // 按 10000 scale 输出进度（protocol spec） / Scale to 10000 (protocol compliance)
        let done = if progress.size > 0 {
            ((progress.uploaded as f64 / progress.size as f64) * 10000.0) as u32
        } else {
            0
        };
        println!("PROGRESS:{}/10000", done);
        
        // 输出上传速度到 stdout（使用 API 返回的实际速度） / Output upload speed from API
        if progress.speed > 0 {
            let speed_mb_s = progress.speed as f64 / 1024.0 / 1024.0;
            println!("STATUS:{:.2} MB/s", speed_mb_s);
        }

        // 规范化 status (处理大小写)
        let status_lower = progress.status.to_lowercase();
        let is_active_upload_status =
            status_lower == "processing"
                || status_lower == "downloading"
                || status_lower == "uploading"
                || status_lower == "telegram";

        if is_active_upload_status && progress.speed > 0 && progress.speed < LOW_SPEED_THRESHOLD_BYTES_PER_SEC {
            if progress_moved {
                low_speed_since = None;
            } else if let Some(since) = low_speed_since {
                let stalled_for = since.elapsed();
                if stalled_for >= LOW_SPEED_ABORT_AFTER {
                    let speed_mb_s = progress.speed as f64 / 1024.0 / 1024.0;
                    eprintln!(
                        "[低速中止] / [Low-speed abort]: {:.2} MB/s 持续 {} 秒，准备取消远程任务并触发重试 / persisted for {} seconds, cancelling remote task and retrying",
                        speed_mb_s,
                        LOW_SPEED_ABORT_AFTER.as_secs(),
                        LOW_SPEED_ABORT_AFTER.as_secs()
                    );
                    match cancel_upload_task(client, api_url, api_token, task_id).await {
                        Ok(()) => {
                            eprintln!(
                                "[低速中止] / [Low-speed abort]: 远程任务已取消 / remote task cancelled: {}",
                                task_id
                            );
                        }
                        Err(cancel_err) => {
                            eprintln!(
                                "[低速中止] / [Low-speed abort]: 取消远程任务失败 / failed to cancel remote task: {}",
                                cancel_err
                            );
                        }
                    }
                    return Err(format!(
                        "{} Upload speed stayed below 1.00 MB/s for {} seconds (current: {:.2} MB/s); remote task cancel requested",
                        RETRY_REASON_LOW_SPEED,
                        LOW_SPEED_ABORT_AFTER.as_secs(),
                        speed_mb_s
                    ));
                }
            } else {
                low_speed_since = Some(Instant::now());
                let speed_mb_s = progress.speed as f64 / 1024.0 / 1024.0;
                eprintln!(
                    "[低速监控] / [Low-speed watch]: {:.2} MB/s，若持续 {} 秒将自动重试 / will retry if it persists for {} seconds",
                    speed_mb_s,
                    LOW_SPEED_ABORT_AFTER.as_secs(),
                    LOW_SPEED_ABORT_AFTER.as_secs()
                );
            }
        } else {
            low_speed_since = None;
        }
        
        if status_lower == "done" || status_lower == "completed" {
            return Ok(UploadResponse {
                status: "done".to_string(),
                filename: progress.filename,
                file_id: progress.file_id,
                ..Default::default()
            });
        }

        // 允许 processing / downloading / uploading / telegram 状态继续等待
        if status_lower == "processing" || status_lower == "downloading" || status_lower == "uploading" || status_lower == "telegram" {
            // 只当进度增加 5% 时输出状态更新 / Only log when progress increases by 5%
            if progress.percent >= last_status_logged + 5 {
                eprintln!("[等待] / [Waiting]: {} ({}%) ETA: {}s", status_lower, progress.percent, progress.eta);
                last_status_logged = progress.percent;  // 更新已记录的状态日志进度 / Update logged status percentage
            }
        } else if status_lower == "failed" || status_lower == "cancelled" || !progress.error.is_empty() {
            let error_msg = if !progress.error.is_empty() { 
                &progress.error 
            } else { 
                msg 
            };
            return Err(format!("Upload failed: {}", error_msg));
        }

        if start.elapsed().as_secs() > max_wait {
            eprintln!("[超时] / [Timeout]: 上传超过 {} 秒 / Upload exceeded {} seconds", max_wait, max_wait);
            match cancel_upload_task(client, api_url, api_token, task_id).await {
                Ok(()) => {
                    eprintln!(
                        "[超时] / [Timeout]: 远程任务已取消 / remote task cancelled: {}",
                        task_id
                    );
                }
                Err(cancel_err) => {
                    eprintln!(
                        "[超时] / [Timeout]: 取消远程任务失败 / failed to cancel remote task: {}",
                        cancel_err
                    );
                }
            }
            return Err(format!(
                "{} Upload timeout after {} seconds; remote task cancel requested",
                RETRY_REASON_TIMEOUT,
                max_wait
            ));
        }
    }
}

/// 创建分享链接
#[allow(dead_code)]
async fn create_share_link(
    client: &reqwest::Client,
    api_url: &str,
    api_token: &str,
    file_path: &str,
) -> Result<ShareResponse, String> {
    let share_url = format!("{}/api/upload-api/share", api_url.trim_end_matches('/'));
    
    let body = serde_json::json!({
        "path": file_path
    });

    let response = client
        .post(&share_url)
        .bearer_auth(api_token)
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            eprintln!("[分享链接请求失败] / [Share link request failed]: {}", e);
            format!("Failed to create share link: {}", e)
        })?;

    let status = response.status();
    let body = response.text().await
        .map_err(|e| format!("Failed to read share response: {}", e))?;

    if !status.is_success() {
        return Err(format!("Share link creation failed: {}", status));
    }

    let share_resp: ShareResponse = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse share response: {}", e))?;

    if !share_resp.error.is_empty() {
        return Err(format!("API error: {}", share_resp.error));
    }

    Ok(share_resp)
}

/// 获取任务状态文件路径 / Get task state file path
fn get_state_file(file_path: &Path) -> PathBuf {
    // 在 data 文件夹中保存状态文件 / Save state file in data folder
    let data_dir = PathBuf::from("data");
    if !data_dir.exists() {
        let _ = fs::create_dir_all(&data_dir);
    }
    
    let file_stem = file_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("task");
    
    data_dir.join(format!("{}.upload_state.json", file_stem))
}

/// 保存任务状态 / Save task state
fn save_task_state(task_state: &UploadTaskState) -> Result<(), String> {
    let state_file = get_state_file(Path::new(&task_state.file_path));
    let json = serde_json::to_string(task_state)
        .map_err(|e| format!("Failed to serialize task state: {}", e))?;
    fs::write(&state_file, json)
        .map_err(|e| format!("Failed to write task state file: {}", e))?;
    eprintln!("[任务状态] / [Task State] 已保存: {}", state_file.display());
    Ok(())
}

/// 加载任务状态 / Load task state
fn load_task_state(file_path: &Path) -> Result<Option<UploadTaskState>, String> {
    let state_file = get_state_file(file_path);
    if !state_file.exists() {
        return Ok(None);
    }
    
    let json = fs::read_to_string(&state_file)
        .map_err(|e| format!("Failed to read task state file: {}", e))?;
    let state: UploadTaskState = serde_json::from_str(&json)
        .map_err(|e| format!("Failed to parse task state: {}", e))?;
    
    eprintln!("[任务状态] / [Task State] 已加载: task_id={}", state.task_id);
    Ok(Some(state))
}

/// 删除任务状态文件 / Delete task state file
fn delete_task_state(file_path: &Path) -> Result<(), String> {
    let state_file = get_state_file(file_path);
    if state_file.exists() {
        fs::remove_file(&state_file)
            .map_err(|e| format!("Failed to delete task state file: {}", e))?;
        eprintln!("[任务状态] / [Task State] 已删除: {}", state_file.display());
    }
    Ok(())
}

/// 测试 API 连接
async fn test_api_connection(
    client: &reqwest::Client,
    api_url: &str,
) -> Result<(), String> {
    let health_url = format!("{}/api/upload-api", api_url.trim_end_matches('/'));
    eprintln!("[连接测试] / [Connection Test]: {}", health_url);
    
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.get(&health_url).send()
    ).await {
        Ok(Ok(_)) => {
            eprintln!("[连接成功] / [Connection OK]");
            Ok(())
        }
        Ok(Err(e)) => {
            eprintln!("[连接失败] / [Connection Failed]: {}", e);
            Err(format!("Cannot connect to TeleCloud API at {}: {}", api_url, e))
        }
        Err(_) => {
            eprintln!("[连接超时] / [Connection Timeout]");
            Err(format!("Connection timeout to TeleCloud API at {} (check if service is running)", api_url))
        }
    }
}

/// 检查文件是否为图片 / Check if file is an image
fn is_image_file(file_path: &Path) -> bool {
    let image_exts = ["jpg", "jpeg", "png", "webp", "gif", "bmp"];
    file_path.extension()
        .and_then(|s| s.to_str())
        .map(|ext| image_exts.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// 尝试在相同目录找到视频文件（如果输入是图片）/ Try to find video file in same directory if input is image
fn find_video_file(image_path: &Path) -> Option<PathBuf> {
    let video_exts = ["mp4", "ts", "mkv", "avi", "mov", "flv"];
    let stem = image_path.file_stem()?.to_str()?;
    let dir = image_path.parent()?;
    
    for ext in &video_exts {
        let video_path = dir.join(format!("{}.{}", stem, ext));
        if video_path.exists() {
            eprintln!("[自动检测] / [Auto-detect] 找到视频文件 / Found video file: {}", video_path.display());
            return Some(video_path);
        }
    }
    
    None
}

#[allow(dead_code)]
fn parse_media_bundle(bundle: &str) -> (PathBuf, Option<PathBuf>) {
    if let Some(sep) = bundle.find('\n') {
        let video = PathBuf::from(&bundle[..sep]);
        let image_str = bundle[sep + 1..].trim();
        let image = if image_str.is_empty() {
            None
        } else {
            Some(PathBuf::from(image_str))
        };
        (video, image)
    } else {
        (PathBuf::from(bundle), None)
    }
}

/// 灵活解析输入：支持 media_bundle（newline 分隔）和普通单文件
/// 兼容 any_file 输入类型，处理来自任何前驱节点的数据
/// 
/// Flexibly parse input: supports media_bundle (newline-separated) and single file.
/// Compatible with any_file input type, handles data from any predecessor node.
fn split_and_validate_input(raw: &str) -> Result<(PathBuf, Option<PathBuf>), String> {
    let paths: Vec<PathBuf> = raw
        .split('\n')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .collect();
    
    if paths.is_empty() {
        return Err("Input is empty".to_string());
    }
    
    let video_path = paths[0].clone();
    let image_path = if paths.len() > 1 { Some(paths[1].clone()) } else { None };
    
    // 验证主文件存在性 / Validate primary file exists
    if !video_path.exists() {
        return Err(format!("Input file not found: {}", video_path.display()));
    }
    
    Ok((video_path, image_path))
}

fn emit_status_signal(kind: &str, value: &str) -> String {
    format!("{}:{}", kind, value)
}

fn output_value_for_pipeline(bundle_input: &str, input: &Path) -> String {
    if bundle_input.contains('\n') {
        bundle_input.to_string()
    } else {
        input.to_string_lossy().to_string()
    }
}

/// 主函数
fn is_retryable_wait_error(err: &str) -> bool {
    err.contains(RETRY_REASON_LOW_SPEED) || err.contains(RETRY_REASON_TIMEOUT)
}

fn clean_retry_error_message(err: &str) -> String {
    err.replace(RETRY_REASON_LOW_SPEED, "")
        .replace(RETRY_REASON_TIMEOUT, "")
        .trim()
        .to_string()
}

fn resolve_server_port() -> u16 {
    if let Ok(p) = env::var("PORT") {
        if let Ok(port) = p.trim().parse::<u16>() {
            return port;
        }
    }

    if let Ok(exe_dir) = env::var("PP_EXE_DIR") {
        let settings_path = Path::new(&exe_dir).join("config").join("settings.json");
        if let Ok(content) = fs::read_to_string(&settings_path) {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(port) = json.get("server_port").and_then(|v| v.as_u64()) {
                    if port > 0 && port <= u16::MAX as u64 {
                        return port as u16;
                    }
                }
            }
        }
    }

    3031
}

fn request_postprocess_restart(path: &Path) -> Result<(), String> {
    let port = resolve_server_port();
    let addr = format!("127.0.0.1:{}", port);
    let body = serde_json::json!({
        "path": path.to_string_lossy()
    })
    .to_string();

    let mut stream = TcpStream::connect(&addr).map_err(|e| format!("connect {}: {}", addr, e))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .ok();

    let request = format!(
        "POST /api/recordings/postprocess HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        addr = addr,
        len = body.len(),
        body = body
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write restart request: {}", e))?;
    stream.flush().ok();

    let mut response = String::new();
    stream.read_to_string(&mut response).ok();
    let status_line = response.lines().next().unwrap_or("");
    if status_line.contains(" 2") {
        Ok(())
    } else if status_line.is_empty() {
        Err("no response from host".to_string())
    } else {
        Err(format!("host returned: {}", status_line))
    }
}

async fn run() -> Result<(), String> {
    let module_input = ModuleInput::read();
    let bundle_input = module_input
        .first_input()
        .ok_or_else(|| "inputs[0] is required".to_string())?;

    // 使用灵活的输入解析，兼容 any_file 输入类型 / Use flexible input parsing compatible with any_file
    let (mut input, cover_from_bundle) = split_and_validate_input(&bundle_input.to_string_lossy())?;

    let cover_path = cover_from_bundle.or_else(|| find_cover(&input));

    // 如果输入是图片，自动查找相同目录下的视频文件 / If input is image, auto-find video in same directory
    if is_image_file(&input) {
        eprintln!("[警告] / [Warning]: 输入文件是图片格式 / Input file is image format: {}", input.display());
        if let Some(video_path) = find_video_file(&input) {
            eprintln!("[切换] / [Switch] 改为上传视频 / Switching to upload video: {}", video_path.display());
            input = video_path;
        } else {
            eprintln!("[错误] / [Error] 无法找到相同目录下的视频文件 / Cannot find video file in same directory");
            eprintln!("[诊断] / [Diagnosis] contact_sheet 或其他模块可能输出了错误的文件类型");
            return Err(format!("Input is image file but no video found in same directory: {}", input.display()));
        }
    }

    // 检查是否有未完成的上传任务可以恢复 / Check if there's an incomplete upload task to resume
    if let Ok(Some(task_state)) = load_task_state(&input) {
        eprintln!("[恢复任务] / [Resume Task]: 检测到未完成的上传任务");
        eprintln!("  Task ID: {}", task_state.task_id);
        
        // 读取参数以获取 API 信息 / Read params to get API info
        let api_url = module_input.param_str("api_url", &task_state.api_url);
        let api_token = module_input.param_str("api_token", "");
        if api_token.is_empty() {
            return Err("api_token is required".to_string());
        }
        
        // 获取超时参数 / Get timeout param
        let timeout_param: u64 = module_input.param_str("timeout_seconds", "0")
            .parse()
            .unwrap_or(0);
        
        let timeout = if timeout_param > 0 {
            timeout_param
        } else {
            let min_kb_per_sec = 100u64;
            let calculated = (task_state.file_size / 1024 / min_kb_per_sec) + 300;
            calculated.clamp(300, 3600)
        };
        
        // 构建 HTTP 客户端 / Build HTTP client
        let proxy = module_input.param_str("proxy", "");
        let client = build_client(&proxy, timeout)?;
        
        eprintln!("[恢复上传] / [Resuming Upload]: 继续等待任务完成...");
        
        match wait_async_upload(&client, &api_url, &api_token, &task_state.task_id, timeout).await {
            Ok(_) => {
                eprintln!("[恢复成功] / [Resume Successful]: 任务已完成");
                let _ = delete_task_state(&input);
                eprintln!("[恢复成功] / [Resume Successful]: {}", emit_status_signal("OUTPUT", &input.display().to_string()));
                return Ok(());
            }
            Err(e) => {
                eprintln!("[恢复失败] / [Resume Failed]: {}", e);
                // 如果恢复失败，继续正常流程重新上传 / If resume fails, continue with normal upload
            }
        }
    }

    // 读取参数
    let api_url = module_input.param_str("api_url", "");
    if api_url.is_empty() {
        return Err("api_url is required".to_string());
    }

    let api_token = module_input.param_str("api_token", "");
    if api_token.is_empty() {
        return Err("api_token is required".to_string());
    }

    let remote_path = module_input.param_str("remote_path", "/");
    let organize_by_model = module_input.param_bool("organize_by_model", true);
    let async_upload = module_input.param_bool("async_upload", false);
    let auto_share = module_input.param_bool("auto_share", false);
    let share_type = module_input.param_str("share_type", "public");
    let overwrite = module_input.param_bool("overwrite", false);
    let proxy = module_input.param_str("proxy", "");
    let should_upload_cover = module_input.param_bool("upload_cover", true);
    let delete_after_upload = module_input.param_bool("delete_after_upload", false);
    
    let max_retries: u32 = module_input.param_str("max_retries", "3")
        .parse()
        .unwrap_or(3);
    let timeout_param: u64 = module_input.param_str("timeout_seconds", "0")
        .parse()
        .unwrap_or(0);

    // 获取文件信息
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("recording");
    let (model_name, timestamp) = parse_stem(stem);
    let duration = video_duration(&input).unwrap_or(0.0);
    let file_size = fs::metadata(&input).map(|m| m.len()).unwrap_or(0);
    
    // 构建最终的远程路径 / Build final remote path
    let final_remote_path = if organize_by_model && !model_name.is_empty() {
        // 格式化路径：确保前后都有 /
        let base = remote_path.trim_end_matches('/').to_string();
        format!("{}/{}/", base, model_name)
    } else {
        // 确保尾部有 /
        if remote_path.ends_with('/') {
            remote_path.to_string()
        } else {
            format!("{}/", remote_path)
        }
    };
    
    // 自动计算超时时间：根据文件大小，假设最慢 50KB/s（Telegram可能很慢）/ Auto-calculate timeout
    // 公式: file_size_kb / min_speed_kb_s + buffer
    let timeout = if timeout_param > 0 {
        timeout_param
    } else {
        let min_kb_per_sec = 50u64;  // 保守估计50KB/s（包括Telegram阶段）/ Conservative 50KB/s for Telegram
        let file_size_kb = file_size / 1024;
        let calculated = (file_size_kb / min_kb_per_sec) + 600; // 600s buffer (10min)
        calculated.clamp(600, 7200) // min 10min, max 2hours
    };

    eprintln!("[TeleCloud API 上传] / [TeleCloud API Upload]");
    eprintln!("  主播 / Model: {}", model_name);
    eprintln!("  时间 / Time: {}", if timestamp.is_empty() { "—".to_string() } else { timestamp });
    eprintln!("  时长 / Duration: {}", format_duration(duration));
    eprintln!("  大小 / Size: {}", format_bytes(file_size));
    eprintln!("  目录 / Path: {} {}", final_remote_path, if organize_by_model { "(organized by model)" } else { "" });
    eprintln!("  [提示] / [Note]: 上传速度受 TeleCloud 后端限制（特别是 Telegram 上传阶段）");
    eprintln!("         Upload speed is limited by TeleCloud backend (especially Telegram upload phase)");

    // 构建 HTTP 客户端
    let client = build_client(&proxy, timeout)?;

    // 测试 API 连接 / Test API connection before uploading
    if let Err(e) = test_api_connection(&client, &api_url).await {
        eprintln!("[警告] / [Warning]: {}", e);
        eprintln!("[继续尝试] / [Attempting anyway]...");
    }

    // 上传文件
    let mut upload_result = None;
    let mut requeue_requested = false;
    for attempt in 1..=max_retries {
        eprintln!("[上传尝试] / [Upload Attempt] {}/{}", attempt, max_retries);
        eprintln!("  超时 / Timeout: {}s", timeout);
        
        match upload_file(UploadFileRequest {
            client: &client,
            api_url: &api_url,
            api_token: &api_token,
            file_path: &input,
            remote_path: &final_remote_path,
            auto_share,
            share_type: &share_type,
            overwrite,
            async_upload,
        })
        .await {
            Ok(resp) => {
                // 如果是异步上传，等待完成
                if async_upload && !resp.task_id.is_empty() {
                    eprintln!("[异步上传] / [Async Upload] Task ID: {}", resp.task_id);
                    eprintln!("[状态] / [Status] {}", emit_status_signal("TASK_ID", &resp.task_id));
                    
                    // 保存任务状态以便在容器重启时恢复 / Save task state for recovery on container restart
                    let task_state = UploadTaskState {
                        task_id: resp.task_id.clone(),
                        file_path: input.display().to_string(),
                        api_url: api_url.clone(),
                        file_size,
                        created_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        updated_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    };
                    if let Err(e) = save_task_state(&task_state) {
                        eprintln!("[警告] / [Warning]: {}", e);
                    }
                    
                    match wait_async_upload(&client, &api_url, &api_token, &resp.task_id, timeout).await {
                        Ok(final_resp) => {
                            // 上传完成，删除状态文件 / Upload complete, delete state file
                            let _ = delete_task_state(&input);
                            upload_result = Some(final_resp);
                            break;
                        }
                        Err(e) => {
                            let is_retryable = is_retryable_wait_error(&e);
                            let display_error = clean_retry_error_message(&e);
                            eprintln!("[等待失败] / [Wait failed]: {}", display_error);

                            // 当前远程任务已经结束/已请求取消，不应保留旧 state 影响下一次重试
                            let _ = delete_task_state(&input);

                            if is_retryable && !requeue_requested {
                                match request_postprocess_restart(&input) {
                                    Ok(()) => {
                                        requeue_requested = true;
                                        eprintln!(
                                            "[重新排队] / [Re-enqueue]: 已通过 host API 重新触发当前文件的后处理 / post-process re-triggered for current file"
                                        );
                                    }
                                    Err(requeue_err) => {
                                        eprintln!(
                                            "[重新排队失败] / [Re-enqueue Failed]: {}",
                                            requeue_err
                                        );
                                    }
                                }
                            }

                            if attempt >= max_retries {
                                return Err(display_error);
                            }

                            let backoff_secs = if is_retryable {
                                3
                            } else if attempt <= 2 {
                                attempt as u64 * 5
                            } else {
                                20
                            };

                            if is_retryable {
                                eprintln!(
                                    "[自动重试] / [Auto Retry]: 检测到低速/超时，{}s 后重新执行上传后处理 / low-speed or timeout detected, restarting post-processing upload in {}s",
                                    backoff_secs,
                                    backoff_secs
                                );
                            } else {
                                eprintln!("[重试] / [Retry] {}s 后重试...", backoff_secs);
                            }

                            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                            continue;
                        }
                    }
                } else {
                    upload_result = Some(resp);
                    break;
                }
            }
            Err(e) => {
                eprintln!("[上传失败] / [Upload failed]: {}", e);
                if attempt >= max_retries {
                    return Err(e);
                }
                // 优化退避：连接错误立即重试，其他错误等待 / Optimized backoff: immediate retry on connection errors
                let backoff_secs = if e.contains("error sending request") || e.contains("Connect") || e.contains("timeout") {
                    5  // 连接错误：快速重试 / Connection error: quick retry
                } else {
                    attempt as u64 * 5  // 其他错误：逐步增加等待时间 / Other errors: gradual backoff
                };
                eprintln!("[重试] / [Retry] {}s 后重试...", backoff_secs);
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            }
        }
    }

    let upload_result = upload_result
        .ok_or_else(|| "Upload failed after all retries".to_string())?;

    eprintln!("[上传完成] / [Upload completed]");
    eprintln!("  文件名 / Filename: {}", upload_result.filename);
    eprintln!("  路径 / Path: {}", upload_result.path);
    eprintln!("  File ID: {}", upload_result.file_id);

    // 并行上传覆盖图（不阻塞主流程）/ Upload cover in parallel (non-blocking)
    let cover_future = if should_upload_cover {
        let client = client.clone();
        let api_url = api_url.clone();
        let api_token = api_token.clone();
        let input_clone = input.clone();
        let final_remote_path = final_remote_path.clone();
        let check_cover = cover_path.clone().or_else(|| find_cover(&input_clone));
        Some(tokio::spawn(async move {
            if let Some(cover) = check_cover {
                match upload_cover(&client, &api_url, &api_token, &cover, &final_remote_path).await {
                    Ok(Some(cover_path_str)) => {
                        Some(PathBuf::from(cover_path_str))
                    }
                    Ok(None) => {
                        None
                    }
                    Err(e) => {
                        eprintln!("[覆盖图上传失败] / [Cover upload failed]: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        }))
    } else {
        None
    };

    // 输出分享链接
    if auto_share && !upload_result.share_link.is_empty() {
        eprintln!("[分享链接] / [Share Link]: {}", upload_result.share_link);
        eprintln!("[状态] / [Status] {}", emit_status_signal("SHARE_LINK", &upload_result.share_link));
    }

    // 等待覆盖图上传完成（如果启用）/ Wait for cover upload to complete
    let mut files_to_delete = Vec::new();
    
    if delete_after_upload {
        files_to_delete.push(input.clone());
    }
    
    if let Some(cover_future) = cover_future {
        match cover_future.await {
            Ok(Some(cover_path)) => {
                eprintln!("[覆盖图上传成功] / [Cover uploaded successfully]");
                if delete_after_upload {
                    files_to_delete.push(cover_path);
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("[覆盖图任务失败] / [Cover task failed]: {}", e),
        }
    }

    // 批量删除本地文件 / Batch delete local files
    if !files_to_delete.is_empty() {
        eprintln!("[删除文件] / [Deleting {} files]", files_to_delete.len());
        for file_path in files_to_delete {
            if file_path.exists() {
                if let Err(e) = fs::remove_file(&file_path) {
                    eprintln!("  ✗ {} - {}", file_path.file_name().unwrap_or_default().to_string_lossy(), e);
                } else {
                    eprintln!("  ✓ {}", file_path.file_name().unwrap_or_default().to_string_lossy());
                }
            }
        }
        eprintln!("[删除完成] / [Deletion completed]");
    }
    
    let output_value = output_value_for_pipeline(&bundle_input.to_string_lossy(), &input);
    let message = if auto_share && !upload_result.share_link.is_empty() {
        format!("Upload completed successfully: {}", upload_result.share_link)
    } else {
        "Upload completed successfully".to_string()
    };
    output_ok(&[&output_value], &message);

    // 上传成功，删除状态文件 / Upload successful, delete state file
    if let Err(e) = delete_task_state(&input) {
        eprintln!("[警告] / [Warning] 删除状态文件失败 / Failed to delete state file: {}", e);
    }

    Ok(())
}

/// 程序入口
#[inline(never)]
fn execute() {
    let worker = std::thread::Builder::new()
        .name("telecloud_upload".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to create Tokio runtime for telecloud_upload");
            rt.block_on(run())
        })
        .expect("failed to start telecloud_upload execution thread");

    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("telecloud_upload execution thread panicked");
            std::process::exit(1);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("--describe") {
        print!("{}", describe_with_version(DESCRIBE, env!("CARGO_PKG_VERSION")));
        return;
    }

    execute();
}

#[cfg(test)]
mod tests {
    use super::{output_value_for_pipeline, parse_media_bundle};
    use std::path::Path;

    #[test]
    fn parse_media_bundle_keeps_video_and_cover() {
        let bundle = "/tmp/video.mp4\n/tmp/cover.webp";
        let (video, cover) = parse_media_bundle(bundle);

        assert_eq!(video, Path::new("/tmp/video.mp4"));
        assert_eq!(cover, Some(Path::new("/tmp/cover.webp").to_path_buf()));
    }

    #[test]
    fn output_value_keeps_bundle_for_pipeline() {
        let bundle = "/tmp/video.mp4\n/tmp/cover.webp";
        let input = Path::new("/tmp/video.mp4");

        assert_eq!(output_value_for_pipeline(bundle, input), bundle);
        assert_eq!(output_value_for_pipeline("/tmp/video.mp4", input), "/tmp/video.mp4");
    }

    #[test]
    fn status_signals_are_compatible_with_json_protocol() {
        assert_eq!(super::emit_status_signal("TASK_ID", "abc123"), "TASK_ID:abc123");
        assert_eq!(super::emit_status_signal("SHARE_LINK", "https://example.test"), "SHARE_LINK:https://example.test");
    }
}
