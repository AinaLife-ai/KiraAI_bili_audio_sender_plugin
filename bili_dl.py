import asyncio
import hashlib
import json
import re
import sys
import time
from pathlib import Path

import httpx

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REFERER = "https://www.bilibili.com"
HEADERS = {"User-Agent": UA, "Referer": REFERER}
BV_RE = re.compile(r"BV[0-9A-Za-z]{10}")
SHORT_RE = re.compile(r"https?://b23\.tv/[0-9A-Za-z]+")
API_BASE = "https://api.bilibili.com"


class BiliError(Exception):
    pass


def _clean(title: str, n: int = 30) -> str:
    return re.sub(r"[\\/:*?\"<>|]", "_", title)[:n]


_ERR_HINTS = {
    -404: "视频不存在或已下架，请换一个候选",
    -403: "该视频无访问权限（可能设置了区域/登录限制）",
    -412: "B站风控拦截，建议在插件配置里填写B站Cookie后重试",
    -400: "请求参数错误",
}


def _api_err(code, raw_msg: str = "") -> str:
    """把 B 站错误码翻译成可读提示，方便 LLM 直接决策（换候选/填Cookie等）。"""
    hint = _ERR_HINTS.get(code)
    if hint:
        return f"{hint}（code={code}）"
    return f"B站接口返回错误（code={code} {raw_msg}）".strip()


def _net_err(e: Exception) -> str:
    """把 httpx 网络异常翻译成可读提示，LLM 可直接决策。"""
    if isinstance(e, httpx.ConnectError):
        return "无法连接B站服务器（网络不通或系统代理异常）"
    if isinstance(e, httpx.ConnectTimeout):
        return "连接B站服务器超时"
    if isinstance(e, httpx.ReadTimeout):
        return "读取B站响应超时"
    if isinstance(e, httpx.HTTPStatusError):
        return f"HTTP {e.response.status_code}"
    return str(e)


def _client(cookie: str = "", timeout: float = 60.0) -> httpx.AsyncClient:
    headers = dict(HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    # trust_env=False：不读系统/环境代理，B站国内服务直连。
    # 否则 Windows 系统代理（如 127.0.0.1:7897）开着但代理软件未启动时，
    # 所有请求都会 ConnectError（曾导致 tool failed）。
    return httpx.AsyncClient(headers=headers, follow_redirects=True,
                             timeout=timeout, trust_env=False)


async def _get_json(client: httpx.AsyncClient, url: str, params: dict | None = None) -> dict:
    try:
        resp = await client.get(url, params=params)
    except httpx.HTTPError as e:
        raise BiliError(f"网络请求失败：{_net_err(e)}") from e
    # 不 raise_for_status：B站 412 等风控会带 JSON body（code=-412），
    # 直接读 body 才能走 _api_err 翻译成可读提示
    try:
        return resp.json()
    except ValueError:
        raise BiliError(f"B站接口返回异常（HTTP {resp.status_code}，非 JSON 响应）") from None


async def search(keyword: str, count: int = 5, cookie: str = "") -> list[dict]:
    """搜索B站视频，返回 [{bvid,title,author,duration,play,desc}]。"""
    async with _client(cookie) as c:
        d = await _get_json(c, f"{API_BASE}/x/web-interface/search/all/v2",
                            {"keyword": keyword, "page": 1})
        if d.get("code") != 0:
            raise BiliError(f"搜索失败：{_api_err(d.get('code'), d.get('message', ''))}")
        results = []
        for item in d.get("data", {}).get("result", []):
            if item.get("result_type") != "video":
                continue
            for v in item.get("data", [])[:count]:
                results.append({
                    "bvid": v.get("bvid", ""),
                    "title": re.sub(r"<[^>]+>", "", v.get("title", "")),
                    "author": v.get("author", ""),
                    "duration": v.get("duration", 0),
                    "play": v.get("play", 0),
                    "desc": re.sub(r"<[^>]+>", "", v.get("description", "") or ""),
                })
            break
        return results[:count]


async def get_info(bvid: str, cookie: str = "", timeout: float = 60.0) -> dict:
    """获取视频信息：cid / title / duration(秒) / desc。下载前判断时长用它。"""
    async with _client(cookie, timeout) as c:
        d = await _get_json(c, f"{API_BASE}/x/web-interface/view", {"bvid": bvid})
        if d.get("code") != 0:
            raise BiliError(_api_err(d.get('code'), d.get('message', '')))
        data = d["data"]
        return {
            "bvid": bvid,
            "cid": data["cid"],
            "title": data.get("title", bvid),
            "duration": int(data.get("duration", 0)),
            "desc": data.get("desc", ""),
        }


async def _wbi_sign(client: httpx.AsyncClient, bvid: str, cid: int, base_params: dict) -> dict:
    """WBI 签名：nav 接口取 img/sub key，排序拼接 mix_key 后 md5 出 w_rid。"""
    nav = await _get_json(client, f"{API_BASE}/x/web-interface/nav")
    wbi = nav.get("data", {}).get("wbi_img", {})
    m1 = re.search(r"/([^/]+)\.png", wbi.get("img_url", ""))
    m2 = re.search(r"/([^/]+)\.png", wbi.get("sub_url", ""))
    if not m1 or not m2:
        raise BiliError("WBI 签名初始化失败")
    mix_key = "".join(sorted([m1.group(1), m2.group(1)]))
    params = dict(base_params)
    params["wts"] = int(time.time())
    query = "&".join(f"{k}={params[k]}" for k in sorted(params)) + mix_key
    params["w_rid"] = hashlib.md5(query.encode()).hexdigest()
    return params


async def _get_audio_url(client: httpx.AsyncClient, bvid: str, cid: int) -> str:
    """获取 DASH 音频流直链；playurl 返回非 0 时走 WBI 签名重试。"""
    params = {"cid": cid, "bvid": bvid, "fnval": 4048, "fnver": 0, "fourk": 1, "platform": "web"}
    d = await _get_json(client, f"{API_BASE}/x/player/playurl", params)
    if d.get("code") != 0:
        d = await _get_json(client, f"{API_BASE}/x/player/wbi/playurl",
                            await _wbi_sign(client, bvid, cid, params))
    if d.get("code") != 0:
        raise BiliError(f"获取音频流失败：{_api_err(d.get('code'), d.get('message', ''))}")
    audio = d.get("data", {}).get("dash", {}).get("audio")
    if not audio:
        raise BiliError("该视频无可用音频流（可能需登录或为纯视频）")
    return audio[0]["baseUrl"]


async def download(bvid: str, out_dir, info: dict | None = None, cookie: str = "",
                   timeout: float = 60.0, max_seconds: int = 0) -> tuple[str, dict]:
    """下载音频到 out_dir/{bvid}_{title[:30]}.m4a，返回 (绝对路径, info)。

    已存在同名文件（且非空）直接返回，实现缓存复用。max_seconds>0 时超限抛 BiliError。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    async with _client(cookie, timeout) as c:
        info = info or await get_info(bvid, cookie, timeout)
        if max_seconds and info["duration"] > max_seconds:
            raise BiliError(f"视频时长 {info['duration']}s 超过上限 {max_seconds}s")
        fpath = out_dir / f"{bvid}_{_clean(info['title'])}.m4a"
        if fpath.exists() and fpath.stat().st_size > 0:
            return str(fpath), info
        url = await _get_audio_url(c, bvid, info["cid"])
        try:
            async with c.stream("GET", url) as resp:
                if resp.status_code != 200:
                    raise BiliError(f"音频流下载失败（HTTP {resp.status_code}）")
                with open(fpath, "wb") as f:
                    async for chunk in resp.aiter_bytes(8192):
                        f.write(chunk)
        except BiliError:
            raise
        except httpx.HTTPError as e:
            # 下载中断时清理半截文件，避免下次误判为有效缓存
            try:
                fpath.unlink(missing_ok=True)
            except Exception:
                pass
            raise BiliError(f"音频流下载失败：{_net_err(e)}") from e
        return str(fpath), info


async def resolve_short(url: str, timeout: float = 15.0) -> str:
    """b23.tv 短链 → 真实 URL。"""
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True,
                                 timeout=timeout, trust_env=False) as c:
        r = await c.get(url)
        return str(r.url)


async def extract_bvid(text: str, timeout: float = 15.0) -> str:
    """从文本提取 BV 号；支持 b23.tv 短链（跟随重定向后取真实 URL 中的 BV）。"""
    m = BV_RE.search(text)
    if m:
        return m.group(0)
    m2 = SHORT_RE.search(text)
    if m2:
        real = await resolve_short(m2.group(0), timeout)
        m3 = BV_RE.search(real)
        if m3:
            return m3.group(0)
    return ""


async def _main(argv):
    if len(argv) < 2:
        print("用法: bili_dl.py <search|info|download> <参数>")
        return
    cmd, arg = argv[0], argv[1]
    if cmd == "search":
        count = int(argv[2]) if len(argv) > 2 else 5
        print(json.dumps(await search(arg, count), ensure_ascii=False))
    elif cmd == "info":
        print(json.dumps(await get_info(arg), ensure_ascii=False))
    elif cmd == "download":
        out = argv[2] if len(argv) > 2 else "files"
        max_sec = int(argv[3]) if len(argv) > 3 else 600
        path, info = await download(arg, out, max_seconds=max_sec)
        print(json.dumps({"filepath": path, "title": info["title"],
                          "duration": info["duration"]}, ensure_ascii=False))
    else:
        print("未知命令: " + cmd)


if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1:]))
