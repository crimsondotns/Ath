import asyncio
import time
import random
import os
from datetime import datetime
import gspread
from curl_cffi.requests import AsyncSession

from core.config_loader import config
from core.logger import log_info, log_success, log_error, log_vpn, log_warn
from core.manager import SheetManager
from core.client import DexClient
from core.utils import format_address, extract_dex_id, extract_quote_token, checksum_url_addresses, extract_pair_address

# Legacy Colors for status specific logging
C_GREEN = "\033[32m"
C_RED = "\033[31m"
C_YELLOW = "\033[33m"
C_RESET = "\033[0m"

class MarketCapTracker:
    def __init__(self):
        self.mgr = SheetManager()
        self.client = DexClient()
        self.tab_in = config.get_tab_name('input')
        self.tab_out = config.get_tab_name('mkt_cap')
        self.active_listeners = {}
        self.live_data = {} # Address -> Full Data
        self.buffer_lock = asyncio.Lock()
        self.request_limit = asyncio.Semaphore(10) # Limit concurrent DNS/HTTP requests
        self.vpn_ready = asyncio.Event()
        self.vpn_ready.set()

    async def rotate_vpn(self):
        log_vpn("🚨 403/429 Detected! Rotating VPN...")
        self.vpn_ready.clear()
        try:
            proc = await asyncio.create_subprocess_shell("rotate.bat")
            await proc.communicate()
            await asyncio.sleep(15) # Stabilization
        except Exception as e:
            log_error(f"Failed to rotate VPN: {e}", context="system=vpn")
            await asyncio.sleep(5)
        finally:
            self.vpn_ready.set()
            log_success("✅ VPN IP Changed! Resuming all requests.", context="system=vpn")

    async def poll_token(self, session, token, network, address, url, pairs, sheet_quote):
        clean_url = checksum_url_addresses(url) if network.lower() in config.evm_networks else url
        context = f"network={network}&address={address}"
        
        # Determine final quote and pair address (URL discovery has priority)
        auto_quote = extract_quote_token(url)
        final_quote = auto_quote if auto_quote else sheet_quote
        
        auto_pair = extract_pair_address(url)
        final_pairs = auto_pair if auto_pair else pairs
        
        await asyncio.sleep(random.uniform(1.0, 5.0))
        
        while True:
            await self.vpn_ready.wait()
            start_req = time.perf_counter()
            try:
                # Use semaphore to prevent overwhelming DNS on VPN
                async with self.request_limit:
                    try:
                        resp = await session.get(clean_url, headers=self.client.headers, timeout=15)
                    except Exception as e:
                        # Handle DNS or temporary connection issues with instant retry
                        if "dns" in str(e).lower() or "resolver" in str(e).lower():
                            log_warn(f"DNS/Connection flicker for {address}. Retrying in 2s...", context="system=vpn")
                            await asyncio.sleep(2)
                            continue
                        raise e # Pass other errors to outer handler
                
                req_time_ms = int((time.perf_counter() - start_req) * 1000)
                status_text = {200: "OK", 403: "Forbidden", 429: "Too Many Requests", 404: "Not Found"}.get(resp.status_code, "Status")
                
                if resp.status_code in [403, 429]:
                    c_code = f"{C_YELLOW if resp.status_code == 429 else C_RED}{resp.status_code}{C_RESET}"
                    log_error(f"{c_code} {status_text} - Blocked by Cloudflare!", context=context)
                    if self.vpn_ready.is_set():
                        asyncio.create_task(self.rotate_vpn())
                    await asyncio.sleep(5)
                    continue
                
                c_code = f"{C_GREEN if resp.status_code == 200 else C_RED}{resp.status_code}{C_RESET}"

                if resp.status_code == 200:
                    data = self.client.parse_binary_price(resp.content)
                    if data:
                        async with self.buffer_lock:
                            self.live_data[address] = {
                                "token": token, "network": network, "address": address, "price": data,
                                "ts": datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
                                "quote": final_quote,
                                "pairs": final_pairs,
                                "dex_id": extract_dex_id(url),
                                "ts_raw": time.time()
                            }
                        if config.log_show_prices:
                            log_info(f"{c_code} {status_text} in {req_time_ms}ms", context=context)
                    else:
                        log_warn(f"{c_code} {status_text} in {req_time_ms}ms (No Price)", context=context)
                elif resp.status_code != 0:
                    log_error(f"{c_code} {status_text}", context=context)
            except Exception as e:
                err_time_ms = int((time.perf_counter() - start_req) * 1000)
                # Filter out CancelledError to avoid log spam during hot-reload
                if not isinstance(e, asyncio.CancelledError):
                    log_warn(f"Failed in {err_time_ms}ms ({type(e).__name__})", context=context)
            
            await asyncio.sleep(random.uniform(10, 15))

    async def sync_to_sheet(self):
        """Rebuild output sheet with sorted data (compaction included)."""
        headers = [
            "Token", "Network", "QuoteToken", "Address", "pair.Address", "DexID",
            "Open", "High", "Low", "Close", "MarketCap", "Timestamp"
        ]
        
        while True:
            await asyncio.sleep(15)
            await self.vpn_ready.wait()
            
            # Filter live_data to only include active listeners (those still in input sheet)
            active_list = []
            async with self.buffer_lock:
                active_addrs = list(self.active_listeners.keys())
                for addr in active_addrs:
                    if addr in self.live_data:
                        active_list.append(self.live_data[addr])
            
            if not active_list: continue

            try:
                # Sort by raw timestamp (newest first)
                active_list.sort(key=lambda x: x["ts_raw"], reverse=True)
                
                log_info(f"Batch Sync & Compact ({len(active_list)} records)...", context="system=sheet_sync")
                
                final_grid = []
                for d in active_list:
                    p = d["price"]
                    final_grid.append([
                        d["token"], d["network"], d["quote"], d["address"], d["pairs"], d["dex_id"],
                        p["open"], p["high"], p["low"], p["close"], p["current_price"],
                        d["ts"]
                    ])

                if final_grid:
                    ws = await asyncio.to_thread(self.mgr.get_worksheet, self.tab_out)
                    start_col = config.output_start_num
                    
                    # Full rewrite strategy to ensure no gaps and perfect sorting
                    # Note: We overwrite from row 1 (headers) downwards
                    num_rows = len(final_grid) + 1
                    num_cols = len(headers)
                    range_name = f"{gspread.utils.rowcol_to_a1(1, start_col)}:{gspread.utils.rowcol_to_a1(num_rows + 50, start_col + num_cols - 1)}"
                    
                    # Prepare the data matrix (padded with empty strings to clear old data)
                    data_matrix = [headers] + final_grid
                    for _ in range(50): # Add some empty rows to clear previous longer lists
                        data_matrix.append([None] * num_cols)
                        
                    await asyncio.to_thread(ws.update, range_name, data_matrix)
                    log_success(f"200 OK (Sync {len(final_grid)} rows)", context="system=sheet_sync")
            except Exception as e:
                log_error(f"Sync Error: {e}", context="system=sheet_sync")

    async def watch_sheet(self):
        """Monitor the INPUT sheet for new tokens to track."""
        c_token = config.get_col_index('token')
        c_chain = config.get_col_index('network')
        c_quote = config.get_col_index('quote')
        c_addr = config.get_col_index('address')
        c_pairs = config.get_col_index('pairs')
        # Specific URL column for mkt_cap
        c_url = config.get_col_index('url', task='mkt_cap')

        async with AsyncSession(impersonate="chrome110") as session:
            while True:
                await self.vpn_ready.wait()
                try:
                    records = await asyncio.to_thread(self.mgr.get_all_records, self.tab_in)
                    current_input_addrs = set()
                    
                    if records:
                        new_count = 0
                        for row in records[1:]: # Skip header
                            if len(row) <= c_url or len(row) <= c_addr: continue
                            
                            address = format_address(row[c_addr])
                            if not address: continue
                            
                            current_input_addrs.add(address)
                            
                            if address not in self.active_listeners:
                                token = row[c_token] if len(row) > c_token else ""
                                network = row[c_chain] if len(row) > c_chain else ""
                                sheet_quote = row[c_quote] if len(row) > c_quote else ""
                                pairs = row[c_pairs] if len(row) > c_pairs else ""
                                url = row[c_url].strip()
                                
                                task = asyncio.create_task(self.poll_token(session, token, network, address, url, pairs, sheet_quote))
                                self.active_listeners[address] = {
                                    "task": task, "url": url, "pairs": pairs, "quote": sheet_quote,
                                    "token": token, "network": network
                                }
                                new_count += 1
                            else:
                                # Check for updates to existing listener
                                current = self.active_listeners[address]
                                url = row[c_url].strip()
                                pairs = row[c_pairs] if len(row) > c_pairs else ""
                                sheet_quote = row[c_quote] if len(row) > c_quote else ""
                                token = row[c_token] if len(row) > c_token else ""
                                network = row[c_chain] if len(row) > c_chain else ""
                                
                                if current["url"] != url:
                                    log_info(f"URL change {C_YELLOW}detected{C_RESET} for {address}. Restarting...", context="system=watcher")
                                    current["task"].cancel()
                                    task = asyncio.create_task(self.poll_token(session, token, network, address, url, pairs, sheet_quote))
                                    self.active_listeners[address] = {
                                        "task": task, "url": url, "pairs": pairs, "quote": sheet_quote,
                                        "token": token, "network": network
                                    }
                        
                        # Cleanup removed tokens
                        removed = []
                        for addr in list(self.active_listeners.keys()):
                            if addr not in current_input_addrs:
                                self.active_listeners[addr]["task"].cancel()
                                del self.active_listeners[addr]
                                async with self.buffer_lock:
                                    if addr in self.live_data: del self.live_data[addr]
                                removed.append(addr)
                        
                        if new_count > 0 or removed:
                            log_info(f"Watcher: +{new_count} new, -{len(removed)} removed. Total: {len(self.active_listeners)}", context="system=watcher")
                except Exception as e:
                    log_error(f"Watcher Error: {e}", context="system=watcher")
                
                await asyncio.sleep(config.watcher_interval)

    async def start(self):
        log_info("Initializing Google Sheets Connection...", context="system=init")
        if not await asyncio.to_thread(self.mgr.connect, config.spreadsheet_url): return
        
        log_info(f"Connected. Input: {self.tab_in} | Output: {self.tab_out}", context="system=init")
        await asyncio.gather(self.watch_sheet(), self.sync_to_sheet())

if __name__ == "__main__":
    tracker = MarketCapTracker()
    try:
        asyncio.run(tracker.start())
    except KeyboardInterrupt:
        log_warn("Stopped by user.")