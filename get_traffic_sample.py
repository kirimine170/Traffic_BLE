#!/usr/bin/env python3
import asyncio
import csv
import json
from datetime import datetime
from bleak import BleakScanner

TRAFFIC_MID = 0x01CE  # 信号機のメーカーID

async def run_ble_scan(output_file: str = "traffic_raw.csv", scan_duration: float = 120.0):
    # CSV のカラム定義
    fieldnames = [
        "timestamp",      # ISO8601 タイムスタンプ
        "address",        # BLE アドレス
        "rssi",           # 受信強度
        "manufacturer_id",# メーカーID
        "raw_mdata",      # manufacturer_data の生バイト列（16進文字列）
        "service_data"    # service_data の生データ(JSON文字列)
    ]

    # ファイルを開いてヘッダを書き出し
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        def callback(device, adv):
            # 信号機だけフィルタ

            mdata = adv.manufacturer_data.get(TRAFFIC_MID)
            if mdata is None:
                return

            # 生データ整形
            raw_mdata = mdata.hex()
            svc_data = {uuid: d.hex() for uuid, d in adv.service_data.items()}

            # 行を追加
            writer.writerow({
                "timestamp":    datetime.now().isoformat(),
                "address":      device.address,
                "rssi":         adv.rssi,
                "manufacturer_id": f"0x{TRAFFIC_MID:04X}",
                "raw_mdata":    raw_mdata,
                "service_data": json.dumps(svc_data, ensure_ascii=False)
            })
            f.flush()  # 念のため都度フラッシュ

        scanner = BleakScanner()
        scanner.register_detection_callback(callback)

        print(f"信号機のBLEアドバタイズを{scan_duration}sスキャンしてデータを「{output_file}」に保存します…")
        await scanner.start()
        await asyncio.sleep(scan_duration)
        await scanner.stop()
        print("スキャン終了。")

if __name__ == "__main__":
    asyncio.run(run_ble_scan())
