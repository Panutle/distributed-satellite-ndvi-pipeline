import os
import sys
import argparse
from datetime import datetime
import requests
import ray

# นำเข้า Task Worker จากไฟล์ที่ปรับชื่อแล้ว
from distributed_ndvi_worker import process_single_date, setup_db, get_last_row_lm, get_path_cut, get_path_fold, var

def process_rice_survey_data(province_id):
    """Orchestrates distributed batch processing for a specific province."""
    setup_db(province_id)
    last_row_lm = get_last_row_lm(province_id)

    if var.add_on in var.ls_pro_cut:
        ls_file = [i.split('.')[0].split('_')[-1] for i in os.listdir(var.get_paht_dir_file(province_id)) if i.endswith('.tif')]
        get_path = get_path_cut
    else:
        ls_file = [i.split('.')[0].split('_')[0] for i in os.listdir(var.paht_dir_fold_id) if i.endswith('.tif')]
        get_path = get_path_fold

    ls_file.sort()
    start_date = var.start_date
    current_date = datetime.now()

    # สร้างรายการ Task อาร์กิวเมนต์สำหรับส่งเข้า Ray Cluster
    ls_arg = [
        (datetime.strptime(file, "%Y%m%d"), province_id, last_row_lm, get_path) 
        for file in ls_file 
        if start_date <= datetime.strptime(file, "%Y%m%d") <= current_date
    ]

    if ls_arg:
        print(f"[*] Starting Distributed Pipeline for Province: {province_id}")
        
        # Dispatch งานเข้า Ray Remote Workers
        tasks = [process_single_date.remote(date_str, province_id, last_row_lm, get_path) for (date_str, province_id, last_row_lm, get_path) in ls_arg]
        
        # รอรับผลลัพธ์ทั้งหมด
        ls_logs = ray.get(tasks)
        js_logs = {}
        for log in ls_logs:
            js_logs[log] = js_logs.get(log, 0) + 1

        print(f"[+] Province {province_id} Completed: {js_logs}")
    else:
        print(f"[-] Up to date. Province: {province_id}")


def run_pipeline():
    s_time = datetime.now()
    f_name = os.path.basename(__file__)
    
    try:
        # ตรวจสอบสถานะทรัพยากรบน Ray Cluster
        cluster_cpus = int(ray.available_resources().get("CPU", 1))
        print(f"Ray Cluster CPUs detected: {cluster_cpus}")

        excluded = {76, 61, 64, 67, 70, 71, 48, 50, 51, 53, 31, 33, 38, 39, 40, 43, 9, 13, 18}
        ls_province_id = [f"{i:02d}" for i in range(1, 78) if i not in excluded]
        ls_province_id.sort()

        for province_id in ls_province_id[var.n_start:]:
            process_rice_survey_data(province_id)

        e_time = datetime.now()
        requests.post(
            "https://ntfy.sh/Notification_Train_Models", 
            data=f"Notification from Node {var.id_machine}: Success! File: {f_name}, Elapsed: {e_time - s_time}"
        )

    except Exception as e:
        print(f"[!] Pipeline Error: {e}")
        e_time = datetime.now()
        requests.post(
            "https://ntfy.sh/Notification_Train_Models", 
            data=f"Notification from Node {var.id_machine}: Error: {e}, File: {f_name}, Elapsed: {e_time - s_time}"
        )


if __name__ == "__main__":
    run_pipeline()