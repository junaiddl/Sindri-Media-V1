import paramiko
import os
import stat
import re
from datetime import datetime, timedelta

SFTP_HOST = "sftp.dk1.peytzmail.com"
SFTP_PORT = 22
SFTP_USER = "3f-sindri"
SFTP_PASS = "sushi-fulfil-discuss-PLACED"

REMOTE_BASE_DIR = "/peytzmail/exports"
LOCAL_BASE_DIR = "downloaded_files_v2"

# Track downloads per folder
download_report = {}

# -------------------------
def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def record_download(remote_folder):
    """Count downloads per directory."""
    download_report[remote_folder] = download_report.get(remote_folder, 0) + 1


def listdir_safe(sftp, remote_dir):
    try:
        for attr in sftp.listdir_iter(remote_dir):
            yield attr.filename, attr
    except AttributeError:
        try:
            for name in sftp.listdir(remote_dir):
                try:
                    attr = sftp.stat(f"{remote_dir}/{name}")
                except Exception:
                    attr = None
                yield name, attr
        except Exception as e:
            raise e


def extract_first_date(filename):
    """
    Extract the first date in YYYY-MM-DD format from a filename.
    Returns a datetime.date object or None if no date found.
    """
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    return None


def download_directory(sftp, remote_dir, local_dir, start_date, end_date):
    """
    Recursively download files from remote_dir only if their first date in filename
    is within start_date and end_date (inclusive).
    """
    ensure_dir(local_dir)

    try:
        entries = list(listdir_safe(sftp, remote_dir))
    except Exception as e:
        print(f"⚠️ Cannot access folder: {remote_dir} ({e})")
        return

    print(f"📥 Checking folder {remote_dir} ...")

    for name, attr in entries:
        remote_path = f"{remote_dir}/{name}"
        local_path = os.path.join(local_dir, name)

        try:
            is_dir = False
            if attr is not None and hasattr(attr, "st_mode"):
                is_dir = stat.S_ISDIR(attr.st_mode)
            else:
                try:
                    st = sftp.stat(remote_path)
                    is_dir = stat.S_ISDIR(st.st_mode)
                except IOError:
                    is_dir = False

            if is_dir:
                download_directory(sftp, remote_path, local_path, start_date, end_date)
            else:
                file_date = extract_first_date(name)
                if file_date and start_date <= file_date <= end_date:
                    if not os.path.exists(local_path):
                        sftp.get(remote_path, local_path)
                        record_download(remote_dir)
        except Exception as e:
            print(f"⚠️ Error processing entry in {remote_dir}: {e}")


def print_summary():
    print("\n📌 DOWNLOAD SUMMARY")
    print("----------------------------------")
    if not download_report:
        print("No files were downloaded.")
    else:
        for folder, count in download_report.items():
            print(f"{folder} → {count} files downloaded")
    print("----------------------------------")


def execution(days_ago):
    """
    Download files from SFTP whose first date in filename falls within
    the last `days_ago` days (excluding today).
    """
    today = datetime.today().date()
    start_date = today - timedelta(days=days_ago)
    end_date = today - timedelta(days=1)  # yesterday

    print(f"🔐 Connecting to SFTP: {SFTP_HOST} ...")
    sftp = connect_sftp()
    print("✅ Connected!\n")

    print(f"📂 Downloading files from {start_date} to {end_date} in {REMOTE_BASE_DIR}\n")
    download_directory(sftp, REMOTE_BASE_DIR, LOCAL_BASE_DIR, start_date, end_date)

    sftp.close()
    print("\n🎉 Download completed!")
    print_summary()


if __name__ == "__main__":
    # Change the number to download files from the last N days
    execution(2)