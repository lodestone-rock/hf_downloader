import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from huggingface_hub import HfFileSystem
import os

def construct_file_list(fs, path):
    """
    Recursively constructs a flat list of file paths in a Hugging Face repository.
    
    Args:
    - fs (HfFileSystem): The Hugging Face file system object.
    - path (str): The current path to explore.
    
    Returns:
    - list: A list of file paths in the repository.
    """
    file_list = []
    
    for entry in fs.ls(path):
        name = entry['name']
        entry_type = entry['type']
        
        if entry_type == 'directory':
            file_list.extend(construct_file_list(fs, name))
        else:
            file_list.append(name)
    
    return file_list

def get_hf_repo_url(repo_id: str) -> list:
    format = f"https://huggingface.co/{repo_id}/resolve/main/"

    fs = HfFileSystem()
    flat_paths = construct_file_list(fs, repo_id)

    urls = []
    for path in flat_paths:
        urls.append([format + path.replace(repo_id + "/", ""), "/".join(path.split("/")[2:-1])])    

    return urls

def generate_wget_commands(urls_and_paths, auth_token=None):
    wget_commands = []
    for url, path in urls_and_paths:
        filename = os.path.basename(url)
        download_path = os.path.join(path, filename)
        if auth_token:
            token = f'--header="Authorization: Bearer {auth_token}"'
        else:
            token = ""
        wget_command = f'wget {token} -nd -P {path} {url}'
        wget_commands.append(wget_command)
    return wget_commands

def execute_wget_command(cmd):
    os.system(cmd)

def main():
    parser = argparse.ArgumentParser(description='Process HF repo paths.')
    parser.add_argument('hf_path', type=str, help='The Hugging Face repository path.')
    parser.add_argument('--flag', type=str, default='download', choices=['download', 'dump'], help='Choose "download" to execute wget commands or "dump_command" to dump wget commands to a txt file.')
    parser.add_argument('--workers', type=int, default=2, help='Number of worker threads for concurrency.')
    parser.add_argument('--auth_token', type=str, help='Number of worker threads for concurrency.')

    args = parser.parse_args()

    urls_and_paths = get_hf_repo_url(args.hf_path)
    wget_cmd = generate_wget_commands(urls_and_paths, args.auth_token)

    try:
        if args.flag == 'download':
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(execute_wget_command, cmd) for cmd in wget_cmd]
                for future in as_completed(futures):
                    future.result()
        elif args.flag == 'dump':
            with open('wget_commands.txt', 'w') as f:
                f.write('\n'.join(wget_cmd))
    except KeyboardInterrupt:
        print("\nReceived KeyboardInterrupt, stopping execution.")
        # You may want to add cleanup code here if needed

if __name__ == "__main__":
    main()
