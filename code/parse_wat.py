from curses import meta
from re import I
from warcio import ArchiveIterator
from typing import BinaryIO
import json
import pathlib 
from tqdm import tqdm

CWD = pathlib.Path(__file__).parent

def extract_imgs(stream: BinaryIO):
    all_links = []
    total = 0
    filtered = 0
    for record in tqdm(ArchiveIterator(stream)):
        if record.rec_type == 'metadata' and record.content_type == 'application/json':
            record_data = json.loads(record.content_stream().read())
            envelope = record_data['Envelope']
            payload = envelope["Payload-Metadata"]
            if "HTTP-Response-Metadata" not in payload:
                continue
            http_resp = payload['HTTP-Response-Metadata']
            if "HTML-Metadata" not in http_resp:
                continue
            metadata = http_resp["HTML-Metadata"]
            if "Links" not in metadata:
                continue
            links = metadata['Links'] 
            filtered_links = [link for link in links if valid_link(link)]
            total += len(links)
            filtered += len(filtered_links)
            all_links += filtered_links
    return all_links

def valid_link(link):
    valid_path = link.get("path", "") == 'IMG@/src'
    valid_img = link.get("url", "").endswith(('.png', '.jpg', '.jpeg'))
    valid_alt = len(link.get('alt', "")) > 0
    valid_http =  link.get("url", "").startswith("http") 
    return (valid_path or valid_img) and valid_path and valid_http

def url_is_img(url):
    rsp = url.lower().endswith(('.png', '.jpg', '.jpeg')) 
    valid_http = rsp.startswith("http")
    return rsp and valid_http


if __name__ == "__main__":
    with (CWD.parent / "data/sample_wat.tar.gz").open("rb") as f:
        links = extract_imgs(f)
        print(links)


    







    




    


    
