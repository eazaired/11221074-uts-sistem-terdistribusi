# UTS Sistem Terdistribusi

**Nama:** Muhammad Izzi Alfatih  
**NIM:** [11221074]  
**Mata Kuliah:** Sistem Paralel dan Terdistribusi  

Link Youtube    : [https://youtu.be/08bNja3tXMk](https://youtu.be/08bNja3tXMk)

## Deskripsi Singkat
Proyek ini mengimplementasikan sistem *publish-subscribe* sederhana dengan deduplication dan idempotent consumer menggunakan Python (FastAPI, asyncio, dan SQLite).  
Sistem ini mendukung **at-least-once delivery**, **deduplication store**, dan **persistent data** melalui Docker volume.

## Fitur Utama
- Endpoint `/publish`, `/events`, dan `/stats`
- Deduplication via SQLite
- Consumer asinkron berbasis `asyncio.Queue`
- Unit testing dengan `pytest` dan `httpx`
- Dockerized deployment + Docker Compose untuk simulasi publisher dan aggregator

## Cara Menjalankan
```bash
# clone repository
git clone https://github.com/eazaired/11221074-uts-sistem-terdistribusi.git

# install dependency
pip install -r requirements.txt

# build image
docker build -t uts-aggregator .

# run container
docker run --rm --name uts-agg -p 8080:8080 -v "${PWD}\data:/app/data" uts-aggregator