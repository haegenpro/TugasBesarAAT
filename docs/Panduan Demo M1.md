# Panduan pelaksanaan demo M1

Panduan operator untuk [spesifikasi M1](IF4031%20-%20Tugas%20Besar%20-%20M1.md), berdasarkan implementasi di repositori ini. Jalankan perintah dari root repositori. Contoh utama menggunakan Linux/Bash; perintah Node dan Docker juga dapat digunakan di Windows. Versi PowerShell untuk pengujian terperinci tersedia di [Test Runbook](Test%20Runbook.md).

## 1. Apa yang wajib disiapkan

Demo **sinkron daring, maksimal 60 menit**, dengan asisten sebagai pengarah skenario uji langsung. Siapkan antarmuka buatan kelompok. CLI `scripts/m1-demo.mjs`, skrip cURL dan k6 dalam repositori memenuhi bentuk antarmuka yang diperbolehkan; dashboard web tambahan tidak diwajibkan. TUI dan Grafana bisa digunakan sebagai pelengkap.

Sebelum sesi, tiga antarmuka berikut harus sudah bisa dijalankan:

| Kebutuhan spesifikasi | Perintah siap pakai | Yang diperlihatkan |
| --- | --- | --- |
| Request ke service dengan pengukuran waktu | `node scripts/m1-demo.mjs request field '/hazards?hazard_type=SEISMIC'` | HTTP, latensi respons dalam ms, correlation ID, JSON |
| Inspeksi isi DB | `node scripts/m1-demo.mjs db psql` | CLI psql langsung di container Postgres |
| Load testing | `BASE_URL="$K6_BASE_URL" EVIDENCE_NAME=p2-sustained bash scripts/load-test.sh sustained 50 65s` | Throughput, p50/p95/p99, error rate dan jumlah 429 |

Setiap anggota wajib menjelaskan rancangan, ikut mendemonstrasikan, dan menjawab pertanyaan teknis. Bagikan P1–P5 sesuai jumlah anggota; operator terminal tidak harus menjelaskan seluruh bagian sendiri. Siapkan laporan PDF, diagram arsitektur, dan bukti pengukuran yang dapat ditelusuri ke kode.

Menurut dokumen tugas, deadline M1 adalah **10 Oktober 2026 23:59 WIB**, pengisian jadwal demo **19 Oktober 18:00 WIB**, dan commit setelah deadline tidak dinilai. Pastikan jadwal serta tag `milestone-1` sudah diurus. Alat bantu dalam perubahan ini dibuat untuk persiapan demo; keberadaannya tidak mengubah commit yang dinilai. Dokumen ini tidak memverifikasi pengumuman atau perubahan jadwal di luar berkas spesifikasi tersebut.

## 2. Matriks kriteria dan bukti

| Bagian | Yang harus dibuktikan langsung | Cara / bukti utama |
| --- | --- | --- |
| Ketentuan umum | Service terpisah; komunikasi jaringan; store hanya milik Aggregator; health; log JSON dengan correlation ID dan latensi outbound | `preflight`, `ownership`, probe `trace`, diagram dan Compose |
| P1.1 | Pemetaan BMKG dan PVMBG ke HazardEvent sesuai aturan, termasuk warning tsunami dan referensi gunung | Probe `mapping`, contoh source dan API, uji batas magnitude |
| P1.2–P1.4 | Mulai skema lama → ubah PVMBG secara live → ingest berlanjut; kebijakan field baru konsisten; tanpa restart/deploy | Probe `schema`; snapshot container, record lama/baru |
| P2.1 | PVMBG benar-benar delay 3 detik; SEISMIC dan VOLCANIC diminta bersamaan; **p95 BMKG-only <300 ms** | `delay 3000`, `source`, k6 `mixed 2 65s` dengan `PURPOSE=isolation` |
| P2.2 | **≥50 koneksi paralel, sustained ≥60 detik**; tidak crash; error selain penolakan terkontrol **<1%** | k6 `sustained 50 65s`, probe `runtime`, JSON hasil; jelaskan batas interpretasi VU/TCP |
| P2.3 | Outage sungguhan; seismik tetap tersedia; vulkanik cache + stale atau status unavailable eksplisit; pulih tanpa restart | Probe `outage`, atau kendali `outage on/off` dan request |
| P3.1 | Kredensial BMKG ditolak PVMBG dan sebaliknya, masing-masing tetap menerima kredensial sendiri | `isolation-demo.sh` |
| P3.2 | Media hanya Ringkasan; request raw ditolak server 401/403; Field/Internal menerima raw | `request`, `smoke.sh`, respons list/detail |
| P3.3 | Token Field kedaluwarsa alami, refresh tanpa login ulang manual, token lama ditolak | `smoke.sh --wait-for-expiry`, TUI opsional |
| P3.4 | Tidak ada kredensial nyata dalam kode/repo; template `.env.example` tersedia | Probe `secrets` dan tinjauan temuan |
| P4.1 | Stop, rebuild, jalankan satu container; container lain tidak restart dan request terkait service lain tetap dilayani | `fanout --rebuild`; API tetap dibaca saat notifier mati |
| P4.2 | Record skema lama dan baru berdampingan, tanpa migrasi untuk field baru | Probe `storage`, `db latest`, `db schema` |
| P4.3 | Hanya Aggregator mengakses Canonical Store; Client API membaca melalui API Aggregator | `ownership`, diagram, konfigurasi jaringan |
| P5.1–P5.3 | Publikasi ke broker; dua subscriber independen menerima ID event sama; satu mati, lainnya lanjut; backlog diputar ulang | `broker`, `fanout` |
| P5.4 | Tambah subscriber ketiga ke stream sama tanpa perubahan producer | `third-consumer`; snapshot producer dan event baru di ketiganya |

Butir “Di laporan” tetap perlu dijelaskan: mekanisme/dukungan perubahan skema P1; model konkurensi dan konfigurasi load P2; alasan keamanan P3; batas service dan minimal dua alternatif storage P4; pilihan broker, delivery semantics dan idempotensi P5. Laporan juga perlu kontribusi, asumsi, batasan, serta deklarasi penggunaan LLM sesuai spesifikasi.

## 3. Alat bantu yang tersedia

- **Baru: `scripts/m1-demo.mjs`**: antarmuka operator, request terukur, source JSON, DB, status broker, inspeksi consumer, kontrol outage, delay 3 detik beserta restore, uji fan-out/rebuild dan consumer ketiga. Token dibaca dari `.env`, tidak dicetak.
- **Baru: `scripts/setup-env.py`**: membuat 20 `.env`, termasuk konfigurasi PostgreSQL Auth/consumer dan subscriber demo, dengan kredensial acak yang konsisten antarservice. Konfigurasi lengkap yang sudah ada dipertahankan; konfigurasi parsial ditolak agar kredensial tidak tercampur. Untuk setup data baru, `--fresh` mencadangkan konfigurasi lama lalu membuat set baru; tidak ada migrasi data lama.
- **Sudah ada: `scripts/evidence-probes.mjs`**: pemeriksaan mapping, schema, storage, health, trace, outage, runtime dan rahasia dalam repo/riwayat.
- **Sudah ada: `scripts/isolation-demo.sh`, `scripts/smoke.sh`**: uji isolasi kredensial dan akses/refresh.
- **Sudah ada: `scripts/load-test.sh` + `load-test.js`**: k6 dalam container; k6 tidak perlu diinstal di host.

Perintah non-interaktif `m1-demo.mjs` menyimpan `evidence/m1-<command>-<timestamp>.json`, termasuk status gagal, waktu, ringkasan, dan snapshot yang relevan. Probe lama memiliki nama evidence tetap: simpan hasil satu latihan sebelum latihan berikutnya. `evidence/` diabaikan Git. Evidence bisa berisi data mentah contoh, tetapi tool tidak menyimpan respons login berisi token. Simpan screenshot terminal dan hasil k6 untuk laporan; keluaran tool bukan pengganti menjelaskan sistem.

## 4. Persiapan sebelum hari demo

### 4.1 Prasyarat dan konfigurasi

Siapkan Docker Engine/Desktop dan Compose v2+, Node.js 18+, Python 3, Bash, cURL, dan Git. Akses socket Docker harus bekerja untuk user operator. Tidak perlu memasang jq, nats CLI, psql host, atau k6 host untuk alur di sini.

```bash
docker compose version
node --version
python3 --version
curl --version
node scripts/m1-demo.mjs --help
node scripts/evidence-probes.mjs --help

# Untuk checkout tanpa .env: membuat konfigurasi lokal dengan secret acak.
# Jika seluruh .env sudah ada, hanya memberi pesan bahwa file dipertahankan.
python3 scripts/setup-env.py
docker compose config --quiet
```

Jika konfigurasi baru sebagian, lengkapi berdasarkan [tabel README](../README.md#per-service-environment-files). Jangan menimpa secret layanan yang DB-nya sudah berisi data: kredensial DB lama harus tetap cocok dengan volume tersebut. File `.env` root mengatur port deployment, sedangkan `.env` per service mengatur kredensial/perilaku. `setup-env.py` tidak mengubah password dalam DB yang sudah ada.

Untuk setiap latihan penuh, gunakan **nama project baru** agar storage, durable dan schema mula-mula bersih tanpa menghapus data latihan lain. Satu stack pada port host yang sama saja yang boleh aktif. Jika stack lama aktif, hentikan project tersebut terlebih dahulu, lalu:

```bash
# Ganti nilainya pada latihan berikutnya. Gunakan nilai sama di semua terminal.
export COMPOSE_PROJECT_NAME=m1-demo-latihan01

docker compose build
docker compose --profile consumer-demo build consumer-dashboard-demo
docker compose --profile demo build client-tui
docker pull grafana/k6
```

Siapkan image dan uji build **sebelum** sesi agar unduhan tidak menghabiskan waktu demo. `fanout --rebuild` tetap menjalankan build saat sesi sebagai bukti P4, biasanya memakai cache. Jika memakai Grafana, build/start profil `monitoring` juga sebelum sesi dan gunakan dashboard dari [monitoring/README.md](../monitoring/README.md).

### 4.2 Helper terminal dan perekaman

Di **setiap terminal**, dari root repo:

```bash
export COMPOSE_PROJECT_NAME=m1-demo-latihan01
source scripts/demo-common.sh
export API_URL="http://localhost:$(demo_env_value "$DEMO_ROOT/.env" CLIENT_API_HOST_PORT)"
export BMKG_URL="http://localhost:$(demo_env_value "$DEMO_ROOT/.env" BMKG_HOST_PORT)"
export PVMBG_URL="http://localhost:$(demo_env_value "$DEMO_ROOT/.env" PVMBG_HOST_PORT)"
export K6_BASE_URL="http://host.docker.internal:$(demo_env_value "$DEMO_ROOT/.env" CLIENT_API_HOST_PORT)"
mkdir -p evidence
set -o pipefail
```

Jangan aktifkan `set -x`, mencetak `.env`, respons login lengkap, atau `docker compose config` tanpa `--quiet` saat berbagi layar. Contoh port template adalah API `18080`, BMKG `18081`, PVMBG `18082`; tool membaca port dari `.env` sehingga tidak bergantung pada angka itu.

### 4.3 Latihan lengkap

Jalankan seluruh bagian 6 sekali sebelum demo dan catat hasil aktual. Pastikan tidak ada `FAIL` atau threshold k6 gagal yang diabaikan. Untuk pengujian alat bantu:

```bash
node --test scripts/*.test.mjs
```

Siapkan tiga terminal: A untuk operator/request/DB, B untuk k6, C untuk runtime/log. Buka diagram arsitektur dan PDF laporan. Bila menggunakan TUI, coba `docker compose --profile demo run --rm client-tui` terlebih dahulu; gunakan identitas Field, Media dan Internal dengan kredensial lokal masing-masing.

## 5. Pembagian waktu maksimum 60 menit

| Menit | Kegiatan | Penanggung jawab |
| --- | --- | --- |
| 0–5 | Lingkup, diagram service/protokol/storage, startup dan baseline | Pembuka + operator |
| 5–14 | P1 mapping dan perubahan skema; segera lanjut P4 storage/ownership | Penanggung jawab ingest/storage |
| 14–22 | P3 isolasi instansi, RBAC, token kedaluwarsa/refresh | Penanggung jawab auth |
| 22–31 | P5 fan-out, notifier mati/rebuild/replay, subscriber ketiga | Penanggung jawab broker |
| 31–40 | P2 delay 3 detik, pengukuran p95, sustained load + runtime | Penanggung jawab concurrency |
| 40–51 | P2 outage 10 menit dan recovery; sambil itu jawab pertanyaan | Operator + seluruh anggota |
| 51–60 | Pertanyaan individual, skenario tambahan asisten, cadangan recovery | Seluruh anggota |

Urutan dapat diarahkan asisten. Outage tidak perlu selalu 10 menit untuk pemeriksaan cepat, tetapi kontrolnya harus bisa mempertahankan kondisi selama yang diminta. Jangan menggunakan hasil outage 30 detik sebagai bukti durasi 10 menit. Jalankan long outage ketika pemeriksaan yang memerlukan PVMBG normal sudah selesai. Recreate PVMBG untuk mengubah delay dilakukan **setelah** bukti skema dan storage selesai.

## 6. Urutan pelaksanaan langsung

### 6.1 Boot dan baseline

```bash
docker compose up -d --wait --wait-timeout 240
node scripts/m1-demo.mjs preflight
node scripts/evidence-probes.mjs health --name boot-health
node scripts/m1-demo.mjs db summary
node scripts/m1-demo.mjs source bmkg
node scripts/m1-demo.mjs source tsunami
node scripts/m1-demo.mjs source pvmbg
node scripts/evidence-probes.mjs mapping --name p1-mapping
```

Tunjukkan sebelas container aplikasi berjalan sendiri-sendiri (TUI one-off; monitoring opsional). Kedua mock memiliki ≥20 seed historis, menghasilkan data baru minimal setiap 10 detik; interval polling default contoh 3 detik. Pada source PVMBG mula-mula `confidence_level` tidak ada. `mapping` membandingkan record source, warning tsunami, referensi gunung dalam DB, severity, timestamp, koordinat dan atribut dengan canonical melalui Client API. Untuk unit test tambahan batas magnitude persis 5.0/6.5, jalankan di host yang memiliki Go sesuai `aggregator/go.mod`:

```bash
(cd aggregator && go test -v ./internal/mapping) | tee evidence/p1-mapping-unit.txt
```

Saat menjelaskan arsitektur, bedakan polling ke mock dari read API: request pengguna membaca Canonical Store melalui Aggregator; tidak menunggu fetch langsung ke kedua instansi. Korelasi tsunami dilakukan oleh Aggregator, bukan client.

### 6.2 P1 perubahan skema + P4 storage

```bash
node scripts/evidence-probes.mjs schema --name p1-schema
node scripts/evidence-probes.mjs storage --name p4-storage
node scripts/m1-demo.mjs db latest
node scripts/m1-demo.mjs db schema
node scripts/m1-demo.mjs ownership
```

Probe `schema` harus dimulai dari source v1 dan mengikuti langkah berikut tanpa restart:

1. Menyimpan record/bentuk kolom DB serta ID, start time dan restart count container sebelum perubahan.
2. Memanggil `POST /admin/schema-version` PVMBG menggunakan admin token terpisah, mengaktifkan writer v2 dan menemukan report baru dengan ID aktual.
3. Membuktikan ingest tetap berjalan ketika reader Aggregator masih v1: field writer yang tidak dikenal diabaikan oleh resolusi Avro.
4. Menunggu reader v2 ditemukan lewat polling, lalu mengaktifkannya melalui endpoint admin Aggregator internal. Respons 409 sebelum discovery berarti belum ada kandidat; probe menunggu dan mencoba lagi untuk kondisi ini.
5. Memastikan report baru setelah aktivasi memiliki `attributes.confidence_level`, bentuk HazardEvent tetap valid, kolom DB tidak berubah, seluruh container tidak restart.

**Nyatakan kebijakannya dengan tepat:** reader Avro v1 mengabaikan field tambahan; reader v2 yang ditemukan dan diaktifkan dapat mempertahankan confidence dalam `attributes`. Tidak ada perubahan kode/redeploy, tetapi adopsi field oleh reader memang melalui kontrol operator. Jangan mengklaim bahwa reader langsung menyimpan semua field baru secara otomatis. Jelaskan dukungan perubahan aditif dan batasnya di laporan; kompatibilitas rename/remove/perubahan tipe bukan jaminan umum. Jika penguji meminta confidence langsung terlihat setelah toggle source saja, tunjukkan fase v1 yang sebenarnya dan jelaskan kontrol reader yang diperlukan.

`storage` memakai `schema-before.json` dan `schema-after.json` dari langkah sebelumnya untuk membuktikan record lama dan baru masih dapat dibaca bersama, confidence sesuai sumber, dan `attributes` adalah JSONB tanpa migrasi penambahan confidence. Jalankan sekarang sebelum recreate PVMBG atau toggle kembali. Pada `db psql`, contoh inspeksi yang berguna:

```sql
SELECT source, count(*) FROM hazard_events GROUP BY source;
SELECT source_ref_id, attributes
FROM hazard_events WHERE source = 'PVMBG'
ORDER BY ingested_at DESC LIMIT 8;
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'hazard_events' ORDER BY ordinal_position;
\q
```

Akses DB ini adalah akses **operator** lewat Docker, bukan client/service lain. Tool `ownership` menunjukkan peer jaringan store hanya Aggregator/Postgres, Postgres tidak memublikasikan port host, kontrol positif koneksi Aggregator berhasil, koneksi Client API/consumer gagal, dan Client API tetap membaca data. Kaitkan dengan konfigurasi URL Aggregator di Client API; isolasi jaringan bukan klaim keamanan terhadap operator Docker.

### 6.3 P3 autentikasi dan hak akses

```bash
bash scripts/isolation-demo.sh | tee evidence/p3-isolation.txt
node scripts/m1-demo.mjs request media /hazards
node scripts/m1-demo.mjs request media '/hazards?view=raw' --expect 403
node scripts/m1-demo.mjs request field /hazards
node scripts/m1-demo.mjs request internal /hazards
```

Bandingkan field. Media hanya `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`. Tidak boleh bocor `source_ref_id`, `latitude`, `longitude`, atau `attributes`. Field dan Internal boleh melihat seluruhnya. `source_status` di envelope adalah metadata kebaruan sumber. Untuk detail, salin ID aktual dari respons:

```bash
# Ganti UUID dengan hazard_id aktual; jangan ketik tanda < >.
node scripts/m1-demo.mjs request media '/hazards/UUID'
node scripts/m1-demo.mjs request media '/hazards/UUID?view=raw' --expect 403

bash scripts/smoke.sh --wait-for-expiry | tee evidence/p3-expiry.txt
node scripts/evidence-probes.mjs secrets --name p3-secrets
```

`smoke.sh` memeriksa tiga identitas, isolasi instansi, list/detail Media, penolakan raw, Field/Internal raw, kemudian menunggu `expires_in + 1` detik tanpa mengubah jam. Urutan yang harus dijelaskan: fresh token 200 → expired token 401 → refresh dengan refresh token tanpa login lagi → token baru 200 → token lama tetap 401. Skrip hanya mencetak ringkasan PASS di akhir; untuk peragaan tiap respons gunakan [Runbook 3.4](Test%20Runbook.md#34-token-expiry-and-refresh-without-manual-re-login-p33) atau TUI (“Uji token lama”). Login baru dalam perintah `request` terpisah bukan bukti refresh; bukti refresh berasal dari smoke/TUI tersebut.

Probe `secrets` bisa memerlukan waktu karena memeriksa riwayat Git. Jalankan sebelum sesi dan ulangi jika diminta. Temuan `REVIEW` harus ditinjau; jangan disamarkan sebagai PASS. Pemindaian memiliki ruang lingkup yang dicetak, bukan jaminan absolut tidak pernah ada secret lama.

### 6.4 P5 distribusi event + P4 rebuild independen

```bash
node scripts/m1-demo.mjs broker
node scripts/m1-demo.mjs fanout --rebuild
node scripts/m1-demo.mjs third-consumer
node scripts/evidence-probes.mjs trace --name shared-correlation
```

`fanout --rebuild` melakukan pemeriksaan yang dapat dijelaskan satu per satu:

1. Memastikan dashboard/notifier siap dan menyimpan snapshot container.
2. Menghentikan notifier sungguhan dan memastikan statusnya exited.
3. Mengambil baseline **sesudah stop**, menunggu hazard baru, lalu memeriksa hazard itu diterima dashboard sementara notifier mati. Request Client API tetap berhasil selama proses ini.
4. Menunjukkan durable notifier memiliki backlog di broker.
5. Menjalankan build dan recreate hanya notifier, menyalakannya kembali, serta menunggu ready dan ID hazard yang sama diterima. `finally` mencoba menyalakan notifier bila pemeriksaan/build gagal atau Ctrl+C.
6. Memastikan ID/start time/restart count/status semua container lain tetap sama.

Untuk P5 saja, gunakan `fanout` tanpa `--rebuild`. Mode rebuild tidak mengubah kode producer atau consumer. Delivery menggunakan durable JetStream dan **at-least-once**; consumer menyimpan hasil secara idempoten menurut hazard ID. Jelaskan perbedaan retry delivery dengan menghasilkan hazard baru dan fungsi outbox di Aggregator.

`third-consumer` mensyaratkan container dan durable ketiga **belum ada**. Ia menambah `consumer-dashboard-demo` dengan durable `dashboard-demo-v1`, DB terpisah, stream sama, lalu menunggu hazard **baru** diterima ketiganya. Snapshot harus membuktikan producer serta container lama tidak berubah. Jika sudah pernah menjalankan perintah ini, gunakan project latihan baru untuk mengulang bukti penambahan pertama. Tool tidak menghapus durable/data lama otomatis.

Probe `trace` mengambil log lima menit terakhir: tunjukkan correlation ID yang sama pada Aggregator/source dan Aggregator/consumer, disertai `latency_ms` panggilan keluar. Jalankan segera setelah ada event baru; jangan menunggu lama sampai alur penting keluar dari jendela log. Broker adalah perantara publikasi; producer tidak memiliki daftar endpoint subscriber.

### 6.5 P2.1 PVMBG 3 detik dan BMKG-only tetap cepat

```bash
node scripts/m1-demo.mjs delay 3000
# Tunggu PVMBG siap; ulangi source probe bila startup belum selesai.
node scripts/m1-demo.mjs source pvmbg
node scripts/m1-demo.mjs source bmkg

PURPOSE=isolation BASE_URL="$K6_BASE_URL" EVIDENCE_NAME=p2-isolation \
  bash scripts/load-test.sh mixed 2 65s | tee evidence/p2-isolation.txt
```

`delay 3000` menyimpan **dua angka delay saja** ke `evidence/pvmbg-delay-original.json`, mengubah konfigurasi min/max, lalu recreate hanya PVMBG. Ini mereset toggle schema/outage, sehingga bukan bagian dari pembuktian “tanpa restart” P1. Mengatur environment shell saja tidak mengubah `env_file` PVMBG.

Source PVMBG harus benar-benar terukur sekitar **3000 ms**, BMKG sekitar **50–150 ms**. Konfigurasi 3000 tanpa hasil ukur bukan bukti bahwa kontainer memakai delay itu. Mixed test menjalankan satu VU SEISMIC dan satu VU VOLCANIC bersamaan melalui Client API; hasil successful SEISMIC **p95 <300 ms**, kedua jenis harus memiliki respons sukses. Satu request cepat tidak cukup untuk klaim p95.

### 6.6 P2.2 sustained load dan bukti tidak crash

Terminal C, mulai sebelum menjalankan k6:

```bash
node scripts/evidence-probes.mjs runtime --seconds 85 --name p2-runtime
```

Terminal B, segera setelah probe runtime mulai (usahakan dalam 5 detik):

```bash
BASE_URL="$K6_BASE_URL" EVIDENCE_NAME=p2-sustained \
  bash scripts/load-test.sh sustained 50 65s | tee evidence/p2-sustained.txt
```

Tunggu kedua perintah selesai. Laporkan nilai aktual dari `evidence/p2-sustained.json` dan teks terminal: konfigurasi constant 50 VU/65 detik, throughput request dan successful hazard/s, p50/p95/p99, error non-429, serta jumlah controlled rejection 429 (termasuk jika nol). Semua threshold harus lulus dan error non-429 <1%. Probe runtime memeriksa ID/start/restart count, health dan koneksi TCP berkala. Cocokkan timestamp agar jendela runtime mencakup seluruh load.

**VU bukan ukuran koneksi TCP/in-flight secara langsung.** k6 memakai virtual user dan keep-alive; sampling TCP mencakup koneksi idle/probe serta tidak membuktikan 50 request sedang diproses pada setiap milidetik. Sajikan konfigurasi constant VUs dan observasi koneksi dengan batas ini. Jika penguji meminta konfigurasi/observasi koneksi yang berbeda, ikuti arahan dan ulangi pengukuran; jangan mengubah label hasil menjadi klaim yang tidak diukur. `PURPOSE=isolation mixed 2` sebelumnya adalah bukti P2.1 saja, bukan pengganti load ≥50 untuk P2.2.

### 6.7 P2.3 outage, cache dan recovery

Pastikan PVMBG fresh dan cache vulkanik terisi dahulu:

```bash
node scripts/m1-demo.mjs request field '/hazards?hazard_type=VOLCANIC'
node scripts/evidence-probes.mjs outage --seconds 600 --name p2-outage-10m
```

Probe menyatakan outage melalui endpoint admin, membuktikan source data 503, lalu memeriksa seismik dan vulkanik tetap 200, data cache dipertahankan dengan `source_status.PVMBG.state=stale` dan `stale_since`, serta mencatat latensi/timeline. Setelah durasi habis, outage dimatikan dan probe menunggu source fresh serta report baru ter-ingest, tanpa restart container. Untuk latihan cepat gunakan `--seconds 30`; untuk arahan 20 menit gunakan `1200`. Simpan bukti sesuai durasi sesungguhnya.

Untuk dikendalikan langsung oleh asisten, gunakan terminal lain:

```bash
node scripts/m1-demo.mjs outage on
node scripts/m1-demo.mjs request field '/hazards?hazard_type=SEISMIC'
node scripts/m1-demo.mjs request field '/hazards?hazard_type=VOLCANIC'
node scripts/m1-demo.mjs outage off
# Tunggu retry poll sukses lalu ulangi untuk menunjukkan state fresh.
node scripts/m1-demo.mjs request field '/hazards?hazard_type=VOLCANIC'
```

Kendali manual `outage on` sengaja bertahan sampai `off`; probe otomatis merestorasi outage di `finally` termasuk saat Ctrl+C. Recovery bisa menunggu retry hingga sekitar `SOURCE_RETRY_MAX_SEC` (contoh 60 detik) ditambah waktu fetch. Health PVMBG tetap hidup saat outage data; tunjukkan source data dan freshness, bukan hanya health hijau. Setelah outage panjang melewati ambang kebaruan, presentasi dapat menjadi unavailable secara eksplisit; dokumentasikan nilai freshness dan batas skenario yang dijalankan. Jangan menjalankan probe schema/mapping atau load normal serentak dengan long outage.

## 7. Selesai demo dan reset untuk latihan berikutnya

```bash
node scripts/m1-demo.mjs outage off
# Hanya jika sebelumnya memakai delay 3000 dan file baseline tersimpan:
node scripts/m1-demo.mjs delay restore
docker compose start consumer-notifier
docker compose --profile consumer-demo --profile monitoring stop
```

`delay restore` mengembalikan dua angka awal dan recreate PVMBG, lalu menghapus file baseline delay jika Compose berhasil. File `.env` lain dipertahankan. Hentikan semua profil yang digunakan; volume tetap tersimpan. Untuk latihan bersih berikutnya, gunakan project baru dan ulangi build/start. Simpan evidence latihan lama di lokasi terpisah sebelum probe bernama tetap dijalankan lagi. Tidak perlu `down -v` untuk menjalankan panduan ini.

Jika hanya perlu membuka DB lagi, `node scripts/m1-demo.mjs db psql` membuka sesi interaktif dengan default transaksi read-only. Gunakan `\q` untuk keluar. Operator Docker secara teknis memiliki hak admin; mode read-only ini mempermudah inspeksi saat presentasi.

## 8. Respons cepat terhadap masalah saat demo

| Gejala | Tindakan |
| --- | --- |
| `preflight` gagal | Periksa `docker compose ps`; tunggu startup registry/DB; lihat log service terkait secara lokal. `healthy` consumer saja tidak membuktikan processing; periksa `/ready`. |
| Source sudah v2 sebelum P1 | Gunakan project demo baru sebelum sesi. Jangan menjalankan schema probe dua kali tanpa menyiapkan baseline source/reader yang tepat. |
| Field v2 belum muncul di attributes | Bedakan writer v2 dan reader v1; tunggu discovery dan aktivasi reader v2, kemudian tunggu **report baru**. |
| P4 storage gagal | Pastikan memakai baseline schema dari run yang sama; jangan recreate/toggle source di antara schema dan storage. |
| `fanout` terputus | Tool mencoba start notifier; jika proses dipaksa mati/terminal tertutup, jalankan `docker compose start consumer-notifier`, periksa ready dan broker backlog. |
| `third-consumer` menolak | Subscriber/durable demo sudah ada. Siapkan project baru untuk bukti penambahan pertama; tidak menghapus data secara otomatis. |
| BMKG p95 ≥300 ms atau error ≥1% | Simpan hasil gagal, periksa resource CPU/memori, freshness/cache, auth latency dan batas concurrency; perbaiki lalu ulangi. Jangan melaporkan angka contoh sebagai hasil ukur. |
| 429 muncul | Penolakan terkontrol; tetap laporkan jumlah. Pastikan masih ada request sukses dan error di luar 429 memenuhi batas. |
| k6 tidak mencapai API | `BASE_URL` k6 harus memakai `host.docker.internal`, bukan `localhost` di container. Wrapper menambahkan host-gateway untuk Linux. |
| Restore delay gagal | Pastikan `evidence/pvmbg-delay-original.json` ada dan berasal dari checkout ini; ulangi restore setelah masalah Compose selesai. |
| PVMBG masih stale setelah `off` | Tunggu jadwal retry; periksa log/freshness, jangan langsung restart saat sedang membuktikan recovery tanpa restart. |

Sebelum mengakhiri sesi, pastikan semua P1–P5 sudah mempunyai bukti aktual dan setiap anggota telah mengambil bagian. Laporan PDF tetap harus memuat bukti, penjelasan alternatif dan batasan; CLI tidak dapat membuktikan kualitas penalaran teknis kelompok dengan sendirinya.

## 9. Verifikasi alat bantu yang sudah dilakukan

Pada 22 Oktober 2026, **22 pengujian skrip lulus**. Stack uji dengan port/volume terpisah dan image lokal berhasil menjalankan preflight, inspeksi DB, isolasi store, penolakan raw Media, fan-out/replay notifier, subscriber ketiga, mapping, schema evolution, storage, kontrol delay/outage, dan restore delay. Source PVMBG setelah pengaturan 3000 ms terukur **3075,58 ms** pada satu request.

Bukti lokal tersimpan di [evidence/tool-validation-2026-10-22](../evidence/tool-validation-2026-10-22/README.txt), yang diabaikan Git. Verifikasi ini memakai image yang sudah tersedia, tanpa build ulang seluruh aplikasi. Sustained load, token expiry alami, serta outage 10 menit belum dijalankan ulang dalam verifikasi ini; lakukan saat latihan. Angka satu request source tersebut bukan p95 atau bukti kelulusan P2.1. Stack uji sementara dibersihkan setelah pengujian.
