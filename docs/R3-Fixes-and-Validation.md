# Perbaikan tiga temuan R3

Perubahan ini menyelesaikan tiga gap kode yang tercatat pada review awal. Pembuktian failover VM, gangguan gabungan, sensor/feed asli dan load testing M2 tetap memerlukan lingkungan integrasi kelompok.

## 1. Payload gagal disimpan utuh

Decoder tetap memakai `MAX_PAYLOAD_BYTES` sebagai batas RAM. Ketika respons melampauinya, recorder menerima kembali seluruh prefix **termasuk byte pendeteksi overflow** dan sisa respons sebagai stream. Aggregator menyimpan stream dalam chunk maksimal 64 KiB, bersama ukuran, SHA-256, content type dan metadata kegagalan, dalam satu transaksi di database yang memang sudah dimiliki Aggregator untuk failure legacy.

`FAILED_INPUT_QUOTA_BYTES` membatasi penyimpanan body besar (default 256 MiB). Gangguan baca atau kuota habis membatalkan transaksi secara eksplisit; tidak ada klaim body lengkap dan cursor sumber tidak dimajukan. Percobaan identik memakai digest lengkap dan tidak menggandakan blob. Tidak ada penghapusan otomatis input yang belum diperiksa.

Ops dapat mengambil byte asli melalui `GET /internal/ingestion/failures/<id>/payload?namespace=legacy-m1`. Ingestion mengautentikasi role Internal Ops, lalu melakukan proxy streaming dengan token service ke pemilik payload. Token service saja tidak membuka endpoint Ops. Replay membaca body lengkap bila batas decoding sudah memadai; bila belum, pesan tetap parked dengan alasan yang menyebut batas yang harus dinaikkan. Ini tidak memulihkan byte pesan lama yang sebelumnya sudah terlanjur terpotong.

## 2. Status coastal konsisten antar-replika

Snapshot disimpan pada database ingestion. Semua pod membaca snapshot yang sama, dan penulisan snapshot memerlukan lease acquisition yang masih valid. Pengganti pemilik lease memulihkan state terakhir. Waktu check, freshness dan expiry menggunakan clock database, bukan perbandingan jam dua pod.

Snapshot tetap menua ketika leader hilang; replika standby tidak membuat sumber sehat menjadi unavailable hanya karena memori lokalnya kosong. Pengulangan observasi lama tidak memperbarui waktu observasi baru terakhir. Gangguan authority status menghasilkan kegagalan eksplisit, lalu Aggregator memakai aturan cache/unavailable yang sudah ada.

## 3. SLO menyertakan operasi yang belum diterima notifier

Akuisisi menyimpan identitas setiap observasi bertanda `critical: true` **sebelum publish raw**. Identitas warning dan waktu akuisisi pertama tetap sama pada pengulangan. Observasi kritis yang gagal validasi tetap terlihat sebagai rejected; perbedaan antara invalid input dan warning yang hanya menunggu pemrosesan dapat diperiksa.

Notifier mengambil ledger melalui HTTP ber-token `COASTAL_SLO_TOKEN`, lalu mencocokkannya dengan receipt miliknya sendiri. Tidak ada akses langsung ke database service lain. Halaman dibatasi 500 identitas dengan snapshot `through/as_of`; request dibatasi 100.000 identitas secara default dan deadline 10 detik.

`evaluated = delivered + rejected_without_receipt + pending_overdue_without_rejection`. Pending yang belum berumur lima detik tetap dilaporkan, tetapi belum dibebankan sebagai kegagalan. `on_time_ratio = on_time / evaluated`; budget terpakai adalah `(1 - on_time_ratio) / 0.05`. Bila denominator kosong, rasio dan budget bernilai null.

p95 receipt tetap tersedia dengan label `delivered_only`. `p95_lower_bound_seconds` memasukkan usia pending, sedangkan `p95_complete` mencegah subset receipt dinyatakan sebagai hasil seluruh cohort. `coverage_since`, `coverage_complete` dan `receipt_only` menunjukkan batas upgrade/historical data. Authority ledger tidak tersedia menghasilkan 503 untuk laporan, sementara pengiriman warning tetap berjalan.

Scope pembuktian endpoint adalah identitas kritis yang sudah diakuisisi secara durable. Untuk membuktikan zero loss dari Sensor Pantai, cocokkan juga dengan daftar identitas sumber/referensi. Latensi memakai clock database ingestion dan notifier; sinkronisasi keduanya harus diverifikasi. Durasi negatif ditandai sebagai clock anomaly dan tidak dihitung on-time. p95 bukan bukti yang sah selama basis clock tidak tervalidasi.

## Deployment

1. Jalankan offline migration canonical sampai `0013_failure_payloads.sql` dan ingestion sampai `0004_coastal_accounting.sql` sebelum memulai binary baru. Migration lama tidak diedit.
2. Tambahkan token acak khusus `COASTAL_SLO_TOKEN` yang sama ke Secret role1 runtime dan `notifier-runtime`, berbeda dari credential service lainnya.
3. Pastikan notifier memakai `WARNING_EXPECTATIONS_URL=http://coastal-processor:8080`. Renderer sudah menambahkan token ke coastal dan izin jaringan dua arah notifier → coastal. Uji enforcement CNI pada cluster.
4. Catat awal coverage setelah rollout. Jangan menganggap periode sebelum ledger aktif sudah tercakup.

## Pengujian

Tes regresi mencakup body oversized utuh hingga byte terakhir, digest/duplikasi blob, rollback saat kuota habis dan koneksi terputus, replay setelah batas dinaikkan, inspeksi hanya Ops, pembacaan status melalui standby, fencing pemilik lama, expiry/pemulihan status, critical publish gagal sebelum cursor commit, ledger sebelum publish, warning tertunda/ditolak dalam budget, pagination 501 item, batas window, kegagalan authority, dan clock anomaly.

Jalankan `python scripts/test-r3-local.py` dan `python scripts/test-r3-local.py --broker-only`. Log JSON dan jumlah skip tersimpan di `evidence/r3-review/<run-id>/`. Pengujian PostgreSQL dan JetStream lokal tidak menggantikan bukti replikasi/failover cluster atau p95 sensor asli.

Hasil verifikasi lokal:

| Run / pemeriksaan | Hasil |
| --- | --- |
| `e5ad5178bf9d` — semua 12 modul Go, vet, integrasi PostgreSQL R3 dan race detector | 31 pemeriksaan lulus; 1.383 eksekusi tes/subtes lulus, 165 skip infrastruktur/tes khusus. |
| `68d7998672d4` — broker dan consumer PostgreSQL/JetStream nyata, race detector | 4 pemeriksaan lulus; 225 eksekusi tes/subtes lulus, 1 skip yang memerlukan registry nyata. |
| Python scripts + platform_lib | 64 + 30 tes lulus. |
| JavaScript evidence/demo/port/summary | 22 tes lulus. |
| Pemeriksaan akhir p95 cohort pada notifier | Empat tes SLO lulus dengan PostgreSQL dan race detector; vet notifier lulus. |

Jumlah eksekusi Go di atas mencakup pengulangan tes pada native, integrasi dan race; bukan jumlah tes unik. Tes yang membutuhkan Citus, broker tiga replika, fault controller dan layanan eksternal tetap dicatat sebagai skip, bukan bukti kelulusan kriteria tersebut.
