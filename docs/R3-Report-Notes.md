# Bahan Laporan R3 (P3, P4 akuisisi, P5)

Catatan ini adalah bahan untuk bagian laporan yang dipegang R3. Isinya mendeskripsikan mekanisme yang sudah diimplementasikan. **Angka hasil pengukuran belum ada.** Isi angka dari bukti game day dan load test; jangan menulis klaim sebelum ada buktinya.

## P3 – Surat dari Tetangga

### Identifikasi masalah

| Kriteria | Persoalan | Nama dalam literatur |
| --- | --- | --- |
| 2 | Pesan gagal harus disimpan utuh dan dibedakan antara yang bisa dicoba lagi dan yang tidak | *Dead Letter Queue*, *poison message*, klasifikasi *transient vs permanent failure* |
| 3 | Input rusak tidak boleh menjatuhkan konsumen atau memblokir pesan lain | *Poison pill*, *head-of-line blocking*, *bulkhead* |
| 4 | Memproses ulang setelah adaptasi tanpa duplikat dan dengan urutan terjaga | *Replay / redrive*, *idempotent consumer*, *per-key ordering* |
| 6 | 202 dikirim lalu penerima mati; laporan tidak boleh hilang atau ganda | *Durable acknowledgement*, *transactional outbox*, *idempotency key*, *at-least-once + idempotensi* |

Konsep pendukung: *schema evolution* (kompatibilitas *backward/forward/full*, *schema registry*), *tolerant reader* vs *strict reader*.

### Kebijakan perubahan skema

| Perubahan | Kebijakan | Alasan |
| --- | --- | --- |
| Field tambahan opsional | Diterima otomatis, disimpan di `attributes` | Tidak mengubah makna field yang sudah dipetakan. |
| Rename, ganti tipe, field wajib hilang | Di-*park* sampai ada adapter yang secara eksplisit menyatakan alias, konversi, atau ketiadaan nilai | Menebak niat produsen berisiko salah. Nilai yang hilang tidak boleh dikarang. |
| Input rusak | Di-*park* dengan byte asli | Tidak akan pernah berhasil tanpa perubahan sistem. |
| Registry, jaringan, atau database tidak tersedia | `retrying` dengan *capped jittered backoff* | Akan pulih sendiri. |

**Implementasi:**
- Kegagalan resolusi skema Avro dibungkus sebagai `ErrIncompatible`, sehingga tercatat `parked` dan tidak dicoba ulang terus-menerus.
- Mock PVMBG punya mode `rename`, `type_change`, dan `missing_required` di `POST /admin/schema-version`. Mode ini memakai subject registry terpisah dengan kompatibilitas `NONE`, sehingga subject utama tetap `FULL_TRANSITIVE`.

### Pemrosesan ulang

1. Adapter v2 diaktifkan lewat konfigurasi (`PVMBG_ADAPTER_VERSION=2` dan reader `volcanic-adapter-v2.avsc`) dengan *rolling update*.
   - Alasannya: semua replika Aggregator harus memakai adapter yang sama.
   - Toggle di memori tidak cukup karena hanya berlaku di satu pod.
2. Ops meminta replay lewat `POST /internal/ingestion/replays`, yang hanya bisa dipakai role `internal`.
3. Aggregator menandai baris `failed_ingestions` sebagai `replaying`.
4. Poller PVMBG memprosesnya di bawah lease acquisition yang sama dengan polling live, urut `(received_at, message_id)`.
5. Hasilnya di-*commit* lewat jalur `CommitVolcanic` yang idempoten per `source_ref_id`.
6. Pesan yang pulih ditandai `recovered` dan hilang dari `/unprocessed`. Pesan yang masih tidak terbaca kembali `parked` dengan alasan baru.

### Jaminan jalur laporan Tim Lapangan

Jaminan: **at-least-once di transport ditambah efek tepat sekali di proyeksi** (*effectively-once*).

Urutan penerimaan:
1. Kunci advisory pada pasangan (submitter, `Idempotency-Key`).
2. Simpan baris `pending` beserta byte dan sidik jari ke database milik ingestion.
3. Publish RawInput ke `BNPB_RAW_REPORTS` (R3, *file storage*) dengan `Nats-Msg-Id`, lalu tunggu PubAck.
4. Tandai baris `accepted`, baru kirim **202**.

Penanganan kegagalan:
- Publish gagal atau hasilnya tidak diketahui → **503 `acceptance_pending`**. Tidak pernah 202.
- *Outbox worker* mem-*publish* ulang baris `pending`.
- Consumer `field-reports-v1` menyisipkan proyeksi dengan `ON CONFLICT DO NOTHING` dan menandai `processed` dalam satu transaksi, baru kemudian *double ack*.

Bukti yang perlu dikumpulkan: kirim laporan, hapus pod ingestion tepat setelah 202, lalu tunjukkan status `processed` dan `field_reports` berisi satu baris.

### Alternatif yang dipertimbangkan

- Menerima 202 setelah ditulis ke memori atau Core NATS. Ditolak karena laporan bisa hilang saat crash.
- Menunggu sampai proyeksi selesai sebelum 202. Ditolak karena 202 jadi lambat saat hilir lambat.
- Deduplikasi hanya lewat *duplicate window* JetStream. Ditolak karena jendelanya cuma 120 detik, sehingga tetap perlu ledger yang tahan lama.

### Batasan yang disadari

- Database ingestion bergantung pada formasi Patroni milik platform untuk durabilitas replika.
- Replay hanya mencakup sumber PVMBG lama (`legacy-m1`) dan BMKG.

## P4 – Akuisisi feed satelit (bagian R3)

### Pola pengambilan

Polling M1 ditinggalkan untuk sumber ini dan diganti **SSE dengan offset**:
- Feed mengirim ribuan deteksi per lintasan lalu diam. Polling bisa melewatkan data atau membuang permintaan.
- Feed menyediakan offset monoton tanpa celah, sehingga posisi bisa dilanjutkan dengan tepat.

### Mekanisme

- Hanya satu replika yang memegang lease `acquisition:SATELIT`. Konsep: *leader election* dengan *fencing token*.
- Event dinormalisasi menjadi `wildfire.Operation` dengan byte asli disimpan.
- RawInput dipublish dengan `Nats-Msg-Id` deterministik.
- Offset di database dimajukan **hanya setelah PubAck**, dengan batch per 256 event atau per 1 detik.
- Saat sambung ulang, konektor mulai dari offset durable:
  - offset lebih kecil dari yang diharapkan → duplikat, dilewati;
  - offset lebih besar → celah, putus dan sambung ulang;
  - 429/503 → mengikuti `Retry-After`;
  - stream diam lebih lama dari `SATELLITE_IDLE_TIMEOUT` → putus.
- Event yang tidak bisa dinormalisasi dikarantina, lalu diganti `heartbeat` yang membawa byte aslinya. Tujuannya agar riwayat wildfire tetap berurutan tanpa celah.
- Jaminan di batas ini: **at-least-once publish + deduplikasi berdasarkan identitas** (JetStream dan `AppendHistory` dengan outcome `duplicate`) = *effectively-once*.

**Belum diverifikasi:** nama field dan apakah `from` inklusif, terhadap feed asli. Perlu token grup.

## P5 – Pelampung yang Berbohong

### Identifikasi masalah

| Kriteria | Persoalan | Nama dalam literatur |
| --- | --- | --- |
| 2 | Data melanggar kontrak | *Input validation*, *quarantine / dead letter* |
| 3 | Sensor bermasalah tidak boleh memperlambat sumber lain | *Bulkhead isolation* |
| 4 | Permintaan ke sensor tidak boleh melonjak; client dijawab cepat | *Circuit breaker*, *retry storm*, *rate limiting*, *fail fast*, *graceful degradation*, *stale-while-error* |
| 5 | Beban berlebih: Tim Lapangan dilayani, Media ditolak terkendali | *Load shedding*, *admission control*, prioritas (dikerjakan R2) |

Konsep lain:
- *Gray failure*: sensor mengaku sehat lewat `/healthz` padahal datanya basi.
- *Exactly-once processing* (*effectively-once*).
- *SLI/SLO/error budget*.

### Mekanisme dan konfigurasi

- **Isolasi.** Coastal berjalan di Deployment `coastal-processor` terpisah, dengan klien HTTP, breaker, rate limit, dan pod sendiri. Sumber lain tetap di `ingestion` dan Aggregator.
- **Circuit breaker per endpoint** (`/v1/observations` dan `/v1/stations`):
  - terbuka setelah 3 kegagalan berturut-turut;
  - masa terbuka 2 detik, berlipat dua per probe gagal sampai maksimal 30 detik;
  - hanya satu probe *half-open* dalam satu waktu.
  - 401 tidak dihitung sebagai gangguan jaringan, melainkan kesalahan konfigurasi.
- **Rate limit bersama.** Maksimal 8 permintaan per detik untuk semua panggilan, termasuk probe. Timeout per permintaan 1 detik; dari hasil karakterisasi, p95 sensor sehat di bawah 20 ms.
- **Data basi.** Kesegaran dihitung dari `observed_at` terhadap `generated_at` dan dari waktu observasi baru terakhir, tidak dari `/healthz`. Statusnya disediakan di `/internal/coastal/status`.
  - Aggregator membaca status ini di latar belakang dan menyajikannya sebagai `source_status.SENSOR_PANTAI`.
  - Akibatnya, permintaan client tidak pernah menunggu sensor.
- **Jalur peringatan kritis.** Jalur ini tidak bergantung pada penulisan canonical:
  1. Akuisisi, lalu RawInput ke `BNPB_RAW_COASTAL`.
  2. `coastal-validation-v1` memvalidasi. Data tidak valid dikarantina; data kritis menjadi `CoastalWarning` Avro di `BNPB_CRITICAL` dengan `Nats-Msg-Id = notification_id` (berbasis isi).
  3. `tim-lapangan-v1` di notifier menyimpan receipt dan inbox dalam satu transaksi, lalu *double ack*.
  - Penulisan HazardEvent COASTAL lewat consumer kedua (`coastal-canonical-v1`), sehingga keterlambatan canonical tidak menunda peringatan.
- **Penjadwalan penerima.** Rasio 3 peringatan baru : 1 *backlog*. Peringatan untuk observasi yang sama tetap urut.

### Target keandalan (SLO)

- **Zero loss:** tidak ada operasi kritis yang hilang. Dicek dengan rekonsiliasi `observation_id` kritis di sumber terhadap `coastal_warning_receipts`.
- **Ketepatan waktu:** ≥ 95% peringatan kritis sampai dalam ≤ 5 detik, diukur dari akuisisi pertama BNPB sampai *commit* di penerima, selama jendela game day.
- **Error budget:** 5% operasi kritis boleh terlambat. `GET /warnings/slo` menghitung `error_budget_used` hanya dari receipt yang sudah terkirim. Operasi tertunda belum masuk denominator endpoint ini; rekonsiliasi terpisah dengan identitas kritis dari sumber wajib dilakukan, dan yang melewati batas lima detik harus dihitung terlambat. Endpoint ini sendiri belum membuktikan zero loss atau error budget seluruh jalur.

### Perilaku menyimpang sensor

Lihat [Sensor-Pantai-Characterization.md](Sensor-Pantai-Characterization.md). Pada profil latihan ditemukan:
- duplikat isi sekitar 2,3%;
- `next_cursor` bernilai null di halaman kosong;
- kualitas `suspect`;
- jam virtual 60×.

Perilaku profil ujian dicatat saat game day dengan skrip `scripts/probe-sensor-pantai.py`.

### Templat post-mortem game day

| Bagian | Isi |
| --- | --- |
| Ringkasan | Apa yang terjadi, kapan, berapa lama. |
| Deteksi | Metrik atau trace mana yang pertama menunjukkan masalah, misalnya `coastal_observations_breaker_state`, `coastal_observation_lag_seconds`, `/warnings/slo`, `satellite_feed_lag_offsets`. Sertakan waktu deteksi. |
| Dampak | Jumlah peringatan kritis terlambat atau tertunda, p95 dan maksimum latensi, sisa error budget, dan sumber lain yang terdampak (harusnya tidak ada). |
| Mitigasi otomatis | Breaker terbuka, data terakhir disajikan dengan status stale, backlog diproses dengan rasio 3:1, dan sebagainya. Tidak ada intervensi manual selama jendela pemulihan. |
| Akar masalah | Berdasarkan log ber-correlation ID dan trace. |
| Perbaikan | Perubahan konfigurasi atau kode beserta alasannya. |
