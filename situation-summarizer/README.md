# Situation Summarizer (Bonus AI)

Layanan opsional yang membuat ringkasan situasi bencana berbahasa Indonesia dan menjawab pertanyaan pengguna lewat chat, dengan LLM melalui OpenRouter. Layanan ini hanya membaca data dan tidak berada di jalur peringatan, sehingga peringatan tetap berjalan normal saat LLM lambat, salah, atau mati.

## Alur

```
Aggregator ──GET /internal/hazards──▶ situation-summarizer ──▶ OpenRouter (LLM)
                                            │
client-api GET /situation ◀── GET /summary ─┤   (timeout 1 detik)
client-api POST /situation/chat ◀── POST /chat ┘   (timeout 75 detik)
```

1. Setiap `SUMMARY_CHECK_INTERVAL`, layanan mengambil hazard dalam `SUMMARY_WINDOW` terakhir dari Aggregator. Fakta seperti jumlah per jenis, jumlah per tingkat, wilayah teratas, kejadian AWAS, dan status sumber dihitung di Go secara deterministik.
2. LLM hanya dipanggil bila data berubah:
   - Perubahan biasa ditunggu sampai `SUMMARY_REFRESH_INTERVAL`.
   - Kejadian SIAGA/AWAS baru atau perubahan status sumber dipanggil setelah `SUMMARY_URGENT_DELAY`.
   - Jarak antar-panggilan minimal `SUMMARY_MIN_CALL_INTERVAL`, dan jumlahnya dibatasi `SUMMARY_MAX_CALLS_PER_DAY`.
3. Jawaban LLM wajib mengikuti JSON schema dan divalidasi. LLM tidak melihat UUID `hazard_id`, tetapi kode pendek `E1`, `E2`, dan seterusnya yang dipetakan kembali oleh Go. Setiap kode harus ada di data, ada batas panjang teks, dan field asing ditolak. Jawaban yang tidak lolos validasi dibuang, dan ringkasan valid sebelumnya tetap dipakai.
4. `GET /summary` selalu menjawab dari memori tanpa menunggu LLM. Statusnya:

| status | arti |
|---|---|
| `fresh` | Ringkasan AI sesuai dengan data terbaru. |
| `stale` | Data sudah berubah atau pengecekan gagal. Ringkasan AI lama tetap ditampilkan dengan alasannya. |
| `unavailable` | Belum ada ringkasan AI yang valid. Bagian `facts` tetap terisi. |

Ringkasan AI hanya alat bantu baca dan bukan peringatan resmi. Data otoritatif tetap `/hazards` dan jalur peringatan pesisir.

## Chat

`POST /chat` menjawab pertanyaan dari snapshot terakhir di memori (diperbarui setiap `SUMMARY_CHECK_INTERVAL`), jadi chat tidak menambah beban ke Aggregator.

```json
{"question": "Gunung mana yang berstatus AWAS?", "history": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]}
```

- Riwayat percakapan disimpan di client dan dikirim setiap giliran. Server hanya memakai `CHAT_MAX_HISTORY` pesan terakhir dan tidak menyimpan sesi, sehingga aman dijalankan dengan beberapa replika.
- Jawaban berisi `answer`, `references` (kejadian asli yang dirujuk, hasil pemetaan kode `E1`, `E2`, dan seterusnya), `model`, `data_as_of`, dan `disclaimer`. Rujukan yang tidak ada di data membuat jawaban ditolak.
- Pertanyaan di luar rentang data dijawab apa adanya, bahwa datanya tidak tersedia.
- Chat dibatasi `CHAT_MAX_CONCURRENT` pertanyaan bersamaan. Bila penuh, langsung dijawab `503` tanpa antre. Kuota harian `CHAT_MAX_CALLS_PER_DAY` terpisah dari kuota ringkasan, sehingga chat yang ramai tidak menghabiskan jatah ringkasan.

| HTTP | Arti |
|---|---|
| `200` | Jawaban berhasil. |
| `400` | Pertanyaan kosong, lebih dari 500 karakter, atau riwayat tidak valid. |
| `429` | Kuota chat harian habis atau OpenRouter membatasi. |
| `502` | Jawaban AI ditolak karena merujuk kejadian yang tidak ada. |
| `503` | Data belum tersedia, AI sedang sibuk, atau layanan AI mati. |
| `504` | AI tidak menjawab dalam `CHAT_TIMEOUT`. |

## Model

`SUMMARY_MODEL` and `OPENROUTER_API_KEY` are explicit operator settings. Model availability and quotas depend on the provider. No live provider check is required for the local test suite. Calls have bounded concurrency, timeouts, and daily quotas; invalid output retains the previous valid summary.

The output ceiling defaults to 4096 tokens (maximum 16384), including up to 1024 reasoning tokens. Set an appropriate model explicitly; the deployment keeps this optional service disabled until credentials and a model are supplied.

## Konfigurasi

Salin `.env.example` ke `.env`, lalu isi `OPENROUTER_API_KEY`. File `.env` sudah di-ignore oleh git.

| Variabel | Default | Keterangan |
|---|---|---|
| `SUMMARIZER_PORT` | wajib | Port HTTP. |
| `AGGREGATOR_BASE_URL` | wajib | URL internal Aggregator. |
| `OPENROUTER_API_KEY` | wajib | API key OpenRouter. |
| `SUMMARY_MODEL` | wajib | Model utama. |
| `SUMMARY_FALLBACK_MODELS` | kosong | Opsional, daftar model cadangan dipisah koma. |
| `SUMMARY_WINDOW` | `2h` | Rentang data yang diringkas. |
| `SUMMARY_CHECK_INTERVAL` | `1m` | Interval pengambilan snapshot. |
| `SUMMARY_REFRESH_INTERVAL` | `30m` | Jeda pembaruan untuk perubahan biasa. |
| `SUMMARY_URGENT_DELAY` | `30s` | Debounce untuk perubahan SIAGA/AWAS. |
| `SUMMARY_MIN_CALL_INTERVAL` | `2m` | Jarak minimum antar-panggilan LLM. |
| `SUMMARY_LLM_TIMEOUT` | `60s` | Batas waktu satu panggilan LLM untuk ringkasan. |
| `SUMMARY_MAX_CALLS_PER_DAY` | `45` | Batas panggilan per hari (UTC). |
| `SUMMARY_MAX_HIGHLIGHTS` | `8` | Jumlah maksimum sorotan kejadian. |
| `LLM_MAX_TOKENS` | `4096` | Batas token output per panggilan (reasoning + jawaban), 2000–16384. |
| `LLM_REASONING_TOKENS` | `1024` | Batas token reasoning, paling banyak setengah `LLM_MAX_TOKENS`. |
| `CHAT_TIMEOUT` | `60s` | Batas waktu satu jawaban chat. Harus di bawah 75 detik, batas tunggu client-api. |
| `CHAT_MAX_CALLS_PER_DAY` | `150` | Kuota chat per hari (UTC). `0` mematikan chat. |
| `CHAT_MAX_CONCURRENT` | `2` | Jumlah pertanyaan yang diproses bersamaan. |
| `CHAT_MAX_HISTORY` | `8` | Jumlah pesan riwayat yang ikut dikirim ke LLM. |
| `SUMMARY_FAULT_MODE` | `off` | `slow` atau `invalid` untuk ringkasan dan chat, hanya untuk demo. |

## Menjalankan

Use the [M2 deployment workflow](../docs/m2/README.md). Set `features.ai=true`, the external API key, and the model in the private deployment configuration. The service runs on B with 128 MiB requested and 256 MiB limited. `SUMMARY_SERVICE_TOKEN` grants read access to Aggregator snapshots; it is separate from user credentials.

For standalone development, export settings from `.env.example`, then run `go run .`. Client API integration uses `SITUATION_BASE_URL=http://situation-summarizer:8094` and exposes authenticated `/situation` and `/situation/chat` endpoints. AI calls use a separate two-request allowance and the general scheduling lane, including calls by Field users.

## Endpoint

| Endpoint | Fungsi |
|---|---|
| `GET /summary` | Ringkasan AI beserta fakta deterministik. |
| `POST /chat` | Tanya-jawab berdasarkan snapshot terakhir. |
| `GET /health` | Liveness. |
| `GET /ready` | Siap setelah snapshot pertama berhasil diambil. |
| `GET /metrics` | `summarizer_llm_calls_<outcome>_total`, `summarizer_status`, `summarizer_summary_age_seconds`, `summarizer_budget_remaining`, `summarizer_chat_<outcome>_total`, `summarizer_chat_budget_remaining`, `summarizer_chat_last_seconds`. |

## Skenario demo

| Skenario | Cara | Yang diharapkan |
|---|---|---|
| LLM lambat | `SUMMARY_FAULT_MODE=slow`, lalu recreate container | Panggilan timeout dan `/situation` tetap menjawab dalam 1 detik dengan status `stale`/`unavailable`. Chat menjawab `504` setelah `CHAT_TIMEOUT`. Peringatan pesisir dan `/hazards` tidak terpengaruh. |
| LLM salah | `SUMMARY_FAULT_MODE=invalid` | Jawaban yang merujuk kejadian palsu ditolak (`summarizer_llm_calls_rejected_total` dan `summarizer_chat_rejected_total` naik), ringkasan lama dipertahankan, dan chat menjawab `502`. |
| LLM mati | Isi `OPENROUTER_API_KEY` dengan nilai salah | Panggilan gagal, backoff eksponensial sampai 1 jam, dan `facts` tetap tersedia. |
| Layanan mati | Stop the optional deployment | `/situation` langsung menjawab `unavailable`, dan endpoint lain berjalan normal. |

## Test

```
go test -race ./...
```
