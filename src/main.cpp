// Shopping Sessions pipeline: native C++, no dependencies.
// Reads $DATA_DIR, writes Parquet tables to $OUTPUT_DIR/{user_daily,campaign_daily}.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <functional>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <memory>

using namespace std;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;

static const i64 DAY_MS = 86400000LL;
static const i64 GAP_MS = 1800000LL;
static const i64 WEEK_MS = 7 * DAY_MS;

static int NT = 8;
static auto T0 = chrono::steady_clock::now();
static void logt(const char* what) {
    double s = chrono::duration<double>(chrono::steady_clock::now() - T0).count();
    fprintf(stderr, "[%7.3f] %s\n", s, what);
}

template <class F> static void par(size_t n, F&& f) {
    atomic<size_t> next(0);
    auto work = [&](int tid) {
        for (;;) {
            size_t i = next.fetch_add(1, memory_order_relaxed);
            if (i >= n) break;
            f(i, tid);
        }
    };
    vector<thread> th;
    int k = (int)min<size_t>(NT, n ? n : 1);
    for (int t = 1; t < k; t++) th.emplace_back(work, t);
    work(0);
    for (auto& t : th) t.join();
}

// ---------------------------------------------------------------- hashing
static inline u64 mix64(u64 x) {
    x ^= x >> 32; x *= 0xd6e8feb86659fd93ULL; x ^= x >> 32; x *= 0xd6e8feb86659fd93ULL; x ^= x >> 32;
    return x;
}
static inline u64 hash_str(const char* p, size_t n) {
    u64 a, b;
    if (n >= 8) {
        memcpy(&a, p, 8);
        memcpy(&b, p + n - 8, 8);
        if (n > 16) {
            u64 h = 0x9E3779B97F4A7C15ULL ^ n;
            size_t i = 0;
            for (; i + 8 <= n; i += 8) { u64 w; memcpy(&w, p + i, 8); h = (h ^ w) * 0xbf58476d1ce4e5b9ULL; h ^= h >> 29; }
            return mix64(h ^ b);
        }
    } else if (n >= 4) {
        u32 x, y; memcpy(&x, p, 4); memcpy(&y, p + n - 4, 4); a = x; b = y;
    } else {
        a = n ? (u64)(unsigned char)p[0] | ((u64)(unsigned char)p[n >> 1] << 8) | ((u64)(unsigned char)p[n - 1] << 16) : 0;
        b = 0;
    }
    return mix64(mix64(a ^ 0x9E3779B97F4A7C15ULL ^ ((u64)n << 56)) ^ b);
}
static inline bool seq(const char* a, u32 an, const char* b, u32 bn) { return an == bn && memcmp(a, b, an) == 0; }
static inline int scmp(const char* a, u32 an, const char* b, u32 bn) {
    int c = memcmp(a, b, min(an, bn));
    if (c) return c;
    return an < bn ? -1 : an > bn ? 1 : 0;
}

// generic read-only string -> int map
struct StrMap {
    struct E { u64 h; const char* s; u32 n; int v; };
    vector<E> t; u64 mask = 0;
    void init(size_t cap) { size_t sz = 16; while (sz < cap * 2) sz <<= 1; t.assign(sz, E{0, nullptr, 0, -1}); mask = sz - 1; }
    // returns existing value or inserts v
    int insert(const char* s, u32 n, int v) {
        u64 h = hash_str(s, n);
        for (u64 i = h & mask;; i = (i + 1) & mask) {
            E& e = t[i];
            if (e.v < 0) { e = E{h, s, n, v}; return v; }
            if (e.h == h && seq(e.s, e.n, s, n)) return e.v;
        }
    }
    int find(const char* s, u32 n) const {
        u64 h = hash_str(s, n);
        for (u64 i = h & mask;; i = (i + 1) & mask) {
            const E& e = t[i];
            if (e.v < 0) return -1;
            if (e.h == h && seq(e.s, e.n, s, n)) return e.v;
        }
    }
};

// ---------------------------------------------------------------- files
struct MFile { const char* p; size_t n; };
static const size_t PAD = 65536;
static vector<MFile> list_csv(const string& dir) {
    vector<MFile> out;
    DIR* d = opendir(dir.c_str());
    if (!d) return out;
    vector<string> names;
    while (dirent* e = readdir(d)) {
        string nm = e->d_name;
        if (nm.size() >= 4 && nm.compare(nm.size() - 4, 4, ".csv") == 0) names.push_back(nm);
    }
    closedir(d);
    sort(names.begin(), names.end());
    for (auto& nm : names) {
        string path = dir + "/" + nm;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;
        struct stat st; fstat(fd, &st);
        if (!S_ISREG(st.st_mode)) { close(fd); continue; }
        size_t n = st.st_size;
        if (n == 0) { close(fd); continue; }
        // Reserve the file plus 64 KB of zero pages after it, so SIMD loads may run past any field.
        size_t pg = 4096, mapped = (n + pg - 1) / pg * pg;
        void* r = mmap(nullptr, mapped + PAD, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (r == MAP_FAILED) { perror("mmap"); exit(2); }
        void* m = mmap(r, n, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
        close(fd);
        if (m == MAP_FAILED) { perror("mmap"); exit(2); }
        madvise(m, n, MADV_SEQUENTIAL);
        out.push_back({(const char*)m, n});
    }
    return out;
}
static string read_file(const string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "cannot open %s\n", path.c_str()); exit(2); }
    string s; char buf[65536]; size_t k;
    while ((k = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, k);
    fclose(f);
    return s;
}

struct Chunk { const char* b; const char* e; };  // lines starting in [b,e)
static vector<Chunk> make_chunks(const vector<MFile>& files, size_t target) {
    vector<Chunk> ch;
    for (auto& f : files) {
        const char* end = f.p + f.n;
        // skip header line
        const char* s = (const char*)memchr(f.p, '\n', f.n);
        if (!s) continue;
        s++;
        while (s < end) {
            const char* e = s + target;
            if (e >= end) e = end;
            else {
                const char* nl = (const char*)memchr(e, '\n', end - e);
                e = nl ? nl + 1 : end;
            }
            ch.push_back({s, e});
            s = e;
        }
    }
    return ch;
}

struct Fld { const char* p; u32 n; };
// Split one line [s,e) into exactly nf fields under the rules' dialect.
static inline bool split_fields(const char* s, const char* e, Fld* f, int nf) {
    int k = 0;
    const char* p = s;
    for (;;) {
        if (p < e && *p == '"') {
            const char* r = p + 1;
            while (r < e && *r != '"' && *r != ',') r++;
            if (r >= e || *r != '"') return false;
            const char* after = r + 1;
            if (after < e && *after != ',') return false;
            if (k >= nf) return false;
            f[k++] = {p + 1, (u32)(r - p - 1)};
            if (after >= e) break;
            p = after + 1;
        } else {
            const char* r = p;
            while (r < e && *r != ',' && *r != '"') r++;
            if (r < e && *r == '"') return false;
            if (k >= nf) return false;
            f[k++] = {p, (u32)(r - p)};
            if (r >= e) break;
            p = r + 1;
        }
    }
    return k == nf;
}
template <class F> static inline void for_lines(const Chunk& c, F&& f) {
    const char* s = c.b;
    while (s < c.e) {
        const char* nl = (const char*)memchr(s, '\n', c.e - s);
        const char* le = nl ? nl : c.e;
        f(s, le);
        s = le + 1;
    }
}

#include <immintrin.h>
// Scan the lines of a chunk with AVX2; calls f(fields) for every well-formed line of exactly NF fields
// (quotes stripped). Malformed lines are skipped.
template <int NF, class F> static void scan_chunk(const Chunk& c, F&& f) {
    const char* base = c.b;
    const size_t len = c.e - c.b;
    Fld fl[NF];
    const char* ls = base;   // line start
    const char* fs = base;   // field start
    int nc = 0;              // commas in the line
    int nq = 0;              // quotes in the line
    auto finish = [&](const char* le) {
        if (nc != NF - 1) return;
        fl[NF - 1] = {fs, (u32)(le - fs)};
        if (nq) {
            int exp = 0;
            for (int k = 0; k < NF; k++) {
                Fld& x = fl[k];
                if (x.n >= 2 && x.p[0] == '"' && x.p[x.n - 1] == '"') { exp += 2; x.p++; x.n -= 2; }
            }
            if (exp != nq) return;
        }
        f(fl);
    };
    const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
    alignas(32) char tail[64];
    for (size_t off = 0; off < len; off += 64) {
        const char* blk = base + off;
        size_t rem = len - off;
        const char* src = blk;
        if (rem < 64) { memset(tail, 0, 64); memcpy(tail, blk, rem); src = tail; }
        __m256i a = _mm256_loadu_si256((const __m256i*)src), b = _mm256_loadu_si256((const __m256i*)(src + 32));
        u64 cm = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
        u64 qm = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
        u64 nm = (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
        if (rem < 64) { u64 keep = (1ULL << rem) - 1; cm &= keep; qm &= keep; nm &= keep; }
        u64 m = cm | nm;
        while (m) {
            int i = __builtin_ctzll(m);
            u64 bit = 1ULL << i;
            m &= m - 1;
            const char* q = blk + i;
            if (cm & bit) {
                if (nc < NF - 1) fl[nc] = {fs, (u32)(q - fs)};
                nc++;
                fs = q + 1;
            } else {
                u64 below = bit - 1;
                nq += __builtin_popcountll(qm & below);
                qm &= ~below;
                finish(q);
                ls = fs = q + 1; nc = 0; nq = 0;
            }
        }
        nq += __builtin_popcountll(qm);
    }
    if (ls < c.e) finish(c.e);
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
    y -= m <= 2;
    const i64 era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (i64)doe - 719468;
}
static inline bool isd(char c) { return (unsigned)(c - '0') < 10; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
// Parses F1/F2/F3. Returns false for a bad form. F3 values too large become INT64_MAX (outside every window).
static inline bool parse_ts_slow(const char* p, u32 n, i64& out) {
    if (n == 0) return false;
    if (isd(p[0]) && n != 20 && n != 25) goto f3;
    if (n == 20 || n == 25) {
        if (!(isd(p[0]) && isd(p[1]) && isd(p[2]) && isd(p[3]) && p[4] == '-' && isd(p[5]) && isd(p[6]) && p[7] == '-' &&
              isd(p[8]) && isd(p[9]) && p[10] == 'T' && isd(p[11]) && isd(p[12]) && p[13] == ':' && isd(p[14]) &&
              isd(p[15]) && p[16] == ':' && isd(p[17]) && isd(p[18])))
            goto f3;
        int Y = d2(p) * 100 + d2(p + 2), M = d2(p + 5), D = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), se = d2(p + 17);
        i64 off = 0;
        if (n == 20) {
            if (p[19] != 'Z') return false;
        } else {
            char sg = p[19];
            if ((sg != '+' && sg != '-') || !isd(p[20]) || !isd(p[21]) || p[22] != ':' || !isd(p[23]) || !isd(p[24])) return false;
            int oh = d2(p + 20), om = d2(p + 23);
            if (oh > 23 || om > 59) return false;
            off = (i64)(oh * 60 + om) * 60000;
            if (sg == '-') off = -off;
        }
        if (M < 1 || M > 12 || D < 1 || h > 23 || mi > 59 || se > 59) return false;
        static const int mdays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int md = mdays[M - 1];
        if (M == 2 && ((Y % 4 == 0 && Y % 100 != 0) || Y % 400 == 0)) md = 29;
        if (D > md) return false;
        i64 days = days_from_civil(Y, M, D);
        out = ((days * 24 + h) * 60 + mi) * 60000 + (i64)se * 1000 - off;
        return true;
    }
f3:
    for (u32 i = 0; i < n; i++)
        if (!isd(p[i])) return false;
    {
        u32 i = 0;
        while (i < n && p[i] == '0') i++;
        if (n - i > 18) { out = INT64_MAX; return true; }
        i64 v = 0;
        for (; i < n; i++) v = v * 10 + (p[i] - '0');
        out = v;
        return true;
    }
}


// Same contract as parse_ts_slow; needs 32 readable bytes at p (files are padded, see list_csv).
static inline bool parse_ts(const char* p, u32 n, i64& out) {
    const __m128i z = _mm_set1_epi8('0'), nine = _mm_set1_epi8(9);
    __m128i v0 = _mm_loadu_si128((const __m128i*)p), v1 = _mm_loadu_si128((const __m128i*)(p + 16));
    __m128i d0 = _mm_sub_epi8(v0, z), d1 = _mm_sub_epi8(v1, z);
    u32 dig = (u32)_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d0, nine), d0)) |
              ((u32)_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d1, nine), d1)) << 16);
    if (n == 20 || n == 25) {
        // digits at 0-3,5-6,8-9,11-12,14-15,17-18 ; '-' 4,7 ; 'T' 10 ; ':' 13,16
        const u32 DIG = 0x6DB6F;  // bits 0,1,2,3,5,6,8,9,11,12,14,15,17,18
        const __m128i sep0 = _mm_setr_epi8(0, 0, 0, 0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0);
        u32 eq0 = (u32)_mm_movemask_epi8(_mm_cmpeq_epi8(v0, sep0));
        bool shape = (dig & DIG) == DIG && (eq0 & 0x2490) == 0x2490 && p[16] == ':';
        if (shape) {
            i64 off = 0;
            bool bad = false;
            if (n == 20) bad = p[19] != 'Z';
            else {
                char sg = p[19];
                bad = (sg != '+' && sg != '-') || (dig & 0x1B00000) != 0x1B00000 || p[22] != ':';
                int oh = d2(p + 20), om = d2(p + 23);
                bad |= oh > 23 || om > 59;
                off = (i64)(oh * 60 + om) * 60000;
                if (sg == '-') off = -off;
            }
            int Y = d2(p) * 100 + d2(p + 2), M = d2(p + 5), D = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), se = d2(p + 17);
            static const uint8_t mdays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            bad |= M < 1 || M > 12 || D < 1 || h > 23 || mi > 59 || se > 59;
            if (bad) return false;
            int md = mdays[M] + (M == 2 && ((Y % 4 == 0 && Y % 100 != 0) || Y % 400 == 0));
            if (D > md) return false;
            out = ((days_from_civil(Y, M, D) * 24 + h) * 60 + mi) * 60000 + (i64)se * 1000 - off;
            return true;
        }
        // not the F1/F2 shape: only an all-digit F3 can still be valid (handled below)
    }
    if (n == 0) return false;
    if (n <= 16) {
        if ((dig & ((1u << n) - 1)) != ((1u << n) - 1)) return false;
        // right-align the n digits into 16 lanes (zeros in front), then multiply-add pairs up
        alignas(16) static const uint8_t base[32] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
                                                     0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        __m128i sh = _mm_loadu_si128((const __m128i*)(base + n));
        __m128i x = _mm_shuffle_epi8(d0, sh);
        __m128i t1 = _mm_maddubs_epi16(x, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        __m128i t2 = _mm_madd_epi16(t1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        __m128i t3 = _mm_packus_epi32(t2, t2);
        __m128i t4 = _mm_madd_epi16(t3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
        u64 hi = (u32)_mm_cvtsi128_si32(t4), lo = (u32)_mm_extract_epi32(t4, 1);
        out = (i64)(hi * 100000000ULL + lo);
        return true;
    }
    return parse_ts_slow(p, n, out);
}

// ---------------------------------------------------------------- records
struct EvRec {      // a parsed event copy, partitioned by event_id hash
    u64 h;
    u64 uh;         // hash of user
    const char* eid;
    const char* user;
    i64 ts;
    i64 ingest;
    u32 eidlen;
    u32 userlen;
    int32_t price;  // >0 for a purchase
    uint8_t cat;
    uint8_t valid;  // bit0 valid, bit1 lost dedup
};
struct PRec {       // a surviving event, partitioned by person
    u64 h;
    const char* name;
    i64 ts;
    u32 namelen;
    int32_t price;
    uint8_t cat;
};
struct CRec {       // a valid click, partitioned by person
    u64 h;
    const char* name;
    const char* cid;
    i64 ts;
    u32 namelen;
    u32 cidlen;
    uint16_t camp;
};

// ---------------------------------------------------------------- parquet writer
struct Buf {
    string s;
    void u8(uint8_t v) { s.push_back((char)v); }
    void varint(u64 v) { while (v >= 0x80) { s.push_back((char)(v | 0x80)); v >>= 7; } s.push_back((char)v); }
    void raw(const void* p, size_t n) { s.append((const char*)p, n); }
    void le32(u32 v) { raw(&v, 4); }
    void le64(u64 v) { raw(&v, 8); }
};
struct Thrift {  // compact protocol writer
    Buf& b; vector<int> stack; int last = 0;
    explicit Thrift(Buf& b_) : b(b_) {}
    void fh(int id, int type) {
        int d = id - last;
        if (d > 0 && d <= 15) b.u8((uint8_t)((d << 4) | type));
        else { b.u8((uint8_t)type); b.varint((u64)((id << 1) ^ (id >> 31))); }
        last = id;
    }
    static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
    void i32(int id, i64 v) { fh(id, 5); b.varint(zz(v)); }
    void i64f(int id, i64 v) { fh(id, 6); b.varint(zz(v)); }
    void str(int id, const string& s) { fh(id, 8); b.varint(s.size()); b.raw(s.data(), s.size()); }
    void begin(int id) { fh(id, 12); stack.push_back(last); last = 0; }
    void end() { b.u8(0); last = stack.back(); stack.pop_back(); }
    void list(int id, int elemtype, size_t n) {
        fh(id, 9);
        if (n < 15) b.u8((uint8_t)((n << 4) | elemtype));
        else { b.u8((uint8_t)(0xF0 | elemtype)); b.varint(n); }
    }
    void lbegin() { stack.push_back(last); last = 0; }  // struct inside a list
    void lend() { end(); }
    void stop() { b.u8(0); }
};
enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTE_ARRAY = 6 };
enum LType { L_NONE, L_STRING, L_DATE };
struct ColOut {
    string name; PType type; LType lt; bool optional;
    Buf data;          // page payload (levels + values)
    size_t nvalues = 0;
};
static void write_parquet(const string& path, vector<ColOut>& cols, size_t nrows) {
    Buf f;
    f.raw("PAR1", 4);
    vector<i64> offs, sizes;
    for (auto& c : cols) {
        Buf ph; Thrift t(ph);
        t.i32(1, 0);  // DATA_PAGE
        t.i32(2, (i64)c.data.s.size());
        t.i32(3, (i64)c.data.s.size());
        t.begin(5);
        t.i32(1, (i64)nrows);
        t.i32(2, 0);  // PLAIN
        t.i32(3, 3);  // RLE
        t.i32(4, 3);
        t.end();
        t.stop();
        offs.push_back((i64)f.s.size());
        f.raw(ph.s.data(), ph.s.size());
        f.raw(c.data.s.data(), c.data.s.size());
        sizes.push_back((i64)(ph.s.size() + c.data.s.size()));
    }
    Buf m; Thrift t(m);
    t.i32(1, 1);
    t.list(2, 12, cols.size() + 1);
    t.lbegin(); t.str(4, "schema"); t.i32(5, (i64)cols.size()); t.lend();
    for (auto& c : cols) {
        t.lbegin();
        t.i32(1, c.type);
        t.i32(3, c.optional ? 1 : 0);
        t.str(4, c.name);
        if (c.lt == L_STRING) t.i32(6, 0);
        if (c.lt == L_DATE) t.i32(6, 6);
        if (c.lt != L_NONE) {
            t.begin(10);
            t.begin(c.lt == L_STRING ? 1 : 6); t.end();
            t.end();
        }
        t.lend();
    }
    t.i64f(3, (i64)nrows);
    t.list(4, 12, 1);
    t.lbegin();
    t.list(1, 12, cols.size());
    i64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
        auto& c = cols[i];
        t.lbegin();
        t.i64f(2, offs[i]);
        t.begin(3);
        t.i32(1, c.type);
        t.list(2, 5, c.optional ? 2 : 1);
        m.varint(Thrift::zz(0));
        if (c.optional) m.varint(Thrift::zz(3));
        t.list(3, 8, 1);
        m.varint(c.name.size()); m.raw(c.name.data(), c.name.size());
        t.i32(4, 0);
        t.i64f(5, (i64)nrows);
        t.i64f(6, sizes[i]);
        t.i64f(7, sizes[i]);
        t.i64f(9, offs[i]);
        t.end();
        t.lend();
        total += sizes[i];
    }
    t.i64f(2, total);
    t.i64f(3, (i64)nrows);
    t.lend();
    t.str(6, "shop-sessions-cpp");
    t.stop();
    f.raw(m.s.data(), m.s.size());
    f.le32((u32)m.s.size());
    f.raw("PAR1", 4);
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(3); }
    fwrite(f.s.data(), 1, f.s.size(), fp);
    fclose(fp);
}
// definition levels (bit width 1) as one bit-packed RLE-hybrid run, with the 4-byte length prefix
static void put_def_levels(Buf& out, const vector<uint8_t>& defs) {
    Buf r;
    size_t groups = (defs.size() + 7) / 8;
    r.varint((groups << 1) | 1);
    for (size_t g = 0; g < groups; g++) {
        uint8_t byte = 0;
        for (int k = 0; k < 8; k++) {
            size_t i = g * 8 + k;
            if (i < defs.size() && defs[i]) byte |= (uint8_t)(1 << k);
        }
        r.u8(byte);
    }
    out.le32((u32)r.s.size());
    out.raw(r.s.data(), r.s.size());
}

// ---------------------------------------------------------------- main
struct URow { const char* name; u32 namelen; int32_t day; i64 sessions, events, revenue; int topcat; };
struct CAgg { i64 sessions, purch, rev, assist; };

int main() {
    const char* ev;
    string DATA = (ev = getenv("DATA_DIR")) ? ev : "/data";
    string OUT = (ev = getenv("OUTPUT_DIR")) ? ev : "/output";
    if ((ev = getenv("NCPU"))) NT = max(1, atoi(ev));
    else NT = max(1u, thread::hardware_concurrency());
    NT = min(NT, 32);

    // manifest
    string man = read_file(DATA + "/manifest.json");
    auto getv = [&](const char* key) -> i64 {
        size_t k = man.find(key);
        size_t q1 = man.find('"', man.find(':', k) + 1);
        size_t q2 = man.find('"', q1 + 1);
        string v = man.substr(q1 + 1, q2 - q1 - 1);
        i64 out;
        char buf[128] = {0}; memcpy(buf, v.data(), min<size_t>(v.size(), 64));
        if (v.size() > 64 || !parse_ts(buf, (u32)v.size(), out)) { fprintf(stderr, "bad manifest ts %s\n", v.c_str()); exit(2); }
        return out;
    };
    const i64 WS = getv("window_start"), WE = getv("window_end");
    const i64 CWS = WS - WEEK_MS;
    auto floordiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
    const i64 DAY0 = floordiv(WS, DAY_MS);
    const int ND = (int)(floordiv(WE - 1, DAY_MS) - DAY0 + 1);

    // products & campaigns (small, kept as strings)
    string prod_s = read_file(DATA + "/products.csv"), camp_s = read_file(DATA + "/campaigns.csv");
    vector<string> catnames;
    vector<pair<Fld, Fld>> prods;
    {
        Chunk c{prod_s.data(), prod_s.data() + prod_s.size()};
        const char* nl = (const char*)memchr(c.b, '\n', c.e - c.b);
        c.b = nl ? nl + 1 : c.e;
        StrMap cm; cm.init(1024);
        for_lines(c, [&](const char* s, const char* e) {
            Fld f[2];
            if (!split_fields(s, e, f, 2) || f[0].n == 0) return;
            prods.push_back({f[0], f[1]});
            if (cm.insert(f[1].p, f[1].n, (int)catnames.size()) == (int)catnames.size()) catnames.emplace_back(f[1].p, f[1].n);
        });
        if (find(catnames.begin(), catnames.end(), string("unknown")) == catnames.end()) catnames.push_back("unknown");
        sort(catnames.begin(), catnames.end());
    }
    const int NCAT = (int)catnames.size();
    const int UNKNOWN = (int)(find(catnames.begin(), catnames.end(), string("unknown")) - catnames.begin());
    StrMap prodmap; prodmap.init(prods.size() + 1);
    for (auto& pr : prods) {
        int ci = (int)(find(catnames.begin(), catnames.end(), string(pr.second.p, pr.second.n)) - catnames.begin());
        prodmap.insert(pr.first.p, pr.first.n, ci);
    }
    vector<string> campids, channels;
    StrMap campmap; campmap.init(4096);
    {
        Chunk c{camp_s.data(), camp_s.data() + camp_s.size()};
        const char* nl = (const char*)memchr(c.b, '\n', c.e - c.b);
        c.b = nl ? nl + 1 : c.e;
        for_lines(c, [&](const char* s, const char* e) {
            Fld f[2];
            if (!split_fields(s, e, f, 2) || f[0].n == 0) return;
            if (campmap.insert(f[0].p, f[0].n, (int)campids.size()) == (int)campids.size()) {
                campids.emplace_back(f[0].p, f[0].n);
                channels.emplace_back(f[1].p, f[1].n);
            }
        });
    }
    const int NCAMP = (int)campids.size();
    const int ORGANIC = NCAMP;

    vector<MFile> lkf = list_csv(DATA + "/links"), evf = list_csv(DATA + "/events"), clf = list_csv(DATA + "/clicks");
    // Ask the kernel to read everything ahead, in the order it will be needed, without blocking the pipeline.
    thread([lkf, evf, clf]() {
        for (auto* v : {&lkf, &evf, &clf})
            for (auto& f : *v) madvise((void*)f.p, f.n, MADV_WILLNEED);
    }).detach();
    logt("setup");

    // ------------------------------------------------ links
    // Lock-free open-addressing table; a link ID's slot index is its id.
    // slot word: 0 empty, 1 being written, (tag << 32) | 2 ready.
    struct LinkEnt { const char* id; const char* name; u64 nameh; u32 idlen, namelen; };
    size_t lsz = 16; u64 lmask = 15;
    unique_ptr<atomic<u64>[]> lslots_a;
    unique_ptr<LinkEnt[]> lent_a;
    {
        vector<Chunk> ch = make_chunks(lkf, 1 << 20);
        vector<vector<Fld>> pairs(ch.size());
        vector<vector<u64>> hs(ch.size());
        par(ch.size(), [&](size_t i, int) {
            auto& v = pairs[i]; auto& h = hs[i];
            scan_chunk<2>(ch[i], [&](Fld* f) {
                if (f[0].n == 0 || f[1].n == 0) return;
                v.push_back(f[0]); v.push_back(f[1]);
                h.push_back(hash_str(f[0].p, f[0].n)); h.push_back(hash_str(f[1].p, f[1].n));
            });
        });
        size_t tot = 0;
        for (auto& v : pairs) tot += v.size();
        while (lsz < tot + 16) lsz <<= 1;
        lmask = lsz - 1;
        lslots_a.reset(new atomic<u64>[lsz]);
        lent_a.reset(new LinkEnt[lsz]);
        atomic<u64>* SL = lslots_a.get();
        LinkEnt* LE = lent_a.get();
        par((lsz + 65535) / 65536, [&](size_t i, int) {
            size_t e = min(lsz, (i + 1) * 65536);
            for (size_t k = i * 65536; k < e; k++) SL[k].store(0, memory_order_relaxed);
        });
        vector<int> parent(lsz);
        vector<vector<int>> ends(ch.size());
        par(ch.size(), [&](size_t c, int) {
            auto& v = pairs[c]; auto& h = hs[c]; auto& out = ends[c];
            out.resize(v.size());
            for (size_t j = 0; j < v.size(); j++) {
                u64 hh = h[j];
                u64 want = ((u64)(u32)(hh >> 32) << 32) | 2;
                for (u64 k = hh & lmask;; k = (k + 1) & lmask) {
                    u64 e = SL[k].load(memory_order_acquire);
                    if (e == 0) {
                        u64 z = 0;
                        if (SL[k].compare_exchange_strong(z, 1, memory_order_acq_rel)) {
                            LE[k].id = v[j].p; LE[k].idlen = v[j].n; LE[k].nameh = hh;
                            parent[k] = (int)k;
                            SL[k].store(want, memory_order_release);
                            out[j] = (int)k;
                            break;
                        }
                        e = z;
                    }
                    while (e == 1) e = SL[k].load(memory_order_acquire);
                    if (e == want && LE[k].nameh == hh && seq(LE[k].id, LE[k].idlen, v[j].p, v[j].n)) { out[j] = (int)k; break; }
                }
            }
        });
        // union toward the smaller slot: parent[x] <= x always
        auto findp = [&](int x) { while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
        for (auto& out : ends)
            for (size_t k = 0; k < out.size(); k += 2) {
                int x = findp(out[k]), y = findp(out[k + 1]);
                if (x != y) { if (x < y) parent[y] = x; else parent[x] = y; }
            }
        // one ascending pass makes every parent a root
        size_t n = 0;
        for (size_t k = 0; k < lsz; k++)
            if (SL[k].load(memory_order_relaxed)) { parent[k] = parent[parent[k]]; n++; }
        vector<int> best(lsz, -1);
        par(NT, [&](size_t t, int) {
            for (size_t k = 0; k < lsz; k++) {
                if (!SL[k].load(memory_order_relaxed)) continue;
                int r = parent[k];
                if ((size_t)(mix64((u64)r) % NT) != t) continue;
                int& bb = best[r];
                if (bb < 0 || scmp(LE[k].id, LE[k].idlen, LE[bb].id, LE[bb].idlen) < 0) bb = (int)k;
            }
        });
        vector<u64> nh(lsz);
        par((lsz + 65535) / 65536, [&](size_t i, int) {
            size_t e = min(lsz, (i + 1) * 65536);
            for (size_t k = i * 65536; k < e; k++) if (SL[k].load(memory_order_relaxed)) nh[k] = LE[k].nameh;
        });
        par((lsz + 65535) / 65536, [&](size_t i, int) {
            size_t e = min(lsz, (i + 1) * 65536);
            for (size_t k = i * 65536; k < e; k++) {
                if (!SL[k].load(memory_order_relaxed)) continue;
                int bb = best[parent[k]];
                LE[k].name = LE[bb].id; LE[k].namelen = LE[bb].idlen; LE[k].nameh = nh[bb];
            }
        });
        fprintf(stderr, "links: %zu ids\n", n);
    }
    logt("links");
    const atomic<u64>* lslots = lslots_a.get();
    const LinkEnt* lent = lent_a.get();
    // link slot for (s, n, h), or -1
    auto lfind = [&](const char* s, u32 n, u64 h) -> int {
        u64 want = ((u64)(u32)(h >> 32) << 32) | 2;
        for (u64 k = h & lmask;; k = (k + 1) & lmask) {
            u64 e = lslots[k].load(memory_order_relaxed);
            if (!e) return -1;
            if (e == want && seq(lent[k].id, lent[k].idlen, s, n)) return (int)k;
        }
    };

    // ------------------------------------------------ parse events and clicks
    const int BSH = 9, NB = 1 << BSH;   // event-id buckets
    const int QSH = 9, NQ = 1 << QSH;   // person partitions
    vector<vector<vector<EvRec>>> evb(NT, vector<vector<EvRec>>(NB));
    size_t EST_EV = 0;
    vector<vector<vector<CRec>>> clq(NT, vector<vector<CRec>>(NQ));
    {
        vector<Chunk> ech = make_chunks(evf, 8 << 20), cch = make_chunks(clf, 8 << 20);
        size_t ne = ech.size();
        size_t evbytes = 0, clbytes = 0;
        for (auto& c : ech) evbytes += c.e - c.b;
        for (auto& c : cch) clbytes += c.e - c.b;
        EST_EV = evbytes / 80;
        size_t per_e = (size_t)(EST_EV * 1.2 / NB / NT) + 16, per_c = (size_t)(clbytes / 40 * 1.2 / NQ / NT) + 16;
        par(NT, [&](size_t t, int) {
            for (auto& v : evb[t]) v.reserve(per_e);
            for (auto& v : clq[t]) v.reserve(per_c);
        });
        // sort chunks by size, largest first, for balance
        par(ne + cch.size(), [&](size_t i, int tid) {
            if (i < ne) {
                auto& B = evb[tid];
                scan_chunk<7>(ech[i], [&](Fld* f) {
                    if (f[0].n == 0) return;
                    EvRec r;
                    r.eid = f[0].p; r.eidlen = f[0].n;
                    r.h = hash_str(r.eid, r.eidlen);
                    r.user = f[1].p; r.userlen = f[1].n;
                    r.uh = hash_str(r.user, r.userlen);
                    i64 ing;
                    if (!parse_ts(f[3].p, f[3].n, ing)) ing = INT64_MAX;
                    r.ingest = ing;
                    r.price = 0; r.cat = 0;
                    bool ok = f[1].n > 0 && f[5].n > 0;
                    i64 ts = 0;
                    if (ok) ok = parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAY_MS;
                    r.ts = ts;
                    if (ok) {
                        const char* t = f[4].p; u32 tn = f[4].n;
                        if (tn == 4 && (memcmp(t, "view", 4) == 0 || memcmp(t, "cart", 4) == 0)) {
                        } else if (tn == 8 && memcmp(t, "purchase", 8) == 0) {
                            const char* q = f[6].p; u32 qn = f[6].n;
                            u32 j = 0;
                            for (; j < qn; j++) if (!isd(q[j])) break;
                            if (qn == 0 || j != qn) ok = false;
                            else {
                                j = 0; while (j < qn && q[j] == '0') j++;
                                if (qn - j > 8) ok = false;
                                else {
                                    i64 v = 0; for (; j < qn; j++) v = v * 10 + (q[j] - '0');
                                    if (v < 1 || v > 10000000) ok = false;
                                    else {
                                        r.price = (int32_t)v;
                                        int c = prodmap.find(f[5].p, f[5].n);
                                        r.cat = (uint8_t)(c < 0 ? UNKNOWN : c);
                                    }
                                }
                            }
                        } else ok = false;
                    }
                    r.valid = ok;
                    B[r.h >> (64 - BSH)].push_back(r);
                });
            } else {
                auto& Q = clq[tid];
                scan_chunk<4>(cch[i - ne], [&](Fld* f) {
                    if (f[0].n == 0 || f[1].n == 0) return;
                    i64 ts;
                    if (!parse_ts(f[3].p, f[3].n, ts) || ts < CWS || ts >= WE) return;
                    int c = campmap.find(f[2].p, f[2].n);
                    if (c < 0) return;
                    CRec r;
                    r.name = f[1].p; r.namelen = f[1].n;
                    r.h = hash_str(r.name, r.namelen);
                    int li = lfind(r.name, r.namelen, r.h);
                    if (li >= 0) { r.name = lent[li].name; r.namelen = lent[li].namelen; r.h = lent[li].nameh; }
                    r.cid = f[0].p; r.cidlen = f[0].n;
                    r.ts = ts; r.camp = (uint16_t)c;
                    Q[r.h >> (64 - QSH)].push_back(r);
                });
            }
        });
    }
    logt("parse");

    // ------------------------------------------------ dedup events, scatter by person
    vector<vector<vector<PRec>>> evq(NT, vector<vector<PRec>>(NQ));
    {
        size_t per = (size_t)(EST_EV * 1.15 / NQ / NT) + 16;
        par(NT, [&](size_t t, int) { for (auto& v : evq[t]) v.reserve(per); });
    }
    par(NB, [&](size_t b, int tid) {
        size_t n = 0;
        for (int t = 0; t < NT; t++) n += evb[t][b].size();
        size_t sz = 16; while (sz < n * 2) sz <<= 1;
        u64 mask = sz - 1;
        vector<u64> tab(sz, 0);   // (tag << 32) | (code + 1)
        auto rec = [&](u32 code) -> EvRec& { return evb[code >> 27][b][code & ((1u << 27) - 1)]; };
        for (int t = 0; t < NT; t++) {
            auto& v = evb[t][b];
            for (u32 i = 0; i < v.size(); i++) {
                EvRec& r = v[i];
                u32 code = ((u32)t << 27) | i;
                u32 tag = (u32)(r.h >> 22);
                for (u64 k = (r.h >> 3) & mask;; k = (k + 1) & mask) {
                    u64 e = tab[k];
                    if (!e) { tab[k] = ((u64)tag << 32) | (code + 1); break; }
                    if ((u32)(e >> 32) != tag) continue;
                    EvRec& o = rec((u32)e - 1);
                    if (o.h == r.h && seq(o.eid, o.eidlen, r.eid, r.eidlen)) {
                        if (r.ingest < o.ingest) { o.valid |= 2; tab[k] = ((u64)tag << 32) | (code + 1); }
                        else r.valid |= 2;
                        break;
                    }
                }
            }
        }
        vector<u64>().swap(tab);
        auto& Q = evq[tid];
        const int BATCH = 32;
        const EvRec* bat[BATCH];
        auto flush = [&](int m) {
            for (int j = 0; j < m; j++) { __builtin_prefetch(&lslots[bat[j]->uh & lmask]); __builtin_prefetch(bat[j]->user); }
            int li[BATCH];
            for (int j = 0; j < m; j++) {
                const EvRec& r = *bat[j];
                u32 tag = (u32)(r.uh >> 32);
                li[j] = -1;
                u64 want = ((u64)tag << 32) | 2;
                for (u64 k = r.uh & lmask;; k = (k + 1) & lmask) {
                    u64 e = lslots[k].load(memory_order_relaxed);
                    if (!e) break;
                    if (e == want) { li[j] = (int)k; __builtin_prefetch(&lent[k]); break; }
                }
            }
            for (int j = 0; j < m; j++) if (li[j] >= 0) __builtin_prefetch(lent[li[j]].id);
            for (int j = 0; j < m; j++) {
                const EvRec& r = *bat[j];
                PRec p;
                int id = li[j];
                if (id >= 0 && !seq(lent[id].id, lent[id].idlen, r.user, r.userlen)) id = lfind(r.user, r.userlen, r.uh);
                if (id >= 0) { p.name = lent[id].name; p.namelen = lent[id].namelen; p.h = lent[id].nameh; }
                else { p.name = r.user; p.namelen = r.userlen; p.h = r.uh; }
                p.ts = r.ts; p.price = r.price; p.cat = r.cat;
                Q[p.h >> (64 - QSH)].push_back(p);
            }
        };
        int m = 0;
        for (int t = 0; t < NT; t++)
            for (auto& r : evb[t][b]) {
                if (r.valid != 1) continue;
                bat[m++] = &r;
                if (m == BATCH) { flush(m); m = 0; }
            }
        if (m) flush(m);
        for (int t = 0; t < NT; t++) { vector<EvRec>().swap(evb[t][b]); }
    });
    logt("dedup");

    // ------------------------------------------------ per person: sessions, attribution
    vector<vector<URow>> urows(NT);
    vector<vector<CAgg>> cagg(NT, vector<CAgg>((size_t)(NCAMP + 1) * ND, CAgg{0, 0, 0, 0}));
    par(NQ, [&](size_t q, int tid) {
        size_t n = 0, nc = 0;
        for (int t = 0; t < NT; t++) { n += evq[t][q].size(); nc += clq[t][q].size(); }
        if (n == 0) return;
        // intern persons
        size_t sz = 16; while (sz < n * 2) sz <<= 1;   // persons <= events
        u64 mask = sz - 1;
        vector<int> tab(sz, -1);
        struct PK { u64 h; const char* name; u32 namelen; };
        vector<PK> pers;   // name per person
        vector<int> pid; pid.reserve(n);
        vector<u32> cnt;
        for (int t = 0; t < NT; t++)
            for (auto& r : evq[t][q]) {
                int id;
                for (u64 k = (r.h >> 2) & mask;; k = (k + 1) & mask) {
                    if (tab[k] < 0) { id = tab[k] = (int)pers.size(); pers.push_back(PK{r.h, r.name, r.namelen}); cnt.push_back(0); break; }
                    const PK* o = &pers[tab[k]];
                    if (o->h == r.h && o->namelen == r.namelen) { id = tab[k]; break; }
                }
                pid.push_back(id);
                cnt[id]++;
            }
        size_t P = pers.size();
        vector<u32> start(P + 1, 0);
        for (size_t i = 0; i < P; i++) start[i + 1] = start[i] + cnt[i];
        struct E { i64 ts; int32_t price; int32_t cat; };
        vector<E> evs(n);
        {
            vector<u32> pos(start.begin(), start.end() - 1);
            size_t j = 0;
            for (int t = 0; t < NT; t++)
                for (auto& r : evq[t][q]) evs[pos[pid[j++]]++] = E{r.ts, r.price, r.cat};
        }
        for (int t = 0; t < NT; t++) vector<PRec>().swap(evq[t][q]);
        // clicks
        vector<u32> cstart(P + 1, 0);
        vector<int> cpid; cpid.reserve(nc);
        auto lookup = [&](const CRec& r) -> int {
            for (u64 k = (r.h >> 2) & mask;; k = (k + 1) & mask) {
                if (tab[k] < 0) return -1;
                const PK* o = &pers[tab[k]];
                if (o->h == r.h && o->namelen == r.namelen) return tab[k];
            }
        };
        for (int t = 0; t < NT; t++)
            for (auto& r : clq[t][q]) { int id = lookup(r); cpid.push_back(id); if (id >= 0) cstart[id + 1]++; }
        for (size_t i = 0; i < P; i++) cstart[i + 1] += cstart[i];
        vector<const CRec*> cls(cstart[P]);
        {
            vector<u32> pos(cstart.begin(), cstart.end() - 1);
            size_t j = 0;
            for (int t = 0; t < NT; t++)
                for (auto& r : clq[t][q]) { int id = cpid[j++]; if (id >= 0) cls[pos[id]++] = &r; }
        }
        auto& rows = urows[tid];
        auto& CA = cagg[tid];
        vector<int> ccount(NCAMP, 0), active, apos(NCAMP, -1);
        vector<i64> catsum(NCAT, 0);
        for (size_t p = 0; p < P; p++) {
            E* a = evs.data() + start[p]; E* ae = evs.data() + start[p + 1];
            sort(a, ae, [](const E& x, const E& y) { return x.ts < y.ts; });
            const CRec** c = cls.data() + cstart[p]; const CRec** ce = cls.data() + cstart[p + 1];
            sort(c, ce, [](const CRec* x, const CRec* y) {
                if (x->ts != y->ts) return x->ts < y->ts;
                return scmp(x->cid, x->cidlen, y->cid, y->cidlen) > 0;
            });
            size_t ncl = ce - c, lo = 0, hi = 0;
            URow cur{pers[p].name, pers[p].namelen, INT32_MIN, 0, 0, 0, -1};
            bool anycat = false;
            auto flush = [&]() {
                if (cur.day == INT32_MIN) return;
                int best = -1;
                if (anycat) {
                    for (int k = 0; k < NCAT; k++)
                        if (catsum[k] > 0 && (best < 0 || catsum[k] > catsum[best])) best = k;
                    fill(catsum.begin(), catsum.end(), 0);
                }
                cur.topcat = best;
                rows.push_back(cur);
                anycat = false;
            };
            E* s = a;
            while (s < ae) {
                i64 S = s->ts;
                E* x = s + 1;
                i64 prev = S;
                while (x < ae && x->ts - prev <= GAP_MS) { prev = x->ts; x++; }
                // session [s, x)
                int day = (int)(floordiv(S, DAY_MS) - DAY0);
                if (day != cur.day) {
                    flush();
                    cur.day = day; cur.sessions = 0; cur.events = 0; cur.revenue = 0;
                }
                i64 rev = 0;
                for (E* y = s; y < x; y++)
                    if (y->price > 0) { rev += y->price; catsum[y->cat] += y->price; anycat = true; }
                cur.sessions++; cur.events += x - s; cur.revenue += rev;
                // attribution
                while (hi < ncl && c[hi]->ts <= S) {
                    int cp = c[hi]->camp;
                    if (ccount[cp]++ == 0) { apos[cp] = (int)active.size(); active.push_back(cp); }
                    hi++;
                }
                while (lo < hi && c[lo]->ts < S - WEEK_MS) {
                    int cp = c[lo]->camp;
                    if (--ccount[cp] == 0) {
                        int last = active.back(); active[apos[cp]] = last; apos[last] = apos[cp]; active.pop_back(); apos[cp] = -1;
                    }
                    lo++;
                }
                int credit = hi > lo ? c[hi - 1]->camp : ORGANIC;
                CAgg& g = CA[(size_t)credit * ND + day];
                g.sessions++; g.rev += rev; if (rev > 0) g.purch++;
                for (int cp : active) CA[(size_t)cp * ND + day].assist++;
                s = x;
            }
            flush();
            while (lo < hi) {
                int cp = c[lo]->camp;
                if (--ccount[cp] == 0) {
                    int last = active.back(); active[apos[cp]] = last; apos[last] = apos[cp]; active.pop_back(); apos[cp] = -1;
                }
                lo++;
            }
        }
    });
    logt("sessions");

    // ------------------------------------------------ write outputs
    string ud = OUT + "/user_daily", cd = OUT + "/campaign_daily";
    { string acc; for (size_t i = 0; i <= OUT.size(); i++) { if (i == OUT.size() || (OUT[i] == '/' && i > 0)) mkdir(OUT.substr(0, i).c_str(), 0777); } }
    mkdir(ud.c_str(), 0777); mkdir(cd.c_str(), 0777);
    par(NT + 1, [&](size_t i, int) {
        if (i < (size_t)NT) {
            auto& R = urows[i];
            vector<ColOut> cols(6);
            cols[0] = {"user_id", P_BYTE_ARRAY, L_STRING, false, {}, 0};
            cols[1] = {"day", P_INT32, L_DATE, false, {}, 0};
            cols[2] = {"sessions", P_INT64, L_NONE, false, {}, 0};
            cols[3] = {"events", P_INT64, L_NONE, false, {}, 0};
            cols[4] = {"revenue_cents", P_INT64, L_NONE, false, {}, 0};
            cols[5] = {"top_category", P_BYTE_ARRAY, L_STRING, true, {}, 0};
            vector<uint8_t> defs(R.size());
            for (size_t k = 0; k < R.size(); k++) defs[k] = R[k].topcat >= 0;
            put_def_levels(cols[5].data, defs);
            for (auto& r : R) {
                cols[0].data.le32(r.namelen); cols[0].data.raw(r.name, r.namelen);
                cols[1].data.le32((u32)(int32_t)(r.day + DAY0));
                cols[2].data.le64((u64)r.sessions);
                cols[3].data.le64((u64)r.events);
                cols[4].data.le64((u64)r.revenue);
                if (r.topcat >= 0) { auto& cn = catnames[r.topcat]; cols[5].data.le32((u32)cn.size()); cols[5].data.raw(cn.data(), cn.size()); }
            }
            write_parquet(ud + "/part-" + to_string(i) + ".parquet", cols, R.size());
        } else {
            vector<CAgg> tot((size_t)(NCAMP + 1) * ND, CAgg{0, 0, 0, 0});
            for (int t = 0; t < NT; t++)
                for (size_t k = 0; k < tot.size(); k++) {
                    tot[k].sessions += cagg[t][k].sessions; tot[k].purch += cagg[t][k].purch;
                    tot[k].rev += cagg[t][k].rev; tot[k].assist += cagg[t][k].assist;
                }
            vector<ColOut> cols(7);
            cols[0] = {"campaign_id", P_BYTE_ARRAY, L_STRING, false, {}, 0};
            cols[1] = {"channel", P_BYTE_ARRAY, L_STRING, false, {}, 0};
            cols[2] = {"day", P_INT32, L_DATE, false, {}, 0};
            cols[3] = {"sessions", P_INT64, L_NONE, false, {}, 0};
            cols[4] = {"purchasing_sessions", P_INT64, L_NONE, false, {}, 0};
            cols[5] = {"revenue_cents", P_INT64, L_NONE, false, {}, 0};
            cols[6] = {"assisted_sessions", P_INT64, L_NONE, false, {}, 0};
            size_t nrows = 0;
            for (int c = 0; c <= NCAMP; c++)
                for (int d = 0; d < ND; d++) {
                    CAgg& g = tot[(size_t)c * ND + d];
                    if (g.sessions == 0 && g.assist == 0) continue;
                    const string& id = c == ORGANIC ? string("organic") : campids[c];
                    const string& chn = c == ORGANIC ? string("organic") : channels[c];
                    cols[0].data.le32((u32)id.size()); cols[0].data.raw(id.data(), id.size());
                    cols[1].data.le32((u32)chn.size()); cols[1].data.raw(chn.data(), chn.size());
                    cols[2].data.le32((u32)(int32_t)(d + DAY0));
                    cols[3].data.le64((u64)g.sessions);
                    cols[4].data.le64((u64)g.purch);
                    cols[5].data.le64((u64)g.rev);
                    cols[6].data.le64((u64)(c == ORGANIC ? 0 : g.assist));
                    nrows++;
                }
            write_parquet(cd + "/part-0.parquet", cols, nrows);
        }
    });
    logt("write");
    fflush(stderr);
    _exit(0);
}
