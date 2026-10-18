#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define TOTAL_DATA_BYTES (256ULL * 1024ULL * 1024ULL) // Stream 256 MB total

static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void pin_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
}

void full_read(int fd, char *buf, size_t n) {
    size_t cur = 0;
    while (cur < n) {
        ssize_t r = read(fd, buf + cur, n - cur);
        if (r <= 0) break;
        cur += r;
    }
}

void full_write(int fd, const char *buf, size_t n) {
    size_t cur = 0;
    while (cur < n) {
        ssize_t w = write(fd, buf + cur, n - cur);
        if (w <= 0) break;
        cur += w;
    }
}

/* ================= 1. PIPE STREAMING ================= */
void stream_pipe(size_t chunk_size) {
    int p[2];
    if (pipe(p) == -1) { perror("pipe"); exit(1); }
    char *buf = malloc(chunk_size);
    memset(buf, 'P', chunk_size);
    size_t iters = TOTAL_DATA_BYTES / chunk_size;

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        close(p[1]);
        for (size_t i = 0; i < iters; i++) full_read(p[0], buf, chunk_size);
        close(p[0]);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        close(p[0]);
        uint64_t t0 = get_time_ns();
        for (size_t i = 0; i < iters; i++) full_write(p[1], buf, chunk_size);
        wait(NULL);
        uint64_t elapsed_ns = get_time_ns() - t0;
        double mb_s = ((double)TOTAL_DATA_BYTES / (1024.0 * 1024.0)) / ((double)elapsed_ns / 1e9);
        printf("Pipe,%zu,%.2f\n", chunk_size, mb_s);
        close(p[1]);
        free(buf);
    }
}

/* ================= 2. UNIX SOCKET STREAMING ================= */
void stream_unix(size_t chunk_size) {
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == -1) { perror("socketpair"); exit(1); }
    char *buf = malloc(chunk_size);
    memset(buf, 'U', chunk_size);
    size_t iters = TOTAL_DATA_BYTES / chunk_size;

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        close(sv[0]);
        for (size_t i = 0; i < iters; i++) full_read(sv[1], buf, chunk_size);
        close(sv[1]);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        close(sv[1]);
        uint64_t t0 = get_time_ns();
        for (size_t i = 0; i < iters; i++) full_write(sv[0], buf, chunk_size);
        wait(NULL);
        uint64_t elapsed_ns = get_time_ns() - t0;
        double mb_s = ((double)TOTAL_DATA_BYTES / (1024.0 * 1024.0)) / ((double)elapsed_ns / 1e9);
        printf("UnixSocket,%zu,%.2f\n", chunk_size, mb_s);
        close(sv[0]);
        free(buf);
    }
}

/* ================= 3. TCP STREAMING ================= */
void stream_tcp(size_t chunk_size) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int flag = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    serv_addr.sin_port = 0;

    bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    socklen_t addrlen = sizeof(serv_addr);
    getsockname(listen_fd, (struct sockaddr*)&serv_addr, &addrlen);
    listen(listen_fd, 1);

    char *buf = malloc(chunk_size);
    memset(buf, 'T', chunk_size);
    size_t iters = TOTAL_DATA_BYTES / chunk_size;

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        int client_fd = socket(AF_INET, SOCK_STREAM, 0);
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        for (size_t i = 0; i < iters; i++) full_read(client_fd, buf, chunk_size);
        close(client_fd);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        int server_conn = accept(listen_fd, NULL, NULL);
        setsockopt(server_conn, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        close(listen_fd);

        uint64_t t0 = get_time_ns();
        for (size_t i = 0; i < iters; i++) full_write(server_conn, buf, chunk_size);
        wait(NULL);
        uint64_t elapsed_ns = get_time_ns() - t0;
        double mb_s = ((double)TOTAL_DATA_BYTES / (1024.0 * 1024.0)) / ((double)elapsed_ns / 1e9);
        printf("TCP_Loopback,%zu,%.2f\n", chunk_size, mb_s);
        close(server_conn);
        free(buf);
    }
}

/* ================= 4. SHARED MEMORY STREAMING ================= */
typedef struct {
    sem_t sem_empty;
    sem_t sem_full;
    char buffer[1048576]; // 1 MB ring slot
} shm_stream_t;

void stream_shm(size_t chunk_size) {
    shm_stream_t *shm = mmap(NULL, sizeof(shm_stream_t), PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    sem_init(&shm->sem_empty, 1, 1);
    sem_init(&shm->sem_full, 1, 0);

    char *buf = malloc(chunk_size);
    memset(buf, 'S', chunk_size);
    size_t iters = TOTAL_DATA_BYTES / chunk_size;

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        for (size_t i = 0; i < iters; i++) {
            sem_wait(&shm->sem_full);
            memcpy(buf, shm->buffer, chunk_size);
            sem_post(&shm->sem_empty);
        }
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        uint64_t t0 = get_time_ns();
        for (size_t i = 0; i < iters; i++) {
            sem_wait(&shm->sem_empty);
            memcpy(shm->buffer, buf, chunk_size);
            sem_post(&shm->sem_full);
        }
        wait(NULL);
        uint64_t elapsed_ns = get_time_ns() - t0;
        double mb_s = ((double)TOTAL_DATA_BYTES / (1024.0 * 1024.0)) / ((double)elapsed_ns / 1e9);
        printf("SharedMem,%zu,%.2f\n", chunk_size, mb_s);

        sem_destroy(&shm->sem_empty);
        sem_destroy(&shm->sem_full);
        munmap(shm, sizeof(shm_stream_t));
        free(buf);
    }
}

int main(void) {
    size_t sizes[] = {64, 256, 1024, 4096, 16384, 65536, 262144, 1048576};
    int count = sizeof(sizes) / sizeof(sizes[0]);

    printf("Mechanism,ChunkBytes,Throughput_MBs\n");
    for (int i = 0; i < count; i++) stream_pipe(sizes[i]);
    for (int i = 0; i < count; i++) stream_unix(sizes[i]);
    for (int i = 0; i < count; i++) stream_tcp(sizes[i]);
    for (int i = 0; i < count; i++) stream_shm(sizes[i]);

    return 0;
}