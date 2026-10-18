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

#define WARMUP_ITERS 1000
#define TEST_ITERS   10000

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

int cmp_uint64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    return (va > vb) - (va < vb);
}

void full_read(int fd, char *buf, size_t total_bytes) {
    size_t received = 0;
    while (received < total_bytes) {
        ssize_t n = read(fd, buf + received, total_bytes - received);
        if (n <= 0) break;
        received += n;
    }
}

void full_write(int fd, const char *buf, size_t total_bytes) {
    size_t sent = 0;
    while (sent < total_bytes) {
        ssize_t n = write(fd, buf + sent, total_bytes - sent);
        if (n <= 0) break;
        sent += n;
    }
}

/* ================= 1. PIPE ================= */
void test_pipe_latency(size_t msg_size) {
    int p1[2], p2[2];
    if (pipe(p1) == -1 || pipe(p2) == -1) { perror("pipe"); exit(1); }

    char *buf = malloc(msg_size);
    memset(buf, 'P', msg_size);

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        close(p1[1]); close(p2[0]);
        for (int i = 0; i < WARMUP_ITERS + TEST_ITERS; i++) {
            full_read(p1[0], buf, msg_size);
            full_write(p2[1], buf, msg_size);
        }
        close(p1[0]); close(p2[1]);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        close(p1[0]); close(p2[1]);
        uint64_t *samples = malloc(sizeof(uint64_t) * TEST_ITERS);

        for (int i = 0; i < WARMUP_ITERS; i++) {
            full_write(p1[1], buf, msg_size);
            full_read(p2[0], buf, msg_size);
        }
        for (int i = 0; i < TEST_ITERS; i++) {
            uint64_t t0 = get_time_ns();
            full_write(p1[1], buf, msg_size);
            full_read(p2[0], buf, msg_size);
            samples[i] = get_time_ns() - t0;
        }
        wait(NULL);

        qsort(samples, TEST_ITERS, sizeof(uint64_t), cmp_uint64);
        printf("Pipe,%zu,%.2f,%.2f\n", msg_size, samples[TEST_ITERS / 2] / 1000.0, samples[(int)(TEST_ITERS * 0.99)] / 1000.0);
        free(samples); free(buf);
        close(p1[1]); close(p2[0]);
    }
}

/* ================= 2. UNIX DOMAIN SOCKET ================= */
void test_unix_socket_latency(size_t msg_size) {
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == -1) { perror("socketpair"); exit(1); }

    char *buf = malloc(msg_size);
    memset(buf, 'U', msg_size);

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        close(sv[0]);
        for (int i = 0; i < WARMUP_ITERS + TEST_ITERS; i++) {
            full_read(sv[1], buf, msg_size);
            full_write(sv[1], buf, msg_size);
        }
        close(sv[1]);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        close(sv[1]);
        uint64_t *samples = malloc(sizeof(uint64_t) * TEST_ITERS);

        for (int i = 0; i < WARMUP_ITERS; i++) {
            full_write(sv[0], buf, msg_size);
            full_read(sv[0], buf, msg_size);
        }
        for (int i = 0; i < TEST_ITERS; i++) {
            uint64_t t0 = get_time_ns();
            full_write(sv[0], buf, msg_size);
            full_read(sv[0], buf, msg_size);
            samples[i] = get_time_ns() - t0;
        }
        wait(NULL);

        qsort(samples, TEST_ITERS, sizeof(uint64_t), cmp_uint64);
        printf("UnixSocket,%zu,%.2f,%.2f\n", msg_size, samples[TEST_ITERS / 2] / 1000.0, samples[(int)(TEST_ITERS * 0.99)] / 1000.0);
        free(samples); free(buf);
        close(sv[0]);
    }
}

/* ================= 3. TCP LOOPBACK SOCKET ================= */
void test_tcp_latency(size_t msg_size) {
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

    char *buf = malloc(msg_size);
    memset(buf, 'T', msg_size);

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        int client_fd = socket(AF_INET, SOCK_STREAM, 0);
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));

        for (int i = 0; i < WARMUP_ITERS + TEST_ITERS; i++) {
            full_read(client_fd, buf, msg_size);
            full_write(client_fd, buf, msg_size);
        }
        close(client_fd);
        free(buf);
        exit(0);
    } else {
        pin_to_core(0);
        int server_conn = accept(listen_fd, NULL, NULL);
        setsockopt(server_conn, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        close(listen_fd);

        uint64_t *samples = malloc(sizeof(uint64_t) * TEST_ITERS);

        for (int i = 0; i < WARMUP_ITERS; i++) {
            full_write(server_conn, buf, msg_size);
            full_read(server_conn, buf, msg_size);
        }
        for (int i = 0; i < TEST_ITERS; i++) {
            uint64_t t0 = get_time_ns();
            full_write(server_conn, buf, msg_size);
            full_read(server_conn, buf, msg_size);
            samples[i] = get_time_ns() - t0;
        }
        wait(NULL);

        qsort(samples, TEST_ITERS, sizeof(uint64_t), cmp_uint64);
        printf("TCP_Loopback,%zu,%.2f,%.2f\n", msg_size, samples[TEST_ITERS / 2] / 1000.0, samples[(int)(TEST_ITERS * 0.99)] / 1000.0);
        free(samples); free(buf);
        close(server_conn);
    }
}

/* ================= 4. POSIX SHARED MEMORY ================= */
typedef struct {
    sem_t sem_parent_to_child;
    sem_t sem_child_to_parent;
    char buffer[1048576]; // 1 MB shared buffer
} shm_channel_t;

void test_shm_latency(size_t msg_size) {
    // Allocate shared memory backed by anonymous physical pages
    shm_channel_t *shm = mmap(NULL, sizeof(shm_channel_t), PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm == MAP_FAILED) { perror("mmap"); exit(1); }

    // Initialize cross-process semaphores (pshared = 1)
    sem_init(&shm->sem_parent_to_child, 1, 0);
    sem_init(&shm->sem_child_to_parent, 1, 0);

    char *local_buf = malloc(msg_size);
    memset(local_buf, 'S', msg_size);

    pid_t pid = fork();
    if (pid == 0) {
        pin_to_core(1);
        for (int i = 0; i < WARMUP_ITERS + TEST_ITERS; i++) {
            sem_wait(&shm->sem_parent_to_child);
            memcpy(local_buf, shm->buffer, msg_size);
            memcpy(shm->buffer, local_buf, msg_size);
            sem_post(&shm->sem_child_to_parent);
        }
        free(local_buf);
        exit(0);
    } else {
        pin_to_core(0);
        uint64_t *samples = malloc(sizeof(uint64_t) * TEST_ITERS);

        for (int i = 0; i < WARMUP_ITERS; i++) {
            memcpy(shm->buffer, local_buf, msg_size);
            sem_post(&shm->sem_parent_to_child);
            sem_wait(&shm->sem_child_to_parent);
            memcpy(local_buf, shm->buffer, msg_size);
        }
        for (int i = 0; i < TEST_ITERS; i++) {
            uint64_t t0 = get_time_ns();
            memcpy(shm->buffer, local_buf, msg_size);
            sem_post(&shm->sem_parent_to_child);
            sem_wait(&shm->sem_child_to_parent);
            memcpy(local_buf, shm->buffer, msg_size);
            samples[i] = get_time_ns() - t0;
        }
        wait(NULL);

        qsort(samples, TEST_ITERS, sizeof(uint64_t), cmp_uint64);
        printf("SharedMem,%zu,%.2f,%.2f\n", msg_size, samples[TEST_ITERS / 2] / 1000.0, samples[(int)(TEST_ITERS * 0.99)] / 1000.0);

        sem_destroy(&shm->sem_parent_to_child);
        sem_destroy(&shm->sem_child_to_parent);
        munmap(shm, sizeof(shm_channel_t));
        free(samples);
        free(local_buf);
    }
}

int main(void) {
    size_t sizes[] = {64, 256, 1024, 4096, 16384, 65536, 262144, 1048576};
    int count = sizeof(sizes) / sizeof(sizes[0]);

    printf("Mechanism,MessageBytes,Median_us,P99_us\n");
    
    for (int i = 0; i < count; i++) test_pipe_latency(sizes[i]);
    for (int i = 0; i < count; i++) test_unix_socket_latency(sizes[i]);
    for (int i = 0; i < count; i++) test_tcp_latency(sizes[i]);
    for (int i = 0; i < count; i++) test_shm_latency(sizes[i]);

    return 0;
}