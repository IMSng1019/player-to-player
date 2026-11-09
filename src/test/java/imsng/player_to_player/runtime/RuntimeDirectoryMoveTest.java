package imsng.player_to_player.runtime;

import com.sun.nio.file.ExtendedOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeDirectoryMoveTest {
    @TempDir Path temp;

    @Test @EnabledOnOs(OS.WINDOWS)
    void windowsTemporaryDeleteSharingLockIsRetriedUntilItCloses() throws Exception {
        Path from = Files.createDirectory(temp.resolve("staging")), to = temp.resolve("runtime");
        Path file = Files.writeString(from.resolve("mod.jar"), "complete-environment");
        CountDownLatch firstDenied = new CountDownLatch(1);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        AtomicInteger attempts = new AtomicInteger();
        try (FileChannel lock = FileChannel.open(file, StandardOpenOption.READ, ExtendedOpenOption.NOSHARE_DELETE)) {
            Future<?> released = closer.submit(() -> {
                try {
                    assertTrue(firstDenied.await(5, TimeUnit.SECONDS), "必须真的遇到Windows共享删除拒绝");
                    Thread.sleep(200);
                    lock.close();
                } catch (Exception failure) { throw new CompletionException(failure); }
            });
            try {
                assertDoesNotThrow(() -> RuntimeDirectoryMove.move(from, to, (source, target, options) -> {
                    attempts.incrementAndGet();
                    try { Files.move(source, target, options); }
                    catch (AccessDeniedException denied) { firstDenied.countDown(); throw denied; }
                }), "暂时被扫描/日志句柄占用不能直接让完整环境安装失败");
                assertTrue(attempts.get() > 1);
                assertEquals("complete-environment", Files.readString(to.resolve("mod.jar")));
                assertFalse(Files.exists(from));
            } finally {
                firstDenied.countDown();
                released.get(5, TimeUnit.SECONDS);
            }
        } finally {
            closer.shutdownNow();
            assertTrue(closer.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @EnabledOnOs(OS.WINDOWS)
    void targetAppearingAfterDenialIsNeverRetriedOrOverwritten() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        AccessDeniedException denied = new AccessDeniedException(from.toString());
        AtomicInteger attempts = new AtomicInteger();
        assertSame(denied, assertThrows(AccessDeniedException.class,
                () -> RuntimeDirectoryMove.move(from, to, (source, target, options) -> {
                    assertEquals(1, attempts.incrementAndGet());
                    Files.createDirectory(to); Files.writeString(to.resolve("owned-by-other"), "keep");
                    throw denied;
                })));
        assertTrue(Files.isDirectory(from));
        assertEquals("keep", Files.readString(to.resolve("owned-by-other")));
    }

    @Test @EnabledOnOs(OS.WINDOWS)
    void sourceDisappearingAfterDenialIsNotRetried() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        AccessDeniedException denied = new AccessDeniedException(from.toString());
        AtomicInteger attempts = new AtomicInteger();
        assertSame(denied, assertThrows(AccessDeniedException.class,
                () -> RuntimeDirectoryMove.move(from, to, (source, target, options) -> {
                    assertEquals(1, attempts.incrementAndGet());
                    Files.move(from, temp.resolve("moved-by-other"));
                    throw denied;
                })));
        assertFalse(Files.exists(to));
    }

    @Test void otherIoFailuresAreNotRetried() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        IOException failure = new IOException("disk failure");
        AtomicInteger attempts = new AtomicInteger();
        assertSame(failure, assertThrows(IOException.class,
                () -> RuntimeDirectoryMove.move(from, to, (source, target, options) -> {
                    attempts.incrementAndGet(); throw failure;
                })));
        assertEquals(1, attempts.get());
    }

    @Test void unsupportedAtomicMoveKeepsTheExistingNoReplaceFallback() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        Files.writeString(from.resolve("data"), "kept");
        AtomicInteger attempts = new AtomicInteger();
        RuntimeDirectoryMove.move(from, to, (source, target, options) -> {
            if (attempts.incrementAndGet() == 1) {
                assertArrayEquals(new CopyOption[]{StandardCopyOption.ATOMIC_MOVE}, options);
                throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "test provider");
            }
            assertEquals(0, options.length);
            Files.move(source, target, options);
        });
        assertEquals(2, attempts.get());
        assertEquals("kept", Files.readString(to.resolve("data")));
    }

    @Test @EnabledOnOs(OS.WINDOWS)
    void interruptionStopsRetriesAndPreservesInterruptedStatus() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        AtomicInteger attempts = new AtomicInteger();
        try {
            assertThrows(InterruptedIOException.class, () -> RuntimeDirectoryMove.move(from, to,
                    (source, target, options) -> {
                        attempts.incrementAndGet();
                        Thread.currentThread().interrupt();
                        throw new AccessDeniedException(source.toString());
                    }));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, attempts.get());
        } finally {
            Thread.interrupted();
        }
    }

    @Test @EnabledOnOs(OS.WINDOWS)
    void permanentAccessDenialFailsWithinTheBoundAndKeepsBothPathsIntact() throws Exception {
        Path from = Files.createDirectory(temp.resolve("from")), to = temp.resolve("to");
        Files.writeString(from.resolve("data"), "kept");
        AtomicInteger attempts = new AtomicInteger();
        long started = System.nanoTime();
        assertThrows(AccessDeniedException.class, () -> RuntimeDirectoryMove.move(from, to,
                (source, target, options) -> {
                    attempts.incrementAndGet(); throw new AccessDeniedException(source.toString());
                }));
        assertTrue(attempts.get() > 1);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2500,
                "重试等待有界，不能把真正ACL错误变成永久挂起");
        assertEquals("kept", Files.readString(from.resolve("data")));
        assertFalse(Files.exists(to));
    }
}
