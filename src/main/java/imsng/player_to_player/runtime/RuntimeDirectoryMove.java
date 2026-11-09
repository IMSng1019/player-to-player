package imsng.player_to_player.runtime;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

/**
 * 运行目录安装、备份与回滚共用的移动边界。
 * Windows扫描器或刚退出的进程可能短暂持有禁止共享删除的句柄；此时完整暂存目录
 * 仍有效，不应因第一次rename被拒就拆除安装。仅对此异常短暂重试，不改权限，
 * 不覆盖后来出现的目标；永久ACL错误仍在两秒等待窗口内失败，交给安装器原有回滚。
 * 本方法由后台安装线程调用，不能放入Minecraft主线程或网络事件循环。
 */
final class RuntimeDirectoryMove {
    private static final long RETRY_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final long RETRY_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    @FunctionalInterface interface MoveOperation {
        void move(Path from, Path to, CopyOption... options) throws IOException;
    }

    private RuntimeDirectoryMove() { }

    static void move(Path from, Path to) throws IOException {
        move(from, to, Files::move);
    }

    static void move(Path from, Path to, MoveOperation operation) throws IOException {
        // ATOMIC_MOVE是否允许替换已有目标取决于平台；安装边界始终要求目标尚不存在。
        if (Files.exists(to, LinkOption.NOFOLLOW_LINKS)) throw new FileAlreadyExistsException(to.toString());
        boolean windows = System.getProperty("os.name", "").startsWith("Windows");
        long deadline = System.nanoTime() + RETRY_WINDOW_NANOS;
        AccessDeniedException previous = null;
        while (true) {
            if (previous != null) {
                long remaining = deadline - System.nanoTime();
                if (!windows || remaining <= 0 || !unchangedPaths(from, to)) throw previous;
                try {
                    TimeUnit.NANOSECONDS.sleep(Math.min(RETRY_DELAY_NANOS, remaining));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    InterruptedIOException failure = new InterruptedIOException("等待运行目录解除短暂占用时被中断");
                    failure.initCause(interrupted);
                    failure.addSuppressed(previous);
                    throw failure;
                }
                // 等待期间源可能已被移走、目标也可能被其他操作创建。必须再次核对，
                // 不能把初次失败时的路径状态当成下一次移动的授权。
                if (System.nanoTime() >= deadline || !unchangedPaths(from, to)) throw previous;
            }
            try {
                moveOnce(from, to, operation);
                return;
            } catch (AccessDeniedException denied) {
                previous = denied;
            }
        }
    }

    private static boolean unchangedPaths(Path from, Path to) {
        // notExists比!exists更保守：无法确认目标状态时不重试，也不跟随链接。
        return Files.exists(from, LinkOption.NOFOLLOW_LINKS)
                && Files.notExists(to, LinkOption.NOFOLLOW_LINKS);
    }

    private static void moveOnce(Path from, Path to, MoveOperation operation) throws IOException {
        try { operation.move(from, to, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException unsupported) { operation.move(from, to); }
    }
}
