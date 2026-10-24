package imsng.player_to_player.env.restart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static imsng.player_to_player.env.restart.RestartTask.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class RestartHelperProcessTest {
    @TempDir Path root;

    @Test void waitsForActualParentExitThenLaunchesExactlyOnceWithUnicodeArguments() throws Exception {
        var store = new RestartTaskStore(root.resolve("共享 目录"));
        Process parent = parent();
        RestartHelperProcess helper = null;
        try {
            var task = task(store, parent);
            Path marker = root.resolve("启动 结果.txt");
            String value = "\"test-only-token 中文 空格 \"quoted\" \\tail\\\"";
            helper = prepare(store, task, fixture("started", marker.toString(), value,
                    store.dataRoot().toString()));
            assertEquals(Stage.READY, store.read(task.id()).stage());
            helper.commit();
            assertEquals(Stage.COMMITTED, store.read(task.id()).stage());
            assertTrue(parent.isAlive());
            assertFalse(Files.exists(marker));
            parent.getOutputStream().close();
            assertTrue(parent.waitFor(5, TimeUnit.SECONDS));
            await(() -> stage(store, task.id()) == Stage.CLIENT_STARTED);
            assertEquals(value, Files.readString(marker));
            assertFalse(Files.readString(store.taskFile(task.id())).contains("test-only-token"));
            assertFalse(store.recoverable().stream().anyMatch(t -> t.id().equals(task.id())));
            var child = store.read(task.id()).child();
            if (child != null) ProcessHandle.of(child.pid()).ifPresent(p -> p.onExit().join());
        } finally {
            if (helper != null) helper.abort();
            parent.destroyForcibly().waitFor();
        }
    }

    @Test void abortBeforeCommitNeverLaunchesEvenAfterParentExits() throws Exception {
        var store = new RestartTaskStore(root);
        Process parent = parent();
        var task = task(store, parent);
        Path marker = root.resolve("must-not-exist");
        var helper = prepare(store, task, fixture("started", marker.toString(), "x", root.toString()));
        try {
            helper.abort();
            parent.getOutputStream().close();
            parent.waitFor();
            assertEquals(Stage.CANCELLED, store.read(task.id()).stage());
            assertFalse(Files.exists(marker));
        } finally { parent.destroyForcibly().waitFor(); }
    }

    @Test void handshakeTimeoutKeepsOldJvmAliveAndRecordsFailure() throws Exception {
        var store = new RestartTaskStore(root);
        Process parent = parent();
        try {
            var task = task(store, parent);
            assertThrows(IOException.class, () -> RestartHelperProcess.prepare(store, task, fixture("crash"),
                    fixture("hang"), Duration.ofMillis(150)));
            assertTrue(parent.isAlive());
            assertEquals(Stage.FAILED, store.read(task.id()).stage());
        } finally { parent.destroyForcibly().waitFor(); }
    }

    @Test void helperCrashDoesNotExitOldJvm() throws Exception {
        var store = new RestartTaskStore(root);
        Process parent = parent();
        try {
            var task = task(store, parent);
            assertThrows(IOException.class, () -> RestartHelperProcess.prepare(store, task, fixture("crash"),
                    fixture("crash"), Duration.ofSeconds(3)));
            assertTrue(parent.isAlive());
        } finally { parent.destroyForcibly().waitFor(); }
    }

    @Test void earlyChildCrashIsRecoverableWithoutSavingLaunchArguments() throws Exception {
        var store = new RestartTaskStore(root);
        Process parent = parent();
        var task = task(store, parent);
        var helper = prepare(store, task, fixture("crash"));
        try {
            helper.commit();
            parent.getOutputStream().close();
            parent.waitFor();
            await(() -> stage(store, task.id()) == Stage.FAILED);
            assertEquals(Failure.CHILD_EXITED, store.read(task.id()).failure());
            await(() -> {
                try { return !store.recoverable().isEmpty(); } catch (IOException e) { return false; }
            });
        } finally { helper.abort(); parent.destroyForcibly().waitFor(); }
    }

    @Test void initializationTimeoutLeavesSlowChildRunningAndDoesNotRestartIt() throws Exception {
        var store = new RestartTaskStore(root);
        Process child = new ProcessBuilder(fixture("hang")).start();
        try {
            var task = task(store, child);
            store.update(task.id(), old -> old.child(ProcessIdentity.of(child.toHandle())).stage(Stage.LAUNCHING, Failure.NONE));
            RestartHelper.awaitStartup(store, task.id(), child, Duration.ofMillis(100));
            assertEquals(Stage.INITIALIZATION_UNCONFIRMED, store.read(task.id()).stage());
            assertTrue(child.isAlive());
            assertTrue(store.recoverable().isEmpty());
        } finally { child.destroyForcibly().waitFor(); }
    }

    @Test void helperUsesOnlySmallFixedJvmOptionsAndNoGameArguments() throws Exception {
        List<String> command = RestartHelperProcess.helperCommand();
        assertTrue(command.contains("-Xmx64m"));
        assertTrue(command.contains("-Xms16m"));
        assertFalse(command.stream().anyMatch(s -> s.contains("accessToken") || s.startsWith("-javaagent:")));
    }

    @Test void inheritedJavaOptionsCannotOverrideHelperMemoryOrLoadAgents() throws Exception {
        var builder = new ProcessBuilder(fixture("parent"));
        builder.environment().put("JAVA_TOOL_OPTIONS", "-Xmx999999999g");
        builder.environment().put("_JAVA_OPTIONS", "-javaagent:missing-test-agent.jar");
        builder.environment().put("JDK_JAVA_OPTIONS", "-Xmx999999999g");
        RestartHelperProcess.clearJavaEnvironment(builder);
        Process process = builder.start();
        try { assertEquals(1, process.getInputStream().read()); }
        finally { process.getOutputStream().close(); assertTrue(process.waitFor(5, TimeUnit.SECONDS)); }
        assertEquals(0, process.exitValue());
    }

    private RestartTask task(RestartTaskStore store, Process parent) throws Exception {
        Path directory = Files.createDirectories(root.resolve("隔离 游戏"));
        var task = store.create(UUID.randomUUID(), "[2001:db8::1]:25579", "a".repeat(64), directory);
        return store.update(task.id(), old -> new RestartTask(old.id(), old.chainId(), old.address(), old.runtimeHash(),
                old.directory(), 1, ProcessIdentity.of(parent.toHandle()), null, null, old.stage(), old.failure(), old.updatedAt()));
    }
    private RestartHelperProcess prepare(RestartTaskStore store, RestartTask task, List<String> command) throws Exception {
        String packaged = System.getProperty("p2p.restart.helperJar");
        if (packaged == null) return RestartHelperProcess.prepare(store, task, command);
        var helper = new ArrayList<>(RestartHelperProcess.helperCommand());
        helper.set(helper.indexOf("-cp") + 1, Path.of(packaged).toAbsolutePath().toString());
        assertTrue(Files.isRegularFile(Path.of(packaged)));
        return RestartHelperProcess.prepare(store, task, command, helper, RestartHelperProcess.HANDSHAKE_TIMEOUT);
    }
    private Process parent() throws Exception {
        Process result = new ProcessBuilder(fixture("parent")).start();
        assertEquals(1, result.getInputStream().read());
        return result;
    }
    private List<String> fixture(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(Path.of(RestartProcessFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                + java.io.File.pathSeparator
                + Path.of(RestartTask.class.getProtectionDomain().getCodeSource().getLocation().toURI()));
        command.add(RestartProcessFixture.class.getName());
        command.addAll(List.of(arguments));
        return command;
    }
    private static Stage stage(RestartTaskStore store, UUID id) {
        try { return store.read(id).stage(); } catch (IOException e) { throw new RuntimeException(e); }
    }
    private static void await(BooleanSupplier ready) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!ready.getAsBoolean()) {
            if (System.nanoTime() > end) fail("process state did not converge");
            Thread.sleep(20);
        }
    }
}
