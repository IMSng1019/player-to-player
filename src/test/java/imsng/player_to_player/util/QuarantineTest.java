package imsng.player_to_player.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 陈旧状态隔离的红线覆盖：<b>隔离而非删除</b>、同级改名、同名不覆盖。
 * 这些性质直接对应"用户的旧世界可能还要用"这一硬约束，改动前请先读
 * {@code docs/plans/2026-10-14-world-identity-mismatch-corruption.md} §5。
 */
class QuarantineTest {

    @TempDir
    Path temporary;

    @Test
    void quarantineMovesDirectoryAndKeepsContent() throws Exception {
        Path save = temporary.resolve("world");
        Files.createDirectories(save.resolve("region"));
        Files.writeString(save.resolve("level.dat"), "旧世界");
        Files.writeString(save.resolve("region/r.0.0.mca"), "区块数据");

        Path moved = Quarantine.quarantine(save, "b0a1c2d3", 1_700_000_000_000L);

        assertNotNull(moved, "存在的目录必须被改名隔离");
        assertTrue(Files.exists(moved.resolve("region/r.0.0.mca")),
                "隔离必须保留内容（绝不删除：旧存档可能还有人工价值）");
        assertTrue(Files.notExists(save), "原目录必须让位，否则重建分支的合并复制会把旧文件留在原地");
        assertEquals(temporary, moved.getParent(), "隔离目录必须仍在同级（原版只能在 savesRoot 下打开存档）");
        assertTrue(moved.getFileName().toString().startsWith("world.stale-b0a1c2d3-"),
                "隔离目录名要能直接看出旧身份与时间: " + moved.getFileName());
    }

    @Test
    void secondQuarantineWithSameTimestampDoesNotOverwrite() throws Exception {
        Path first = temporary.resolve("world");
        Files.createDirectories(first);
        Files.writeString(first.resolve("level.dat"), "第一次");
        Path firstMoved = Quarantine.quarantine(first, "same", 1_700_000_000_000L);

        Path second = temporary.resolve("world");
        Files.createDirectories(second);
        Files.writeString(second.resolve("level.dat"), "第二次");
        Path secondMoved = Quarantine.quarantine(second, "same", 1_700_000_000_000L);

        assertNotNull(firstMoved);
        assertNotNull(secondMoved);
        assertTrue(!firstMoved.equals(secondMoved), "同名冲突必须自动加序号，不能覆盖上一份隔离存档");
        assertEquals("第一次", Files.readString(firstMoved.resolve("level.dat")));
        assertEquals("第二次", Files.readString(secondMoved.resolve("level.dat")));
    }

    @Test
    void missingTargetIsNotAnError() throws Exception {
        assertNull(Quarantine.quarantine(temporary.resolve("不存在"), "x", 0L),
                "目标不存在属正常情况（首次启动、无陈旧状态）");
        assertNull(Quarantine.quarantine(null, "x", 0L), "null 目标按不存在处理");
    }

    @Test
    void fileTargetIsRejected() throws Exception {
        Path file = temporary.resolve("mutations.jsonl");
        Files.writeString(file, "{}");
        assertThrows(IOException.class,
                () -> Quarantine.quarantine(file, "x", 0L),
                "只隔离目录：文件级隔离须由调用方显式决定，避免误伤单个状态文件");
    }

    @Test
    void labelIsSanitizedForFileName() throws Exception {
        Path dir = temporary.resolve("chunk-journal");
        Files.createDirectories(dir);
        Path moved = Quarantine.quarantine(dir, "a/b:c", 1_700_000_000_000L);
        assertNotNull(moved);
        // 时间戳按本机时区渲染，故只断言形态（yyyyMMdd-HHmmss），不写死具体时刻
        assertTrue(moved.getFileName().toString().matches("chunk-journal\\.stale-a_b_c-\\d{8}-\\d{6}"),
                "标签里的路径分隔符必须被替换，否则会拼到别的目录里去: " + moved.getFileName());
    }
}
