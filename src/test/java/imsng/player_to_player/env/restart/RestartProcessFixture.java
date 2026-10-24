package imsng.player_to_player.env.restart;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** 真实 JVM 冒烟的轻量游戏替身；不初始化 Minecraft，也不需要账号。 */
public final class RestartProcessFixture {
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "parent" -> {
                System.out.write(1);
                System.out.flush();
                System.in.read();
            }
            case "crash" -> System.exit(23);
            case "hang" -> Thread.sleep(60_000);
            case "started" -> {
                Files.writeString(Path.of(args[1]), args[2]);
                var store = new RestartTaskStore(Path.of(args[3]));
                UUID id = UUID.fromString(System.getProperty(RestartTask.TASK_PROPERTY));
                var task = store.read(id);
                if (!store.clientStarted(id, Path.of(System.getProperty("user.dir")), task.runtimeHash()))
                    throw new IllegalStateException("startup acknowledgment rejected");
            }
            default -> throw new IllegalArgumentException("Unknown fixture");
        }
    }
}
