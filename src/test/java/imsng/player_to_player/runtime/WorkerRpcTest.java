package imsng.player_to_player.runtime;

import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/** 断连和请求登记必须处于同一生命周期边界，否则无超时RPC可能永远挂起。 */
class WorkerRpcTest {
    @Test
    void disconnectDuringOpenCheckCannotLeaveAnUncompletedRequest() throws Exception {
        CountDownLatch checkingOpen = new CountDownLatch(1);
        CountDownLatch releaseOpenCheck = new CountDownLatch(1);
        WorkerRpc rpc = new WorkerRpc(message -> CompletableFuture.completedFuture(WorkerRpc.ok()),
                (connection, message) -> { });
        rpc.attach(new ControlConnection() {
            @Override public boolean isOpen() {
                checkingOpen.countDown();
                try { assertTrue(releaseOpenCheck.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { throw new AssertionError(error); }
                // 返回进入检查时观察到的“仍开启”，模拟真正断开与状态读取的竞争窗口。
                return true;
            }
            @Override public void send(ControlMessage message) { /* Netty关闭后send只丢弃，不抛异常。 */ }
            @Override public CompletableFuture<ControlMessage> request(ControlMessage message) { throw new UnsupportedOperationException(); }
            @Override public SocketAddress remoteAddress() { return null; }
            @Override public UUID peerId() { return null; }
            @Override public void setPeerId(UUID peerId) { }
            @Override public void close() { }
        });
        var executor = Executors.newFixedThreadPool(2);
        try {
            var issuing = executor.submit(() -> rpc.request("stop", null));
            assertTrue(checkingOpen.await(5, TimeUnit.SECONDS));
            var disconnecting = executor.submit(rpc::disconnected);
            // 正确实现可让断连等待登记锁；旧实现则会先清空pending，再登记一个遗漏请求。
            try { disconnecting.get(100, TimeUnit.MILLISECONDS); }
            catch (TimeoutException expectedWhenSerialized) { }
            releaseOpenCheck.countDown();
            var pending = issuing.get(5, TimeUnit.SECONDS);
            disconnecting.get(5, TimeUnit.SECONDS);
            assertTrue(pending.isCompletedExceptionally(), "断连后全部RPC必须异常完成，不能遗留永久等待");
        } finally {
            releaseOpenCheck.countDown();
            executor.shutdownNow();
        }
    }
}
