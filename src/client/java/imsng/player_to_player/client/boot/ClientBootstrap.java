package imsng.player_to_player.client.boot;

import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeScoreProvider;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.p2p.NatDetector;
import imsng.player_to_player.p2p.NatInfo;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayConnectionEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * 客户端引导（对应规范"玩家加载模组"与"玩家加入世界"事件的客户端部分，
 * 见 player_to_player-prompt.txt 第 54/55 行与 DESIGN.md 第 6/7 节）。
 * <p>
 * 职责：
 * <ul>
 *   <li>模组加载：异步算力检测（CPU 单核 + 可用内存）、NAT 类型探测，
 *       结果写入 {@link NodeContext}，供加入世界时 HELLO 握手上报；</li>
 *   <li>加入世界：注册 Fabric 的 JOIN / DISCONNECT 钩子，把世界会话的
 *       开合转交 {@link WorldSession}（连接控制端口、环境同步、算力上报等）。</li>
 * </ul>
 * <p>
 * 所有探测都在后台线程进行，本方法自身瞬间返回，绝不拖慢客户端启动；
 * 任何探测失败只降级（日志告警 + 缺省值），不影响玩家正常游玩。
 */
public final class ClientBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/client_boot");

    /**
     * 算力检测结果 future。{@link WorldSession} 在 HELLO 握手前若发现
     * {@link NodeContext#computeScore()} 尚未就绪，会在此 future 上限时等待
     * （最多 20 秒；规范：玩家加入世界时向服务端给出算力能力）。
     */
    private static volatile CompletableFuture<ComputeScore> computeScoreFuture;

    private ClientBootstrap() {
    }

    /** 客户端入口（ClientModInitializer.onInitializeClient）调用。 */
    public static void onClientInit() {
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        if (config == null) {
            // 公共引导（P2PBootstrap.onCommonInit）理应先于客户端入口执行；
            // 走到这里说明公共初始化失败，客户端引导整体跳过 —— 模组降级为
            // "什么都不做"，玩家仍可正常游玩原版内容。
            LOGGER.error("总配置未初始化（公共引导失败？），客户端引导跳过，模组功能不可用");
            return;
        }

        // ---------------------------------------------------------------
        // a. 异步算力检测（规范：模组检查该玩家的 cpu 得出其单核算力）。
        //    ComputeScoreProvider 内部自行调度到 ThreadPools（Geekbench API
        //    查询走 io、本地跑分走 compute），这里只挂回调。
        // ---------------------------------------------------------------
        CompletableFuture<ComputeScore> scoreFuture;
        try {
            scoreFuture = ComputeScoreProvider.detect(config);
        } catch (Throwable t) {
            // detect 同步抛异常也不能打断客户端启动，包成失败 future 统一走降级路径
            scoreFuture = CompletableFuture.failedFuture(t);
        }
        computeScoreFuture = scoreFuture;
        scoreFuture.whenComplete((score, err) -> {
            if (err != null || score == null) {
                LOGGER.warn("算力检测失败（降级：HELLO 将不携带算力信息，本客户端可能只被指派为副客户端）", err);
                return;
            }
            NodeContext.get().setComputeScore(score);
            LOGGER.info("算力检测完成: cpu=\"{}\" 单核={} 来源={} 可用内存={}MB/总内存{}MB",
                    score.cpuModel(), score.singleCoreScore(), score.source(),
                    score.freeMemoryBytes() / (1024 * 1024),
                    score.totalMemoryBytes() / (1024 * 1024));
        });

        // ---------------------------------------------------------------
        // b. 异步 NAT 探测（规范：检查玩家路由器的 nat 类型以及是否可以 p2p 链接）。
        //    失败按 UNKNOWN 处理：服务端打洞协助时会倾向直接走中转降级。
        // ---------------------------------------------------------------
        try {
            NatDetector.detect(config.p2pUdpPort).whenComplete((nat, err) -> {
                if (err != null || nat == null) {
                    LOGGER.warn("NAT 探测失败（降级：按 UNKNOWN 上报，P2P 可能改走中转）", err);
                    NodeContext.get().setNatInfo(NatInfo.UNKNOWN);
                    return;
                }
                NodeContext.get().setNatInfo(nat);
                LOGGER.info("NAT 探测完成: type={} 公网端点={}:{} 本地UDP端口={}",
                        nat.type(), nat.publicIp(), nat.publicPort(), nat.localPort());
            });
        } catch (Throwable t) {
            LOGGER.warn("NAT 探测启动失败（降级：按 UNKNOWN 上报）", t);
            NodeContext.get().setNatInfo(NatInfo.UNKNOWN);
        }

        // ---------------------------------------------------------------
        // c. 加入/离开世界钩子 → 世界会话开合（具体流程全部在 WorldSession）。
        //    JOIN 在客户端主线程触发，WorldSession 内部立刻转交 io 线程，
        //    保证不阻塞渲染。
        // ---------------------------------------------------------------
        ClientPlayConnectionEvents.JOIN.register((handler, sender, client) -> WorldSession.onJoin(client));
        ClientPlayConnectionEvents.DISCONNECT.register((handler, client) -> WorldSession.onLeave(client));

        LOGGER.info("客户端引导完成：算力/NAT 探测已在后台进行，世界加入钩子已注册");
    }

    /**
     * 算力检测 future（永不返回 null）。
     * <p>
     * 引导尚未执行（或执行失败）时返回"已完成、值为 null"的 future，
     * 调用方按"无算力信息"降级处理即可，不需要判空 future 本身。
     */
    public static CompletableFuture<ComputeScore> computeScoreFuture() {
        CompletableFuture<ComputeScore> future = computeScoreFuture;
        return future != null ? future : CompletableFuture.completedFuture(null);
    }
}
