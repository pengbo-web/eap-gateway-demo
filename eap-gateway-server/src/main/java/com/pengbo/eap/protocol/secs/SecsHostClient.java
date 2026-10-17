package com.pengbo.eap.protocol.secs;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * HSMS 主动式主机客户端(自研)
 * <p>
 * 状态机: TCP_CONNECTED -> Select.req/rsp -> SELECTED -> S1F13/S1F14 -> ESTABLISHED;
 * 接收 S6F11(W) 回 S6F12 并回调参数; 支持 S2F41 远程指令(等待 S2F42, 3s 超时);
 * 断线指数退避重连(1s->30s)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
public class SecsHostClient implements AutoCloseable {

    public enum State {DISCONNECTED, SELECTED, ESTABLISHED}

    private final String host;
    private final int port;
    private final NioEventLoopGroup group;
    private final Consumer<List<Double>> eventReportCallback;
    private final ConcurrentHashMap<Integer, CompletableFuture<HsmsMessage>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger systemGen = new AtomicInteger(0x20000000);
    private final AtomicInteger reconnectAttempts = new AtomicInteger();

    private volatile Channel channel;
    private volatile State state = State.DISCONNECTED;
    private volatile boolean closed;
    private volatile long lastRecoverMillis;

    public SecsHostClient(String host, int port, Consumer<List<Double>> eventReportCallback) {
        this.host = host;
        this.port = port;
        this.eventReportCallback = eventReportCallback;
        this.group = new NioEventLoopGroup(1);
    }

    public State getState() {
        return state;
    }

    public long getLastRecoverMillis() {
        return lastRecoverMillis;
    }

    public synchronized void connect() {
        if (closed) {
            return;
        }
        Bootstrap b = new Bootstrap();
        b.group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4, 0, 0));
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            ByteBuf buf = (ByteBuf) msg;
                            try {
                                onMessage(HsmsMessage.decode(buf));
                            } finally {
                                buf.release();
                            }
                        }

                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) {
                            log.warn("HSMS 连接断开: {}:{} 进入重连退避", host, port);
                            state = State.DISCONNECTED;
                            failAll();
                            scheduleReconnect();
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            ctx.close();
                        }
                    });
                }
            });
        b.connect(host, port).addListener(f -> {
            if (f.isSuccess()) {
                channel = ((io.netty.channel.ChannelFuture) f).channel();
                sendControl(HsmsMessage.ST_SELECT_REQ, new byte[0]);
            } else {
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (closed) {
            return;
        }
        int attempt = reconnectAttempts.incrementAndGet();
        long delay = Math.min(30_000L, 1000L << Math.min(attempt - 1, 5));
        log.info("HSMS 重连调度: {}:{} 第{}次, {}ms 后", host, port, attempt, delay);
        group.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }

    private void failAll() {
        pending.values().forEach(f -> f.completeExceptionally(new IllegalStateException("connection lost")));
        pending.clear();
    }

    private void onMessage(HsmsMessage m) {
        if (m.stype() == HsmsMessage.ST_SELECT_RSP) {
            state = State.SELECTED;
            if (reconnectAttempts.getAndSet(0) > 0) {
                lastRecoverMillis = System.currentTimeMillis();
            }
            sendEstablish();
            return;
        }
        if (m.stype() == HsmsMessage.ST_LINKTEST_REQ) {
            sendControl(HsmsMessage.ST_LINKTEST_RSP, new byte[0], m.system());
            return;
        }
        if (!m.isData()) {
            return;
        }
        if (m.stream() == 1 && m.function() == 14) {
            state = State.ESTABLISHED;
            log.info("HSMS 通信建立完成: {}:{}", host, port);
            CompletableFuture<HsmsMessage> f = pending.remove(m.system());
            if (f != null) {
                f.complete(m);
            }
            return;
        }
        if (m.stream() == 6 && m.function() == 11) {
            List<Double> values = extractReportValues(m.data());
            ack(m);
            eventReportCallback.accept(values);
            return;
        }
        CompletableFuture<HsmsMessage> f = pending.remove(m.system());
        if (f != null) {
            f.complete(m);
        }
    }

    /**
     * 解析 S6F11: L,3 [U4 dataid, U4 ceid, L,n [ L,2 [U4 rptid, L,m [values]]]]
     */
    private List<Double> extractReportValues(byte[] data) {
        List<Double> values = new ArrayList<>();
        try {
            SecsItem root = (SecsItem) SecsItem.decode(data, 0)[0];
            for (SecsItem report : root.children.get(2).children) {
                SecsItem valueList = report.children.get(1);
                for (SecsItem v : valueList.children) {
                    if (v.kind == SecsItem.Kind.F8) {
                        values.add(v.f8);
                    } else if (v.kind == SecsItem.Kind.U4) {
                        values.add((double) v.u4);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("S6F11 解析失败: {}", e.getMessage());
        }
        return values;
    }

    private void ack(HsmsMessage m) {
        ByteBuf buf = allocBuf();
        new HsmsMessage(m.session(), 6, false, 12, 0, HsmsMessage.ST_DATA, m.system(),
            SecsItem.binary(new byte[]{0}).encode()).encode(buf);
        channel.writeAndFlush(buf);
    }

    private void sendEstablish() {
        CompletableFuture<HsmsMessage> f = new CompletableFuture<>();
        int sys = systemGen.incrementAndGet();
        pending.put(sys, f);
        ByteBuf buf = allocBuf();
        new HsmsMessage(0, 1, true, 13, 0, HsmsMessage.ST_DATA, sys, new byte[0]).encode(buf);
        channel.writeAndFlush(buf);
    }

    private void sendControl(int stype, byte[] data) {
        sendControl(stype, data, systemGen.incrementAndGet());
    }

    private void sendControl(int stype, byte[] data, int system) {
        ByteBuf buf = allocBuf();
        new HsmsMessage(0, 0, false, 0, 0, stype, system, data).encode(buf);
        channel.writeAndFlush(buf);
    }

    /**
     * S2F41 远程指令, 等待 S2F42
     */
    public CompletableFuture<HsmsMessage> sendRemoteCommand(String rcmd) {
        Channel ch = channel;
        if (ch == null || !ch.isActive() || state != State.ESTABLISHED) {
            return CompletableFuture.failedFuture(new IllegalStateException("not established"));
        }
        int sys = systemGen.incrementAndGet();
        CompletableFuture<HsmsMessage> f = new CompletableFuture<>();
        pending.put(sys, f);
        byte[] body = SecsItem.list(SecsItem.ascii(rcmd), SecsItem.list()).encode();
        ByteBuf buf = allocBuf();
        new HsmsMessage(0, 2, true, 41, 0, HsmsMessage.ST_DATA, sys, body).encode(buf);
        ch.writeAndFlush(buf);
        return f.orTimeout(3, TimeUnit.SECONDS);
    }

    private ByteBuf allocBuf() {
        return channel.alloc().buffer();
    }

    @Override
    public synchronized void close() {
        closed = true;
        failAll();
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
    }
}
