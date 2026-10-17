package com.pengbo.eap.protocol.modbus;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Modbus TCP 主站客户端(自研 Netty 实现)
 * <p>
 * 事务号匹配请求/响应; 断线指数退避重连(1s 起, 30s 封顶), 重连序列打日志供验收。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
public class ModbusMasterClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final int unitId;
    private final NioEventLoopGroup group;
    private final ConcurrentHashMap<Integer, CompletableFuture<ModbusFrame>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger txGen = new AtomicInteger();
    private final AtomicInteger reconnectAttempts = new AtomicInteger();

    private volatile Channel channel;
    private volatile boolean closed;
    private volatile long lastRecoverMillis;

    public ModbusMasterClient(String host, int port, int unitId) {
        this.host = host;
        this.port = port;
        this.unitId = unitId;
        this.group = new NioEventLoopGroup(1);
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
                    ch.pipeline().addLast(new ModbusFrameDecoder());
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            ModbusFrame frame = (ModbusFrame) msg;
                            CompletableFuture<ModbusFrame> f = pending.remove(frame.transactionId());
                            if (f != null) {
                                f.complete(frame);
                            }
                        }

                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) {
                            log.warn("Modbus 连接断开: {}:{} , 进入重连退避", host, port);
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
                if (reconnectAttempts.get() > 0) {
                    lastRecoverMillis = System.currentTimeMillis();
                    log.info("Modbus 重连成功: {}:{} 累计重试{}次", host, port, reconnectAttempts.get());
                }
                reconnectAttempts.set(0);
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
        log.info("Modbus 重连调度: {}:{} 第{}次, {}ms 后", host, port, attempt, delay);
        group.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }

    private void failAll() {
        pending.values().forEach(f -> f.completeExceptionally(new IllegalStateException("connection lost")));
        pending.clear();
    }

    /**
     * FC3/FC4 读寄存器, 2s 超时
     */
    public CompletableFuture<int[]> read(byte fc, int startAddr, int quantity) {
        Channel ch = channel;
        if (ch == null || !ch.isActive()) {
            return CompletableFuture.failedFuture(new IllegalStateException("not connected"));
        }
        int tx = txGen.incrementAndGet() & 0xFFFF;
        CompletableFuture<ModbusFrame> f = new CompletableFuture<>();
        pending.put(tx, f);
        io.netty.buffer.ByteBuf buf = ch.alloc().buffer();
        ModbusFrame.readRequest(tx, unitId, fc, startAddr, quantity).encode(buf);
        ch.writeAndFlush(buf).addListener(w -> {
            if (!w.isSuccess()) {
                pending.remove(tx);
                f.completeExceptionally(w.cause());
            }
        });
        return f.orTimeout(2, TimeUnit.SECONDS).thenApply(frame -> {
            if (frame.isError()) {
                throw new IllegalStateException("modbus exception: " + (frame.data().length > 0 ? frame.data()[0] : -1));
            }
            return frame.responseRegisters();
        });
    }

    public boolean isConnected() {
        Channel ch = channel;
        return ch != null && ch.isActive();
    }

    public long getLastRecoverMillis() {
        return lastRecoverMillis;
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
