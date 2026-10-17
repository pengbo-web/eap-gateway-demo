package com.pengbo.eap.protocol.modbus;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Modbus TCP 从站模拟器(自研, 兼作断线重连测试靶)
 * <p>
 * 寄存器区: 0=电压×100, 1=电流×100, 2=有功功率×100, 其余=秒级计数; 每次读请求刷新随机工况。
 * 支持运行时 stop/start 以验证主站断线重连。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Component
public class ModbusSlaveSimulator {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private NioEventLoopGroup boss;
    private NioEventLoopGroup worker;
    private Channel serverChannel;
    private int port = 1502;

    public synchronized boolean start(int port) {
        if (running.get()) {
            return false;
        }
        this.port = port;
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup(2);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ModbusFrameDecoder());
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ModbusFrame>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ModbusFrame req) {
                                handle(ctx, req);
                            }
                        });
                    }
                });
            serverChannel = b.bind(port).sync().channel();
            running.set(true);
            log.info("Modbus 从站模拟器启动: port={}", port);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public synchronized boolean stop() {
        if (!running.get()) {
            return false;
        }
        try {
            serverChannel.close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        boss.shutdownGracefully();
        worker.shutdownGracefully();
        running.set(false);
        log.warn("Modbus 从站模拟器停止: port={}", port);
        return true;
    }

    public boolean isRunning() {
        return running.get();
    }

    private void handle(ChannelHandlerContext ctx, ModbusFrame req) {
        ModbusFrame resp;
        if (req.functionCode() == ModbusFrame.FC_READ_HOLDING || req.functionCode() == ModbusFrame.FC_READ_INPUT) {
            int start = ((req.data()[0] & 0xFF) << 8) | (req.data()[1] & 0xFF);
            int qty = ((req.data()[2] & 0xFF) << 8) | (req.data()[3] & 0xFF);
            int[] regs = new int[qty];
            for (int i = 0; i < qty; i++) {
                regs[i] = registerValue(start + i);
            }
            resp = ModbusFrame.readResponse(req.transactionId(), req.unitId(), req.functionCode(), regs);
        } else {
            resp = new ModbusFrame(req.transactionId(), req.unitId(), (byte) (req.functionCode() | 0x80), new byte[]{0x01});
        }
        ByteBuf buf = ctx.alloc().buffer();
        resp.encode(buf);
        ctx.writeAndFlush(buf);
    }

    private int registerValue(int addr) {
        return switch (addr) {
            case 0 -> 20000 + ThreadLocalRandom.current().nextInt(0, 6000);
            case 1 -> 1000 + ThreadLocalRandom.current().nextInt(0, 4000);
            case 2 -> 300000 + ThreadLocalRandom.current().nextInt(0, 50000);
            default -> (int) (System.currentTimeMillis() / 1000 % 65536);
        };
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}
