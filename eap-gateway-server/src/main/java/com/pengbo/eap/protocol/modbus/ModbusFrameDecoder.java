package com.pengbo.eap.protocol.modbus;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Modbus TCP 拆包器: 以 MBAP 长度字段解决粘包/半包
 * <p>
 * 长度字段偏移 4、占 2 字节、值为 unitId+PDU 长度, 帧总长 = 6 + length, 故 lengthAdjustment = -0?
 * 此处 lengthField 计数字节含 unitId(1字节), 而 LengthFieldBasedFrameDecoder 的 length 从长度字段后起算
 * (即 unitId+PDU), 与 MBAP 语义一致, adjustment=0, 不剥离头(initialBytesToStrip=0)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
public class ModbusFrameDecoder extends LengthFieldBasedFrameDecoder {

    public ModbusFrameDecoder() {
        super(260, 4, 2, 0, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        try {
            return ModbusFrame.decode(frame);
        } finally {
            frame.release();
        }
    }
}
