package com.pengbo.eap.protocol.secs;

import io.netty.buffer.ByteBuf;

/**
 * HSMS-2 消息(10 字节头 + SECS-II 数据), 4 字节长度前缀由帧解码器处理
 * <p>
 * 头: session u16 | stream u8(bit7=W) | function u8 | ptype u8 | stype u8 | system u32
 * stype: 0=Data 1=Select.req 2=Select.rsp 3=Linktest.req 4=Linktest.rsp 9=Separate.req
 *
 * @author pengbo
 * @date 2026-10-17
 */
public record HsmsMessage(int session, int stream, boolean w, int function, int ptype, int stype,
                          int system, byte[] data) {

    public static final int ST_DATA = 0;
    public static final int ST_SELECT_REQ = 1;
    public static final int ST_SELECT_RSP = 2;
    public static final int ST_LINKTEST_REQ = 3;
    public static final int ST_LINKTEST_RSP = 4;

    public void encode(ByteBuf out) {
        out.writeInt(10 + data.length);
        out.writeShort(session);
        out.writeByte(w ? stream | 0x80 : stream);
        out.writeByte(function);
        out.writeByte(ptype);
        out.writeByte(stype);
        out.writeInt(system);
        out.writeBytes(data);
    }

    public static HsmsMessage decode(ByteBuf in) {
        in.readInt(); // length prefix
        int session = in.readUnsignedShort();
        int streamByte = in.readUnsignedByte();
        int function = in.readUnsignedByte();
        int ptype = in.readUnsignedByte();
        int stype = in.readUnsignedByte();
        int system = in.readInt();
        byte[] data = new byte[in.readableBytes()];
        in.readBytes(data);
        return new HsmsMessage(session, streamByte & 0x7F, (streamByte & 0x80) != 0,
            function, ptype, stype, system, data);
    }

    public boolean isData() {
        return stype == ST_DATA;
    }
}
