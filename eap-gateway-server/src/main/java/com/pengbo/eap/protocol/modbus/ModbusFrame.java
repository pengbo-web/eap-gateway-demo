package com.pengbo.eap.protocol.modbus;

import io.netty.buffer.ByteBuf;

/**
 * Modbus TCP 帧(MBAP + PDU)模型与编解码
 * <p>
 * MBAP: transactionId(u16) protocolId(u16=0) length(u16=unitId+pdu长度) unitId(u8)
 * PDU:  functionCode(u8) data(...)
 *
 * @author pengbo
 * @date 2026-10-17
 */
public record ModbusFrame(int transactionId, int unitId, byte functionCode, byte[] data) {

    public static final byte FC_READ_HOLDING = 0x03;
    public static final byte FC_READ_INPUT = 0x04;

    public void encode(ByteBuf out) {
        out.writeShort(transactionId);
        out.writeShort(0);
        // MBAP length = unitId(1) + functionCode(1) + data
        out.writeShort(2 + data.length);
        out.writeByte(unitId);
        out.writeByte(functionCode);
        out.writeBytes(data);
    }

    public static ModbusFrame decode(ByteBuf in) {
        int tx = in.readUnsignedShort();
        in.readUnsignedShort(); // protocolId
        in.readUnsignedShort(); // length
        int unit = in.readUnsignedByte();
        byte fc = in.readByte();
        byte[] data = new byte[in.readableBytes()];
        in.readBytes(data);
        return new ModbusFrame(tx, unit, fc, data);
    }

    /**
     * 构造 FC3/FC4 读请求
     */
    public static ModbusFrame readRequest(int transactionId, int unitId, byte fc, int startAddr, int quantity) {
        byte[] data = new byte[4];
        data[0] = (byte) (startAddr >> 8);
        data[1] = (byte) startAddr;
        data[2] = (byte) (quantity >> 8);
        data[3] = (byte) quantity;
        return new ModbusFrame(transactionId, unitId, fc, data);
    }

    /**
     * 构造 FC3/FC4 正常响应(data = 寄存器值)
     */
    public static ModbusFrame readResponse(int transactionId, int unitId, byte fc, int[] registers) {
        byte[] data = new byte[1 + registers.length * 2];
        data[0] = (byte) (registers.length * 2);
        for (int i = 0; i < registers.length; i++) {
            data[1 + i * 2] = (byte) (registers[i] >> 8);
            data[2 + i * 2] = (byte) registers[i];
        }
        return new ModbusFrame(transactionId, unitId, fc, data);
    }

    /**
     * 解析响应寄存器
     */
    public int[] responseRegisters() {
        int byteCount = data[0] & 0xFF;
        int[] regs = new int[byteCount / 2];
        for (int i = 0; i < regs.length; i++) {
            regs[i] = ((data[1 + i * 2] & 0xFF) << 8) | (data[2 + i * 2] & 0xFF);
        }
        return regs;
    }

    public boolean isError() {
        return (functionCode & 0x80) != 0;
    }
}
