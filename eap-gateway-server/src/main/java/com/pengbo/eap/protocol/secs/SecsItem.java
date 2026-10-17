package com.pengbo.eap.protocol.secs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * SECS-II 数据项编解码(自研, 格式码对照 SEMI E5 / 开源 secsgem 编码实现)
 * <p>
 * format_byte = (code &lt;&lt; 2) | 长度字节数; code: List=0, B=8, A=16, F8=32, U4=44
 *
 * @author pengbo
 * @date 2026-10-17
 */
public class SecsItem {

    public enum Kind {LIST, BINARY, ASCII, F8, U4}

    public final Kind kind;
    public final List<SecsItem> children = new ArrayList<>();
    public byte[] binary = new byte[0];
    public String ascii = "";
    public double f8;
    public long u4;

    public SecsItem(Kind kind) {
        this.kind = kind;
    }

    public static SecsItem list(SecsItem... items) {
        SecsItem li = new SecsItem(Kind.LIST);
        for (SecsItem it : items) {
            li.children.add(it);
        }
        return li;
    }

    public static SecsItem u4(long v) {
        SecsItem it = new SecsItem(Kind.U4);
        it.u4 = v;
        return it;
    }

    public static SecsItem f8(double v) {
        SecsItem it = new SecsItem(Kind.F8);
        it.f8 = v;
        return it;
    }

    public static SecsItem ascii(String s) {
        SecsItem it = new SecsItem(Kind.ASCII);
        it.ascii = s;
        return it;
    }

    public static SecsItem binary(byte[] b) {
        SecsItem it = new SecsItem(Kind.BINARY);
        it.binary = b;
        return it;
    }

    // ---------- 编码 ----------

    public byte[] encode() {
        byte[] payload;
        int code;
        switch (kind) {
            case LIST -> {
                code = 0;
                java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
                bos.write(children.size() & 0xFF);
                for (SecsItem c : children) {
                    bos.writeBytes(c.encode());
                }
                payload = bos.toByteArray();
            }
            case BINARY -> {
                code = 8;
                payload = binary;
            }
            case ASCII -> {
                code = 16;
                payload = ascii.getBytes(StandardCharsets.US_ASCII);
            }
            case F8 -> {
                code = 32;
                payload = new byte[8];
                long bits = Double.doubleToLongBits(f8);
                for (int i = 0; i < 8; i++) {
                    payload[i] = (byte) (bits >>> (56 - 8 * i));
                }
            }
            case U4 -> {
                code = 44;
                payload = new byte[]{(byte) (u4 >>> 24), (byte) (u4 >>> 16), (byte) (u4 >>> 8), (byte) u4};
            }
            default -> throw new IllegalStateException("unknown kind");
        }
        byte[] lenBytes;
        if (payload.length < 256) {
            lenBytes = new byte[]{(byte) ((code << 2) | 1), (byte) payload.length};
        } else if (payload.length < 65536) {
            lenBytes = new byte[]{(byte) ((code << 2) | 2), (byte) (payload.length >>> 8), (byte) payload.length};
        } else {
            lenBytes = new byte[]{(byte) ((code << 2) | 3),
                (byte) (payload.length >>> 16), (byte) (payload.length >>> 8), (byte) payload.length};
        }
        byte[] out = new byte[lenBytes.length + payload.length];
        System.arraycopy(lenBytes, 0, out, 0, lenBytes.length);
        System.arraycopy(payload, 0, out, lenBytes.length, payload.length);
        return out;
    }

    // ---------- 解码 ----------

    /**
     * @return [item, 消费字节数]
     */
    public static Object[] decode(byte[] buf, int off) {
        int fb = buf[off] & 0xFF;
        int code = fb >> 2;
        int nlen = fb & 0x03;
        int len = 0;
        for (int i = 0; i < nlen; i++) {
            len = (len << 8) | (buf[off + 1 + i] & 0xFF);
        }
        int dataOff = off + 1 + nlen;
        SecsItem item;
        switch (code) {
            case 0 -> {
                int count = buf[dataOff] & 0xFF;
                item = new SecsItem(Kind.LIST);
                int p = dataOff + 1;
                for (int i = 0; i < count; i++) {
                    Object[] r = decode(buf, p);
                    item.children.add((SecsItem) r[0]);
                    p += (Integer) r[1];
                }
                return new Object[]{item, p - off};
            }
            case 8 -> {
                item = new SecsItem(Kind.BINARY);
                item.binary = java.util.Arrays.copyOfRange(buf, dataOff, dataOff + len);
            }
            case 16 -> {
                item = new SecsItem(Kind.ASCII);
                item.ascii = new String(buf, dataOff, len, StandardCharsets.US_ASCII);
            }
            case 32 -> {
                item = new SecsItem(Kind.F8);
                long bits = 0;
                for (int i = 0; i < 8; i++) {
                    bits = (bits << 8) | (buf[dataOff + i] & 0xFF);
                }
                item.f8 = Double.longBitsToDouble(bits);
            }
            case 44 -> {
                item = new SecsItem(Kind.U4);
                long v = 0;
                for (int i = 0; i < len; i++) {
                    v = (v << 8) | (buf[dataOff + i] & 0xFF);
                }
                item.u4 = v;
            }
            default -> throw new IllegalStateException("unsupported format code: " + code);
        }
        return new Object[]{item, dataOff + len - off};
    }
}
