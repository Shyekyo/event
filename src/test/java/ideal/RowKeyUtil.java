package ideal;

import java.security.MessageDigest;

/**
 * Created by zhangxiaofan on 2019/8/8.
 */
public class RowKeyUtil {
    public static void main(String[] args) {
        String userid = "641374874671487";
        String rowkey = RowKeyUtil.getWithPre4(userid);
        System.out.println(rowkey);
        String userid1 = "641374874671488";
        String rowkey1 = RowKeyUtil.getWithPre4(userid1);
        System.out.println(rowkey1);
    }

    public static String getWithPre4(String row) {
        byte[] carray = MD5New(row);
        int prefix = 0;
        for (byte b : carray) {
            prefix ^= b;
            prefix &= 0x1f;
        }
        String pre = "";
        if (prefix < 10)
            pre = "0" + prefix;
        else
            pre = prefix + "";

        pre += toHex4(carray[0]);

        return pre + ":" + row;
    }

    static byte[] MD5New(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(s.getBytes("utf-8"));
            return bytes;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String toHex4(byte bytes) {

        final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();
        StringBuilder ret = new StringBuilder(2);

        ret.append(HEX_DIGITS[(bytes >> 4) & 0x0f]);
        ret.append(HEX_DIGITS[bytes & 0x0f]);

        return ret.toString();
    }

}
