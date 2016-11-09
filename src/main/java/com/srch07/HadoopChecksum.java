package com.srch07;

import org.apache.commons.io.IOUtils;
import com.srch07.NullOutputStream;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.util.DataChecksum;
import com.srch07.MD5MD5CRCMessageDigest;

import java.io.*;
import java.security.DigestInputStream;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.*;

/**
 * Created by aanand on 07/11/16.
 */
public class HadoopChecksum {

    public static void main(String [] args){
        try{
            if(args ==null || !(args.length == 1 || args.length== 3 || args.length==4)){
                System.out.println("Argument is Invalid");
                System.out.println("Argument should be provided in either of the following way");
                System.out.println("filename_with_path");
                System.out.println("filename_with_path BytesPerChecksum ChecksumPerBlock");
                System.out.println("filename_with_path BytesPerChecksum ChecksumPerBlock AlgorithmType(CRC32/CRC32C/NULL/DEFAULT/MIXED)");
                return;
            }
            if(!((new File(args[0])).exists())){
                System.out.println("File : "+args[0]+" does not exist or is not readable");
                return;
            }
            if(args.length == 1){
                System.out.println(calculate(args[0]));
                return;
            }
            if(!(NumberUtils.isNumber(args[1]) && NumberUtils.isNumber(args[2]))){
                System.out.println(String.format("Provided values of BytesPerChecksum : %s or Checksum per block : %s are not Integer",args[1],args[2]));
                return;
            }
            if(args.length == 3){
                System.out.println(calculate(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2])));
                return;
            }
            String algorithmType = args[3];
            if(!("NULL".equalsIgnoreCase(algorithmType)||"CRC32".equalsIgnoreCase(algorithmType)
                    ||"CRC32C".equalsIgnoreCase(algorithmType)||"DEFAULT".equalsIgnoreCase(algorithmType)
                    ||"MIXED".equalsIgnoreCase(algorithmType))){
                System.out.println("Algorithm type must be either (NULL/CRC32/CRC32C/DEFAULT/MIXED), but was provided : "+algorithmType);
                return;
            }
            DataChecksum.Type checksumType = null;
            if("NULL".equalsIgnoreCase(algorithmType)){
                checksumType = DataChecksum.Type.NULL;
            }else if("CRC32".equalsIgnoreCase(algorithmType)){
                checksumType = DataChecksum.Type.CRC32;
            }else if("CRC32C".equalsIgnoreCase(algorithmType)){
                checksumType = DataChecksum.Type.CRC32C;
            }else if("DEFAULT".equalsIgnoreCase(algorithmType)){
                checksumType = DataChecksum.Type.DEFAULT;
            }else if("MIXED".equalsIgnoreCase(algorithmType)){
                checksumType = DataChecksum.Type.MIXED;
            }
            if(args.length == 4){
                System.out.println(calculate(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), checksumType));
                return;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Param filePath : Local file path
     * @Param bytesPerChecksum : HDFS bytes per checksum. Default 512.
     * @Param crcsPerBlock : HDFS checksum per block. Default 134217728.
     * @Param checksumType : DataChecksum type. Ideally DataChecksum.Type.CRC32C
     */
    public static String calculate(String filePath, int bytesPerChecksum, int crcsPerBlock, DataChecksum.Type checksumType) throws NoSuchAlgorithmException, IOException {
        MD5MD5CRCMessageDigest messageDigest = new MD5MD5CRCMessageDigest(bytesPerChecksum, crcsPerBlock, checksumType);
        FileInputStream fileInputStream = new FileInputStream(filePath);
        DigestInputStream digestInputStream = new DigestInputStream(fileInputStream, messageDigest);
        OutputStream outputStream = new NullOutputStream();
        IOUtils.copy(digestInputStream,outputStream);
        return new String(Hex.encodeHex(messageDigest.digest()));
    }
    public static String calculate(String filePath, int bytesPerChecksum, int crcsPerBlock) throws IOException, NoSuchAlgorithmException {
        return calculate(filePath, bytesPerChecksum, crcsPerBlock, DataChecksum.Type.CRC32C);
    }
    public static String calculate(String filePath) throws NoSuchAlgorithmException, IOException {
        return calculate(filePath, 512, 134217728, DataChecksum.Type.CRC32C);
    }
}
