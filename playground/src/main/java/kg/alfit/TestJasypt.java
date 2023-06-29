package kg.alfit;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;
public class TestJasypt
{
    public static void main( String[] args ) {
        StandardPBEStringEncryptor standardPBEStringEncryptor = new StandardPBEStringEncryptor();
        standardPBEStringEncryptor.setPassword("QWEasdzxc123!");
        standardPBEStringEncryptor.setAlgorithm("PBEWithHMACSHA512AndAES_256");
        standardPBEStringEncryptor.setIvGenerator(new RandomIvGenerator());
        String result = standardPBEStringEncryptor.encrypt("test");
        System.out.println(result);
        System.out.println(standardPBEStringEncryptor.decrypt(result));

    }
}
