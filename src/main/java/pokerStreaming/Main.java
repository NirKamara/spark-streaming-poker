package pokerStreaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {

        org.apache.log4j.BasicConfigurator.configure();



//        AnnotationConfigApplicationContext sparkConf = new AnnotationConfigApplicationContext("sparkConf");
//        SessionService sessionService = sparkConf.getBean(SessionService.class);

        SessionService.initSpark();
    }
}


