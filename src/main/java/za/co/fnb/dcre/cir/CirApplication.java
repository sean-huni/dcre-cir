package za.co.fnb.dcre.cir;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;

@SpringBootApplication
public class CirApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(CirApplication.class, args);
    }
}
