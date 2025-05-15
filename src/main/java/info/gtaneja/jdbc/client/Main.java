package info.gtaneja.jdbc.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.ConfigFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;
    }

    public static final String CONFIG_PATH ="test-client";
    public static void main(String[] args) throws SQLException, InterruptedException {
        Args argv = new Args();
        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var configMap = new HashMap<String, String>();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                var e = c.indexOf("=");
                var key = c.substring(0, e);
                var value = c.substring(e, c.length() - 1);
                configMap.put(key, value);
            });
        }
        var commandlineConfig = ConfigFactory.parseMap(configMap);
        var config = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        String url = config.getString("url");
        int numConnection = config.getInt("numConnections");
        int repetitions = config.getInt("repetitions");
        String sql = config.getString("sql");
        AtomicLong count = new AtomicLong();
        Thread[] threads = new Thread[numConnection];
        for(int i = 0 ; i < numConnection; i++){
            var t = new Thread(() -> {
                try (var connection = DriverManager.getConnection(url)){
                    var c = count.incrementAndGet();
                    while (c <= repetitions) {
                        try (var st = connection.createStatement()) {
                            if(c%100 == 0) {
                                System.out.println("Running " + c);
                            }
                            st.execute(sql);
                            try (var rs = st.getResultSet()) {
                                while (rs.next()) {
                                    // System.out.println(rs.getInt(1));
                                }
                            }
                            c = count.incrementAndGet();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("finished thread");
            });
            t.start();
            threads[i] = t;
        }

        while (threads[0].isAlive()) {
            Thread.sleep(100);
        }
    }
}