import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.isession.SessionDataSet;
// If you get red text on imports, remember the Alt+Enter trick!

public class IoTDBDataGenerator {
    public static void main(String[] args) {
        System.out.println(">>> Starting Table Mode Experiment...");

        // FIX: Use TableSessionBuilder instead of 'new Session()'
        try (ITableSession session = new TableSessionBuilder()
                .nodeUrls(java.util.Collections.singletonList("127.0.0.1:6667"))
                .username("root")
                .password("root")
                .build()) {

//            session.open();
            System.out.println(">>> Connected explicitly in Table Mode!");

            // 1. Create Database
            session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS factory_db");
            session.executeNonQueryStatement("USE factory_db");

            // 2. Create Table
            String createTableSQL = "CREATE TABLE IF NOT EXISTS machines (" +
                    "device_id STRING TAG, " +
                    "location STRING TAG, " +
                    "temperature FLOAT FIELD, " +
                    "status BOOLEAN FIELD)";
            session.executeNonQueryStatement(createTableSQL);
            System.out.println(">>> Table Created!");

            // 3. Insert Data
            String insertSQL = "INSERT INTO machines(device_id, location, temperature, status) " +
                    "VALUES('drill_01', 'warehouse_A', 45.5, true)";
            session.executeNonQueryStatement(insertSQL);
            System.out.println(">>> Data Inserted via SQL!");

            // 4. Query Data
            SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM machines");

            System.out.println("--- Query Results ---");
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}