import org.jdbi.v3.core.Jdbi;

public class Main {
    public static void main(String[] args) {
        Jdbi jdbi = Jdbi.create("jdbc:phoenix:hbase:2181:/hbase");

        dropTableIfExists(jdbi);

        createTable(jdbi);

        saveDataToTable(jdbi);

        String helloWorldMessage = getDataFromTable(jdbi);

        System.out.println(helloWorldMessage);
    }

    private static void dropTableIfExists(Jdbi jdbi) {
        jdbi.withHandle(handle -> handle.createUpdate("drop table if exists example").execute());
    }

    private static void createTable(Jdbi jdbi) {
        jdbi.withHandle(handle -> handle.createUpdate("create table example(id varchar primary key)").execute());
    }

    private static void saveDataToTable(Jdbi jdbi) {
        jdbi.useTransaction(transaction -> {
            transaction.createUpdate("upsert into example(id) values ('hello.world')").execute();
            transaction.commit();
        });
    }

    private static String getDataFromTable(Jdbi jdbi) {
        return jdbi.withHandle(handle -> handle.select("select id from example")
                .mapTo(String.class)
                .first());
    }
}