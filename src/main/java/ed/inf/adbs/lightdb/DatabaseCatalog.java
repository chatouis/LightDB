package ed.inf.adbs.lightdb;

public class DatabaseCatalog {
    private static DatabaseCatalog databaseCatalog = new DatabaseCatalog();

    private DatabaseCatalog() {}

    public static DatabaseCatalog getInstance() {
        return databaseCatalog;
    }

    public String getTablePath(String tableName) {
        return "samples/db/data/" + tableName + ".csv";
    }
}
