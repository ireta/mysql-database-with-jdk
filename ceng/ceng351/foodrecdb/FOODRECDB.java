package ceng.ceng351.foodrecdb;

import java.sql.*;
import java.util.Vector;

public class FOODRECDB implements IFOODRECDB{

    private static String user = "e2380632"; // TODO: Your userName
    private static String password = "dhbfsw9NTPXy5YsV"; //  TODO: Your password
    private static String host = "momcorp.ceng.metu.edu.tr"; // host name
    private static String database = "db2380632"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;

    public void initialize() {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    private int implementQueries(String queries[]){
        int result;
        int count = 0;
        for(String query : queries) {
            try {
                Statement statement = this.connection.createStatement();

                result = statement.executeUpdate(query);
                //System.out.println(result);
                count++;

                //close
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    public int createTables() {
        String[] queriesCreateTable = new String[5];
        queriesCreateTable[0] = "CREATE TABLE MenuItems(" +
                "itemID INT NOT NULL," +
                "itemName VARCHAR(40)," +
                "cuisine VARCHAR(20)," +
                "price INT," +
                "PRIMARY KEY (itemID));";

        queriesCreateTable[1] = "CREATE TABLE Ingredients(" +
                "ingredientID INT NOT NULL," +
                "ingredientName VARCHAR(40)," +
                "PRIMARY KEY (ingredientID));";

        queriesCreateTable[2] = "CREATE TABLE Includes(" +
                "itemID INT NOT NULL," +
                "ingredientID INT NOT NULL," +
                "PRIMARY KEY (itemID, ingredientID)," +
                "FOREIGN KEY (itemID) References MenuItems(itemID)," +
                "FOREIGN KEY (ingredientID) References Ingredients(ingredientID));";

        queriesCreateTable[3] = "CREATE TABLE Ratings(" +
                "ratingID INT NOT NULL," +
                "itemID INT NOT NULL," +
                "rating INT," +
                "ratingDate DATE," +
                "PRIMARY KEY (ratingID)," +
                "FOREIGN KEY (itemID) References MenuItems(itemID));";

        queriesCreateTable[4] = "CREATE TABLE DietaryCategories(" +
                "ingredientID INT NOT NULL," +
                "dietaryCategory VARCHAR(20) NOT NULL," +
                "PRIMARY KEY (ingredientID, dietaryCategory)," +
                "FOREIGN KEY (ingredientID) References Ingredients(ingredientID));";

        return implementQueries(queriesCreateTable);
    }


    public int dropTables() {

        String[] queriesDropTable = new String[5];

        queriesDropTable[0] = "DROP TABLE IF EXISTS DietaryCategories";
        queriesDropTable[1] = "DROP TABLE IF EXISTS Includes";
        queriesDropTable[2] = "DROP TABLE IF EXISTS Ratings";
        queriesDropTable[3] = "DROP TABLE IF EXISTS Ingredients";
        queriesDropTable[4] = "DROP TABLE IF EXISTS MenuItems";

        return implementQueries(queriesDropTable);
    }


    public int insertMenuItems(MenuItem[] items) {
        String[] queriesInsertMenuItems = new String[items.length];
        for(int i=0 ; i<items.length ; ++i){
            queriesInsertMenuItems[i] = "INSERT INTO MenuItems(" +
                    "itemID, itemName, cuisine, price)" +
                    "VALUES ('" + items[i].getItemID() +
                    "', '" + items[i].getItemName() +
                    "', '" + items[i].getCuisine() +
                    "', '" + items[i].getPrice() + "');";
        }
        return implementQueries(queriesInsertMenuItems);
    }


    public int insertIngredients(Ingredient[] ingredients) {
        String[] queriesInsertIngredients = new String[ingredients.length];
        for(int i=0 ; i<ingredients.length ; ++i){
            queriesInsertIngredients[i] = "INSERT INTO Ingredients(" +
                    "ingredientID, ingredientName)" +
                    "VALUES ('" + ingredients[i].getIngredientID() +
                    "', '" + ingredients[i].getIngredientName() + "');";
        }
        return implementQueries(queriesInsertIngredients);
    }


    public int insertIncludes(Includes[] includes) {
        String[] queriesInsertIncludes = new String[includes.length];
        for(int i=0 ; i<includes.length ; ++i){
            queriesInsertIncludes[i] = "INSERT INTO Includes(" +
                    "itemID, ingredientID)" +
                    "VALUES ('" + includes[i].getItemID() +
                    "', '" + includes[i].getIngredientID() + "');";
        }
        return implementQueries(queriesInsertIncludes);
    }


    public int insertDietaryCategories(DietaryCategory[] categories) {
        String[] queriesInsertDietaryCategories = new String[categories.length];
        for(int i=0 ; i<categories.length ; ++i){
            queriesInsertDietaryCategories[i] = "INSERT INTO DietaryCategories(" +
                    "ingredientID, dietaryCategory)" +
                    "VALUES ('" + categories[i].getIngredientID() +
                    "', '" + categories[i].getDietaryCategory() + "');";
        }
        return implementQueries(queriesInsertDietaryCategories);
    }


    public int insertRatings(Rating[] ratings) {
        String[] queriesInsertRatings = new String[ratings.length];
        for(int i=0 ; i<ratings.length ; ++i){
            queriesInsertRatings[i] = "INSERT INTO Ratings(" +
                    "ratingID, itemID, rating, ratingDate)" +
                    "VALUES ('" + ratings[i].getRatingID() +
                    "', '" + ratings[i].getItemID() +
                    "', '" + ratings[i].getRating() +
                    "', '" + ratings[i].getRatingDate() + "');";
        }
        return implementQueries(queriesInsertRatings);
    }


    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {
        Vector<MenuItem> vectorResult = new Vector<MenuItem>(0);
        ResultSet rs;
        String query = "SELECT MI.itemID, MI.itemName, MI.cuisine, MI.price " +
                "FROM MenuItems MI, Includes Inc, Ingredients Ing " +
                "WHERE MI.itemID=Inc.itemID AND Inc.ingredientID=Ing.ingredientID " +
                "AND Ing.ingredientName = '" + name + "' " +
                "ORDER BY MI.itemID ASC;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                MenuItem tmp = new MenuItem(rs.getInt("itemID"), rs.getString("itemName"), rs.getString("cuisine"), rs.getInt("price"));
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        MenuItem[] MenuItemsWithIngredient = new MenuItem[vectorResult.size()];
        for(int i=0 ; i<vectorResult.size() ; ++i){
            //System.out.println(tmp.toString());
            MenuItemsWithIngredient[i] = new MenuItem(0, "", "", 0);
            MenuItemsWithIngredient[i].setItemID(vectorResult.elementAt(i).getItemID());
            MenuItemsWithIngredient[i].setItemName(vectorResult.elementAt(i).getItemName());
            MenuItemsWithIngredient[i].setCuisine(vectorResult.elementAt(i).getCuisine());
            MenuItemsWithIngredient[i].setPrice(vectorResult.elementAt(i).getPrice());
            //System.out.println(MenuItemsWithIngredient[i].toString());
        }

        return MenuItemsWithIngredient;
    }


    public MenuItem[] getMenuItemsWithoutAnyIngredient() {
        Vector<MenuItem> vectorResult = new Vector<MenuItem>(0);
        ResultSet rs;
        String query = "SELECT MI.itemID, MI.itemName, MI.cuisine, MI.price " +
                "FROM MenuItems MI " +
                "WHERE NOT EXISTS (SELECT Inc.itemID " +
                                    "FROM Includes Inc " +
                                    "WHERE Inc.itemID=MI.itemID) " +
                "ORDER BY MI.itemID ASC;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                MenuItem tmp = new MenuItem(rs.getInt("itemID"), rs.getString("itemName"), rs.getString("cuisine"), rs.getInt("price"));
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        MenuItem[] MenuItemsWithoutIngredient = new MenuItem[vectorResult.size()];
        for(int i=0 ; i<vectorResult.size() ; ++i){
            //System.out.println(tmp.toString());
            MenuItemsWithoutIngredient[i] = new MenuItem(0, "", "", 0);
            MenuItemsWithoutIngredient[i].setItemID(vectorResult.elementAt(i).getItemID());
            MenuItemsWithoutIngredient[i].setItemName(vectorResult.elementAt(i).getItemName());
            MenuItemsWithoutIngredient[i].setCuisine(vectorResult.elementAt(i).getCuisine());
            MenuItemsWithoutIngredient[i].setPrice(vectorResult.elementAt(i).getPrice());
            //System.out.println(MenuItemsWithIngredient[i].toString());
        }

        return MenuItemsWithoutIngredient;
    }


    public Ingredient[] getNotIncludedIngredients() {
        Vector<Ingredient> vectorResult = new Vector<Ingredient>(0);
        ResultSet rs;
        String query = "SELECT Ing.ingredientID, Ing.ingredientName " +
                "FROM Ingredients Ing " +
                "WHERE Ing.ingredientID NOT IN (SELECT Inc.ingredientID " +
                                                "FROM Includes Inc " +
                                                "WHERE Inc.ingredientID=Ing.ingredientID) " +
                "ORDER BY Ing.ingredientID ASC;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                Ingredient tmp = new Ingredient(rs.getInt("ingredientID"), rs.getString("ingredientName"));
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        Ingredient[] notIncludedIngredients = new Ingredient[vectorResult.size()];

        return vectorResult.toArray(notIncludedIngredients);
    }


    public MenuItem getMenuItemWithMostIngredients() {
        MenuItem MenuItemWithMostIngredient = new MenuItem(0, "", "", 0);
        ResultSet rs;
        String query = "SELECT MI.itemID, MI.itemName, MI.cuisine, MI.price " +
                "FROM MenuItems MI, Includes I " +
                "WHERE MI.itemID = I.itemID " +
                "GROUP BY MI.itemID " +
                "HAVING COUNT(*)=(SELECT MAX(TEMP.C) " +
                                "FROM (SELECT COUNT(*) AS C " +
                                        "FROM Includes I1 " +
                                        "GROUP BY I1.itemID) AS TEMP);";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);
            rs.next();
            MenuItemWithMostIngredient.setItemID(rs.getInt("itemID"));
            MenuItemWithMostIngredient.setItemName(rs.getString("itemName"));
            MenuItemWithMostIngredient.setCuisine(rs.getString("cuisine"));
            MenuItemWithMostIngredient.setPrice(rs.getInt("price"));
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());

        return MenuItemWithMostIngredient;
    }


    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings() {
        Vector<QueryResult.MenuItemAverageRatingResult> vectorResult = new Vector(0);
        ResultSet rs;
        String query = "SELECT DISTINCT MI.itemID, MI.itemName, AVG(R.rating) AS avg " +
                        "FROM MenuItems MI " +
                        "LEFT JOIN Ratings R ON R.itemID = MI.itemID " +
                        "GROUP BY MI.itemID " +
                        "ORDER BY avg DESC;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                String itemID = rs.getString("itemID");
                String itemName = rs.getString("itemName");
                String avg = rs.getString("avg");
                QueryResult.MenuItemAverageRatingResult tmp = new QueryResult.MenuItemAverageRatingResult(itemID, itemName, avg);
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        QueryResult.MenuItemAverageRatingResult[] arr = new QueryResult.MenuItemAverageRatingResult[vectorResult.size()];

        return vectorResult.toArray(arr);
    }


    public MenuItem[] getMenuItemsForDietaryCategory(String category) {
        Vector<MenuItem> vectorResult = new Vector<MenuItem>(0);
        ResultSet rs;
        String query = "SELECT MI.itemID, MI.itemName, MI.cuisine, MI.price " +
                        "FROM MenuItems MI " +
                        "WHERE EXISTS (SELECT * " +
                                        "FROM Includes I1 " +
                                        "WHERE I1.itemID = MI.itemID) " +
                        "GROUP BY MI.itemID " +
                        "HAVING NOT EXISTS (SELECT * " +
                                            "FROM Includes I " +
                                            "WHERE I.itemID = MI.itemID " +
                                            "GROUP BY I.ingredientID " +
                                            "HAVING NOT EXISTS (SELECT * " +
                                                        "FROM DietaryCategories D " +
                                                        "WHERE D.ingredientID = I.ingredientID AND " +
                                                        "D.DietaryCategory = '" + category + "' ));";


        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                MenuItem tmp = new MenuItem(rs.getInt("itemID"), rs.getString("itemName"), rs.getString("cuisine"), rs.getInt("price"));
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        MenuItem[] arr = new MenuItem[vectorResult.size()];

        return vectorResult.toArray(arr);
    }


    public Ingredient getMostUsedIngredient() {
        Ingredient result = new Ingredient(0, "");
        ResultSet rs;
        String query = "SELECT Ing.ingredientID, Ing.ingredientName " +
                        "FROM Ingredients Ing,  Includes I " +
                        "WHERE Ing.ingredientID = I.ingredientID " +
                        "GROUP BY Ing.ingredientID " +
                        "HAVING COUNT(*)=(SELECT MAX(TEMP.C) " +
                                        "FROM (SELECT COUNT(*) AS C " +
                                                "FROM Includes I1 " +
                                                "GROUP BY I1.ingredientID) AS TEMP);";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);
            rs.next();
            result.setIngredientID(rs.getInt("ingredientID"));
            result.setIngredientName(rs.getString("ingredientName"));

            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        return result;
    }


    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating() {
        Vector<QueryResult.CuisineWithAverageResult> vectorResult = new Vector(0);
        ResultSet rs;
        String query = "SELECT MI.cuisine, AVG(R.rating) AS avg " +
                        "FROM MenuItems MI " +
                        "LEFT JOIN Ratings R ON R.itemID = MI.itemID " +
                        "GROUP BY MI.cuisine " +
                        "ORDER BY avg DESC;";


        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                String cuisine = rs.getString("cuisine");
                String avg = rs.getString("avg");
                QueryResult.CuisineWithAverageResult tmp = new QueryResult.CuisineWithAverageResult(cuisine, avg);
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        QueryResult.CuisineWithAverageResult[] arr = new QueryResult.CuisineWithAverageResult[vectorResult.size()];

        return vectorResult.toArray(arr);
    }


    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount() {
        Vector<QueryResult.CuisineWithAverageResult> vectorResult = new Vector(0);
        ResultSet rs;
        String query = "SELECT DISTINCT TEMP.cuisine, AVG(TEMP.C) AS avg " +
                        "FROM (SELECT MI.itemID, MI.cuisine, COUNT(I.itemID) AS C " +
                                "FROM MenuItems MI " +
                                "NATURAL LEFT JOIN Includes I " +
                                "GROUP BY MI.itemID) AS TEMP " +
                        "GROUP BY TEMP.cuisine " +
                        "ORDER BY avg DESC;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                String cuisine = rs.getString("cuisine");
                String avg = rs.getString("avg");
                QueryResult.CuisineWithAverageResult tmp = new QueryResult.CuisineWithAverageResult(cuisine, avg);
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        QueryResult.CuisineWithAverageResult[] arr = new QueryResult.CuisineWithAverageResult[vectorResult.size()];

        return vectorResult.toArray(arr);
    }


    public int increasePrice(String ingredientName, String increaseAmount) {
        int rowcount = 0;
        String query = "UPDATE MenuItems MI " +
                        "SET MI.price = MI.price + " + increaseAmount + " " +
                        "WHERE MI.itemID IN (SELECT I.itemID " +
                                            "FROM Includes I, Ingredients Ing " +
                                            "WHERE I.ingredientID = Ing.ingredientID AND " +
                                            "Ing.ingredientName = '" + ingredientName + "');";

        try {
            Statement st = this.connection.createStatement();
            rowcount = st.executeUpdate(query);
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }

        return rowcount;
    }


    public Rating[] deleteOlderRatings(String date) {
        Vector<Rating> vectorResult = new Vector(0);
        ResultSet rs;
        String query = "SELECT R.ratingID, R.itemID, R.rating, R.ratingDate " +
                        "FROM Ratings R " +
                        "WHERE R.ratingDate < '" + date + "' " +
                        "ORDER BY R.ratingID;";

        try {
            Statement st = this.connection.createStatement();
            rs = st.executeQuery(query);

            while(rs.next()) {
                Rating tmp = new Rating(rs.getInt("ratingID"), rs.getInt("itemID"), rs.getInt("rating"), rs.getString("ratingDate"));
                //System.out.println(tmp.toString());
                //System.out.println("hello");
                vectorResult.addElement(tmp);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        query = "DELETE " +
                "FROM Ratings R " +
                "WHERE R.ratingDate < '" + date + "';";
        try {
            Statement st = this.connection.createStatement();
            st.executeUpdate(query);
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        //System.out.println(vectorResult.size());
        Rating[] arr = new Rating[vectorResult.size()];

        return vectorResult.toArray(arr);
    }
}
