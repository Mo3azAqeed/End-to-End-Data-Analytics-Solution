CREATE TABLE Dim_Store (
    StoreID SERIAL PRIMARY KEY,
    StoreName VARCHAR(255),
    Location VARCHAR(255)
);

CREATE TABLE Dim_Product (
    ProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(255),
    BrandID INT REFERENCES Dim_Brand(BrandID),
    SupplierID INT REFERENCES Dim_Supplier(SupplierID),
    Size VARCHAR(50),
    Price DECIMAL,
    Cost DECIMAL
);

CREATE TABLE Dim_Supplier (
    SupplierID SERIAL PRIMARY KEY,
    SupplierName VARCHAR(255),
    Country VARCHAR(50)
);

CREATE TABLE Dim_Brand (
    BrandID SERIAL PRIMARY KEY,
    BrandName VARCHAR(255),
    SupplierID INT REFERENCES Dim_Supplier(SupplierID)
);

CREATE TABLE Dim_Time (
    TimeID SERIAL PRIMARY KEY,
    Date DATE,
    DayOfWeek VARCHAR(25),
    Month INT,
    Quarter INT,
    Year INT,
    IsHoliday BOOLEAN
);


CREATE TABLE Fact_Order (
    OrderID SERIAL PRIMARY KEY,
    OrderDateID INT REFERENCES Dim_Time(TimeID),
    InvoiceDateID INT REFERENCES Dim_Time(TimeID),
    PayDateID INT REFERENCES Dim_Time(TimeID),
    StoreID INT REFERENCES Dim_Store(StoreID),
    SupplierID INT REFERENCES Dim_Supplier(SupplierID),
    QuantityOrdered INT,
    TotalPurchaseCost DECIMAL,
    FreightCost DECIMAL
);

CREATE TABLE Fact_OrderItems (
    OrderItemID SERIAL PRIMARY KEY,
    OrderID INT REFERENCES Fact_Order(OrderID),
    ProductID INT REFERENCES Dim_Product(ProductID),
    QuantityOrdered INT,
    PurchasePrice DECIMAL,
    TotalPurchaseCost DECIMAL
);

CREATE TABLE Fact_Inventory (
    InventoryID SERIAL PRIMARY KEY,
    ReceivedDateID INT REFERENCES Dim_Time(TimeID),
    StoreID INT REFERENCES Dim_Store(StoreID),
    ProductID INT REFERENCES Dim_Product(ProductID),
    QuantityReceived INT,
    InventoryCost DECIMAL
);

CREATE TABLE Fact_Sales (
    SaleID SERIAL PRIMARY KEY,
    SaleDateID INT REFERENCES Dim_Time(TimeID),
    StoreID INT REFERENCES Dim_Store(StoreID),
    ProductID INT REFERENCES Dim_Product(ProductID),
    InventoryID INT REFERENCES Fact_Inventory(InventoryID),
    QuantitySold INT,
    SalePrice DECIMAL
);
